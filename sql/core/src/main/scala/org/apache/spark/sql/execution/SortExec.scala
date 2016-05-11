/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.RadixSortSupport
import org.apache.spark.util.collection.unsafe.sort.RadixSort
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorter

/**
 * Performs (external) sorting.
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every `frequency` records.
 */
case class SortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
  extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  private val enableRadixSort = sqlContext.conf.enableRadixSort

  override private[sql] lazy val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  def createSorter(): UnsafeExternalRowSorter = {
    val ordering = newOrdering(sortOrder, output)

    // The comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    val genSort = SortExec.genSorter(
      schema.length, sortOrder.map(s => BindReferences.bindReference(s, output)), prefixComparator)

    // The generator for prefix
    val prefixProjection = UnsafeProjection.create(Seq(SortPrefix(boundSortExpression)))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      override def computePrefix(row: InternalRow): Long = {
        prefixProjection.apply(row).getLong(0)
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    val sorter = new UnsafeExternalRowSorter(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort, genSort)

    if (testSpillFrequency > 0) {
      sorter.setTestSpillFrequency(testSpillFrequency)
    }
    sorter
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")
    val sortTime = longMetric("sortTime")

    child.execute().mapPartitionsInternal { iter =>
      val sorter = createSorter()

      val metrics = TaskContext.get().taskMetrics()
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val spillSizeBefore = metrics.memoryBytesSpilled
      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
      sortTime += sorter.getSortTimeNanos / 1000000
      peakMemory += sorter.getPeakMemoryUsage
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator
    }
  }

  override def usedInputs: AttributeSet = AttributeSet(Seq.empty)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // Name of sorter variable used in codegen.
  private var sorterVariable: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort = ctx.freshName("needToSort")
    ctx.addMutableState("boolean", needToSort, s"$needToSort = true;")

    // Initialize the class member variables. This includes the instance of the Sorter and
    // the iterator to return sorted rows.
    val thisPlan = ctx.addReferenceObj("plan", this)
    sorterVariable = ctx.freshName("sorter")
    ctx.addMutableState(classOf[UnsafeExternalRowSorter].getName, sorterVariable,
      s"$sorterVariable = $thisPlan.createSorter();")
    val metrics = ctx.freshName("metrics")
    ctx.addMutableState(classOf[TaskMetrics].getName, metrics,
      s"$metrics = org.apache.spark.TaskContext.get().taskMetrics();")
    val sortedIterator = ctx.freshName("sortedIter")
    ctx.addMutableState("scala.collection.Iterator<UnsafeRow>", sortedIterator, "")

    val addToSorter = ctx.freshName("addToSorter")
    ctx.addNewFunction(addToSorter,
      s"""
        | private void $addToSorter() throws java.io.IOException {
        |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
        | }
      """.stripMargin.trim)

    // The child could change `copyResult` to true, but we had already consumed all the rows,
    // so `copyResult` should be reset to `false`.
    ctx.copyResult = false

    val outputRow = ctx.freshName("outputRow")
    val peakMemory = metricTerm(ctx, "peakMemory")
    val spillSize = metricTerm(ctx, "spillSize")
    val spillSizeBefore = ctx.freshName("spillSizeBefore")
    val sortTime = metricTerm(ctx, "sortTime")
    s"""
       | if ($needToSort) {
       |   long $spillSizeBefore = $metrics.memoryBytesSpilled();
       |   $addToSorter();
       |   $sortedIterator = $sorterVariable.sort();
       |   $sortTime.add($sorterVariable.getSortTimeNanos() / 1000000);
       |   $peakMemory.add($sorterVariable.getPeakMemoryUsage());
       |   $spillSize.add($metrics.memoryBytesSpilled() - $spillSizeBefore);
       |   $metrics.incPeakExecutionMemory($sorterVariable.getPeakMemoryUsage());
       |   $needToSort = false;
       | }
       |
       | while ($sortedIterator.hasNext()) {
       |   UnsafeRow $outputRow = (UnsafeRow)$sortedIterator.next();
       |   ${consume(ctx, null, outputRow)}
       |   if (shouldStop()) return;
       | }
     """.stripMargin.trim
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       |${row.code}
       |$sorterVariable.insertRow((UnsafeRow)${row.value});
     """.stripMargin
  }
}

object SortExec {
  def genSorter(numFields: Int, ordering: Seq[SortOrder], pcmp: PrefixComparator): UnsafeSorter = {
    val ctx = new CodegenContext()
    ctx.addReferenceObj(
      "memoryManager",
      Option(TaskContext.get()).map(_.taskMemoryManager()).orNull,
      "org.apache.spark.memory.TaskMemoryManager");

    val prefixComparatorCode = pcmp match {
      case r: RadixSortSupport =>
        if (r.sortSigned()) {
          if (r.sortDescending()) {
            "return (b < a) ? -1 : (b > a) ? 1 : 0;"
          } else {
            "return (a < b) ? -1 : (a > b) ? 1 : 0;"
          }
        } else {
          if (r.sortDescending()) {
            "return com.google.common.primitives.UnsignedLongs.compare(b, a);"
          } else {
            "return com.google.common.primitives.UnsignedLongs.compare(a, b);"
          }
        }
      case _ =>
        ctx.addReferenceObj(
          "prefixCmp", pcmp, "org.apache.spark.util.collection.unsafe.sort.PrefixComparator");
        "return prefixCmp.compare(a, b);"
    }

    ctx.addReferenceObj("numFields", numFields, "Integer")
    ctx.addMutableState(
      "org.apache.spark.sql.catalyst.expressions.UnsafeRow", "row1",
      "row1 = new UnsafeRow(numFields);")
    ctx.addMutableState(
      "org.apache.spark.sql.catalyst.expressions.UnsafeRow", "row2",
      "row2 = new UnsafeRow(numFields);")

    val prefixOnlySort = ordering.length == 0 ||
      (ordering.length == 1 && SortPrefixUtils.canSortFullyWithPrefix(ordering.head))
    val comparisons = if (prefixOnlySort) {
      "assert false : \"Record comparisons not needed\";"
    } else {
      GenerateOrdering.genComparisons(ctx, ordering)
    }

    val code = s"""
      public SpecificUnsafeSorter generate(Object[] references) {
        return new SpecificUnsafeSorter(references);
      }

      class SpecificUnsafeSorter implements
          org.apache.spark.util.collection.unsafe.sort.UnsafeSorter {
        private Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificUnsafeSorter(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        private final void sort1(org.apache.spark.unsafe.array.LongArray x, int off, int len) {
            // Insertion sort on smallest arrays
            if (len < 7) {
                for (int i=off; i<len+off; i++)
                    for (int j=i; j>off && compare(x, j-1, j) > 0; j--)
                        swap(x, j, j-1);
                return;
            }

            // Choose a partition element, v
            int m = off + (len >> 1);       // Small arrays, middle element
            if (len > 7) {
                int l = off;
                int n = off + len - 1;
                if (len > 40) {        // Big arrays, pseudomedian of 9
                    int s = len/8;
                    l = med3(x, l,     l+s, l+2*s);
                    m = med3(x, m-s,   m,   m+s);
                    n = med3(x, n-2*s, n-s, n);
                }
                m = med3(x, l, m, n); // Mid-size, med of 3
            }

            doPartition(x, m, off, len);
        }

        private final void doPartition(
            org.apache.spark.unsafe.array.LongArray x, int m, int off, int len) {

            long vPtr = x.get(m*2);
            long vPfx = x.get(m*2+1);

            // Establish Invariant: v* (<v)* (>v)* v*
            int a = off, b = a, c = off + len - 1, d = c;
            while(true) {
                while (b <= c && compare(x, b, vPtr, vPfx) <= 0) {
                    if (compare(x, b, vPtr, vPfx) == 0)
                        swap(x, a++, b);
                    b++;
                }
                while (c >= b && compare(x, c, vPtr, vPfx) >= 0) {
                    if (compare(x, c, vPtr, vPfx) == 0)
                        swap(x, c, d--);
                    c--;
                }
                if (b > c)
                    break;
                swap(x, b++, c--);
            }

            // Swap partition elements back to middle
            int s, n = off + len;
            s = Math.min(a-off, b-a  );  vecswap(x, off, b-s, s);
            s = Math.min(d-c,   n-d-1);  vecswap(x, b,   n-s, s);

            // Recursively sort non-partition-elements
            if ((s = b-a) > 1)
                sort1(x, off, s);
            if ((s = d-c) > 1)
                sort1(x, n-s, s);
        }

        private final void swap(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
            long k1 = x.get(a*2);
            long v1 = x.get(a*2+1);
            x.set(a*2, x.get(b*2));
            x.set(a*2+1, x.get(b*2+1));
            x.set(b*2, k1);
            x.set(b*2+1, v1);
        }

        private final int med3(org.apache.spark.unsafe.array.LongArray x, int a, int b, int c) {
            return (compare(x, a, b) < 0?
                    (compare(x, b, c) < 0 ? b : compare(x, a, c) < 0 ? c : a) :
                    (compare(x, b, c) > 0 ? b : compare(x, a, c) > 0 ? c : a));
        }

        private final void vecswap(
            org.apache.spark.unsafe.array.LongArray x, int a, int b, int n) {

            for (int i=0; i<n; i++, a++, b++)
                swap(x, a, b);
        }

        private final int compare(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
          long prefix1 = x.get(a*2+1);
          long prefix2 = x.get(b*2+1);
          int prefixComparisonResult = comparePrefix(prefix1, prefix2);
          if (prefixComparisonResult == 0 && !$prefixOnlySort) {
            final long r1RecordPointer = x.get(a*2);
            final long r2RecordPointer = x.get(b*2);
            final Object baseObject1 = memoryManager.getPage(r1RecordPointer);
            final long baseOffset1 = memoryManager.getOffsetInPage(r1RecordPointer) + 4;
            final Object baseObject2 = memoryManager.getPage(r2RecordPointer);
            final long baseOffset2 = memoryManager.getOffsetInPage(r2RecordPointer) + 4;
            return compareRecords(baseObject1, baseOffset1, baseObject2, baseOffset2);
          } else {
            return prefixComparisonResult;
          }
        }

        private final int compare(
            org.apache.spark.unsafe.array.LongArray x, int a, long vPtr, long vPfx) {
          long prefix1 = x.get(a*2+1);
          long prefix2 = vPfx;
          int prefixComparisonResult = comparePrefix(prefix1, prefix2);
          if (prefixComparisonResult == 0 && !$prefixOnlySort) {
            final long r1RecordPointer = x.get(a*2);
            final long r2RecordPointer = vPtr;
            final Object baseObject1 = memoryManager.getPage(r1RecordPointer);
            final long baseOffset1 = memoryManager.getOffsetInPage(r1RecordPointer) + 4;
            final Object baseObject2 = memoryManager.getPage(r2RecordPointer);
            final long baseOffset2 = memoryManager.getOffsetInPage(r2RecordPointer) + 4;
            return compareRecords(baseObject1, baseOffset1, baseObject2, baseOffset2);
          } else {
            return prefixComparisonResult;
          }
        }

        java.util.concurrent.atomic.AtomicLong numCompares =
          new java.util.concurrent.atomic.AtomicLong();
        private final int comparePrefix(long a, long b) {
          numCompares.getAndIncrement();
          $prefixComparatorCode
        }

        private final int compareRecords(
            Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
          row1.pointTo(baseObj1, baseOff1, -1);
          row2.pointTo(baseObj2, baseOff2, -1);
          org.apache.spark.sql.catalyst.expressions.UnsafeRow a = row1;
          org.apache.spark.sql.catalyst.expressions.UnsafeRow b = row2;
          org.apache.spark.sql.catalyst.expressions.UnsafeRow ${ctx.INPUT_ROW} = null;
          $comparisons
          return 0;
        }

        @Override
        public void sort(org.apache.spark.unsafe.array.LongArray arr, int lo, int hi) {
          assert lo == 0 : "Base offset must be zero";
          sort1(arr, 0, hi);
          System.out.println("Num compares: " + numCompares.get());
        }
      }"""

//    println(s"Generated Sort:\n${CodeFormatter.format(code)}")

    CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[UnsafeSorter]
  }
}
