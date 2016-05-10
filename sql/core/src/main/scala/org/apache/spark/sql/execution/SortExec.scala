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

    val genSort = genSorter(ordering, prefixComparator)

    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

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

  def genSorter(ordering: Ordering[InternalRow], pcmp: PrefixComparator): UnsafeSorter = {
    val ctx = new CodegenContext()
    ctx.addReferenceObj("ordering", ordering, "scala.math.Ordering")
    ctx.addReferenceObj(
      "memoryManager", SparkEnv.get.memoryManager, "org.apache.spark.memory.MemoryManager");

    // TODO(ekl) inline the prefix comparisons
    ctx.addReferenceObj(
      "prefixCmp", pcmp, "org.apache.spark.util.collection.unsafe.sort.PrefixComparator");

    val code = s"""
      public SpecificUnsafeSorter generate(Object[] references) {
        return new SpecificUnsafeSorter(references);
      }

      class SpecificUnsafeSorter implements
          org.apache.spark.util.collection.unsafe.sort.UnsafeSorter {
        private Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        private final org.apache.spark.sql.catalyst.expressions.UnsafeRow row1;
        private final org.apache.spark.sql.catalyst.expressions.UnsafeRow row2;

        public SpecificUnsafeSorter(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        private static void sort1(org.apache.spark.unsafe.array.LongArray x, int off, int len) {
            // Insertion sort on smallest arrays
            if (len < 7) {
                for (int i=off; i<len+off; i++)
                    for (int j=i; j>off && gt(x, j-1, j); j--)
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
            /* long v = x[m]; */

            // Establish Invariant: v* (<v)* (>v)* v*
            int a = off, b = a, c = off + len - 1, d = c;
            while(true) {
                while (b <= c && le(x, b, m)) {
                    if (eq(x, b, m))
                        swap(x, a++, b);
                    b++;
                }
                while (c >= b && ge(x, c, m)) {
                    if (eq(x, c, m))
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

        private static void swap(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
            long k1 = x.get(a*2);
            long v1 = x.get(a*2+1);
            x.set(a*2, x.get(b*2));
            x.set(a*2+1, x.get(b*2+1));
            x.set(b*2, k1);
            x.set(b*2+1, v1);
        }

        private static int med3(org.apache.spark.unsafe.array.LongArray x, int a, int b, int c) {
            return (lt(x, a, b) ?
                    (lt(x, b, c) ? b : lt(x, a, c) ? c : a) :
                    (gt(x, b, c) ? b : gt(x, a, c) ? c : a));
        }

        private static void vecswap(
            org.apache.spark.unsafe.array.LongArray x, int a, int b, int n) {

            for (int i=0; i<n; i++, a++, b++)
                swap(x, a, b);
        }

        private static boolean lt(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
          return compare(x, a, b) < 0;
        }

        private static boolean le(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
          return compare(x, a, b) <= 0;
        }

        private static boolean gt(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
          return compare(x, a, b) > 0;
        }

        private static boolean ge(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
          return compare(x, a, b) >= 0;
        }

        private static boolean eq(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
          return compare(x, a, b) == 0;
        }

        private static int compare(org.apache.spark.unsafe.array.LongArray x, int a, int b) {
          long prefix1 = x.get(a*2+1);
          long prefix2 = x.get(b*2+1);
          int prefixComparisonResult = prefixCmp.compare(prefix1, prefix2);
          if (prefixComparisonResult == 0) {
            final int r1RecordPointer = x.get(a*2);
            final int r2RecordPointer = x.get(b*2);
            final Object baseObject1 = memoryManager.getPage(r1recordPointer);
            final long baseOffset1 = memoryManager.getOffsetInPage(r1recordPointer) + 4;
            final Object baseObject2 = memoryManager.getPage(r2recordPointer);
            final long baseOffset2 = memoryManager.getOffsetInPage(r2recordPointer) + 4;
            return compareRecords(baseObject1, baseOffset1, baseObject2, baseOffset2);
          } else {
            return prefixComparisonResult;
          }
        }

        private static int compareRecords(
            Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
          row1.pointTo(baseObj1, baseOff1, -1);
          row2.pointTo(baseObj2, baseOff2, -1);
          return ordering.compare(row1, row2);
        }

        @Override
        public void sort(org.apache.spark.unsafe.array.LongArray arr, int lo, int hi) {
          assert lo == 0 : "Base offset must be zero";
          baseObj = arr.getBaseObject();
          baseOffset = arr.getBaseOffset();
          sort1(arr, 0, hi);
        }
      }"""

    println(s"Generated Sort:\n${CodeFormatter.format(code)}")

    CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[UnsafeSorter]
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
      val beforeSort = System.nanoTime()

      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])

      sortTime += (System.nanoTime() - beforeSort) / 1000000
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
    val startTime = ctx.freshName("startTime")
    val sortTime = metricTerm(ctx, "sortTime")
    s"""
       | if ($needToSort) {
       |   long $spillSizeBefore = $metrics.memoryBytesSpilled();
       |   long $startTime = System.nanoTime();
       |   $addToSorter();
       |   $sortedIterator = $sorterVariable.sort();
       |   $sortTime.add((System.nanoTime() - $startTime) / 1000000);
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
