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

package org.apache.spark.sql.execution.benchmark

import java.util.{Arrays, Comparator}

import org.apache.spark.sql.execution.SortExec
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.Benchmark
import org.apache.spark.util.collection.Sorter
import org.apache.spark.util.collection.unsafe.sort._
import org.apache.spark.util.random.XORShiftRandom

/**
 * Benchmark to measure performance for aggregate primitives.
 * To run this:
 *  build/sbt "sql/test-only *benchmark.SortBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class SortBenchmark extends BenchmarkBase {

  private def referenceKeyPrefixSort(buf: LongArray, lo: Int, hi: Int, refCmp: PrefixComparator) {
    new Sorter(UnsafeSortDataFormat.INSTANCE).sort(
      buf, lo, hi, new Comparator[RecordPointerAndKeyPrefix] {
        override def compare(
          r1: RecordPointerAndKeyPrefix,
          r2: RecordPointerAndKeyPrefix): Int = {
          refCmp.compare(r1.keyPrefix, r2.keyPrefix)
        }
      })
  }

  private def generateKeyPrefixTestData(size: Int, rand: => Long): (LongArray, LongArray) = {
    val ref = Array.tabulate[Long](size * 2) { i => rand }
    val extended = ref ++ Array.fill[Long](size * 2)(0)
    (new LongArray(MemoryBlock.fromLongArray(ref)),
      new LongArray(MemoryBlock.fromLongArray(extended)))
  }

  test("sort") {
    val size = 25000000
    val rand = new XORShiftRandom(123)
    val benchmark = new Benchmark("in-memory sort " + size, size)
    benchmark.addTimerCase("reference TimSort key prefix array") { timer =>
      val array = Array.tabulate[Long](size * 2) { i => rand.nextLong }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      referenceKeyPrefixSort(buf, 0, size, PrefixComparators.BINARY)
      timer.stopTiming()
    }
    benchmark.addTimerCase("codegen qsort key prefix array") { timer =>
      val array = Array.tabulate[Long](size * 2) { i => rand.nextLong }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      // Do a sort without record comparisons
      val sorter = SortExec.genSorter(0, Nil, PrefixComparators.BINARY)
      timer.startTiming()
      sorter.sort(buf, 0, size)
      timer.stopTiming()
    }
    benchmark.addTimerCase("reference Arrays.sort") { timer =>
      val ref = Array.tabulate[Long](size) { i => rand.nextLong }
      timer.startTiming()
      Arrays.sort(ref)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort one byte") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong & 0xff
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort two bytes") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong & 0xffff
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort eight bytes") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort key prefix array") { timer =>
      val (_, buf2) = generateKeyPrefixTestData(size, rand.nextLong)
      timer.startTiming()
      RadixSort.sortKeyPrefixArray(buf2, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.run()

    /*
      Running benchmark: in-memory sort 25000000
      OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-35-generic
      Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
      in-memory sort 25000000:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      reference TimSort key prefix array     10961 / 11150          2.3         438.5       1.0X
      codegen qsort key prefix array           6160 / 6195          4.1         246.4       1.8X
      reference Arrays.sort                    1983 / 2032         12.6          79.3       5.5X
      radix sort one byte                       127 /  137        197.2           5.1      86.4X
      radix sort two bytes                      236 /  237        105.8           9.4      46.4X
      radix sort eight bytes                    896 /  902         27.9          35.8      12.2X
      radix sort key prefix array              1474 / 1595         17.0          58.9       7.4X
    */
  }
}
