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

package org.apache.spark.sql.execution.datasources

import java.io.FileNotFoundException

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus, Path}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


/**
 * A base class for [[BasicFileCatalog]]s that need a [[SparkSession]] and the ability to find leaf
 * files in a list of HDFS paths.
 *
 * @param sparkSession a [[SparkSession]]
 * @param ignoreFileNotFound (see [[ListingFileCatalog]])
 */
abstract class SessionFileCatalog(sparkSession: SparkSession)
    extends BasicFileCatalog with Logging {
  protected val hadoopConf: Configuration

  /**
   * List leaf files of given paths. This method will submit a Spark job to do parallel
   * listing whenever there is a path having more files than the parallel partition discovery
   * discovery threshold.
   *
   * This is publicly visible for testing.
   */
  def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = {
    if (paths.length >= sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold) {
      HadoopFsRelation.listLeafFilesInParallel(paths, hadoopConf, sparkSession)
    } else {
      // Right now, the number of paths is less than the value of
      // parallelPartitionDiscoveryThreshold. So, we will list file statues at the driver.
      // If there is any child that has more files than the threshold, we will use parallel
      // listing.

      // Dummy jobconf to get to the pathFilter defined in configuration
      val jobConf = new JobConf(hadoopConf, this.getClass)
      val pathFilter = FileInputFormat.getInputPathFilter(jobConf)

      val statuses: Seq[FileStatus] = paths.flatMap { path =>
        val fs = path.getFileSystem(hadoopConf)
        logTrace(s"Listing $path on driver")

        val childStatuses = {
          try {
            val stats = fs.listStatus(path)
            if (pathFilter != null) stats.filter(f => pathFilter.accept(f.getPath)) else stats
          } catch {
            case _: FileNotFoundException =>
              logWarning(s"The directory $path was not found. Was it deleted very recently?")
              Array.empty[FileStatus]
          }
        }

        childStatuses.map {
          case f: LocatedFileStatus => f

          // NOTE:
          //
          // - Although S3/S3A/S3N file system can be quite slow for remote file metadata
          //   operations, calling `getFileBlockLocations` does no harm here since these file system
          //   implementations don't actually issue RPC for this method.
          //
          // - Here we are calling `getFileBlockLocations` in a sequential manner, but it should not
          //   be a big deal since we always use to `listLeafFilesInParallel` when the number of
          //   paths exceeds threshold.
          case f =>
            if (f.isDirectory ) {
              // If f is a directory, we do not need to call getFileBlockLocations (SPARK-14959).
              f
            } else {
              HadoopFsRelation.createLocatedFileStatus(f, fs.getFileBlockLocations(f, 0, f.getLen))
            }
        }
      }.filterNot { status =>
        val name = status.getPath.getName
        HadoopFsRelation.shouldFilterOut(name)
      }

      val (dirs, files) = statuses.partition(_.isDirectory)

      // It uses [[LinkedHashSet]] since the order of files can affect the results. (SPARK-11500)
      if (dirs.isEmpty) {
        mutable.LinkedHashSet(files: _*)
      } else {
        mutable.LinkedHashSet(files: _*) ++ listLeafFiles(dirs.map(_.getPath))
      }
    }
  }

  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  protected def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }
}
