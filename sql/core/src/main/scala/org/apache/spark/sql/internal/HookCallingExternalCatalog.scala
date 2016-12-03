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
package org.apache.spark.sql.internal

import org.apache.spark.sql.catalog.CatalogHooks
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * This is a wrapper for [[ExternalCatalog]] that calls the hooks defined in the
 * [[CatalogHooks]] interface when we call a method on the [[ExternalCatalog]] for which a
 * before/after hook is defined.
 */
class HookCallingExternalCatalog(val delegate: ExternalCatalog, val hooks: CatalogHooks)
  extends ExternalCatalog {

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val db = dbDefinition.name
    if (ignoreIfExists && delegate.databaseExists(db)) {
      return
    }
    hooks.beforeCreateDatabase(db)
    delegate.createDatabase(dbDefinition, ignoreIfExists)
    hooks.afterCreateDatabase(db)
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    if (ignoreIfNotExists && !delegate.databaseExists(db)) {
      return
    }
    hooks.beforeDropDatabase(db)
    delegate.dropDatabase(db, ignoreIfNotExists, cascade)
    hooks.afterDropDatabase(db)
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val db = dbDefinition.name
    hooks.beforeAlterDatabase(db)
    delegate.alterDatabase(dbDefinition)
    hooks.afterAlterDatabase(db)
  }

  override def getDatabase(db: String): CatalogDatabase = {
    hooks.beforeGetDatabase(db)
    val catalogDatabase = delegate.getDatabase(db)
    hooks.afterGetDatabase(db)
    catalogDatabase
  }

  override def databaseExists(db: String): Boolean = {
    hooks.beforeExistsDatabase(db)
    val exists = delegate.databaseExists(db)
    hooks.afterExistsDatabase(db)
    exists
  }

  override def listDatabases(): Seq[String] = {
    hooks.beforeListDatabases(None)
    val databases = delegate.listDatabases()
    hooks.afterListDatabases(None)
    databases
  }

  override def listDatabases(pattern: String): Seq[String] = {
    hooks.beforeListDatabases(Option(pattern))
    val databases = delegate.listDatabases(pattern)
    hooks.afterListDatabases(Option(pattern))
    databases

  }

  override def setCurrentDatabase(db: String): Unit = {
    delegate.setCurrentDatabase(db)
  }

  override def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = {
    val db = tableDefinition.database
    val table = tableDefinition.identifier.table
    if (ignoreIfExists && delegate.tableExists(db, table)) {
      return
    }
    hooks.beforeCreateTable(db, table)
    delegate.createTable(tableDefinition, ignoreIfExists)
    hooks.afterCreateTable(db, table)
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    if (ignoreIfNotExists && !delegate.tableExists(db, table)) {
      return
    }
    hooks.beforeDropTable(db, table)
    delegate.dropTable(db, table, ignoreIfNotExists, purge  )
    hooks.afterDropTable(db, table)
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    hooks.beforeRenameTable(db, oldName, newName)
    delegate.renameTable(db, oldName, newName)
    hooks.afterRenameTable(db, oldName, newName)
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    val db = tableDefinition.database
    val table = tableDefinition.identifier.table
    hooks.beforeAlterTable(db, table)
    delegate.alterTable(tableDefinition)
    hooks.afterAlterTable(db, table)
  }

  override def getTable(db: String, table: String): CatalogTable = {
    hooks.beforeGetTable(db, table)
    val result = delegate.getTable(db, table)
    hooks.afterGetTable(db, table)
    result
  }

  override def getTableOption(db: String, table: String): Option[CatalogTable] = {
    hooks.beforeGetTable(db, table)
    val result = delegate.getTableOption(db, table)
    hooks.afterGetTable(db, table)
    result
  }

  override def tableExists(db: String, table: String): Boolean = {
    hooks.beforeExistsTable(db, table)
    val result = delegate.tableExists(db, table)
    hooks.afterExistsTable(db, table)
    result
  }

  override def listTables(db: String): Seq[String] = {
    hooks.beforeListTables(db, None)
    val result = delegate.listTables(db)
    hooks.afterListTables(db, None)
    result
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    hooks.beforeListTables(db, Option(pattern))
    val result = delegate.listTables(db, pattern)
    hooks.afterListTables(db, Option(pattern))
    result
  }

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      holdDDLTime: Boolean): Unit = {
    delegate.loadTable(db, table, loadPath, isOverwrite, holdDDLTime)
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean): Unit = {
    delegate.loadPartition(db,
      table,
      loadPath,
      partition,
      isOverwrite,
      holdDDLTime,
      inheritTableSpecs)
  }

  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean): Unit = {
    delegate.loadDynamicPartitions(db, table, loadPath, partition, replace, numDP, holdDDLTime)
  }

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    delegate.createPartitions(db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    delegate.dropPartitions(db, table, parts, ignoreIfNotExists, purge, retainData)
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    delegate.renamePartitions(db, table, specs, newSpecs)
  }

  override def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    delegate.alterPartitions(db, table, parts)
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    delegate.getPartition(db, table, spec)
  }

  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    delegate.getPartitionOption(db, table, spec)
  }

  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    delegate.listPartitions(db, table, partialSpec)
  }

  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    delegate.listPartitionsByFilter(db, table, predicates)
  }

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    val funcName = funcDefinition.identifier.funcName
    hooks.beforeCreateFunction(db, funcName)
    delegate.createFunction(db, funcDefinition)
    hooks.afterCreateFunction(db, funcName)
  }

  override def dropFunction(db: String, funcName: String): Unit = {
    hooks.beforeDropFunction(db, funcName)
    delegate.dropFunction(db, funcName)
    hooks.afterDropFunction(db, funcName)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    hooks.beforeRenameFunction(db, oldName, newName)
    delegate.renameFunction(db, oldName, newName)
    hooks.afterRenameFunction(db, oldName, newName)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    hooks.beforeGetFunction(db, funcName)
    val result = delegate.getFunction(db, funcName)
    hooks.afterGetFunction(db, funcName)
    result
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    hooks.beforeExistsFunction(db, funcName)
    val result = delegate.functionExists(db, funcName)
    hooks.afterExistsFunction(db, funcName)
    result
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    hooks.beforeListFunctions(db, Option(pattern))
    val result = delegate.listFunctions(db, pattern)
    hooks.afterListFunctions(db, Option(pattern))
    result
  }
}
