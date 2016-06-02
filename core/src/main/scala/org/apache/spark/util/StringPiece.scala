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

package org.apache.spark.util

final class StringPiece(children: Array[Object]) {
  final val length: Int = {
    var total = 0
    var i = 0
    while (i < children.length) {
      children(i) match {
        case s: String =>
          total += s.length
        case s: StringPiece =>
          total += s.length
      }
      i += 1
    }
    total
  }

  def trimmed(limit: Int): String = {
    if (length <= limit) {
      toString
    } else {
      var childLimit = limit / children.length
      var numOver = 0
      var extraLength = 0
      children.foreach {
        case s: String =>
          if (s.length <= childLimit) {
            extraLength += childLimit - s.length
          } else {
            numOver += 1
          }
        case s: StringPiece =>
          if (s.length <= childLimit) {
            extraLength += childLimit - s.length
          } else {
            numOver += 1
          }
      }
      if (numOver * 25 > extraLength) {
        s"[$length bytes truncated]"
      } else {
        childLimit += extraLength / numOver
        children.map {
          case s: String =>
            if (s.length < childLimit) {
              s
            } else {
              s"[${s.length} bytes truncated]"
            }
          case s: StringPiece =>
            s.trimmed(childLimit)
        }.mkString("")
      }
    }
  }

//  def +(s: StringPiece): StringPiece = new StringPiece(Array(this, s))
//  def +(s: String): StringPiece = new StringPiece(Array(this, s))
//  def +:(s: StringPiece): StringPiece = new StringPiece(Array(s, this))
//  def +:(s: String): StringPiece = new StringPiece(Array(s, this))

  def appendTo(builder: StringBuilder): Unit = {
    var i = 0
    while (i < children.length) {
      children(i) match {
        case s: String => builder.append(s)
        case s: StringPiece => s.appendTo(builder)
      }
      i += 1
    }
  }

  override def toString: String = {
    val builder = new StringBuilder()
    appendTo(builder)
    builder.toString
  }
}
