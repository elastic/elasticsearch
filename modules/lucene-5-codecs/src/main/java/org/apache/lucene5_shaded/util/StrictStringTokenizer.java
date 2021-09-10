/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.util;


/** Used for parsing Version strings so we don't have to
 *  use overkill String.split nor StringTokenizer (which silently
 *  skips empty tokens). */

final class StrictStringTokenizer {

  public StrictStringTokenizer(String s, char delimiter) {
    this.s = s;
    this.delimiter = delimiter;
  }

  public final String nextToken() {
    if (pos < 0) {
      throw new IllegalStateException("no more tokens");
    }

    int pos1 = s.indexOf(delimiter, pos);
    String s1;
    if (pos1 >= 0) {
      s1 = s.substring(pos, pos1);
      pos = pos1+1;
    } else {
      s1 = s.substring(pos);
      pos=-1;
    }

    return s1;
  }

  public final boolean hasMoreTokens() {
    return pos >= 0;
  }

  private final String s;
  private final char delimiter;
  private int pos;
}
