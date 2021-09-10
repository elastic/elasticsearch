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
package org.apache.lucene5_shaded.analysis;

import java.io.Reader;


/** Internal class to enable reuse of the string reader by {@link Analyzer#tokenStream(String,String)} */
final class ReusableStringReader extends Reader {
  private int pos = 0, size = 0;
  private String s = null;
  
  void setValue(String s) {
    this.s = s;
    this.size = s.length();
    this.pos = 0;
  }
  
  @Override
  public int read() {
    if (pos < size) {
      return s.charAt(pos++);
    } else {
      s = null;
      return -1;
    }
  }
  
  @Override
  public int read(char[] c, int off, int len) {
    if (pos < size) {
      len = Math.min(len, size-pos);
      s.getChars(pos, pos+len, c, off);
      pos += len;
      return len;
    } else {
      s = null;
      return -1;
    }
  }
  
  @Override
  public void close() {
    pos = size; // this prevents NPE when reading after close!
    s = null;
  }
}
