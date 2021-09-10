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
package org.apache.lucene5_shaded.util.automaton;


/**
 * Automaton representation for matching UTF-8 byte[].
 */
public class ByteRunAutomaton extends RunAutomaton {

  /** Converts incoming automaton to byte-based (UTF32ToUTF8) first */
  public ByteRunAutomaton(Automaton a) {
    this(a, false, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }
  
  /** expert: if utf8 is true, the input is already byte-based */
  public ByteRunAutomaton(Automaton a, boolean isBinary, int maxDeterminizedStates) {
    super(isBinary ? a : new UTF32ToUTF8().convert(a), 256, true, maxDeterminizedStates);
  }

  /**
   * Returns true if the given byte array is accepted by this automaton
   */
  public boolean run(byte[] s, int offset, int length) {
    int p = initial;
    int l = offset + length;
    for (int i = offset; i < l; i++) {
      p = step(p, s[i] & 0xFF);
      if (p == -1) return false;
    }
    return accept[p];
  }
}
