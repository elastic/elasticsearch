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
package org.apache.lucene5_shaded.index;

import org.apache.lucene5_shaded.util.Bits;


/**
 * Exposes a slice of an existing Bits as a new Bits.
 *
 * @lucene.internal
 */
final class BitsSlice implements Bits {
  private final Bits parent;
  private final int start;
  private final int length;

  // start is inclusive; end is exclusive (length = end-start)
  public BitsSlice(Bits parent, ReaderSlice slice) {
    this.parent = parent;
    this.start = slice.start;
    this.length = slice.length;
    assert length >= 0: "length=" + length;
  }
    
  @Override
  public boolean get(int doc) {
    if (doc >= length) {
      throw new RuntimeException("doc " + doc + " is out of bounds 0 .. " + (length-1));
    }
    assert doc < length: "doc=" + doc + " length=" + length;
    return parent.get(doc+start);
  }

  @Override
  public int length() {
    return length;
  }
}
