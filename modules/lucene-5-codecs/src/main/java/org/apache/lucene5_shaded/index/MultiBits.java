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
 * Concatenates multiple Bits together, on every lookup.
 *
 * <p><b>NOTE</b>: This is very costly, as every lookup must
 * do a binary search to locate the right sub-reader.
 *
 * @lucene.experimental
 */
final class MultiBits implements Bits {
  private final Bits[] subs;

  // length is 1+subs.length (the last entry has the maxDoc):
  private final int[] starts;

  private final boolean defaultValue;

  public MultiBits(Bits[] subs, int[] starts, boolean defaultValue) {
    assert starts.length == 1+subs.length;
    this.subs = subs;
    this.starts = starts;
    this.defaultValue = defaultValue;
  }

  private boolean checkLength(int reader, int doc) {
    final int length = starts[1+reader]-starts[reader];
    assert doc - starts[reader] < length: "doc=" + doc + " reader=" + reader + " starts[reader]=" + starts[reader] + " length=" + length;
    return true;
  }

  @Override
  public boolean get(int doc) {
    final int reader = ReaderUtil.subIndex(doc, starts);
    assert reader != -1;
    final Bits bits = subs[reader];
    if (bits == null) {
      return defaultValue;
    } else {
      assert checkLength(reader, doc);
      return bits.get(doc-starts[reader]);
    }
  }
  
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(subs.length + " subs: ");
    for(int i=0;i<subs.length;i++) {
      if (i != 0) {
        b.append("; ");
      }
      if (subs[i] == null) {
        b.append("s=" + starts[i] + " l=null");
      } else {
        b.append("s=" + starts[i] + " l=" + subs[i].length() + " b=" + subs[i]);
      }
    }
    b.append(" end=" + starts[subs.length]);
    return b.toString();
  }

  /**
   * Represents a sub-Bits from 
   * {@link MultiBits#getMatchingSub(ReaderSlice) getMatchingSub()}.
   */
  public final static class SubResult {
    public boolean matches;
    public Bits result;
  }

  /**
   * Returns a sub-Bits matching the provided <code>slice</code>
   * <p>
   * Because <code>null</code> usually has a special meaning for
   * Bits (e.g. no deleted documents), you must check
   * {@link SubResult#matches} instead to ensure the sub was 
   * actually found.
   */
  public SubResult getMatchingSub(ReaderSlice slice) {
    int reader = ReaderUtil.subIndex(slice.start, starts);
    assert reader != -1;
    assert reader < subs.length: "slice=" + slice + " starts[-1]=" + starts[starts.length-1];
    final SubResult subResult = new SubResult();
    if (starts[reader] == slice.start && starts[1+reader] == slice.start+slice.length) {
      subResult.matches = true;
      subResult.result = subs[reader];
    } else {
      subResult.matches = false;
    }
    return subResult;
  }

  @Override
  public int length() {
    return starts[starts.length-1];
  }
}
