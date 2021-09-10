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
package org.apache.lucene5_shaded.search;


import org.apache.lucene5_shaded.index.DocValues;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.SortedNumericDocValues;
import org.apache.lucene5_shaded.util.NumericUtils;

/** 
 * Selects a value from the document's list to use as the representative value 
 * <p>
 * This provides a NumericDocValues view over the SortedNumeric, for use with sorting,
 * expressions, function queries, etc.
 */
public class SortedNumericSelector {
  
  /** 
   * Type of selection to perform.
   */
  public enum Type {
    /** 
     * Selects the minimum value in the set 
     */
    MIN,
    /** 
     * Selects the maximum value in the set 
     */
    MAX,
    // TODO: we could do MEDIAN in constant time (at most 2 lookups)
  }
  
  /** 
   * Wraps a multi-valued SortedNumericDocValues as a single-valued view, using the specified selector 
   * and numericType.
   */
  public static NumericDocValues wrap(SortedNumericDocValues sortedNumeric, Type selector, SortField.Type numericType) {
    if (numericType != SortField.Type.INT &&
        numericType != SortField.Type.LONG && 
        numericType != SortField.Type.FLOAT &&
        numericType != SortField.Type.DOUBLE) {
      throw new IllegalArgumentException("numericType must be a numeric type");
    }
    final NumericDocValues view;
    NumericDocValues singleton = DocValues.unwrapSingleton(sortedNumeric);
    if (singleton != null) {
      // it's actually single-valued in practice, but indexed as multi-valued,
      // so just sort on the underlying single-valued dv directly.
      // regardless of selector type, this optimization is safe!
      view = singleton;
    } else { 
      switch(selector) {
        case MIN: 
          view = new MinValue(sortedNumeric);
          break;
        case MAX:
          view = new MaxValue(sortedNumeric);
          break;
        default: 
          throw new AssertionError();
      }
    }
    // undo the numericutils sortability
    switch(numericType) {
      case FLOAT:
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return NumericUtils.sortableFloatBits((int) view.get(docID));
          }
        };
      case DOUBLE:
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return NumericUtils.sortableDoubleBits(view.get(docID));
          }
        };
      default:
        return view;
    }
  }
  
  /** Wraps a SortedNumericDocValues and returns the first value (min) */
  static class MinValue extends NumericDocValues {
    final SortedNumericDocValues in;
    
    MinValue(SortedNumericDocValues in) {
      this.in = in;
    }

    @Override
    public long get(int docID) {
      in.setDocument(docID);
      if (in.count() == 0) {
        return 0; // missing
      } else {
        return in.valueAt(0);
      }
    }
  }
  
  /** Wraps a SortedNumericDocValues and returns the last value (max) */
  static class MaxValue extends NumericDocValues {
    final SortedNumericDocValues in;
    
    MaxValue(SortedNumericDocValues in) {
      this.in = in;
    }

    @Override
    public long get(int docID) {
      in.setDocument(docID);
      final int count = in.count();
      if (count == 0) {
        return 0; // missing
      } else {
        return in.valueAt(count-1);
      }
    }
  }
}
