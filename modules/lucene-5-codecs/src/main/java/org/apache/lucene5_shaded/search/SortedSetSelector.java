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
import org.apache.lucene5_shaded.index.RandomAccessOrds;
import org.apache.lucene5_shaded.index.SortedDocValues;
import org.apache.lucene5_shaded.index.SortedSetDocValues;
import org.apache.lucene5_shaded.util.BytesRef;

/** Selects a value from the document's set to use as the representative value */
public class SortedSetSelector {
  
  /** 
   * Type of selection to perform.
   * <p>
   * Limitations:
   * <ul>
   *   <li>Fields containing {@link Integer#MAX_VALUE} or more unique values
   *       are unsupported.
   *   <li>Selectors other than ({@link Type#MIN}) require 
   *       optional codec support. However several codecs provided by Lucene, 
   *       including the current default codec, support this.
   * </ul>
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
    /** 
     * Selects the middle value in the set.
     * <p>
     * If the set has an even number of values, the lower of the middle two is chosen.
     */
    MIDDLE_MIN,
    /** 
     * Selects the middle value in the set.
     * <p>
     * If the set has an even number of values, the higher of the middle two is chosen
     */
    MIDDLE_MAX
  }
  
  /** Wraps a multi-valued SortedSetDocValues as a single-valued view, using the specified selector */
  public static SortedDocValues wrap(SortedSetDocValues sortedSet, Type selector) {
    if (sortedSet.getValueCount() >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("fields containing more than " + (Integer.MAX_VALUE-1) + " unique terms are unsupported");
    }
    
    SortedDocValues singleton = DocValues.unwrapSingleton(sortedSet);
    if (singleton != null) {
      // it's actually single-valued in practice, but indexed as multi-valued,
      // so just sort on the underlying single-valued dv directly.
      // regardless of selector type, this optimization is safe!
      return singleton;
    } else if (selector == Type.MIN) {
      return new MinValue(sortedSet);
    } else {
      if (sortedSet instanceof RandomAccessOrds == false) {
        throw new UnsupportedOperationException("codec does not support random access ordinals, cannot use selector: " + selector + " docValsImpl: " + sortedSet.toString());
      }
      RandomAccessOrds randomOrds = (RandomAccessOrds) sortedSet;
      switch(selector) {
        case MAX: return new MaxValue(randomOrds);
        case MIDDLE_MIN: return new MiddleMinValue(randomOrds);
        case MIDDLE_MAX: return new MiddleMaxValue(randomOrds);
        case MIN: 
        default: 
          throw new AssertionError();
      }
    }
  }
  
  /** Wraps a SortedSetDocValues and returns the first ordinal (min) */
  static class MinValue extends SortedDocValues {
    final SortedSetDocValues in;
    
    MinValue(SortedSetDocValues in) {
      this.in = in;
    }

    @Override
    public int getOrd(int docID) {
      in.setDocument(docID);
      return (int) in.nextOrd();
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return (int) in.getValueCount();
    }

    @Override
    public int lookupTerm(BytesRef key) {
      return (int) in.lookupTerm(key);
    }
  }
  
  /** Wraps a SortedSetDocValues and returns the last ordinal (max) */
  static class MaxValue extends SortedDocValues {
    final RandomAccessOrds in;
    
    MaxValue(RandomAccessOrds in) {
      this.in = in;
    }

    @Override
    public int getOrd(int docID) {
      in.setDocument(docID);
      final int count = in.cardinality();
      if (count == 0) {
        return -1;
      } else {
        return (int) in.ordAt(count-1);
      }
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return (int) in.getValueCount();
    }
    
    @Override
    public int lookupTerm(BytesRef key) {
      return (int) in.lookupTerm(key);
    }
  }
  
  /** Wraps a SortedSetDocValues and returns the middle ordinal (or min of the two) */
  static class MiddleMinValue extends SortedDocValues {
    final RandomAccessOrds in;
    
    MiddleMinValue(RandomAccessOrds in) {
      this.in = in;
    }

    @Override
    public int getOrd(int docID) {
      in.setDocument(docID);
      final int count = in.cardinality();
      if (count == 0) {
        return -1;
      } else {
        return (int) in.ordAt((count-1) >>> 1);
      }
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return (int) in.getValueCount();
    }
    
    @Override
    public int lookupTerm(BytesRef key) {
      return (int) in.lookupTerm(key);
    }
  }
  
  /** Wraps a SortedSetDocValues and returns the middle ordinal (or max of the two) */
  static class MiddleMaxValue extends SortedDocValues {
    final RandomAccessOrds in;
    
    MiddleMaxValue(RandomAccessOrds in) {
      this.in = in;
    }

    @Override
    public int getOrd(int docID) {
      in.setDocument(docID);
      final int count = in.cardinality();
      if (count == 0) {
        return -1;
      } else {
        return (int) in.ordAt(count >>> 1);
      }
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return (int) in.getValueCount();
    }
    
    @Override
    public int lookupTerm(BytesRef key) {
      return (int) in.lookupTerm(key);
    }
  }
}
