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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene5_shaded.util.BytesRef;

import org.apache.lucene5_shaded.util.automaton.CompiledAutomaton;

/**
 * Exposes flex API, merged from flex API of
 * sub-segments.
 *
 * @lucene.experimental
 */

public final class MultiTerms extends Terms {
  private final Terms[] subs;
  private final ReaderSlice[] subSlices;
  private final boolean hasFreqs;
  private final boolean hasOffsets;
  private final boolean hasPositions;
  private final boolean hasPayloads;

  /** Sole constructor.
   *
   * @param subs The {@link Terms} instances of all sub-readers. 
   * @param subSlices A parallel array (matching {@code
   *        subs}) describing the sub-reader slices.
   */
  public MultiTerms(Terms[] subs, ReaderSlice[] subSlices) throws IOException {
    this.subs = subs;
    this.subSlices = subSlices;
    
    assert subs.length > 0 : "inefficient: don't use MultiTerms over one sub";
    boolean _hasFreqs = true;
    boolean _hasOffsets = true;
    boolean _hasPositions = true;
    boolean _hasPayloads = false;
    for(int i=0;i<subs.length;i++) {
      _hasFreqs &= subs[i].hasFreqs();
      _hasOffsets &= subs[i].hasOffsets();
      _hasPositions &= subs[i].hasPositions();
      _hasPayloads |= subs[i].hasPayloads();
    }

    hasFreqs = _hasFreqs;
    hasOffsets = _hasOffsets;
    hasPositions = _hasPositions;
    hasPayloads = hasPositions && _hasPayloads; // if all subs have pos, and at least one has payloads.
  }

  /** Expert: returns the Terms being merged. */
  public Terms[] getSubTerms() {
    return subs;
  }

  /** Expert: returns  pointers to the sub-readers corresponding to the Terms being merged. */
  public ReaderSlice[] getSubSlices() {
    return subSlices;
  }

  @Override
  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    final List<MultiTermsEnum.TermsEnumIndex> termsEnums = new ArrayList<>();
    for(int i=0;i<subs.length;i++) {
      final TermsEnum termsEnum = subs[i].intersect(compiled, startTerm);
      if (termsEnum != null) {
        termsEnums.add(new MultiTermsEnum.TermsEnumIndex(termsEnum, i));
      }
    }

    if (termsEnums.size() > 0) {
      return new MultiTermsEnum(subSlices).reset(termsEnums.toArray(MultiTermsEnum.TermsEnumIndex.EMPTY_ARRAY));
    } else {
      return TermsEnum.EMPTY;
    }
  }
  
  @Override
  public BytesRef getMin() throws IOException {
    BytesRef minTerm = null;
    for(Terms terms : subs) {
      BytesRef term = terms.getMin();
      if (minTerm == null || term.compareTo(minTerm) < 0) {
        minTerm = term;
      }
    }

    return minTerm;
  }

  @Override
  public BytesRef getMax() throws IOException {
    BytesRef maxTerm = null;
    for(Terms terms : subs) {
      BytesRef term = terms.getMax();
      if (maxTerm == null || term.compareTo(maxTerm) > 0) {
        maxTerm = term;
      }
    }

    return maxTerm;
  }

  @Override
  public TermsEnum iterator() throws IOException {

    final List<MultiTermsEnum.TermsEnumIndex> termsEnums = new ArrayList<>();
    for(int i=0;i<subs.length;i++) {
      final TermsEnum termsEnum = subs[i].iterator();
      if (termsEnum != null) {
        termsEnums.add(new MultiTermsEnum.TermsEnumIndex(termsEnum, i));
      }
    }

    if (termsEnums.size() > 0) {
      return new MultiTermsEnum(subSlices).reset(termsEnums.toArray(MultiTermsEnum.TermsEnumIndex.EMPTY_ARRAY));
    } else {
      return TermsEnum.EMPTY;
    }
  }

  @Override
  public long size() {
    return -1;
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
    long sum = 0;
    for(Terms terms : subs) {
      final long v = terms.getSumTotalTermFreq();
      if (v == -1) {
        return -1;
      }
      sum += v;
    }
    return sum;
  }
  
  @Override
  public long getSumDocFreq() throws IOException {
    long sum = 0;
    for(Terms terms : subs) {
      final long v = terms.getSumDocFreq();
      if (v == -1) {
        return -1;
      }
      sum += v;
    }
    return sum;
  }
  
  @Override
  public int getDocCount() throws IOException {
    int sum = 0;
    for(Terms terms : subs) {
      final int v = terms.getDocCount();
      if (v == -1) {
        return -1;
      }
      sum += v;
    }
    return sum;
  }

  @Override
  public boolean hasFreqs() {
    return hasFreqs;
  }

  @Override
  public boolean hasOffsets() {
    return hasOffsets;
  }

  @Override
  public boolean hasPositions() {
    return hasPositions;
  }
  
  @Override
  public boolean hasPayloads() {
    return hasPayloads;
  }
}

