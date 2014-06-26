package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.MultiTermsEnum.TermsEnumIndex;
import org.apache.lucene.index.MultiTermsEnum.TermsEnumWithSlice;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;

/** maps per-segment ordinals to/from global ordinal space */
// TODO: we could also have a utility method to merge Terms[] and use size() as a weight when we need it
// TODO: use more efficient packed ints structures?
// TODO: pull this out? its pretty generic (maps between N ord()-enabled TermsEnums) 
public class XOrdinalMap implements Accountable {

static {
  assert org.elasticsearch.Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_4_9: "Remove this code once we upgrade to Lucene 4.10 (LUCENE-5780, LUCENE-5782)";
}
  
  private static class SegmentMap implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SegmentMap.class);
    
    /** Build a map from an index into a sorted view of `weights` to an index into `weights`. */
    private static int[] map(final long[] weights) {
      final int[] newToOld = new int[weights.length];
      for (int i = 0; i < weights.length; ++i) {
        newToOld[i] = i;
      }
      new InPlaceMergeSorter() {
        @Override
        protected void swap(int i, int j) {
          final int tmp = newToOld[i];
          newToOld[i] = newToOld[j];
          newToOld[j] = tmp;
        }
        @Override
        protected int compare(int i, int j) {
          // j first since we actually want higher weights first
          return Long.compare(weights[newToOld[j]], weights[newToOld[i]]);
        }
      }.sort(0, weights.length);
      return newToOld;
    }
    
    /** Inverse the map. */
    private static int[] inverse(int[] map) {
      final int[] inverse = new int[map.length];
      for (int i = 0; i < map.length; ++i) {
        inverse[map[i]] = i;
      }
      return inverse;
    }
    
    private final int[] newToOld, oldToNew;
    
    SegmentMap(long[] weights) {
      newToOld = map(weights);
      oldToNew = inverse(newToOld);
      assert Arrays.equals(newToOld, inverse(oldToNew));
    }
    
    int newToOld(int segment) {
      return newToOld[segment];
    }
    
    int oldToNew(int segment) {
      return oldToNew[segment];
    }
    
    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(newToOld) + RamUsageEstimator.sizeOf(oldToNew);
    }
    
  }
  
  /**
   * Create an ordinal map that uses the number of unique values of each
   * {@link SortedDocValues} instance as a weight.
   * @see #build(Object, TermsEnum[], long[], float)
   */
  public static XOrdinalMap build(Object owner, SortedDocValues[] values, float acceptableOverheadRatio) throws IOException {
    final TermsEnum[] subs = new TermsEnum[values.length];
    final long[] weights = new long[values.length];
    for (int i = 0; i < values.length; ++i) {
      subs[i] = values[i].termsEnum();
      weights[i] = values[i].getValueCount();
    }
    return build(owner, subs, weights, acceptableOverheadRatio);
  }
  
  /**
   * Create an ordinal map that uses the number of unique values of each
   * {@link SortedSetDocValues} instance as a weight.
   * @see #build(Object, TermsEnum[], long[], float)
   */
  public static XOrdinalMap build(Object owner, SortedSetDocValues[] values, float acceptableOverheadRatio) throws IOException {
    final TermsEnum[] subs = new TermsEnum[values.length];
    final long[] weights = new long[values.length];
    for (int i = 0; i < values.length; ++i) {
      subs[i] = values[i].termsEnum();
      weights[i] = values[i].getValueCount();
    }
    return build(owner, subs, weights, acceptableOverheadRatio);
  }
  
  /** 
   * Creates an ordinal map that allows mapping ords to/from a merged
   * space from <code>subs</code>.
   * @param owner a cache key
   * @param subs TermsEnums that support {@link TermsEnum#ord()}. They need
   *             not be dense (e.g. can be FilteredTermsEnums}.
   * @param weights a weight for each sub. This is ideally correlated with
   *             the number of unique terms that each sub introduces compared
   *             to the other subs
   * @throws IOException if an I/O error occurred.
   */
  public static XOrdinalMap build(Object owner, TermsEnum subs[], long[] weights, float acceptableOverheadRatio) throws IOException {
    if (subs.length != weights.length) {
      throw new IllegalArgumentException("subs and weights must have the same length");
    }
    
    // enums are not sorted, so let's sort to save memory
    final SegmentMap segmentMap = new SegmentMap(weights);
    return new XOrdinalMap(owner, subs, segmentMap, acceptableOverheadRatio);
  }
  
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(XOrdinalMap.class);
  
  // cache key of whoever asked for this awful thing
  final Object owner;
  // globalOrd -> (globalOrd - segmentOrd) where segmentOrd is the the ordinal in the first segment that contains this term
  final MonotonicAppendingLongBuffer globalOrdDeltas;
  // globalOrd -> first segment container
  final AppendingPackedLongBuffer firstSegments;
  // for every segment, segmentOrd -> globalOrd
  final LongValues segmentToGlobalOrds[];
  // the map from/to segment ids
  final SegmentMap segmentMap;
  // ram usage
  final long ramBytesUsed;
  
  XOrdinalMap(Object owner, TermsEnum subs[], SegmentMap segmentMap, float acceptableOverheadRatio) throws IOException {
    // create the ordinal mappings by pulling a termsenum over each sub's 
    // unique terms, and walking a multitermsenum over those
    this.owner = owner;
    this.segmentMap = segmentMap;
    // even though we accept an overhead ratio, we keep these ones with COMPACT
    // since they are only used to resolve values given a global ord, which is
    // slow anyway
    globalOrdDeltas = new MonotonicAppendingLongBuffer(PackedInts.COMPACT);
    firstSegments = new AppendingPackedLongBuffer(PackedInts.COMPACT);
    final MonotonicAppendingLongBuffer[] ordDeltas = new MonotonicAppendingLongBuffer[subs.length];
    for (int i = 0; i < ordDeltas.length; i++) {
      ordDeltas[i] = new MonotonicAppendingLongBuffer(acceptableOverheadRatio);
    }
    long[] ordDeltaBits = new long[subs.length];
    long segmentOrds[] = new long[subs.length];
    ReaderSlice slices[] = new ReaderSlice[subs.length];
    TermsEnumIndex indexes[] = new TermsEnumIndex[slices.length];
    for (int i = 0; i < slices.length; i++) {
      slices[i] = new ReaderSlice(0, 0, i);
      indexes[i] = new TermsEnumIndex(subs[segmentMap.newToOld(i)], i);
    }
    MultiTermsEnum mte = new MultiTermsEnum(slices);
    mte.reset(indexes);
    long globalOrd = 0;
    while (mte.next() != null) {        
      TermsEnumWithSlice matches[] = mte.getMatchArray();
      int firstSegmentIndex = Integer.MAX_VALUE;
      long globalOrdDelta = Long.MAX_VALUE;
      for (int i = 0; i < mte.getMatchCount(); i++) {
        int segmentIndex = matches[i].index;
        long segmentOrd = matches[i].terms.ord();
        long delta = globalOrd - segmentOrd;
        // We compute the least segment where the term occurs. In case the
        // first segment contains most (or better all) values, this will
        // help save significant memory
        if (segmentIndex < firstSegmentIndex) {
          firstSegmentIndex = segmentIndex;
          globalOrdDelta = delta;
        }
        // for each per-segment ord, map it back to the global term.
        while (segmentOrds[segmentIndex] <= segmentOrd) {
          ordDeltaBits[segmentIndex] |= delta;
          ordDeltas[segmentIndex].add(delta);
          segmentOrds[segmentIndex]++;
        }
      }
      // for each unique term, just mark the first segment index/delta where it occurs
      assert firstSegmentIndex < segmentOrds.length;
      firstSegments.add(firstSegmentIndex);
      globalOrdDeltas.add(globalOrdDelta);
      globalOrd++;
    }
    firstSegments.freeze();
    globalOrdDeltas.freeze();
    for (int i = 0; i < ordDeltas.length; ++i) {
      ordDeltas[i].freeze();
    }
    // ordDeltas is typically the bottleneck, so let's see what we can do to make it faster
    segmentToGlobalOrds = new LongValues[subs.length];
    long ramBytesUsed = BASE_RAM_BYTES_USED + globalOrdDeltas.ramBytesUsed()
        + firstSegments.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds)
        + segmentMap.ramBytesUsed();
    for (int i = 0; i < ordDeltas.length; ++i) {
      final MonotonicAppendingLongBuffer deltas = ordDeltas[i];
      if (ordDeltaBits[i] == 0L) {
        // segment ords perfectly match global ordinals
        // likely in case of low cardinalities and large segments
        segmentToGlobalOrds[i] = LongValues.IDENTITY;
      } else {
        final int bitsRequired = ordDeltaBits[i] < 0 ? 64 : PackedInts.bitsRequired(ordDeltaBits[i]);
        final long monotonicBits = deltas.ramBytesUsed() * 8;
        final long packedBits = bitsRequired * deltas.size();
        if (deltas.size() <= Integer.MAX_VALUE
            && packedBits <= monotonicBits * (1 + acceptableOverheadRatio)) {
          // monotonic compression mostly adds overhead, let's keep the mapping in plain packed ints
          final int size = (int) deltas.size();
          final PackedInts.Mutable newDeltas = PackedInts.getMutable(size, bitsRequired, acceptableOverheadRatio);
          final MonotonicAppendingLongBuffer.Iterator it = deltas.iterator();
          for (int ord = 0; ord < size; ++ord) {
            newDeltas.set(ord, it.next());
          }
          assert !it.hasNext();
          segmentToGlobalOrds[i] = new LongValues() {
            @Override
            public long get(long ord) {
              return ord + newDeltas.get((int) ord);
            }
          };
          ramBytesUsed += newDeltas.ramBytesUsed();
        } else {
          segmentToGlobalOrds[i] = new LongValues() {
            @Override
            public long get(long ord) {
              return ord + deltas.get(ord);
            }
          };
          ramBytesUsed += deltas.ramBytesUsed();
        }
        ramBytesUsed += RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds[i]);
      }
    }
    this.ramBytesUsed = ramBytesUsed;
  }
  
  /** 
   * Given a segment number, return a {@link LongValues} instance that maps
   * segment ordinals to global ordinals.
   */
  public LongValues getGlobalOrds(int segmentIndex) {
    return segmentToGlobalOrds[segmentMap.oldToNew(segmentIndex)];
  }
  
  /**
   * Given global ordinal, returns the ordinal of the first segment which contains
   * this ordinal (the corresponding to the segment return {@link #getFirstSegmentNumber}).
   */
  public long getFirstSegmentOrd(long globalOrd) {
    return globalOrd - globalOrdDeltas.get(globalOrd);
  }
  
  /** 
   * Given a global ordinal, returns the index of the first
   * segment that contains this term.
   */
  public int getFirstSegmentNumber(long globalOrd) {
    return segmentMap.newToOld((int) firstSegments.get(globalOrd));
  }
  
  /**
   * Returns the total number of unique terms in global ord space.
   */
  public long getValueCount() {
    return globalOrdDeltas.size();
  }
  
  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }
}
