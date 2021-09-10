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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene5_shaded.index.MultiTermsEnum.TermsEnumIndex;
import org.apache.lucene5_shaded.index.MultiTermsEnum.TermsEnumWithSlice;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.InPlaceMergeSorter;
import org.apache.lucene5_shaded.util.LongValues;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.packed.PackedInts;
import org.apache.lucene5_shaded.util.packed.PackedLongValues;

/**
 * A wrapper for CompositeIndexReader providing access to DocValues.
 * 
 * <p><b>NOTE</b>: for multi readers, you'll get better
 * performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * atomic leaves and then operate per-LeafReader,
 * instead of using this class.
 * 
 * <p><b>NOTE</b>: This is very costly.
 *
 * @lucene.experimental
 * @lucene.internal
 */
public class MultiDocValues {
  
  /** No instantiation */
  private MultiDocValues() {}
  
  /** Returns a NumericDocValues for a reader's norms (potentially merging on-the-fly).
   * <p>
   * This is a slow way to access normalization values. Instead, access them per-segment
   * with {@link LeafReader#getNormValues(String)}
   * </p> 
   */
  public static NumericDocValues getNormValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getNormValues(field);
    }
    FieldInfo fi = MultiFields.getMergedFieldInfos(r).fieldInfo(field);
    if (fi == null || fi.hasNorms() == false) {
      return null;
    }

    boolean anyReal = false;
    final NumericDocValues[] values = new NumericDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      NumericDocValues v = context.reader().getNormValues(field);
      if (v == null) {
        v = DocValues.emptyNumeric();
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    assert anyReal;

    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        int subIndex = ReaderUtil.subIndex(docID, starts);
        return values[subIndex].get(docID - starts[subIndex]);
      }
    };
  }

  /** Returns a NumericDocValues for a reader's docvalues (potentially merging on-the-fly) 
   * <p>
   * This is a slow way to access numeric values. Instead, access them per-segment
   * with {@link LeafReader#getNumericDocValues(String)}
   * </p> 
   * */
  public static NumericDocValues getNumericValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getNumericDocValues(field);
    }

    boolean anyReal = false;
    final NumericDocValues[] values = new NumericDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      NumericDocValues v = context.reader().getNumericDocValues(field);
      if (v == null) {
        v = DocValues.emptyNumeric();
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();

    if (!anyReal) {
      return null;
    } else {
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          int subIndex = ReaderUtil.subIndex(docID, starts);
          return values[subIndex].get(docID - starts[subIndex]);
        }
      };
    }
  }
  
  /** Returns a Bits for a reader's docsWithField (potentially merging on-the-fly) 
   * <p>
   * This is a slow way to access this bitset. Instead, access them per-segment
   * with {@link LeafReader#getDocsWithField(String)}
   * </p> 
   * */
  public static Bits getDocsWithField(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getDocsWithField(field);
    }

    boolean anyReal = false;
    boolean anyMissing = false;
    final Bits[] values = new Bits[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      Bits v = context.reader().getDocsWithField(field);
      if (v == null) {
        v = new Bits.MatchNoBits(context.reader().maxDoc());
        anyMissing = true;
      } else {
        anyReal = true;
        if (v instanceof Bits.MatchAllBits == false) {
          anyMissing = true;
        }
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();

    if (!anyReal) {
      return null;
    } else if (!anyMissing) {
      return new Bits.MatchAllBits(r.maxDoc());
    } else {
      return new MultiBits(values, starts, false);
    }
  }

  /** Returns a BinaryDocValues for a reader's docvalues (potentially merging on-the-fly)
   * <p>
   * This is a slow way to access binary values. Instead, access them per-segment
   * with {@link LeafReader#getBinaryDocValues(String)}
   * </p>  
   */
  public static BinaryDocValues getBinaryValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getBinaryDocValues(field);
    }
    
    boolean anyReal = false;
    final BinaryDocValues[] values = new BinaryDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      BinaryDocValues v = context.reader().getBinaryDocValues(field);
      if (v == null) {
        v = DocValues.emptyBinary();
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    if (!anyReal) {
      return null;
    } else {
      return new BinaryDocValues() {
        @Override
        public BytesRef get(int docID) {
          int subIndex = ReaderUtil.subIndex(docID, starts);
          return values[subIndex].get(docID - starts[subIndex]);
        }
      };
    }
  }
  
  /** Returns a SortedNumericDocValues for a reader's docvalues (potentially merging on-the-fly) 
   * <p>
   * This is a slow way to access sorted numeric values. Instead, access them per-segment
   * with {@link LeafReader#getSortedNumericDocValues(String)}
   * </p> 
   * */
  public static SortedNumericDocValues getSortedNumericValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedNumericDocValues(field);
    }

    boolean anyReal = false;
    final SortedNumericDocValues[] values = new SortedNumericDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      SortedNumericDocValues v = context.reader().getSortedNumericDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedNumeric(context.reader().maxDoc());
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();

    if (!anyReal) {
      return null;
    } else {
      return new SortedNumericDocValues() {
        SortedNumericDocValues current;

        @Override
        public void setDocument(int doc) {
          int subIndex = ReaderUtil.subIndex(doc, starts);
          current = values[subIndex];
          current.setDocument(doc - starts[subIndex]);
        }

        @Override
        public long valueAt(int index) {
          return current.valueAt(index);
        }

        @Override
        public int count() {
          return current.count();
        }
      };
    }
  }
  
  /** Returns a SortedDocValues for a reader's docvalues (potentially doing extremely slow things).
   * <p>
   * This is an extremely slow way to access sorted values. Instead, access them per-segment
   * with {@link LeafReader#getSortedDocValues(String)}
   * </p>  
   */
  public static SortedDocValues getSortedValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedDocValues(field);
    }
    
    boolean anyReal = false;
    final SortedDocValues[] values = new SortedDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      SortedDocValues v = context.reader().getSortedDocValues(field);
      if (v == null) {
        v = DocValues.emptySorted();
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    if (!anyReal) {
      return null;
    } else {
      OrdinalMap mapping = OrdinalMap.build(r.getCoreCacheKey(), values, PackedInts.DEFAULT);
      return new MultiSortedDocValues(values, starts, mapping);
    }
  }
  
  /** Returns a SortedSetDocValues for a reader's docvalues (potentially doing extremely slow things).
   * <p>
   * This is an extremely slow way to access sorted values. Instead, access them per-segment
   * with {@link LeafReader#getSortedSetDocValues(String)}
   * </p>  
   */
  public static SortedSetDocValues getSortedSetValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedSetDocValues(field);
    }
    
    boolean anyReal = false;
    final SortedSetDocValues[] values = new SortedSetDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      SortedSetDocValues v = context.reader().getSortedSetDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedSet();
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    if (!anyReal) {
      return null;
    } else {
      OrdinalMap mapping = OrdinalMap.build(r.getCoreCacheKey(), values, PackedInts.DEFAULT);
      return new MultiSortedSetDocValues(values, starts, mapping);
    }
  }

  /** maps per-segment ordinals to/from global ordinal space */
  // TODO: we could also have a utility method to merge Terms[] and use size() as a weight when we need it
  // TODO: use more efficient packed ints structures?
  // TODO: pull this out? it's pretty generic (maps between N ord()-enabled TermsEnums) 
  public static class OrdinalMap implements Accountable {

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

      @Override
      public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
      }
    }

    /**
     * Create an ordinal map that uses the number of unique values of each
     * {@link SortedDocValues} instance as a weight.
     * @see #build(Object, TermsEnum[], long[], float)
     */
    public static OrdinalMap build(Object owner, SortedDocValues[] values, float acceptableOverheadRatio) throws IOException {
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
    public static OrdinalMap build(Object owner, SortedSetDocValues[] values, float acceptableOverheadRatio) throws IOException {
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
    public static OrdinalMap build(Object owner, TermsEnum subs[], long[] weights, float acceptableOverheadRatio) throws IOException {
      if (subs.length != weights.length) {
        throw new IllegalArgumentException("subs and weights must have the same length");
      }

      // enums are not sorted, so let's sort to save memory
      final SegmentMap segmentMap = new SegmentMap(weights);
      return new OrdinalMap(owner, subs, segmentMap, acceptableOverheadRatio);
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OrdinalMap.class);

    // cache key of whoever asked for this awful thing
    final Object owner;
    // globalOrd -> (globalOrd - segmentOrd) where segmentOrd is the the ordinal in the first segment that contains this term
    final PackedLongValues globalOrdDeltas;
    // globalOrd -> first segment container
    final PackedLongValues firstSegments;
    // for every segment, segmentOrd -> globalOrd
    final LongValues segmentToGlobalOrds[];
    // the map from/to segment ids
    final SegmentMap segmentMap;
    // ram usage
    final long ramBytesUsed;
    
    OrdinalMap(Object owner, TermsEnum subs[], SegmentMap segmentMap, float acceptableOverheadRatio) throws IOException {
      // create the ordinal mappings by pulling a termsenum over each sub's 
      // unique terms, and walking a multitermsenum over those
      this.owner = owner;
      this.segmentMap = segmentMap;
      // even though we accept an overhead ratio, we keep these ones with COMPACT
      // since they are only used to resolve values given a global ord, which is
      // slow anyway
      PackedLongValues.Builder globalOrdDeltas = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
      PackedLongValues.Builder firstSegments = PackedLongValues.packedBuilder(PackedInts.COMPACT);
      final PackedLongValues.Builder[] ordDeltas = new PackedLongValues.Builder[subs.length];
      for (int i = 0; i < ordDeltas.length; i++) {
        ordDeltas[i] = PackedLongValues.monotonicBuilder(acceptableOverheadRatio);
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
      this.firstSegments = firstSegments.build();
      this.globalOrdDeltas = globalOrdDeltas.build();
      // ordDeltas is typically the bottleneck, so let's see what we can do to make it faster
      segmentToGlobalOrds = new LongValues[subs.length];
      long ramBytesUsed = BASE_RAM_BYTES_USED + this.globalOrdDeltas.ramBytesUsed()
          + this.firstSegments.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds)
          + segmentMap.ramBytesUsed();
      for (int i = 0; i < ordDeltas.length; ++i) {
        final PackedLongValues deltas = ordDeltas[i].build();
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
            final PackedLongValues.Iterator it = deltas.iterator();
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

    @Override
    public Collection<Accountable> getChildResources() {
      List<Accountable> resources = new ArrayList<>();
      resources.add(Accountables.namedAccountable("global ord deltas", globalOrdDeltas));
      resources.add(Accountables.namedAccountable("first segments", firstSegments));
      resources.add(Accountables.namedAccountable("segment map", segmentMap));
      // TODO: would be nice to return actual child segment deltas too, but the optimizations are confusing
      return resources;
    }
  }
  
  /** 
   * Implements SortedDocValues over n subs, using an OrdinalMap
   * @lucene.internal
   */
  public static class MultiSortedDocValues extends SortedDocValues {
    /** docbase for each leaf: parallel with {@link #values} */
    public final int docStarts[];
    /** leaf values */
    public final SortedDocValues values[];
    /** ordinal map mapping ords from <code>values</code> to global ord space */
    public final OrdinalMap mapping;
  
    /** Creates a new MultiSortedDocValues over <code>values</code> */
    MultiSortedDocValues(SortedDocValues values[], int docStarts[], OrdinalMap mapping) throws IOException {
      assert docStarts.length == values.length + 1;
      this.values = values;
      this.docStarts = docStarts;
      this.mapping = mapping;
    }
       
    @Override
    public int getOrd(int docID) {
      int subIndex = ReaderUtil.subIndex(docID, docStarts);
      int segmentOrd = values[subIndex].getOrd(docID - docStarts[subIndex]);
      return segmentOrd == -1 ? segmentOrd : (int) mapping.getGlobalOrds(subIndex).get(segmentOrd);
    }
 
    @Override
    public BytesRef lookupOrd(int ord) {
      int subIndex = mapping.getFirstSegmentNumber(ord);
      int segmentOrd = (int) mapping.getFirstSegmentOrd(ord);
      return values[subIndex].lookupOrd(segmentOrd);
    }
 
    @Override
    public int getValueCount() {
      return (int) mapping.getValueCount();
    }
  }
  
  /** 
   * Implements MultiSortedSetDocValues over n subs, using an OrdinalMap 
   * @lucene.internal
   */
  public static class MultiSortedSetDocValues extends SortedSetDocValues {
    /** docbase for each leaf: parallel with {@link #values} */
    public final int docStarts[];
    /** leaf values */
    public final SortedSetDocValues values[];
    /** ordinal map mapping ords from <code>values</code> to global ord space */
    public final OrdinalMap mapping;
    int currentSubIndex;
    LongValues currentGlobalOrds;
    
    /** Creates a new MultiSortedSetDocValues over <code>values</code> */
    MultiSortedSetDocValues(SortedSetDocValues values[], int docStarts[], OrdinalMap mapping) throws IOException {
      assert docStarts.length == values.length + 1;
      this.values = values;
      this.docStarts = docStarts;
      this.mapping = mapping;
    }
    
    @Override
    public long nextOrd() {
      long segmentOrd = values[currentSubIndex].nextOrd();
      if (segmentOrd == NO_MORE_ORDS) {
        return segmentOrd;
      } else {
        return currentGlobalOrds.get(segmentOrd);
      }
    }

    @Override
    public void setDocument(int docID) {
      currentSubIndex = ReaderUtil.subIndex(docID, docStarts);
      currentGlobalOrds = mapping.getGlobalOrds(currentSubIndex);
      values[currentSubIndex].setDocument(docID - docStarts[currentSubIndex]);
    }
 
    @Override
    public BytesRef lookupOrd(long ord) {
      int subIndex = mapping.getFirstSegmentNumber(ord);
      long segmentOrd = mapping.getFirstSegmentOrd(ord);
      return values[subIndex].lookupOrd(segmentOrd);
    }
 
    @Override
    public long getValueCount() {
      return mapping.getValueCount();
    }
  }
}
