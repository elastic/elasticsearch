/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;

/**
 * A {@link SingleDimensionValuesSource} for keyword/ordinal fields that orders composite buckets by {@code _key} using
 * per-segment ordinals. Because a composite source is always sorted by value and the queue only keeps the top
 * {@code size} buckets, within a segment we compare on the cheap <b>segment</b> ordinal, and at every segment boundary we
 * remap the (at most {@code size}) queue slots into the new segment's ordinal space by re-seeking their stored bytes.
 * Memory is therefore O(size), not O(distinct).
 * <p>
 * Slot encoding (per segment): a term present at ordinal {@code o} is encoded as {@code 2*o} (even); a term that is
 * absent from the segment sits at an insertion point {@code ip} and is encoded as {@code 2*ip - 1} (odd). Opposite
 * parity guarantees a real document value (always even) never compares equal to an absent slot, so the per-document hot
 * path stays a pure {@code long} compare. The only ambiguity is two <i>distinct</i> absent terms sharing an insertion
 * point; that is resolved by a bytes comparison in {@link #compare(int, int)} (slot-vs-slot only, used by heap
 * maintenance and {@code equals}), never on the per-document path.
 * <p>
 * When the field is indexed, collection dynamically prunes: once the queue is full, a {@link DocIdSetIterator} built from
 * the inverted index visits only documents whose ordinal falls in the still-competitive range.
 */
class SegmentOrdinalValuesSource extends SingleDimensionValuesSource<BytesRef> {

    static final long MISSING = Long.MIN_VALUE;

    // Only prune when the competitive ordinal range is narrow enough to enumerate into a postings disjunction.
    static final int MAX_TERMS_FOR_DYNAMIC_PRUNING = 128;

    private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;
    private final LongConsumer breakerConsumer;
    private LongArray values;                            // per-slot encoded ordinal in the current segment
    private ObjectArray<BytesRefBuilder> valueBuilders;  // per-slot canonical bytes, stable across segments
    private SortedSetDocValues lookup;                   // current segment doc values
    private long currentValue;                           // encoded ordinal of the current document (even = real ord)
    private long afterEncoded = MISSING;                 // encoded after value for the current segment

    // Dynamic pruning: once the composite queue is full we know the largest competitive key, so we can skip documents
    // whose value is out of range using the inverted index.
    private BytesRef highestCompetitiveValue;
    private SegmentCompetitiveIterator currentCompetitiveIterator;

    // Profiling counters: segments visited with a competitive iterator, and those where pruning actually engaged.
    private int totalSegments;
    private int segmentsDynamicPruningUsed;

    // Scratch for the boundary remap: slot indices to re-resolve, sorted by their bytes so we can walk the segment's
    // TermsEnum forward once (sequential block decode) instead of doing `size` independent binary-search lookupTerms.
    private int[] remapOrder = new int[0];
    private final IntroSorter remapSorter = new IntroSorter() {
        private BytesRef pivot;

        @Override
        protected void setPivot(int i) {
            pivot = valueBuilders.get(remapOrder[i]).get();
        }

        @Override
        protected int comparePivot(int j) {
            return pivot.compareTo(valueBuilders.get(remapOrder[j]).get());
        }

        @Override
        protected int compare(int i, int j) {
            return valueBuilders.get(remapOrder[i]).get().compareTo(valueBuilders.get(remapOrder[j]).get());
        }

        @Override
        protected void swap(int i, int j) {
            int tmp = remapOrder[i];
            remapOrder[i] = remapOrder[j];
            remapOrder[j] = tmp;
        }
    };

    SegmentOrdinalValuesSource(
        BigArrays bigArrays,
        LongConsumer breakerConsumer,
        MappedFieldType type,
        CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, type, missingBucket, missingOrder, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.breakerConsumer = breakerConsumer;
        this.values = bigArrays.newLongArray(Math.min(size, 100), false);
        boolean success = false;
        try {
            this.valueBuilders = bigArrays.newObjectArray(Math.min(size, 100));
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    boolean requiresMapRebuildPerSegment() {
        return true;
    }

    private static long encodeOrd(long ord) {
        return ord << 1;
    }

    /** Encode {@code term}'s position in the current segment: {@code 2*ord} if present, else {@code 2*ip - 1}. */
    private long encode(BytesRef term) throws IOException {
        long ord = lookup.lookupTerm(term);
        return ord >= 0 ? (ord << 1) : (((-ord - 1) << 1) - 1);
    }

    private int compareEncoded(long lhs, long rhs) {
        int mul = (lhs == MISSING || rhs == MISSING) ? missingOrder.compareAnyValueToMissing(reverseMul) : reverseMul;
        return Long.compare(lhs, rhs) * mul;
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot + 1);
        valueBuilders = bigArrays.grow(valueBuilders, slot + 1);
        values.set(slot, currentValue);
        if (missingBucket && currentValue == MISSING) {
            // Clear any stale builder left by a previous (evicted) value in this reused slot, so remapSlots skips this
            // MISSING slot instead of re-encoding it from those stale bytes. toComparable returns null for MISSING.
            valueBuilders.set(slot, null);
            return;
        }
        BytesRefBuilder builder = valueBuilders.get(slot);
        int byteSize = builder == null ? 0 : builder.bytes().length;
        if (builder == null) {
            builder = new BytesRefBuilder();
            valueBuilders.set(slot, builder);
        }
        try {
            builder.copyBytes(lookup.lookupOrd(currentValue >> 1));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        breakerConsumer.accept(builder.bytes().length - byteSize);
    }

    @Override
    int compare(int from, int to) {
        long a = values.get(from);
        long b = values.get(to);
        int cmp = compareEncoded(a, b);
        if (cmp == 0 && a == b && a != MISSING && (a & 1L) == 1L) {
            // Two distinct absent terms collided on the same insertion point; disambiguate by their bytes.
            return valueBuilders.get(from).get().compareTo(valueBuilders.get(to).get()) * reverseMul;
        }
        return cmp;
    }

    @Override
    int compareCurrent(int slot) {
        // currentValue is always a real (even) ordinal of the current segment, so equal encodings mean equal values.
        return compareEncoded(currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        return compareEncoded(currentValue, afterEncoded);
    }

    @Override
    int hashCode(int slot) {
        return Long.hashCode(values.get(slot));
    }

    @Override
    int hashCodeCurrent() {
        return Long.hashCode(currentValue);
    }

    @Override
    void setAfter(Comparable<?> value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else if (value.getClass() == String.class) {
            afterValue = format.parseBytesRef(value);
        } else if (value.getClass() == BytesRef.class) {
            afterValue = (BytesRef) value;
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) {
        long v = values.get(slot);
        if (missingBucket && v == MISSING) {
            return null;
        }
        return valueBuilders.get(slot).get();
    }

    /**
     * Re-encode every populated slot into this segment's ordinal space. Bounded to {@code size} slots, resolved by a
     * single forward {@link TermsEnum} walk in sorted order (sequential, cache-friendly) rather than one random
     * binary-search {@code lookupTerm} per slot.
     */
    private void remapSlots() throws IOException {
        final int n = (int) valueBuilders.size();
        if (remapOrder.length < n) {
            remapOrder = new int[n];
        }
        int count = 0;
        for (int slot = 0; slot < n; slot++) {
            if (valueBuilders.get(slot) != null) { // skip unused slots and MISSING slots (values already holds MISSING)
                remapOrder[count++] = slot;
            }
        }
        if (count == 0) {
            return;
        }
        remapSorter.sort(0, count);
        final TermsEnum termsEnum = lookup.termsEnum();
        final long valueCount = lookup.getValueCount();
        for (int i = 0; i < count; i++) {
            final int slot = remapOrder[i];
            final TermsEnum.SeekStatus status = termsEnum.seekCeil(valueBuilders.get(slot).get());
            final long encoded;
            if (status == TermsEnum.SeekStatus.FOUND) {
                encoded = termsEnum.ord() << 1;
            } else if (status == TermsEnum.SeekStatus.END) {
                encoded = (valueCount << 1) - 1; // sorts after the last term in this segment
            } else { // NOT_FOUND: positioned at the next term, whose ordinal is the insertion point
                encoded = (termsEnum.ord() << 1) - 1;
            }
            values.set(slot, encoded);
        }
    }

    /**
     * Whether this source can dynamically prune (ascending only, indexed field with terms and doc values, no missing
     * bucket). The {@link CompositeValuesCollectorQueue} consults this to decide whether to feed back competitive bounds.
     */
    boolean mayDynamicallyPrune() {
        return missingBucket == false && fieldType != null && fieldType.indexType().hasTerms() && fieldType.hasDocValues();
    }

    /**
     * Called by the queue when the queue is full and the largest competitive key may have changed. Records that key's
     * bytes and asks the current segment's iterator to tighten its bounds.
     */
    void updateHighestCompetitiveValue(int slot) throws IOException {
        final BytesRefBuilder builder = valueBuilders.get(slot);
        if (builder == null) {
            return;
        }
        highestCompetitiveValue = BytesRef.deepCopyOf(builder.get());
        if (currentCompetitiveIterator != null) {
            currentCompetitiveIterator.updateBounds();
        }
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        lookup = docValuesFunc.apply(context);
        remapSlots();
        afterEncoded = afterValue != null ? encode(afterValue) : MISSING;
        totalSegments++;
        final SegmentCompetitiveIterator competitiveIterator = mayDynamicallyPrune() ? new SegmentCompetitiveIterator(context) : null;
        currentCompetitiveIterator = competitiveIterator;
        final SortedDocValues singleton = DocValues.unwrapSingleton(lookup);
        if (singleton != null) {
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (singleton.advanceExact(doc)) {
                        currentValue = encodeOrd(singleton.ordValue());
                        next.collect(doc, bucket);
                    } else if (missingBucket) {
                        currentValue = MISSING;
                        next.collect(doc, bucket);
                    }
                }

                @Override
                public DocIdSetIterator competitiveIterator() {
                    return competitiveIterator;
                }
            };
        }
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (lookup.advanceExact(doc)) {
                    for (int i = 0; i < lookup.docValueCount(); i++) {
                        currentValue = encodeOrd(lookup.nextOrd());
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValue = MISSING;
                    next.collect(doc, bucket);
                }
            }

            @Override
            public DocIdSetIterator competitiveIterator() {
                return competitiveIterator;
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable<BytesRef> value, LeafReaderContext context, LeafBucketCollector next) {
        throw new UnsupportedOperationException("segment ordinals composite source does not support a forced lead value");
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        // This source compares on per-segment ordinals and does not support being driven by a forced lead value.
        return null;
    }

    void collectDebugInfo(String namespace, BiConsumer<String, Object> add) {
        add.accept(Strings.format("%s.segments_collected", namespace), totalSegments);
        add.accept(Strings.format("%s.segments_dynamic_pruning_used", namespace), segmentsDynamicPruningUsed);
    }

    /**
     * A competitive iterator (ascending) that, once the queue is full, restricts collection to documents whose value
     * falls in the competitive ordinal range {@code (afterValue, highestCompetitiveValue]}. When that range spans at most
     * {@link #MAX_TERMS_FOR_DYNAMIC_PRUNING} terms in the current segment, it pulls their inverted-index postings into a
     * disjunction so the search skips non-competitive documents; otherwise it passes documents through unchanged.
     */
    private class SegmentCompetitiveIterator extends DocIdSetIterator {
        private final LeafReaderContext context;
        private final int maxDoc;
        private int doc = -1;
        private boolean exhausted = false;       // no competitive documents remain in this segment
        private PriorityQueue<PostingsEnum> disjunction; // null => no pruning yet (pass through)
        private boolean pruningCounted = false;  // whether this segment has been counted as having pruned

        SegmentCompetitiveIterator(LeafReaderContext context) {
            this.context = context;
            this.maxDoc = context.reader().maxDoc();
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            if (exhausted) {
                return doc = NO_MORE_DOCS;
            }
            if (disjunction == null) {
                return doc = target >= maxDoc ? NO_MORE_DOCS : target; // not pruning yet
            }
            PostingsEnum top = disjunction.top();
            if (top == null) {
                return doc = NO_MORE_DOCS;
            }
            while (top.docID() < target) {
                top.advance(target);
                top = disjunction.updateTop();
            }
            return doc = top.docID();
        }

        @Override
        public long cost() {
            return maxDoc;
        }

        /**
         * Recompute the competitive ordinal range from the current after/highest-competitive values and re-arm. The
         * competitive window is {@code (after, highestCompetitive]} ascending and {@code [highestCompetitive, after)}
         * descending; {@code highestCompetitiveValue} is the queue's worst key (largest ascending, smallest descending).
         */
        void updateBounds() throws IOException {
            if (highestCompetitiveValue == null) {
                return;
            }
            final long competitive = lookup.lookupTerm(highestCompetitiveValue);
            final long minOrd;
            final long maxOrd;
            if (reverseMul == 1) {
                // ascending: largest ordinal <= highestCompetitive, down to the ordinal of `after` (inclusive: with
                // additional sources the `after` leading value may still have uncollected buckets).
                maxOrd = competitive >= 0 ? competitive : (-competitive - 1) - 1;
                if (afterValue != null) {
                    final long afterTerm = lookup.lookupTerm(afterValue);
                    minOrd = afterTerm >= 0 ? afterTerm : (-afterTerm - 1);
                } else {
                    minOrd = 0;
                }
            } else {
                // descending: smallest ordinal >= highestCompetitive, up to the ordinal of `after` (inclusive).
                minOrd = competitive >= 0 ? competitive : (-competitive - 1);
                if (afterValue != null) {
                    final long afterTerm = lookup.lookupTerm(afterValue);
                    maxOrd = afterTerm >= 0 ? afterTerm : (-afterTerm - 1) - 1;
                } else {
                    maxOrd = lookup.getValueCount() - 1;
                }
            }
            update(minOrd, maxOrd);
        }

        private void update(long minOrd, long maxOrd) throws IOException {
            if (minOrd > maxOrd) {
                exhausted = true; // the competitive window is empty in this segment
                return;
            }
            if (maxOrd - minOrd + 1 > MAX_TERMS_FOR_DYNAMIC_PRUNING) {
                disjunction = null; // too wide to enumerate; fall back to a full pass
                return;
            }
            final Terms terms = context.reader().terms(fieldType.name());
            if (terms == null) {
                disjunction = null;
                return;
            }
            final BytesRef minTerm = BytesRef.deepCopyOf(lookup.lookupOrd(minOrd));
            final BytesRef maxTerm = BytesRef.deepCopyOf(lookup.lookupOrd(maxOrd));
            final TermsEnum termsEnum = terms.iterator();
            if (termsEnum.seekCeil(minTerm) == TermsEnum.SeekStatus.END) {
                exhausted = true;
                return;
            }
            final int capacity = (int) (maxOrd - minOrd + 1);
            final PriorityQueue<PostingsEnum> pq = new PriorityQueue<>(capacity) {
                @Override
                protected boolean lessThan(PostingsEnum a, PostingsEnum b) {
                    return a.docID() < b.docID();
                }
            };
            for (BytesRef term = termsEnum.term(); term != null && term.compareTo(maxTerm) <= 0; term = termsEnum.next()) {
                final PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
                postings.nextDoc(); // position on the first matching document so the queue orders correctly
                pq.add(postings);
            }
            disjunction = pq;
            if (pruningCounted == false) {
                segmentsDynamicPruningUsed++;
                pruningCounted = true;
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(values, valueBuilders);
    }
}
