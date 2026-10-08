/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import java.io.IOException;
import java.util.Arrays;

/**
 * A merge-only, forward-scan ordinal map for SORTED and SORTED_SET doc-values fields.
 *
 * <p>Compared to Lucene's full {@code OrdinalMap}, this class omits the {@code globalOrdDeltas}
 * and {@code firstSegments} packed arrays that together account for roughly 81% of the allocation
 * cost during a large merge (≈ 185 MB + 166 MB of a 430 MB peak observed in a 32-segment,
 * ~1.9e8-ordinal production merge). Those arrays exist solely to allow the merged
 * {@link TermsEnum} to jump to any global ordinal in O(1) time. During a merge every unique term
 * is always visited in strictly ascending global-ord order, so random access is never needed.
 *
 * <p>The merged {@link TermsEnum} is instead produced by re-running a priority-queue merge over
 * fresh per-segment {@code TermsEnum} iterators on each call to {@link #mergedTermsEnum}. This is
 * I/O-neutral — the same iterators are opened regardless — and costs O(log N) comparisons per
 * term rather than a packed-array lookup (N = number of segments, typically ≤ 32).
 *
 * <p><b>This class is strictly merge-time.</b> It must never be used in search-time global-ordinal
 * code paths ({@code GlobalOrdinalsBuilder}, {@code GlobalOrdinalsIndexFieldData},
 * {@code FlattenedFieldMapper}, etc.) because it does not implement
 * {@code getFirstSegmentNumber}/{@code getFirstSegmentOrd}. The naming is intentionally
 * <em>not</em> {@code XOrdinalMap}: the {@code X}-prefix convention in this package denotes
 * verbatim Lucene forks that are drop-in substitutes for their Lucene counterparts; this class is
 * a strict subset and is not substitutable.
 *
 * <p>Only wired in from the {@code MergeStats.supported()} optimised-merge path, which already
 * requires {@code needsIndexSort == true} and no live-doc bitmaps — the same pre-conditions that
 * guarantee per-segment ordinals are compact (no gaps, so the inner gap-filling loop in the
 * constructor runs exactly once per segment ordinal).
 */
final class MergeOrdinalMap {

    // -------------------------------------------------------------------------
    // SegmentMap
    //
    // Verbatim copy of org.apache.lucene.index.OrdinalMap.SegmentMap (Lucene 10.5.1).
    // Sorts segments by descending weight so the highest-weight segment's per-segment
    // ordinal deltas are most likely to be all-zero, enabling LongValues.IDENTITY.
    // -------------------------------------------------------------------------

    private static final class SegmentMap {
        final int[] newToOld;
        final int[] oldToNew;

        SegmentMap(long[] weights) {
            newToOld = buildNewToOld(weights);
            oldToNew = inverse(newToOld);
        }

        private static int[] buildNewToOld(final long[] weights) {
            final int[] result = new int[weights.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = i;
            }
            new InPlaceMergeSorter() {
                @Override
                protected void swap(int i, int j) {
                    int tmp = result[i];
                    result[i] = result[j];
                    result[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    // j first: larger weights get smaller new-indices
                    return Long.compare(weights[result[j]], weights[result[i]]);
                }
            }.sort(0, weights.length);
            return result;
        }

        private static int[] inverse(int[] map) {
            int[] inv = new int[map.length];
            for (int i = 0; i < map.length; i++) {
                inv[map[i]] = i;
            }
            return inv;
        }

        int newToOld(int newIdx) {
            return newToOld[newIdx];
        }

        int oldToNew(int origIdx) {
            return oldToNew[origIdx];
        }
    }

    // -------------------------------------------------------------------------
    // SubTermsEnum
    //
    // Adapted from org.apache.lucene.index.TermsEnumIndex (Lucene 10.5.1, package-private).
    // Cannot be re-used directly: this repo has no org/apache/lucene/** source directory
    // from which a same-package class could access the package-private original.
    // The 8-byte prefix cache is kept; it significantly speeds up comparisons between
    // enums with long shared prefixes (e.g. _tsid values).
    // -------------------------------------------------------------------------

    private static final class SubTermsEnum {
        /** The original (un-remapped) segment index in the merge. */
        final int origSegIdx;
        final TermsEnum termsEnum;
        private BytesRef currentTerm;
        private long currentPrefix8;

        SubTermsEnum(TermsEnum termsEnum, int origSegIdx) {
            this.termsEnum = termsEnum;
            this.origSegIdx = origSegIdx;
        }

        BytesRef term() {
            return currentTerm;
        }

        BytesRef next() throws IOException {
            currentTerm = termsEnum.next();
            currentPrefix8 = (currentTerm == null) ? 0L : prefix8(currentTerm);
            return currentTerm;
        }

        int compareTermTo(SubTermsEnum other) {
            if (currentPrefix8 != other.currentPrefix8) {
                return Long.compareUnsigned(currentPrefix8, other.currentPrefix8);
            }
            return Arrays.compareUnsigned(
                currentTerm.bytes,
                currentTerm.offset,
                currentTerm.offset + currentTerm.length,
                other.currentTerm.bytes,
                other.currentTerm.offset,
                other.currentTerm.offset + other.currentTerm.length
            );
        }

        void copyStateTo(TermSnap snap) {
            snap.term.copyBytes(currentTerm);
            snap.prefix8 = currentPrefix8;
        }

        boolean termEquals(TermSnap snap) {
            if (currentPrefix8 != snap.prefix8) {
                return false;
            }
            return Arrays.equals(
                currentTerm.bytes,
                currentTerm.offset,
                currentTerm.offset + currentTerm.length,
                snap.term.bytes(),
                0,
                snap.term.length()
            );
        }

        /** Lightweight snapshot of the current term to avoid heap allocation in the hot loop. */
        static final class TermSnap {
            final BytesRefBuilder term = new BytesRefBuilder();
            long prefix8;
        }

        private static long prefix8(BytesRef t) {
            if (t.length >= Long.BYTES) {
                return (long) BitUtil.VH_BE_LONG.get(t.bytes, t.offset);
            }
            long l;
            int o;
            if (Integer.BYTES <= t.length) {
                l = (int) BitUtil.VH_BE_INT.get(t.bytes, t.offset);
                o = Integer.BYTES;
            } else {
                l = 0;
                o = 0;
            }
            if (o + Short.BYTES <= t.length) {
                l = (l << Short.SIZE) | Short.toUnsignedLong((short) BitUtil.VH_BE_SHORT.get(t.bytes, t.offset + o));
                o += Short.BYTES;
            }
            if (o < t.length) {
                l = (l << Byte.SIZE) | Byte.toUnsignedLong(t.bytes[t.offset + o]);
            }
            l <<= (Long.BYTES - t.length) << 3;
            return l;
        }
    }

    // -------------------------------------------------------------------------
    // Priority queue
    // -------------------------------------------------------------------------

    private static final class SubTermsEnumPQ extends PriorityQueue<SubTermsEnum> {
        SubTermsEnumPQ(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(SubTermsEnum a, SubTermsEnum b) {
            return a.compareTermTo(b) < 0;
        }
    }

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    private final long valueCount;
    /**
     * Indexed by <em>new</em> (weight-sorted) segment index. Use
     * {@link #getGlobalOrds(int)} with the original segment index.
     */
    private final LongValues[] segmentToGlobalOrds;
    private final SegmentMap segmentMap;
    private final int numSubs;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Builds a merge ordinal map from the provided sub-enumerators.
     *
     * <p>This is the equivalent of {@code OrdinalMap.build(null, subs, weights, PackedInts.COMPACT)}
     * with the {@code globalOrdDeltas} and {@code firstSegments} builders removed. The
     * {@code segmentToGlobalOrds} arrays are built identically to the original.
     *
     * @param subs    one {@link TermsEnum} per segment, in <em>original</em> segment order;
     *                each must support {@link TermsEnum#ord()}
     * @param weights number of unique values in each sub (used to order segments by weight)
     * @throws IOException if any {@link TermsEnum#next()} call throws
     */
    MergeOrdinalMap(TermsEnum[] subs, long[] weights) throws IOException {
        if (subs.length != weights.length) {
            throw new IllegalArgumentException("subs and weights must have the same length");
        }
        numSubs = subs.length;
        segmentMap = new SegmentMap(weights);

        // One builder per weight-sorted segment index (same indexing as segmentToGlobalOrds)
        final PackedLongValues.Builder[] deltaBuilders = new PackedLongValues.Builder[numSubs];
        for (int i = 0; i < numSubs; i++) {
            deltaBuilders[i] = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        }
        final long[] ordDeltaBits = new long[numSubs];
        final long[] segmentOrds = new long[numSubs];

        // Populate the PQ in weight-sorted (newToOld) order — same as OrdinalMap
        final SubTermsEnumPQ queue = new SubTermsEnumPQ(numSubs);
        for (int newIdx = 0; newIdx < numSubs; newIdx++) {
            int origIdx = segmentMap.newToOld(newIdx);
            SubTermsEnum sub = new SubTermsEnum(subs[origIdx], origIdx);
            if (sub.next() != null) {
                queue.add(sub);
            }
        }

        final SubTermsEnum.TermSnap topSnap = new SubTermsEnum.TermSnap();
        long globalOrd = 0;

        while (queue.size() != 0) {
            SubTermsEnum top = queue.top();
            top.copyStateTo(topSnap);

            // Advance past all sub-enums sharing the same term, recording per-segment deltas
            while (true) {
                long segmentOrd = top.termsEnum.ord();
                long delta = globalOrd - segmentOrd;
                int segNewIdx = segmentMap.oldToNew(top.origSegIdx);
                ordDeltaBits[segNewIdx] |= delta;

                assert segmentOrds[segNewIdx] <= segmentOrd;
                // Gap-filling inner loop: mirrors OrdinalMap exactly. On the deletion-free
                // optimised-merge path the loop body runs exactly once per iteration.
                do {
                    deltaBuilders[segNewIdx].add(delta);
                    segmentOrds[segNewIdx]++;
                } while (segmentOrds[segNewIdx] <= segmentOrd);

                if (top.next() == null) {
                    queue.pop();
                    if (queue.size() == 0) {
                        break;
                    }
                    top = queue.top();
                } else {
                    top = queue.updateTop();
                }
                if (top.termEquals(topSnap) == false) {
                    break;
                }
            }
            globalOrd++;
        }

        valueCount = globalOrd;

        // Build segmentToGlobalOrds — identical logic to OrdinalMap (PackedInts.COMPACT overhead)
        segmentToGlobalOrds = new LongValues[numSubs];
        for (int i = 0; i < numSubs; i++) {
            final PackedLongValues deltas = deltaBuilders[i].build();
            if (ordDeltaBits[i] == 0L) {
                // Segment ords == global ords for this segment (typically the heaviest segment)
                segmentToGlobalOrds[i] = LongValues.IDENTITY;
            } else {
                final int bitsRequired = ordDeltaBits[i] < 0 ? 64 : PackedInts.bitsRequired(ordDeltaBits[i]);
                final long monotonicBits = deltas.ramBytesUsed() * 8;
                final long packedBits = bitsRequired * deltas.size();
                // Use plain packed ints if no larger than monotonic encoding (COMPACT ratio = 0)
                if (deltas.size() <= Integer.MAX_VALUE && packedBits <= monotonicBits) {
                    final int size = (int) deltas.size();
                    final PackedInts.Mutable packed = PackedInts.getMutable(size, bitsRequired, PackedInts.COMPACT);
                    final PackedLongValues.Iterator it = deltas.iterator();
                    for (int ord = 0; ord < size; ord++) {
                        packed.set(ord, it.next());
                    }
                    assert it.hasNext() == false;
                    segmentToGlobalOrds[i] = new LongValues() {
                        @Override
                        public long get(long ord) {
                            return ord + packed.get((int) ord);
                        }
                    };
                } else {
                    segmentToGlobalOrds[i] = new LongValues() {
                        @Override
                        public long get(long ord) {
                            return ord + deltas.get(ord);
                        }
                    };
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // API
    // -------------------------------------------------------------------------

    /**
     * Total number of unique terms across all segments (= the global ordinal space size).
     */
    long getValueCount() {
        return valueCount;
    }

    /**
     * Returns a {@link LongValues} that maps per-segment ordinals to global ordinals for the
     * segment at {@code originalSegmentIndex} (0-based, in the original merge order).
     */
    LongValues getGlobalOrds(int originalSegmentIndex) {
        return segmentToGlobalOrds[segmentMap.oldToNew(originalSegmentIndex)];
    }

    /**
     * Returns a forward-only {@link TermsEnum} that yields all unique terms in ascending
     * global-ordinal order by re-running a priority-queue merge over the provided sub-enumerators.
     *
     * <p>Each call produces an independent iterator: callers may invoke this method multiple times
     * (e.g. for the terms-dict pass and the reverse-index pass in
     * {@link AbstractTSDBDocValuesConsumer}) and each returned iterator advances independently.
     *
     * <p>The returned enum supports:
     * <ul>
     *   <li>{@link TermsEnum#next()} — advance to the next unique term</li>
     *   <li>{@link TermsEnum#term()} — the current term</li>
     *   <li>{@link TermsEnum#ord()} — the current global ordinal (0-based)</li>
     *   <li>{@link TermsEnum#seekExact(long)} — forward-only; throws
     *       {@link UnsupportedOperationException} for backward seeks</li>
     * </ul>
     * All other operations throw {@link UnsupportedOperationException} so that a stray binary
     * search cannot silently degrade to O(G · log G) complexity.
     *
     * @param freshSubs one fresh {@link TermsEnum} per segment, in <em>original</em> segment order
     * @throws IOException if any initial {@link TermsEnum#next()} call throws during PQ setup
     */
    TermsEnum mergedTermsEnum(TermsEnum[] freshSubs) throws IOException {
        if (freshSubs.length != numSubs) {
            throw new IllegalArgumentException("freshSubs.length=" + freshSubs.length + " != numSubs=" + numSubs);
        }

        // Populate the PQ in the same weight-sorted order as the constructor so that global ord
        // assignment is deterministic and matches segmentToGlobalOrds
        final SubTermsEnumPQ queue = new SubTermsEnumPQ(numSubs);
        for (int newIdx = 0; newIdx < numSubs; newIdx++) {
            int origIdx = segmentMap.newToOld(newIdx);
            SubTermsEnum sub = new SubTermsEnum(freshSubs[origIdx], origIdx);
            if (sub.next() != null) {
                queue.add(sub);
            }
        }

        return new BaseTermsEnum() {
            private long currentOrd = -1;
            private BytesRef currentTerm;
            private final SubTermsEnum.TermSnap topSnap = new SubTermsEnum.TermSnap();
            private final BytesRefBuilder termCopy = new BytesRefBuilder();

            @Override
            public BytesRef next() throws IOException {
                if (queue.size() == 0) {
                    currentTerm = null;
                    return null;
                }
                SubTermsEnum top = queue.top();
                top.copyStateTo(topSnap);
                // Save a copy of the term before the PQ advances past it
                termCopy.copyBytes(top.term());
                currentTerm = termCopy.get();
                currentOrd++;

                // Drain all sub-enums sharing this term from the PQ
                while (true) {
                    if (top.next() == null) {
                        queue.pop();
                        if (queue.size() == 0) {
                            break;
                        }
                        top = queue.top();
                    } else {
                        top = queue.updateTop();
                    }
                    if (top.termEquals(topSnap) == false) {
                        break;
                    }
                }
                return currentTerm;
            }

            @Override
            public BytesRef term() {
                return currentTerm;
            }

            @Override
            public long ord() {
                return currentOrd;
            }

            @Override
            public void seekExact(long targetOrd) throws IOException {
                if (targetOrd < currentOrd) {
                    throw new UnsupportedOperationException("seekExact backward: targetOrd=" + targetOrd + " < currentOrd=" + currentOrd);
                }
                while (currentOrd < targetOrd) {
                    if (next() == null) {
                        throw new IOException("seekExact past end of enum: targetOrd=" + targetOrd + " valueCount=" + valueCount);
                    }
                }
            }

            @Override
            public TermsEnum.SeekStatus seekCeil(BytesRef text) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int docFreq() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long totalTermFreq() {
                throw new UnsupportedOperationException();
            }

            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ImpactsEnum impacts(int flags) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TermState termState() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
