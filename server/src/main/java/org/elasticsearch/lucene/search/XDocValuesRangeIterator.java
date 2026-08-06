/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SkipBlockRangeIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.Version;

import java.io.IOException;

/**
 * Fork of Lucene 10.5.0's {@code DocValuesRangeIterator}, fixing {@code docIDRunEnd()} to return
 * the current doc ID (an empty run) for {@code MAYBE} and {@code YES_IF_PRESENT} blocks, where the
 * current doc is only an approximation candidate and not a confirmed match.
 *
 * <p>The upstream fix is in apache/lucene#16450, which will land in Lucene 10.5.1. Delete this
 * class and revert callers to Lucene's {@code DocValuesRangeIterator} once Elasticsearch upgrades
 * to a Lucene release containing that fix.
 */
public abstract sealed class XDocValuesRangeIterator extends TwoPhaseIterator {

    static {
        assert Version.LUCENE_10_5_0.onOrAfter(Version.LATEST) : "Remove this class as fix is part of 10.5.1 and later";
    }

    /**
     * Creates an XDocValuesRangeIterator over a NumericDocValues instance
     *
     * @param values  the doc values
     * @param skipper an optional skipper to exclude non-matching blocks
     * @param min     skip documents with values lower than this
     * @param max     skip documents with values higher than this
     */
    public static XDocValuesRangeIterator forRange(NumericDocValues values, DocValuesSkipper skipper, long min, long max) {
        IOBooleanSupplier check = () -> {
            long v = values.longValue();
            return v >= min && v <= max;
        };
        return skipper == null
            ? new DocValuesValueRangeIterator(values, check, 2)
            : new BulkNumericRangeIterator(values, new SkipBlockRangeIterator(skipper, min, max), check, 2, min, max);
    }

    /**
     * Creates an XDocValuesRangeIterator over a SortedNumericDocValues instance
     *
     * @param values  the doc values
     * @param skipper an optional skipper to exclude non-matching blocks
     * @param min     skip documents with all values lower than this
     * @param max     skip documents with all values higher than this
     */
    public static XDocValuesRangeIterator forRange(SortedNumericDocValues values, DocValuesSkipper skipper, long min, long max) {
        IOBooleanSupplier check = () -> {
            for (int i = 0; i < values.docValueCount(); i++) {
                long v = values.nextValue();
                if (v >= min) {
                    return v <= max;
                }
            }
            return false;
        };
        return skipper == null
            ? new DocValuesValueRangeIterator(values, check, 5)
            : new BulkSortedNumericRangeIterator(values, new SkipBlockRangeIterator(skipper, min, max), check, 2, min, max);
    }

    /**
     * Creates an XDocValuesRangeIterator over a SortedDocValues instance
     *
     * @param values  the doc values
     * @param skipper an optional skipper to exclude non-matching blocks
     * @param min     skip documents with ordinal values lower than this
     * @param max     skip documents with ordinal values higher than this
     */
    public static XDocValuesRangeIterator forOrdinalRange(SortedDocValues values, DocValuesSkipper skipper, long min, long max) {
        IOBooleanSupplier check = () -> {
            long ord = values.ordValue();
            return ord >= min && ord <= max;
        };
        return skipper == null
            ? new DocValuesValueRangeIterator(values, check, 2)
            : new BulkOrdinalRangeIterator(values, new SkipBlockRangeIterator(skipper, min, max), check, 2);
    }

    /**
     * Creates an XDocValuesRangeIterator over a SortedSetDocValues instance
     *
     * @param values  the doc values
     * @param skipper an optional skipper to exclude non-matching blocks
     * @param min     skip documents with all ordinal values lower than this
     * @param max     skip documents with all ordinal values higher than this
     */
    public static XDocValuesRangeIterator forOrdinalRange(SortedSetDocValues values, DocValuesSkipper skipper, long min, long max) {
        IOBooleanSupplier check = () -> {
            for (int i = 0; i < values.docValueCount(); i++) {
                long v = values.nextOrd();
                if (v >= min) {
                    return v <= max;
                }
            }
            return false;
        };
        return skipper == null
            ? new DocValuesValueRangeIterator(values, check, 5)
            : new BulkOrdinalRangeIterator(values, new SkipBlockRangeIterator(skipper, min, max), check, 5);
    }

    /**
     * @param contiguous true iff every ordinal in [min, max] is set, in which case the set is
     *                   equivalent to the contiguous range [min, max] and a cheaper range check can be used in
     *                   place of the per-doc bit lookup.
     */
    private record OrdinalSet(long min, long max, LongBitSet ords, boolean contiguous) {

        boolean disjoint(DocValuesSkipper skipper) {
            if (skipper == null) {
                return false;
            }
            return min > skipper.maxValue() || max < skipper.minValue();
        }
    }

    private static OrdinalSet buildOrdinalSet(TermsEnum termsEnum, long ordCount) throws IOException {
        if (termsEnum.next() == null) {
            return null;
        }
        LongBitSet ords = new LongBitSet(ordCount);
        long min = termsEnum.ord();
        ords.set(min);
        long max = min;
        long distinctCount = 1;
        while (termsEnum.next() != null) {
            max = termsEnum.ord();
            if (ords.getAndSet(max) == false) {
                distinctCount++;
            }
        }
        return new OrdinalSet(min, max, ords, distinctCount == max - min + 1);
    }

    /**
     * Creates an XDocValuesRangeIterator over a SortedDocValues instance
     *
     * @param values  the doc values
     * @param skipper an optional skipper to exclude non-matching blocks
     * @param terms   a TermsEnum containing the ordinal values to match
     */
    public static XDocValuesRangeIterator forOrdinalSet(SortedDocValues values, DocValuesSkipper skipper, TermsEnum terms)
        throws IOException {
        OrdinalSet ordinalSet = buildOrdinalSet(terms, values.getValueCount());
        if (ordinalSet == null || ordinalSet.disjoint(skipper)) {
            return new EmptyRangeIterator();
        }
        if (ordinalSet.contiguous) {
            return forOrdinalRange(values, skipper, ordinalSet.min, ordinalSet.max);
        }
        IOBooleanSupplier check = () -> ordinalSet.ords.get(values.ordValue());
        return skipper == null
            ? new DocValuesValueRangeIterator(values, check, 2)
            : new DocValuesBlockRangeIterator(values, new SkipBlockRangeIterator(skipper, ordinalSet.min, ordinalSet.max), check, 2);
    }

    /**
     * Creates an XDocValuesRangeIterator over a SortedSetDocValues instance
     *
     * @param values  the doc values
     * @param skipper an optional skipper to exclude non-matching blocks
     * @param terms   a TermsEnum containing the ordinal values to match
     */
    public static XDocValuesRangeIterator forOrdinalSet(SortedSetDocValues values, DocValuesSkipper skipper, TermsEnum terms)
        throws IOException {
        OrdinalSet ordinalSet = buildOrdinalSet(terms, values.getValueCount());
        return forOrdinalSet(values, skipper, ordinalSet);
    }

    /**
     * Creates an XDocValuesRangeIterator over a SortedSetDocValues instance
     *
     * @param values  the doc values
     * @param skipper an optional skipper to exclude non-matching blocks
     * @param minOrd  skip documents with all ordinal values lower than this
     * @param maxOrd  skip documents with all ordinal values higher than this
     * @param ords    skip documents with all values not in this set
     */
    public static XDocValuesRangeIterator forOrdinalSet(
        SortedSetDocValues values,
        DocValuesSkipper skipper,
        long minOrd,
        long maxOrd,
        LongBitSet ords
    ) {
        return forOrdinalSet(values, skipper, new OrdinalSet(minOrd, maxOrd, ords, false));
    }

    private static XDocValuesRangeIterator forOrdinalSet(SortedSetDocValues values, DocValuesSkipper skipper, OrdinalSet ordinalSet) {
        if (ordinalSet == null || ordinalSet.disjoint(skipper)) {
            return new EmptyRangeIterator();
        }
        if (ordinalSet.contiguous) {
            return forOrdinalRange(values, skipper, ordinalSet.min, ordinalSet.max);
        }
        IOBooleanSupplier check = () -> {
            for (int i = 0; i < values.docValueCount(); i++) {
                long v = values.nextOrd();
                if (v > ordinalSet.max) {
                    return false;
                }
                if (v >= ordinalSet.min && ordinalSet.ords.get(v)) {
                    return true;
                }
            }
            return false;
        };
        return skipper == null
            ? new DocValuesValueRangeIterator(values, check, 5)
            : new DocValuesBlockRangeIterator(values, new SkipBlockRangeIterator(skipper, ordinalSet.min, ordinalSet.max), check, 5);
    }

    /**
     * Skip-indexed range iterator that confirms every candidate one doc at a time. Used for arbitrary
     * ordinal sets, where even a YES block may contain docs whose ordinals fall in the gaps of the
     * set.
     *
     * <p>The block-aware bulk variants extend this class and override {@link #matches()},
     * {@link #docIDRunEnd()} and {@link #intoBitSet} to exploit the block classification.
     */
    private static sealed class DocValuesBlockRangeIterator extends XDocValuesRangeIterator {

        final SkipBlockRangeIterator blockIterator;
        final DocIdSetIterator disi;
        final IOBooleanSupplier predicate;
        private final float matchCost;

        private DocValuesBlockRangeIterator(
            DocIdSetIterator disi,
            SkipBlockRangeIterator blockIterator,
            IOBooleanSupplier predicate,
            float matchCost
        ) {
            super(blockIterator);
            this.disi = disi;
            this.blockIterator = blockIterator;
            this.predicate = predicate;
            this.matchCost = matchCost;
        }

        final boolean advanceDisi(int target) throws IOException {
            if (disi.docID() >= target) {
                return disi.docID() == target;
            }
            return disi.advance(target) == target;
        }

        @Override
        public boolean matches() throws IOException {
            return advanceDisi(blockIterator.docID()) && predicate.get();
        }

        // Note: docIDRunEnd() intentionally inherits TwoPhaseIterator.docIDRunEnd(), which returns
        // approximation().docID() — an empty run. This fixes the bug in Lucene 10.5.0 where the
        // override returned blockIterator.docID() + 1, incorrectly claiming the unconfirmed
        // approximation doc was a match. (apache/lucene#16450)

        @Override
        public final float matchCost() {
            return matchCost;
        }
    }

    /**
     * Base class for the block-aware bulk variants: YES runs are set in one shot, YES_IF_PRESENT
     * runs are marked by presence, and MAYBE runs are confirmed per doc.
     */
    private abstract static sealed class BulkBlockRangeIterator extends DocValuesBlockRangeIterator {

        private BulkBlockRangeIterator(
            DocIdSetIterator disi,
            SkipBlockRangeIterator blockIterator,
            IOBooleanSupplier predicate,
            float matchCost
        ) {
            super(disi, blockIterator, predicate, matchCost);
        }

        @Override
        public final boolean matches() throws IOException {
            return switch (blockIterator.getMatch()) {
                case YES -> true;
                case YES_IF_PRESENT -> advanceDisi(blockIterator.docID());
                case MAYBE -> advanceDisi(blockIterator.docID()) && predicate.get();
            };
        }

        @Override
        public final int docIDRunEnd() throws IOException {
            // docIDRunEnd() may be called on non-matches, so only YES proves that the current doc and
            // the rest of the run are actual matches. (apache/lucene#16450 fix)
            return switch (blockIterator.getMatch()) {
                case YES -> blockIterator.docIDRunEnd();
                case YES_IF_PRESENT, MAYBE -> blockIterator.docID();
            };
        }

        @Override
        public final void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            while (blockIterator.docID() < upTo) {
                int blockStart = blockIterator.docID();
                SkipBlockRangeIterator.Match match = blockIterator.getMatch();
                // For MAYBE blocks docIDRunEnd() is conservative (doc+1 in Lucene 10.5.0's
                // SkipBlockRangeIterator), so use the full block boundary to evaluate the whole block.
                int blockEnd = match == SkipBlockRangeIterator.Match.MAYBE
                    ? Math.min(upTo, blockIterator.blockEnd())
                    : Math.min(upTo, blockIterator.docIDRunEnd());
                switch (match) {
                    case YES -> bitSet.set(blockStart - offset, blockEnd - offset);
                    case YES_IF_PRESENT -> {
                        if (disi.docID() < blockStart) {
                            disi.advance(blockStart);
                        }
                        disi.intoBitSet(blockEnd, bitSet, offset);
                    }
                    case MAYBE -> intoMaybeBlock(blockStart, blockEnd, bitSet, offset);
                }
                blockIterator.advance(blockEnd);
            }
        }

        /** Confirms the docs of a single MAYBE block in {@code [blockStart, blockEnd)}. */
        abstract void intoMaybeBlock(int blockStart, int blockEnd, FixedBitSet bitSet, int offset) throws IOException;
    }

    /** Bulk range iterator over single-valued numeric doc values. */
    private static final class BulkNumericRangeIterator extends BulkBlockRangeIterator {

        private final NumericDocValues numericValues;
        private final long minValue;
        private final long maxValue;

        private BulkNumericRangeIterator(
            NumericDocValues values,
            SkipBlockRangeIterator blockIterator,
            IOBooleanSupplier predicate,
            float matchCost,
            long minValue,
            long maxValue
        ) {
            super(values, blockIterator, predicate, matchCost);
            this.numericValues = values;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        @Override
        void intoMaybeBlock(int blockStart, int blockEnd, FixedBitSet bitSet, int offset) throws IOException {
            int from = Math.max(blockStart, numericValues.docID());
            numericValues.rangeIntoBitSet(from, blockEnd, minValue, maxValue, bitSet, offset);
        }
    }

    /** Bulk range iterator over multi-valued sorted-numeric doc values. */
    private static final class BulkSortedNumericRangeIterator extends BulkBlockRangeIterator {

        private final SortedNumericDocValues sortedNumericValues;
        private final long minValue;
        private final long maxValue;

        private BulkSortedNumericRangeIterator(
            SortedNumericDocValues values,
            SkipBlockRangeIterator blockIterator,
            IOBooleanSupplier predicate,
            float matchCost,
            long minValue,
            long maxValue
        ) {
            super(values, blockIterator, predicate, matchCost);
            this.sortedNumericValues = values;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        @Override
        void intoMaybeBlock(int blockStart, int blockEnd, FixedBitSet bitSet, int offset) throws IOException {
            int from = Math.max(blockStart, sortedNumericValues.docID());
            sortedNumericValues.rangeIntoBitSet(from, blockEnd, minValue, maxValue, bitSet, offset);
        }
    }

    /**
     * Bulk range iterator over ordinal (sorted / sorted-set) doc values. MAYBE blocks confirm the
     * ordinal predicate one doc at a time.
     */
    private static final class BulkOrdinalRangeIterator extends BulkBlockRangeIterator {

        private BulkOrdinalRangeIterator(
            DocIdSetIterator values,
            SkipBlockRangeIterator blockIterator,
            IOBooleanSupplier predicate,
            float matchCost
        ) {
            super(values, blockIterator, predicate, matchCost);
        }

        @Override
        void intoMaybeBlock(int blockStart, int blockEnd, FixedBitSet bitSet, int offset) throws IOException {
            if (disi.docID() < blockStart) {
                disi.advance(blockStart);
            }
            for (int doc = disi.docID(); doc < blockEnd; doc = disi.nextDoc()) {
                if (predicate.get()) {
                    bitSet.set(doc - offset);
                }
            }
        }
    }

    private static final class DocValuesValueRangeIterator extends XDocValuesRangeIterator {

        private final IOBooleanSupplier predicate;
        private final float matchCost;

        private DocValuesValueRangeIterator(DocIdSetIterator disi, IOBooleanSupplier predicate, float matchCost) {
            super(disi);
            this.predicate = predicate;
            this.matchCost = matchCost;
        }

        @Override
        public boolean matches() throws IOException {
            return predicate.get();
        }

        @Override
        public float matchCost() {
            return matchCost;
        }
    }

    private static final class EmptyRangeIterator extends XDocValuesRangeIterator {

        private EmptyRangeIterator() {
            super(DocIdSetIterator.empty());
        }

        @Override
        public boolean matches() throws IOException {
            return false;
        }

        @Override
        public float matchCost() {
            return 0;
        }
    }

    private XDocValuesRangeIterator(DocIdSetIterator approximation) {
        super(approximation);
    }
}
