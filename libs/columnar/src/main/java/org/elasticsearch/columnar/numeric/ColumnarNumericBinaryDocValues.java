/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

/**
 * A numeric column at the {@code BINARY} surface: {@link #binaryValue} re-emits a document's values as a
 * {@link NumericBinaryPayload}, while {@link #bulkLongs} and {@link #rangeIterator} read decoded longs
 * directly. Those fast paths require a dense single-valued column; otherwise the caller falls back to
 * {@link #binaryValue}. The skip index lives inside the column, not on the Lucene surface.
 */
public final class ColumnarNumericBinaryDocValues extends BinaryDocValues {

    private static final float RANGE_MATCH_COST = 2f;

    private final NumericColumnReader reader;
    private final ColumnIterator iterator;
    private final int maxDoc;
    /** Single-valued: a rank is its own value ordinal, so a run of ranks is a contiguous slice of a block. */
    private final boolean singleValued;
    /** Dense single-valued: a document id is its own value ordinal, so a value block maps onto a doc-id window. */
    private final boolean vectorizable;
    private final int blockShift;
    private final int blockMask;
    private final NumericColumnMetadata.Skipper skipperMeta;
    private final IndexInput data;

    private final BytesRefBuilder payload = new BytesRefBuilder();
    private long[] values = new long[8];
    /** Reused across {@link #bulkLongs} calls; grown to the batch size, never to the column size. */
    private int[] ranks = new int[8];

    public ColumnarNumericBinaryDocValues(
        NumericColumnReader reader,
        ColumnIterator iterator,
        int maxDoc,
        NumericColumnMetadata.Skipper skipperMeta,
        IndexInput data
    ) {
        this.reader = reader;
        this.iterator = iterator;
        this.maxDoc = maxDoc;
        this.singleValued = reader.multiValued() == false;
        this.vectorizable = iterator.isDense() && this.singleValued;
        this.blockShift = Integer.numberOfTrailingZeros(reader.blockSize());
        this.blockMask = reader.blockSize() - 1;
        this.skipperMeta = skipperMeta;
        this.data = data;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        final int rank = iterator.index();
        final long first = reader.firstOrdinal(rank);
        final long count = reader.valueCount(rank);
        if (values.length < count) {
            values = new long[ArrayUtil.oversize((int) count, Long.BYTES)];
        }
        for (int i = 0; i < count; i++) {
            values[i] = reader.valueForOrdinal(first + i);
        }
        return NumericBinaryPayload.encode(values, (int) count, payload);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return iterator.advanceExact(target);
    }

    @Override
    public int docID() {
        return iterator.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return iterator.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return iterator.advance(target);
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        iterator.intoBitSet(upTo, bitSet, offset);
    }

    @Override
    public long cost() {
        return iterator.cost();
    }

    /**
     * A streaming cursor that reads this column's values directly off the data input — block-decoded,
     * without a payload round-trip.
     */
    public NumericColumnValues directValues() {
        return new NumericColumnValues() {
            private long first;
            private long count;
            private int upto;

            @Override
            public int valueCount() {
                return (int) count;
            }

            @Override
            public long nextValue() throws IOException {
                return reader.valueForOrdinal(first + upto++);
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return position(iterator.nextDoc());
            }

            @Override
            public int advance(int target) throws IOException {
                return position(iterator.advance(target));
            }

            @Override
            public long cost() {
                return iterator.cost();
            }

            private int position(int doc) {
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    int rank = iterator.index();
                    first = reader.firstOrdinal(rank);
                    count = reader.valueCount(rank);
                    upto = 0;
                }
                return doc;
            }
        };
    }

    /**
     * Reads the values of {@code docs[offset..offset+count)} (ascending doc ids) into {@code sink}, one
     * value per document, as runs sliced straight out of a decoded block.
     *
     * <p>Density is not a precondition. Documents are resolved to value ordinals through
     * {@link ColumnIterator#ranks}, which each presence shape answers as well as it can — a dense column
     * with no I/O at all, a sparse one a run at a time — so a sparse column takes the same bulk value path
     * a dense one does. Ordinals, not document ids, drive the run detection, which is what makes that
     * work: a sparse column's ordinals are contiguous exactly when the requested documents are adjacent
     * in the presence set, a weaker condition than adjacent document ids.
     *
     * <p>Returns {@code false} without touching the sink — so the caller reads per document instead — when
     * one value per document is not well defined (a multi-valued column), when any requested document has
     * no value (there is no value to append for it; see {@code LongBlockSink}), or when
     * {@code mayContainDuplicates} is set, since run detection identifies a run from its endpoints alone
     * and that is only sound when the document ids are unique.
     */
    public boolean bulkLongs(int[] docs, int offset, int count, boolean mayContainDuplicates, LongBlockSink sink) throws IOException {
        if (singleValued == false || mayContainDuplicates) {
            return false;
        }
        if (ranks.length < count) {
            ranks = new int[ArrayUtil.oversize(count, Integer.BYTES)];
        }
        iterator.ranks(docs, offset, count, ranks);
        for (int i = 0; i < count; i++) {
            if (ranks[i] == ColumnIterator.NO_RANK) {
                return false;
            }
        }
        for (int i = 0; i < count;) {
            final int ordinal = ranks[i]; // single-valued: rank == value ordinal
            final long[] block = reader.block(ordinal >>> blockShift);
            final int inBlock = ordinal & blockMask;
            final int remaining = Math.min(blockMask + 1 - inBlock, count - i);
            int length = 1;
            for (int candidate = remaining; candidate > 1; candidate >>= 1) {
                // A run is contiguous when its last ordinal is exactly candidate-1 above the first.
                if (ranks[i + candidate - 1] - ordinal == candidate - 1) {
                    length = candidate;
                    break;
                }
            }
            sink.appendLongs(block, inBlock, length);
            i += length;
        }
        return true;
    }

    /**
     * A {@link DocIdSetIterator} over documents whose value is in {@code [lowerValue, upperValue]}, or
     * {@code null} when this column is not dense single-valued. Consults the skip index when present.
     */
    public DocIdSetIterator rangeIterator(long lowerValue, long upperValue) throws IOException {
        if (vectorizable == false) {
            return null;
        }
        final DocIdSetIterator approximation = DocIdSetIterator.all(maxDoc);
        final BlockMask mask = new BlockMask(lowerValue, upperValue);
        final DocValuesSkipper skipper = skipperMeta == null ? null : SkipIndexCodec.forId(skipperMeta.codecId()).reader(skipperMeta, data);
        final TwoPhaseIterator twoPhase = skipper == null
            ? scanningTwoPhase(approximation, mask, lowerValue, upperValue)
            : skippingTwoPhase(approximation, mask, skipper, lowerValue, upperValue);
        return TwoPhaseIterator.asDocIdSetIterator(twoPhase);
    }

    /** No skip index: every block is tested with the vectorized range check. */
    private TwoPhaseIterator scanningTwoPhase(DocIdSetIterator approximation, BlockMask mask, long lowerValue, long upperValue) {
        return new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
                final int doc = approximation.docID();
                mask.load(doc >>> blockShift);
                return mask.matches.get(doc & blockMask);
            }

            @Override
            public float matchCost() {
                return RANGE_MATCH_COST;
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                final int doc = approximation.docID();
                upTo = Math.min(upTo, maxDoc);
                if (doc >= upTo) {
                    return;
                }
                final int firstBlock = doc >>> blockShift;
                final int lastBlock = (upTo - 1) >>> blockShift;
                for (int blockId = firstBlock; blockId <= lastBlock; blockId++) {
                    mask.load(blockId);
                    final int firstInBlock = blockId == firstBlock ? doc & blockMask : 0;
                    final int lastInBlock = blockId == lastBlock ? (upTo - 1) & blockMask : blockMask;
                    mask.matches.forEach(firstInBlock, lastInBlock + 1, (blockId << blockShift) - offset, bitSet::set);
                }
                approximation.advance(upTo);
            }

            @Override
            public int docIDRunEnd() throws IOException {
                return mask.runEnd(approximation.docID());
            }
        };
    }

    /** With a skip index: coarse interval skipping, then the vectorized scan only on straddling intervals. */
    private TwoPhaseIterator skippingTwoPhase(
        DocIdSetIterator approximation,
        BlockMask mask,
        DocValuesSkipper skipper,
        long lowerValue,
        long upperValue
    ) {
        return new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
                final int doc = approximation.docID();
                if (skipper.maxDocID(0) < doc) {
                    skipper.advance(doc);
                }
                final long minVal = skipper.minValue(0);
                final long maxVal = skipper.maxValue(0);
                if (lowerValue <= minVal && maxVal <= upperValue) {
                    return true; // whole interval in range: no decode needed
                }
                if (minVal > upperValue || maxVal < lowerValue) {
                    return false; // no overlap
                }
                mask.load(doc >>> blockShift);
                return mask.matches.get(doc & blockMask);
            }

            @Override
            public float matchCost() {
                return RANGE_MATCH_COST;
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                upTo = Math.min(upTo, maxDoc);
                int doc = approximation.docID();
                while (doc < upTo) {
                    if (skipper.maxDocID(0) < doc) {
                        skipper.advance(doc);
                        if (skipper.maxDocID(0) == DocIdSetIterator.NO_MORE_DOCS) {
                            approximation.advance(maxDoc);
                            return;
                        }
                    }
                    final int firstInInterval = Math.max(doc, skipper.minDocID(0));
                    final int lastInInterval = Math.min(skipper.maxDocID(0), upTo - 1);
                    final long minVal = skipper.minValue(0);
                    final long maxVal = skipper.maxValue(0);
                    if (lowerValue <= minVal && maxVal <= upperValue) {
                        bitSet.set(firstInInterval - offset, lastInInterval + 1 - offset);
                    } else if (minVal <= upperValue && lowerValue <= maxVal) {
                        final int firstBlock = firstInInterval >>> blockShift;
                        final int lastBlock = lastInInterval >>> blockShift;
                        for (int blockId = firstBlock; blockId <= lastBlock; blockId++) {
                            mask.load(blockId);
                            final int firstInBlock = blockId == firstBlock ? firstInInterval & blockMask : 0;
                            final int lastInBlock = blockId == lastBlock ? lastInInterval & blockMask : blockMask;
                            mask.matches.forEach(firstInBlock, lastInBlock + 1, (blockId << blockShift) - offset, bitSet::set);
                        }
                    }
                    doc = lastInInterval + 1;
                }
                if (approximation.docID() < upTo) {
                    approximation.advance(upTo);
                }
            }

            @Override
            public int docIDRunEnd() throws IOException {
                final int doc = approximation.docID();
                if (skipper.maxDocID(0) < doc) {
                    skipper.advance(doc);
                }
                if (lowerValue <= skipper.minValue(0) && skipper.maxValue(0) <= upperValue) {
                    return skipper.maxDocID(0) + 1; // whole interval matches
                }
                return mask.runEnd(doc);
            }
        };
    }

    /**
     * A one-block vectorized range mask, cached by block so a block decodes at most once. Also serves
     * the {@code docIDRunEnd} run computation, which must only start from a confirmed match.
     */
    private final class BlockMask {
        private final FixedBitSet matches = new FixedBitSet(blockMask + 1);
        private final long lowerValue;
        private final long upperValue;
        private int maskBlock = -1;

        BlockMask(long lowerValue, long upperValue) {
            this.lowerValue = lowerValue;
            this.upperValue = upperValue;
        }

        void load(int blockIndex) throws IOException {
            if (blockIndex != maskBlock) {
                matches.clear();
                ESVectorUtil.inRangeBitmask(reader.block(blockIndex), lowerValue, upperValue, matches.getBits());
                maskBlock = blockIndex;
            }
        }

        int runEnd(int doc) {
            final int blockId = doc >>> blockShift;
            // docIDRunEnd may be called on an unconfirmed candidate, so never claim a run from a non-match.
            if (maskBlock == blockId && matches.get(doc & blockMask)) {
                return Math.min((blockId << blockShift) + nextClearBit((doc & blockMask) + 1, matches), maxDoc);
            }
            return doc;
        }
    }

    /** First clear (0) bit at or after {@code from}; {@code matches.length()} if none. */
    private static int nextClearBit(int from, FixedBitSet matches) {
        final long[] bits = matches.getBits();
        int wordIdx = from >>> 6;
        if (wordIdx >= bits.length) {
            return matches.length();
        }
        long word = ~bits[wordIdx] >>> (from & 63);
        if (word != 0) {
            return from + Long.numberOfTrailingZeros(word);
        }
        for (int i = wordIdx + 1; i < bits.length; i++) {
            word = ~bits[i];
            if (word != 0) {
                return (i << 6) + Long.numberOfTrailingZeros(word);
            }
        }
        return matches.length();
    }

    /**
     * A streaming write cursor ({@link NumericColumnValues}) over a {@code BinaryDocValues} of
     * {@link NumericBinaryPayload}s — used to feed the encoder pipeline on ingest, decoding one
     * payload at a time (nothing materialized).
     */
    public static NumericColumnValues decodePayloads(BinaryDocValues binary) {
        return new NumericColumnValues() {
            private final long[][] buffer = { new long[8] };
            private int count;
            private int upto;

            @Override
            public int valueCount() {
                return count;
            }

            @Override
            public long nextValue() {
                return buffer[0][upto++];
            }

            @Override
            public int docID() {
                return binary.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return position(binary.nextDoc());
            }

            @Override
            public int advance(int target) throws IOException {
                return position(binary.advance(target));
            }

            @Override
            public long cost() {
                return binary.cost();
            }

            private int position(int doc) throws IOException {
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    count = NumericBinaryPayload.decode(binary.binaryValue(), buffer);
                    upto = 0;
                }
                return doc;
            }
        };
    }
}
