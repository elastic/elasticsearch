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
    /** Single-valued: a rank is its own value address, so a run of ranks is a contiguous slice of a block. */
    private final boolean singleValued;
    private final int blockShift;
    private final int blockMask;
    private final NumericColumnMetadata.Skipper skipperMeta;
    private final IndexInput skipIndex;

    private final BytesRefBuilder payload = new BytesRefBuilder();
    private long[] values = new long[8];
    /** Reused across {@link #bulkLongs} calls; grown to the batch size, never to the column size. */
    private int[] ranks = new int[8];

    public ColumnarNumericBinaryDocValues(
        NumericColumnReader reader,
        ColumnIterator iterator,
        int maxDoc,
        NumericColumnMetadata.Skipper skipperMeta,
        IndexInput skipIndex
    ) {
        this.reader = reader;
        this.iterator = iterator;
        this.maxDoc = maxDoc;
        this.singleValued = reader.multiValued() == false;
        this.blockShift = Integer.numberOfTrailingZeros(reader.blockSize());
        this.blockMask = reader.blockSize() - 1;
        this.skipperMeta = skipperMeta;
        this.skipIndex = skipIndex;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        final int rank = iterator.rank();
        final long first = reader.firstValueAddress(rank);
        final long count = reader.valueCount(rank);
        if (values.length < count) {
            values = new long[ArrayUtil.oversize((int) count, Long.BYTES)];
        }
        for (int i = 0; i < count; i++) {
            values[i] = reader.valueAt(first + i);
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
                return reader.valueAt(first + upto++);
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
                    int rank = iterator.rank();
                    first = reader.firstValueAddress(rank);
                    count = reader.valueCount(rank);
                    upto = 0;
                }
                return doc;
            }
        };
    }

    /**
     * Reads the values of {@code docs[offset..offset+count)} (ascending doc ids) into {@code sink}, one
     * value per document, as runs sliced out of a decoded block. Documents are resolved to value addresses
     * through {@link ColumnIterator#ranks}, and the run detection compares value addresses rather than document
     * ids, so the column may be dense or sparse.
     *
     * <p>Returns {@code false} without touching the sink when the column is multi-valued, when any
     * requested document has no value, or when {@code mayContainDuplicates} is set, since run detection
     * identifies a run from its endpoints alone and that requires unique document ids.
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
            final int valueAddress = ranks[i]; // single-valued: rank == value address
            final long[] block = reader.block(valueAddress >>> blockShift);
            final int inBlock = valueAddress & blockMask;
            final int remaining = Math.min(blockMask + 1 - inBlock, count - i);
            int length = 1;
            for (int candidate = remaining; candidate > 1; candidate >>= 1) {
                // A run is contiguous when its last value address is exactly candidate-1 above the first.
                if (ranks[i + candidate - 1] - valueAddress == candidate - 1) {
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
     * {@code null} when the column is multi-valued. Consults the skip index when present.
     *
     * <p>The approximation is the column's own iterator, so only documents that have a value are visited,
     * and the block work is keyed on value addresses. The column may be dense or sparse.
     */
    public DocIdSetIterator rangeIterator(long lowerValue, long upperValue) throws IOException {
        if (singleValued == false) {
            return null;
        }
        final ColumnIterator column = reader.iterator();
        final BlockMask mask = new BlockMask(lowerValue, upperValue);
        final DocValuesSkipper skipper = skipperMeta == null
            ? null
            : SkipIndexCodec.forId(skipperMeta.codecId()).reader(skipperMeta, skipIndex);
        final TwoPhaseIterator twoPhase = skipper == null
            ? scanningTwoPhase(column, mask, lowerValue, upperValue)
            : skippingTwoPhase(column, mask, skipper, lowerValue, upperValue);
        return TwoPhaseIterator.asDocIdSetIterator(twoPhase);
    }

    /**
     * Fills {@code bitSet} with the matching documents in {@code [column.docID(), upTo)}, leaving the
     * iterator on the first document at or after {@code upTo}.
     *
     * <p>Proceeds in runs of documents known present ({@link ColumnIterator#docIDRunEnd()}). Within a run
     * document ids and value addresses advance in lockstep, so a position in a decoded block maps back to a
     * document id by a constant offset, and one {@code forEach} per block fills that stretch.
     */
    private void maskIntoBitSet(ColumnIterator column, BlockMask mask, int upTo, FixedBitSet bitSet, int offset) throws IOException {
        int doc = column.docID();
        while (doc < upTo) {
            final int valueAddress = column.rank();
            final int runEnd = Math.min(column.docIDRunEnd(), upTo);
            if (runEnd - doc == 1) {
                // A single document: setting the bit directly is cheaper than the block-at-a-time path.
                mask.load(valueAddress >>> blockShift);
                if (mask.matches.get(valueAddress & blockMask)) {
                    bitSet.set(doc - offset);
                }
                doc = column.nextDoc();
                continue;
            }
            final int valueAddressToDoc = doc - valueAddress; // constant for as long as the run lasts
            final int firstValueAddress = valueAddress;
            final int lastValueAddress = valueAddress + (runEnd - doc) - 1;
            final int firstBlock = firstValueAddress >>> blockShift;
            final int lastBlock = lastValueAddress >>> blockShift;
            for (int blockId = firstBlock; blockId <= lastBlock; blockId++) {
                mask.load(blockId);
                final int firstInBlock = blockId == firstBlock ? firstValueAddress & blockMask : 0;
                final int lastInBlock = blockId == lastBlock ? lastValueAddress & blockMask : blockMask;
                mask.matches.forEach(firstInBlock, lastInBlock + 1, (blockId << blockShift) + valueAddressToDoc - offset, bitSet::set);
            }
            doc = column.advance(runEnd);
        }
    }

    /** No skip index: every block is tested with the vectorized range check. */
    private TwoPhaseIterator scanningTwoPhase(ColumnIterator column, BlockMask mask, long lowerValue, long upperValue) {
        return new TwoPhaseIterator(column) {
            @Override
            public boolean matches() throws IOException {
                final int valueAddress = column.rank();
                mask.load(valueAddress >>> blockShift);
                return mask.matches.get(valueAddress & blockMask);
            }

            @Override
            public float matchCost() {
                return RANGE_MATCH_COST;
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                upTo = Math.min(upTo, maxDoc);
                maskIntoBitSet(column, mask, upTo, bitSet, offset);
            }

            @Override
            public int docIDRunEnd() throws IOException {
                return matchingRunEnd(column, mask);
            }
        };
    }

    /** With a skip index: coarse interval skipping, then the vectorized scan only on straddling intervals. */
    private TwoPhaseIterator skippingTwoPhase(
        ColumnIterator column,
        BlockMask mask,
        DocValuesSkipper skipper,
        long lowerValue,
        long upperValue
    ) {
        return new TwoPhaseIterator(column) {
            @Override
            public boolean matches() throws IOException {
                final int doc = column.docID();
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
                final int valueAddress = column.rank();
                mask.load(valueAddress >>> blockShift);
                return mask.matches.get(valueAddress & blockMask);
            }

            @Override
            public float matchCost() {
                return RANGE_MATCH_COST;
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                upTo = Math.min(upTo, maxDoc);
                int doc = column.docID();
                while (doc < upTo) {
                    if (skipper.maxDocID(0) < doc) {
                        skipper.advance(doc);
                        if (skipper.maxDocID(0) == DocIdSetIterator.NO_MORE_DOCS) {
                            column.advance(maxDoc);
                            return;
                        }
                    }
                    final int intervalEnd = Math.min(skipper.maxDocID(0) + 1, upTo);
                    final long minVal = skipper.minValue(0);
                    final long maxVal = skipper.maxValue(0);
                    if (lowerValue <= minVal && maxVal <= upperValue) {
                        // Whole interval in range, so every document in it that has a value matches. The
                        // iterator fills them directly — on a sparse column the documents in between have no
                        // value and must not be set, which a plain range set would get wrong.
                        column.intoBitSet(intervalEnd, bitSet, offset);
                    } else if (minVal <= upperValue && lowerValue <= maxVal) {
                        maskIntoBitSet(column, mask, intervalEnd, bitSet, offset);
                    } else {
                        column.advance(intervalEnd); // no overlap: skip the interval outright
                    }
                    doc = column.docID();
                }
                if (column.docID() < upTo) {
                    column.advance(upTo);
                }
            }

            @Override
            public int docIDRunEnd() throws IOException {
                final int doc = column.docID();
                if (skipper.maxDocID(0) < doc) {
                    skipper.advance(doc);
                }
                if (lowerValue <= skipper.minValue(0) && skipper.maxValue(0) <= upperValue) {
                    // The interval matches throughout, but only where documents actually have a value.
                    return Math.min(column.docIDRunEnd(), skipper.maxDocID(0) + 1);
                }
                return matchingRunEnd(column, mask);
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

        /** The first value address at or after {@code valueAddress} that does not match, in value-address space. */
        int runEnd(int valueAddress) {
            final int blockId = valueAddress >>> blockShift;
            // docIDRunEnd may be called on an unconfirmed candidate, so never claim a run from a non-match.
            if (maskBlock == blockId && matches.get(valueAddress & blockMask)) {
                return (blockId << blockShift) + nextClearBit((valueAddress & blockMask) + 1, matches);
            }
            return valueAddress;
        }
    }

    /**
     * The end of the run of matching documents at the iterator's current position: the lesser of how far
     * the values keep matching, from the block mask in value-address space, and how far the documents stay
     * present, from the iterator in document space.
     */
    private int matchingRunEnd(ColumnIterator column, BlockMask mask) throws IOException {
        final int doc = column.docID();
        final int valueAddress = column.rank();
        final int matchingValueAddresses = mask.runEnd(valueAddress) - valueAddress;
        return Math.min(column.docIDRunEnd(), Math.min(doc + matchingValueAddresses, maxDoc));
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
