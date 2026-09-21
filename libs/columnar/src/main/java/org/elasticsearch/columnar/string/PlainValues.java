/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.LongBlocks;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ChunkIndexMetadata;
import org.elasticsearch.columnar.substrate.ChunkedBytesReader;
import org.elasticsearch.columnar.substrate.ChunkedBytesWriter;
import org.elasticsearch.columnar.substrate.ColumnInputs;
import org.elasticsearch.columnar.substrate.ColumnOutputs;
import org.elasticsearch.columnar.substrate.MonotonicReader;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * The values of a plain column: their bytes one after another in the data, and each one's length as a column
 * of its own in the lengths file.
 *
 * <p>Each slot stores a code: {@link #NULL} for a null, {@link #REPEAT} for the value of the slot before, which
 * stores no bytes again, and otherwise two more than the value's byte count. The codes are bit-packed a block at a
 * time with runs and outliers taken out first, and a block's first slot is never a repeat, so every block decodes
 * on its own. Where a value begins is the sum of the byte counts before it: the navigation keeps that sum at the
 * start of every block, and a read adds up the ones inside its block.
 *
 * <p>A column whose values all have one length and none of them null keeps neither: a value begins at its
 * address times that length.
 *
 * <p>Values are read a block of {@code valuesPerBlock} at a time, one span of the byte stream for the block.
 */
final class PlainValues {

    private PlainValues() {}

    /** The code of a null slot. */
    static final long NULL = 0;
    /** The code of a slot holding the same value as the slot before it. */
    static final long REPEAT = 1;
    /** What a value's byte count is stored above. */
    private static final long LENGTH_BASE = 2;

    /**
     * Where a plain column's values are, and the units they were written in. {@code starts} and {@code lengths}
     * are absent when {@code constantLength} is not {@code -1}.
     */
    record Metadata(
        int valuesPerBlock,
        long numValues,
        int constantLength,
        ChunkIndexMetadata chunks,
        MonotonicWriter.Table starts,
        LongBlocks.Metadata lengths
    ) {

        boolean constant() {
            return constantLength >= 0;
        }

        void writeTo(DataOutput out) throws IOException {
            out.writeVInt(valuesPerBlock);
            out.writeVLong(numValues);
            out.writeVInt(constantLength + 1);
            chunks.writeTo(out);
            if (constant() == false) {
                out.writeVLong(starts.dataOffset());
                out.writeVLong(starts.dataLength());
                out.writeVInt(starts.meta().length);
                out.writeBytes(starts.meta(), 0, starts.meta().length);
                lengths.writeTo(out);
            }
        }

        static Metadata readFrom(DataInput in) throws IOException {
            final int valuesPerBlock = in.readVInt();
            final long numValues = in.readVLong();
            final int constantLength = in.readVInt() - 1;
            final ChunkIndexMetadata chunks = ChunkIndexMetadata.readFrom(in);
            if (constantLength >= 0) {
                return new Metadata(valuesPerBlock, numValues, constantLength, chunks, MonotonicWriter.Table.NONE, null);
            }
            final long startsOffset = in.readVLong();
            final long startsLength = in.readVLong();
            final byte[] startsMeta = new byte[in.readVInt()];
            in.readBytes(startsMeta, 0, startsMeta.length);
            final LongBlocks.Metadata lengths = LongBlocks.Metadata.readFrom(in);
            return new Metadata(
                valuesPerBlock,
                numValues,
                constantLength,
                chunks,
                new MonotonicWriter.Table(startsOffset, startsLength, startsMeta),
                lengths
            );
        }

        Reader open(ColumnInputs inputs) throws IOException {
            return new Reader(this, inputs);
        }
    }

    /** Appends a column's values in slot order. */
    static final class Writer {
        private final ChunkedBytesWriter chunks;
        /** Both null when every value has {@link #constantLength}. */
        private final LongBlocks.Writer lengths;
        private final MonotonicWriter starts;
        private final int constantLength;
        private final int valuesPerBlock;
        private final int lengthBlockSize;
        private final long numValues;
        private long count;
        private long valueBytes;
        private int minLength = -1;
        private int maxLength = -1;

        /** Runs of equal values, counting a run of nulls as one, so a reader can tell whether naming a page's values pays. */
        private long runs;
        private final BytesRefBuilder previous = new BytesRefBuilder();
        private boolean previousNull;

        /**
         * @param constantLength the length every value has, which the caller knows before writing any of them,
         *                       or {@code -1}; a column with one keeps no lengths
         */
        Writer(
            ChunkCodec codec,
            ChunkBounds bounds,
            int valuesPerBlock,
            int lengthBlockSize,
            long numValues,
            int constantLength,
            ColumnOutputs outputs
        ) {
            assert valuesPerBlock <= lengthBlockSize : valuesPerBlock + " > " + lengthBlockSize;
            this.chunks = new ChunkedBytesWriter(codec, bounds, outputs.data(), outputs.navigation());
            this.constantLength = constantLength;
            if (constantLength >= 0) {
                this.lengths = null;
                this.starts = null;
            } else {
                this.lengths = new LongBlocks.Writer(
                    NumericPipeline.runsAndOutliersPipeline(lengthBlockSize),
                    BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                    outputs.lengths(),
                    outputs.navigation()
                );
                this.starts = new MonotonicWriter(outputs.navigation());
            }
            this.valuesPerBlock = valuesPerBlock;
            this.lengthBlockSize = lengthBlockSize;
            this.numValues = numValues;
        }

        void add(BytesRef value) throws IOException {
            if (constantLength >= 0 && value.length != constantLength) {
                // The column's addresses are computed from the length, so one value off it misplaces every one after.
                throw new IllegalStateException("a value of " + value.length + " bytes in a column counted at " + constantLength);
            }
            final boolean repeat = count > 0 && previousNull == false && previous.get().bytesEquals(value);
            startSlot();
            if (lengths != null && repeat && count % lengthBlockSize != 0) {
                lengths.add(REPEAT);
            } else {
                chunks.append(value.bytes, value.offset, value.length);
                if (lengths != null) {
                    lengths.add(value.length + LENGTH_BASE);
                }
            }
            valueBytes += value.length;
            minLength = minLength < 0 ? value.length : Math.min(minLength, value.length);
            maxLength = Math.max(maxLength, value.length);
            if (repeat == false) {
                runs++;
                previous.copyBytes(value);
            }
            previousNull = false;
            count++;
        }

        void addNull() throws IOException {
            if (constantLength >= 0) {
                throw new IllegalStateException("a null in a column counted as holding none");
            }
            startSlot();
            lengths.add(NULL);
            if (count == 0 || previousNull == false) {
                runs++;
            }
            previousNull = true;
            count++;
        }

        private void startSlot() throws IOException {
            if (starts != null && count % lengthBlockSize == 0) {
                starts.add(chunks.uncompressedLength());
            }
            if (count % valuesPerBlock == 0) {
                // A block of values is read as one span, so a chunk bounded by values counts it whole.
                chunks.boundary((int) Math.min(valuesPerBlock, numValues - count));
            }
        }

        long runs() {
            return runs;
        }

        long valueBytes() {
            return valueBytes;
        }

        /** The shortest value written, in bytes, or {@code -1} when none was. */
        int minLength() {
            return minLength;
        }

        /** The longest value written, in bytes, or {@code -1} when none was. */
        int maxLength() {
            return maxLength;
        }

        Metadata finish() throws IOException {
            assert count == numValues : "wrote " + count + " values, told " + numValues;
            final ChunkIndexMetadata index = ChunkIndexMetadata.of(chunks.finish());
            if (constantLength >= 0) {
                return new Metadata(valuesPerBlock, count, constantLength, index, MonotonicWriter.Table.NONE, null);
            }
            return new Metadata(valuesPerBlock, count, -1, index, starts.finish(), lengths.finish());
        }
    }

    /** Reads a column's values by value address, a block of them and a block of lengths at a time. */
    static final class Reader {
        private final ChunkedBytesReader chunks;
        private final long numValues;
        private final int constantLength;
        private final int valuesShift;
        private final int valuesPerBlock;

        /** Null when every value has {@link #constantLength}. */
        private final LongValues starts;
        private final LongBlocks.Reader lengths;
        private final int lengthShift;
        private final int lengthMask;

        /** Where each slot of the loaded block of lengths begins in the byte stream, and how many bytes it holds. */
        private final long[] slotStarts;
        private final int[] slotLengths;
        /** The loaded block of codes as stored, which is what says a slot is null. */
        private long[] stored;
        private long loadedLengths = -1;
        private int loadedCount;

        /** The bytes of the loaded block of values, and the stream position they begin at. */
        private final BytesRef block = new BytesRef();
        private long loadedBlock = -1;
        private long blockStart;

        private Reader(Metadata meta, ColumnInputs inputs) throws IOException {
            this.numValues = meta.numValues();
            this.constantLength = meta.constantLength();
            this.chunks = meta.chunks().open(inputs);
            this.valuesShift = Integer.numberOfTrailingZeros(meta.valuesPerBlock());
            this.valuesPerBlock = meta.valuesPerBlock();
            if (meta.constant()) {
                this.starts = null;
                this.lengths = null;
                this.lengthShift = 0;
                this.lengthMask = 0;
                this.slotStarts = null;
                this.slotLengths = null;
                return;
            }
            final int lengthBlockSize = meta.lengths().blockSize();
            this.lengthShift = Integer.numberOfTrailingZeros(lengthBlockSize);
            this.lengthMask = lengthBlockSize - 1;
            this.starts = MonotonicReader.open(
                inputs.navigation(),
                meta.starts().meta(),
                meta.lengths().numBlocks(),
                meta.starts().dataOffset(),
                meta.starts().dataLength()
            );
            this.lengths = new LongBlocks.Reader(meta.lengths(), inputs.lengths(), inputs.navigation());
            this.slotStarts = new long[lengthBlockSize];
            this.slotLengths = new int[lengthBlockSize];
        }

        long numValues() {
            return numValues;
        }

        /** Whether the slot at {@code valueAddress} is null, which its stored length says. */
        boolean isNull(long valueAddress) throws IOException {
            if (constantLength >= 0) {
                return false;
            }
            loadLengths(valueAddress >>> lengthShift);
            return stored[(int) (valueAddress & lengthMask)] == NULL;
        }

        /** The length in bytes of the value at {@code valueAddress}; zero for a null. */
        int length(long valueAddress) throws IOException {
            if (constantLength >= 0) {
                return constantLength;
            }
            loadLengths(valueAddress >>> lengthShift);
            return slotLengths[(int) (valueAddress & lengthMask)];
        }

        /** Points {@code dst} at the value at {@code valueAddress}; the bytes are valid until the next call. */
        void get(long valueAddress, BytesRef dst) throws IOException {
            read(valueAddress, dst);
        }

        /**
         * Points {@code dst} at the value and answers where it begins in the byte stream. Two reads with equal
         * answers and lengths read the same stored bytes.
         */
        long read(long valueAddress, BytesRef dst) throws IOException {
            assert valueAddress >= 0 && valueAddress < numValues : valueAddress + " out of [0, " + numValues + ")";
            final long valueBlock = valueAddress >>> valuesShift;
            final long start;
            final int length;
            if (constantLength >= 0) {
                start = valueAddress * constantLength;
                length = constantLength;
                if (valueBlock != loadedBlock) {
                    final long first = valueBlock << valuesShift;
                    final long end = Math.min(first + valuesPerBlock, numValues);
                    blockStart = first * constantLength;
                    chunks.span(blockStart, (int) ((end - first) * constantLength), block);
                    loadedBlock = valueBlock;
                }
            } else {
                loadLengths(valueAddress >>> lengthShift);
                final int i = (int) (valueAddress & lengthMask);
                start = slotStarts[i];
                length = slotLengths[i];
                if (valueBlock != loadedBlock) {
                    // Starts never decrease, a repeat taking the start of the value before it, so the block's
                    // bytes run from its first slot's start to its last slot's end.
                    final int first = (int) ((valueBlock << valuesShift) & lengthMask);
                    final int last = Math.min(first + valuesPerBlock, loadedCount) - 1;
                    blockStart = slotStarts[first];
                    chunks.span(blockStart, (int) (slotStarts[last] + slotLengths[last] - blockStart), block);
                    loadedBlock = valueBlock;
                }
            }
            dst.bytes = block.bytes;
            dst.offset = block.offset + (int) (start - blockStart);
            dst.length = length;
            return start;
        }

        private void loadLengths(long lengthBlock) throws IOException {
            if (lengthBlock == loadedLengths) {
                return;
            }
            stored = lengths.block(lengthBlock);
            loadedCount = (int) Math.min(slotStarts.length, numValues - (lengthBlock << lengthShift));
            long at = starts.get(lengthBlock);
            for (int i = 0; i < loadedCount; i++) {
                final long code = stored[i];
                if (code == REPEAT) {
                    // Never a block's first slot, and always right after a value.
                    slotStarts[i] = slotStarts[i - 1];
                    slotLengths[i] = slotLengths[i - 1];
                } else {
                    final int length = code == NULL ? 0 : (int) (code - LENGTH_BASE);
                    slotStarts[i] = at;
                    slotLengths[i] = length;
                    at += length;
                }
            }
            loadedLengths = lengthBlock;
            loadedBlock = -1;
        }
    }
}
