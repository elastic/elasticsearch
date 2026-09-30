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
import java.util.Arrays;

/**
 * The values of a plain column: their bytes one after another in the data, and each one's length as a column
 * of its own in the lengths file.
 *
 * <p>Each slot stores a code: {@link #NULL_CODE} for a null, {@link #REPEAT} for the value of the slot before, which
 * stores no bytes again, and otherwise two more than the value's byte count. The codes are bit-packed a block at a
 * time with runs and outliers taken out first, and a block's first slot is never a repeat, so every block decodes
 * on its own. Where a value begins is the sum of the byte counts before it: the navigation keeps that sum at the
 * start of every block, and a read adds up the ones inside its block. A merge that copies another column's chunks
 * ({@link Writer#copy}) may instead start a block at the value the block's first slot repeats, whose bytes are
 * already behind it; the block still decodes on its own, and the starts never decrease.
 *
 * <p>A column whose values all have one length and none of them null keeps neither: a value begins at its
 * address times that length. Having no codes, it marks no repeats either, so a value equal to the one before
 * it is stored again.
 *
 * <p>Values are read a block of {@code valuesPerBlock} at a time, one span of the byte stream for the block.
 */
final class PlainValues {

    private PlainValues() {}

    /** The code of a null slot. */
    static final long NULL_CODE = 0;
    /** The code of a slot holding the same value as the slot before it. */
    static final long REPEAT = 1;
    /** What a value's byte count is stored above. */
    private static final long LENGTH_BASE = 2;

    /** The code a value of {@code length} bytes is stored as, unless it repeats the one before it. */
    static long code(long length) {
        return length + LENGTH_BASE;
    }

    /**
     * Whole chunks a merge has to be able to copy out of a run before it copies any: copying closes the chunk
     * being filled early, and a run holding none has nothing to copy that pays for the short chunk.
     */
    static final int MIN_COPIED_CHUNKS = 1;

    /** The most bytes read at once around the chunks a run copies, so appending them holds no chunk-sized buffer. */
    private static final int COPY_PIECE_BYTES = 64 * 1024;

    /**
     * What {@link Writer#copy} appended: its slots and how many of them are null, and the chunks of the source it
     * copied as they were stored, with the bytes they decode to.
     */
    record Copied(long slots, long nulls, long chunks, long bytes) {}

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

        /** Holds the bytes a copy appends around the chunks it copies, a piece at a time. */
        private byte[] copyBuffer = new byte[0];

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
                    lengths.add(code(value.length));
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
            lengths.add(NULL_CODE);
            if (count == 0 || previousNull == false) {
                runs++;
            }
            previousNull = true;
            count++;
        }

        /**
         * Appends the slots {@code [from, to)} of {@code source} with their chunks copied as stored rather than
         * compressed again, and answers what was copied, or null, having written nothing, when copying does not
         * apply or would not pay.
         *
         * <p>A chunk can only be copied if the bytes it holds land in this stream unchanged, and a plain column's
         * bytes differ from one writer to the next only where they disagree on which slots are repeats. So the
         * slots copied take the source's decisions rather than making their own: a value the source stored is
         * stored, a repeat is a repeat. The one decision that is not the source's to make is the first slot of
         * each block of this column's lengths, which is never a repeat. A repeat landing there is recorded as the
         * value it repeats, with the block starting where that value's bytes begin — behind the stream's end,
         * over bytes already written — so the block decodes on its own and no byte is written twice.
         *
         * <p>Leading repeats refer to a value before the run, which this stream holds under bytes of its own, so
         * they go through {@link #add} like any value arriving from outside. The bytes before the run's first
         * whole chunk and after its last are appended as they are read; the chunks between are copied.
         */
        Copied copy(Reader source, long from, long to) throws IOException {
            assert from < to && to <= source.numValues() : "[" + from + ", " + to + ") of " + source.numValues();
            final ChunkedBytesReader sourceChunks = source.chunks();
            if (sourceChunks.codecId() != chunks.codecId()) {
                return null;
            }
            if (constantLength >= 0 && source.constantLength() != constantLength) {
                // This column's addresses are computed from its one length; a source with codes may hold repeats
                // this column would have to store.
                return null;
            }
            long first = from;
            while (first < to && repeatCode(source, first) == REPEAT) {
                first++;
            }
            if (first == to) {
                return null;
            }
            // The run's bytes, which the source holds one after another, since a repeat or a null stores none.
            final long begin = source.start(first);
            final long end = source.start(to - 1) + source.length(to - 1);
            final long firstChunk = sourceChunks.firstChunkAtOrAfter(begin);
            long lastChunk = firstChunk;
            while (lastChunk < sourceChunks.numChunks() && sourceChunks.chunkStart(lastChunk + 1) <= end) {
                lastChunk++;
            }
            if (lastChunk - firstChunk < MIN_COPIED_CHUNKS) {
                // Copying closes this stream's pending chunk early, which costs a short chunk; a run that holds
                // no whole chunk has nothing to copy that is worth it.
                return null;
            }

            final BytesRef scratch = new BytesRef();
            for (long v = from; v < first; v++) {
                source.get(v, scratch);
                add(scratch);
            }

            final long base = chunks.uncompressedLength();
            long nulls = 0;
            for (long v = first; v < to; v++) {
                final long code = repeatCode(source, v);
                final int length = source.length(v);
                // A repeat starts where the value it repeats did, which lies inside the run: no repeat follows a
                // null, and the run's first slot is not one. So every start maps by the same offset.
                final long start = base + source.start(v) - begin;
                final boolean blockStart = count % lengthBlockSize == 0;
                if (starts != null && blockStart) {
                    starts.add(start);
                }
                if (lengths != null) {
                    lengths.add(code == REPEAT && blockStart ? length + LENGTH_BASE : code);
                }
                if (code == NULL_CODE) {
                    nulls++;
                    if (count == 0 || previousNull == false) {
                        runs++;
                    }
                    previousNull = true;
                } else {
                    // A value the source stored again at one of its own block starts is counted as a run of its
                    // own, which is what the source's codes say; comparing it with the value before would mean
                    // reading bytes the copy exists not to read.
                    if (code != REPEAT) {
                        runs++;
                    }
                    valueBytes += length;
                    minLength = minLength < 0 ? length : Math.min(minLength, length);
                    maxLength = Math.max(maxLength, length);
                    previousNull = false;
                }
                count++;
            }

            appendFrom(sourceChunks, begin, sourceChunks.chunkStart(firstChunk));
            chunks.flush();
            for (long chunk = firstChunk; chunk < lastChunk; chunk++) {
                sourceChunks.copyChunk(chunk, chunks);
            }
            appendFrom(sourceChunks, sourceChunks.chunkStart(lastChunk), end);
            assert chunks.uncompressedLength() == base + end - begin
                : "stream at " + chunks.uncompressedLength() + " after copying [" + begin + ", " + end + ") from " + base;

            if (previousNull == false) {
                // What the next value added is compared against to decide whether it repeats.
                source.get(to - 1, scratch);
                previous.copyBytes(scratch);
            }
            return new Copied(
                to - from,
                nulls,
                lastChunk - firstChunk,
                sourceChunks.chunkStart(lastChunk) - sourceChunks.chunkStart(firstChunk)
            );
        }

        /**
         * The code of the slot at {@code v} as a repeat or not, whichever way the source stored it. A source written
         * by a copy may hold a repeat at the start of a block of lengths as the length of the value it repeats,
         * starting over bytes already behind it; here that is a repeat all the same, since this stream lays out
         * its own blocks and would otherwise read those bytes from where the value before ends.
         */
        private static long repeatCode(Reader source, long v) throws IOException {
            final long code = source.code(v);
            if (code > LENGTH_BASE && v > 0 && source.constantLength() < 0 && source.code(v - 1) != NULL_CODE) {
                if (source.start(v) < source.start(v - 1) + source.length(v - 1)) {
                    return REPEAT;
                }
            }
            return code;
        }

        /** Appends the source's bytes in {@code [begin, end)} as they are read, a bounded piece at a time. */
        private void appendFrom(ChunkedBytesReader source, long begin, long end) throws IOException {
            for (long at = begin; at < end;) {
                final int length = (int) Math.min(end - at, COPY_PIECE_BYTES);
                copyBuffer = source.read(at, length, copyBuffer);
                chunks.append(copyBuffer, 0, length);
                at += length;
            }
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
        /** The loaded block of codes, which is what says a slot is null. */
        private long[] codes;
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

        /** The length every value has, or {@code -1} when the column keeps a code per slot instead. */
        int constantLength() {
            return constantLength;
        }

        /** The byte stream the values are stored in. */
        ChunkedBytesReader chunks() {
            return chunks;
        }

        /**
         * The code stored for the slot at {@code valueAddress}: {@link #NULL_CODE}, {@link #REPEAT}, or two more
         * than the byte count of a value stored there. A column of one length stores every value and no code.
         */
        long code(long valueAddress) throws IOException {
            if (constantLength >= 0) {
                return constantLength + LENGTH_BASE;
            }
            loadLengths(valueAddress >>> lengthShift);
            return codes[(int) (valueAddress & lengthMask)];
        }

        /**
         * Where the slot at {@code valueAddress} begins in the byte stream: a repeat begins where the value it
         * repeats does, and a null where the next stored value will.
         */
        long start(long valueAddress) throws IOException {
            if (constantLength >= 0) {
                return valueAddress * constantLength;
            }
            loadLengths(valueAddress >>> lengthShift);
            return slotStarts[(int) (valueAddress & lengthMask)];
        }

        /** Whether the slot at {@code valueAddress} is null, which its code says. */
        boolean isNull(long valueAddress) throws IOException {
            if (constantLength >= 0) {
                return false;
            }
            loadLengths(valueAddress >>> lengthShift);
            return codes[(int) (valueAddress & lengthMask)] == NULL_CODE;
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
            loadBlock(valueAddress >>> valuesShift);
            final long start;
            final int length;
            if (constantLength >= 0) {
                start = valueAddress * constantLength;
                length = constantLength;
            } else {
                final int i = (int) (valueAddress & lengthMask);
                start = slotStarts[i];
                length = slotLengths[i];
            }
            dst.bytes = block.bytes;
            dst.offset = block.offset + (int) (start - blockStart);
            dst.length = length;
            return start;
        }

        /**
         * The stored codes, a block of them at a time: {@link #NULL_CODE}, {@link #REPEAT}, or a length above
         * {@link #LENGTH_BASE}. A column of one length stores none, and is answered as a block of that length.
         */
        StringColumnReader.SlotBlocks codes() {
            if (constantLength >= 0) {
                final long[] constant = new long[valuesPerBlock];
                Arrays.fill(constant, PlainValues.code(constantLength));
                return new StringColumnReader.SlotBlocks() {
                    @Override
                    public int blockSize() {
                        return valuesPerBlock;
                    }

                    @Override
                    public long numValues() {
                        return numValues;
                    }

                    @Override
                    public long[] block(long index) {
                        return constant;
                    }
                };
            }
            return new StringColumnReader.SlotBlocks() {
                @Override
                public int blockSize() {
                    return lengthMask + 1;
                }

                @Override
                public long numValues() {
                    return numValues;
                }

                @Override
                public long[] block(long index) throws IOException {
                    return lengths.block(index);
                }
            };
        }

        private void loadBlock(long valueBlock) throws IOException {
            final long first = valueBlock << valuesShift;
            if (constantLength >= 0) {
                if (valueBlock != loadedBlock) {
                    final long end = Math.min(first + valuesPerBlock, numValues);
                    blockStart = first * constantLength;
                    chunks.span(blockStart, (int) ((end - first) * constantLength), block);
                    loadedBlock = valueBlock;
                }
                return;
            }
            loadLengths(first >>> lengthShift);
            if (valueBlock != loadedBlock) {
                // Starts never decrease, a repeat taking the start of the value before it, so the block's
                // bytes run from its first slot's start to its last slot's end.
                final int at = (int) (first & lengthMask);
                final int last = Math.min(at + valuesPerBlock, loadedCount) - 1;
                blockStart = slotStarts[at];
                chunks.span(blockStart, (int) (slotStarts[last] + slotLengths[last] - blockStart), block);
                loadedBlock = valueBlock;
            }
        }

        private void loadLengths(long lengthBlock) throws IOException {
            if (lengthBlock == loadedLengths) {
                return;
            }
            codes = lengths.block(lengthBlock);
            loadedCount = (int) Math.min(slotStarts.length, numValues - (lengthBlock << lengthShift));
            long at = starts.get(lengthBlock);
            for (int i = 0; i < loadedCount; i++) {
                final long code = codes[i];
                if (code == REPEAT) {
                    // Never a block's first slot, and always right after a value.
                    slotStarts[i] = slotStarts[i - 1];
                    slotLengths[i] = slotLengths[i - 1];
                } else {
                    final int length = code == NULL_CODE ? 0 : (int) (code - LENGTH_BASE);
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
