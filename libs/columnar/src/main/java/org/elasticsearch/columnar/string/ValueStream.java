/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ChunkIndexMetadata;
import org.elasticsearch.columnar.substrate.ChunkedBytesReader;
import org.elasticsearch.columnar.substrate.ChunkedBytesWriter;
import org.elasticsearch.columnar.substrate.MonotonicReader;
import org.elasticsearch.columnar.substrate.MonotonicWriter;
import org.elasticsearch.columnar.substrate.internal.ByteArrayInts;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * An indexed sequence of byte values, addressed in blocks of {@link #VALUES_PER_BLOCK} values and compressed
 * in chunks of a fixed number of bytes. One offset is recorded per block rather than per value, so reading
 * value {@code i} reads its block and walks the lengths within it — which keeps the offset table a fraction
 * of the size a per-value table would be.
 *
 * <p>A block records its values one of three ways, chosen per block by what the values in it look like.
 * <b>Inline</b> puts each length beside its own value, where a length and its value compress together as one
 * pattern. <b>Packed</b> puts the lengths bit-packed at one fixed bit width at the block's head and the
 * values contiguously after them, where the exact bit count is narrower than a whole byte and the value bytes
 * are uninterrupted. <b>Runs</b> stores each distinct value once with how many values in a row hold it, for a
 * block whose values repeat — the shape a column sorted on this field takes. The block's first byte says which
 * of the three was picked.
 *
 * <p>A stream's {@link Layouts} controls which layouts it may use. {@link Layouts#CONTIGUOUS_VALUES} forbids
 * inline, so every block keeps its value bytes contiguous — nothing interleaved between them — which both
 * tightens the length header and gives a compressor an unbroken run of value bytes to match against.
 *
 * <p>Blocks and chunks are separate on purpose. A block of long values and a block of short ones are the same
 * count of values and nothing like the same number of bytes, so the unit that is addressed cannot also be the
 * unit that is compressed. A chunk closes only on a block boundary, so no value spans two of them.
 */
public final class ValueStream {

    /** Values behind one offset. Larger trades a longer walk on random access for a smaller offset table. */
    public static final int VALUES_PER_BLOCK = 128;

    /**
     * The three on-disk block layouts. The first byte of every block is the {@link BlockLayout#id} of the
     * layout that block was written with; any other value is a corrupt index.
     */
    enum BlockLayout {
        /** Lengths interleaved with values: one vInt length in front of each value. */
        INLINE((byte) 0),
        /** Bit-packed lengths at the block head, values contiguous after. */
        PACKED((byte) 1),
        /** Repeated values stored once each with a repeat count. */
        RUNS((byte) 2);

        /** The byte written as the first byte of every block of this layout. */
        final byte id;

        BlockLayout(byte id) {
            this.id = id;
        }

        /**
         * Returns the layout for the given marker byte, or {@code null} if the byte names no known layout.
         * A {@code null} result means the index is corrupt.
         */
        static BlockLayout fromId(byte b) {
            return switch (b) {
                case 0 -> INLINE;
                case 1 -> PACKED;
                case 2 -> RUNS;
                default -> null;
            };
        }
    }

    /** Mean value length below which a block keeps its lengths inline, when the stream allows it. */
    private static final int INLINE_MEAN_LENGTH = 32;

    /**
     * The block layouts a stream may write.
     *
     * <p>A stream can be restricted to {@link #CONTIGUOUS_VALUES} at construction time, which keeps every
     * block's value bytes together behind one length header. Blocks that repeat naturally still take
     * {@link BlockLayout#RUNS} regardless of which layouts is set.
     */
    public enum Layouts {
        /**
         * All three layouts: inline, packed and runs. A block of short values keeps each length beside
         * its own value, where the two repeat as one pattern a compressor matches whole.
         */
        ANY,
        /**
         * Runs and packed only, so every block's values sit contiguously behind one length header.
         * Removing the interleaved lengths gives a compressor an unbroken run of value bytes to match.
         */
        CONTIGUOUS_VALUES
    }

    /**
     * What a written stream records about itself.
     *
     * @param valueBytes the total length of the values before compression, counted as they are written.
     *                   Nothing downstream recovers it: the stored length is what the chunks compressed to,
     *                   and the block offsets address the stream rather than measure it.
     */
    public record Metadata(long numValues, long valueBytes, int valuesPerBlock, ChunkIndexMetadata chunks, MonotonicWriter.Table offsets) {

        public static Metadata empty() {
            return new Metadata(0, 0, VALUES_PER_BLOCK, ChunkIndexMetadata.empty(), MonotonicWriter.Table.NONE);
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeVLong(numValues);
            if (numValues == 0) {
                return;
            }
            out.writeVLong(valueBytes);
            out.writeVInt(valuesPerBlock);
            chunks.writeTo(out);
            out.writeVLong(offsets.dataOffset());
            out.writeVLong(offsets.dataLength());
            out.writeVInt(offsets.meta().length);
            out.writeBytes(offsets.meta(), 0, offsets.meta().length);
        }

        public static Metadata readFrom(DataInput in) throws IOException {
            final long numValues = in.readVLong();
            if (numValues == 0) {
                return empty();
            }
            final long valueBytes = in.readVLong();
            final int valuesPerBlock = in.readVInt();
            final ChunkIndexMetadata chunks = ChunkIndexMetadata.readFrom(in);
            final long dataOffset = in.readVLong();
            final long dataLength = in.readVLong();
            final byte[] meta = new byte[in.readVInt()];
            in.readBytes(meta, 0, meta.length);
            return new Metadata(numValues, valueBytes, valuesPerBlock, chunks, new MonotonicWriter.Table(dataOffset, dataLength, meta));
        }

        public Reader open(IndexInput data) throws IOException {
            if (numValues == 0) {
                return new Reader(null, null, 0, valuesPerBlock);
            }
            final long blocks = (numValues + valuesPerBlock - 1) / valuesPerBlock;
            return new Reader(
                chunks.open(data),
                MonotonicReader.open(data, offsets.meta(), blocks + 1L, offsets.dataOffset(), offsets.dataLength()),
                numValues,
                valuesPerBlock
            );
        }
    }

    /** Appends values in order, closing a chunk only on a block boundary so no block spans two chunks. */
    public static final class Writer implements Closeable {

        private final ChunkedBytesWriter chunks;
        private final IndexOutput data;
        private final MonotonicWriter offsets;
        private final int valuesPerBlock;
        private final Layouts layouts;
        private long count = 0;
        private long valueBytes = 0;
        private boolean closed = false;
        // A block's lengths are written ahead of its bytes, so the block is buffered until it is full. It
        // holds valuesPerBlock values, which is bounded and independent of the column.
        private final int[] pending;
        private byte[] pendingBytes = new byte[1024];
        // Holds a block's length header, or one value's length as a vint, so neither is allocated per block.
        private byte[] scratch = new byte[0];
        // What stageRuns found, read by the sizing and the write that follow it.
        private int[] runStarts = new int[0];
        private int[] runLens = new int[0];
        private int[] runReps = new int[0];
        private int pendingCount = 0;
        private int pendingLength = 0;

        public Writer(
            ChunkCodec codec,
            int targetChunkBytes,
            int valuesPerBlock,
            long numValues,
            Directory dir,
            IOContext ctx,
            String prefix,
            IndexOutput data,
            Layouts layouts
        ) throws IOException {
            this.valuesPerBlock = valuesPerBlock;
            this.layouts = layouts;
            this.data = data;
            this.pending = new int[valuesPerBlock];
            // Both hold a temporary file of their own. Whichever opens first is closed here if the one after
            // it fails, since a writer that never finished being built is one nothing else can close.
            ChunkedBytesWriter chunks = null;
            MonotonicWriter offsets = null;
            boolean success = false;
            try {
                chunks = new ChunkedBytesWriter(codec, targetChunkBytes, dir, ctx, prefix, data);
                final long blocks = (numValues + valuesPerBlock - 1) / valuesPerBlock;
                offsets = new MonotonicWriter(dir, ctx, prefix, blocks + 1L);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(chunks, offsets);
                }
            }
            this.chunks = chunks;
            this.offsets = offsets;
        }

        public void add(BytesRef value) throws IOException {
            if (pendingBytes.length < pendingLength + value.length) {
                pendingBytes = ArrayUtil.grow(pendingBytes, pendingLength + value.length);
            }
            System.arraycopy(value.bytes, value.offset, pendingBytes, pendingLength, value.length);
            pending[pendingCount++] = value.length;
            pendingLength += value.length;
            count++;
            valueBytes += value.length;
            if (pendingCount == valuesPerBlock) {
                flushBlock();
            }
        }

        /**
         * Emits the buffered block in whichever of the allowed layouts fits it.
         *
         * <p><b>Runs</b> stores each distinct value once with how many values in a row hold it. It is taken
         * first and only where it is genuinely smaller, sized against what the stream would otherwise write.
         *
         * <p><b>Packed</b> bit-packs the lengths at their exact bit width ahead of the bytes, so a block
         * whose values are long or dissimilar keeps them contiguous and hands a compressor an unbroken run.
         *
         * <p><b>Inline</b> keeps each length in front of its own value. It suits short repeated values when
         * {@link Layouts#ANY} is set, because the length and the value then repeat as one pattern that a
         * compressor matches whole — splitting them apart costs more than the packing saves. A stream set to
         * {@link Layouts#CONTIGUOUS_VALUES} never writes inline, so all its value bytes sit together.
         */
        private void flushBlock() throws IOException {
            chunks.boundary();
            offsets.add(chunks.uncompressedLength());
            // max and its bit width feed both runsAreSmaller (for sizing) and writePacked.
            int max = 0;
            for (int i = 0; i < pendingCount; i++) {
                max = Math.max(max, pending[i]);
            }
            final int bits = ByteArrayInts.bitsRequired(max);
            // A run of equal values is stored once with a repeat, which is what a column sorted on this
            // field is made of. Worth it only where the runs are long enough to pay for the repeats, so the
            // two forms are sized against each other rather than guessed at.
            // Finding the runs is the part that compares bytes, so it is done once and what it found is what
            // the sizing and the write both read.
            final int runCount = stageRuns();
            if (runsAreSmaller(runCount, bits)) {
                writeRuns(runCount);
                pendingCount = 0;
                pendingLength = 0;
                return;
            }
            // Which layout is smaller is decided after compression, so an uncompressed byte count cannot
            // choose between them. What separates them is how long the values are: short ones repeat
            // together with their length as a single pattern, and splitting the two apart costs more than
            // the walk saves. The threshold is where the measured shapes turn over.
            if (layouts == Layouts.ANY && pendingLength < pendingCount * INLINE_MEAN_LENGTH) {
                writeInline();
            } else {
                writePacked(bits);
            }
            pendingCount = 0;
            pendingLength = 0;
        }

        private void writePacked(int bits) throws IOException {
            // [PACKED marker] [bitsPerValue] [bit-packed lengths] [value bytes follow via second append]
            final int headerBytes = ByteArrayInts.bitPackedLength(pendingCount, bits);
            final int length = 2 + headerBytes;
            scratch = ArrayUtil.growNoCopy(scratch, length);
            scratch[0] = BlockLayout.PACKED.id;
            scratch[1] = (byte) bits;
            ByteArrayInts.writeBitPacked(pending, pendingCount, bits, scratch, 2);
            chunks.append(scratch, 0, length);
            chunks.append(pendingBytes, 0, pendingLength);
        }

        /**
         * Finds the runs the staged values hold, recording where each one's bytes start, how long they are
         * and how many values carry them. This is the only walk that compares bytes; sizing the two forms
         * against each other and writing the runs both read what it left.
         */
        private int stageRuns() {
            if (runStarts.length < pendingCount) {
                runStarts = new int[pendingCount];
                runLens = new int[pendingCount];
                runReps = new int[pendingCount];
            }
            int runs = 0;
            int at = 0;
            for (int i = 0; i < pendingCount; i++) {
                final boolean sameAsPrevious = i > 0
                    && pending[i] == pending[i - 1]
                    && Arrays.equals(pendingBytes, at - pending[i - 1], at, pendingBytes, at, at + pending[i]);
                if (sameAsPrevious) {
                    runReps[runs - 1]++;
                } else {
                    runStarts[runs] = at;
                    runLens[runs] = pending[i];
                    runReps[runs] = 1;
                    runs++;
                }
                at += pending[i];
            }
            return runs;
        }

        private boolean runsAreSmaller(int runCount, int bits) {
            if (runCount == pendingCount) {
                return false;
            }
            long runBytes = 0;
            for (int r = 0; r < runCount; r++) {
                runBytes += runLens[r];
            }
            // Compare runs against whichever layout the stream would actually write. Inline costs one vint
            // per value; packed costs a bitsPerValue byte plus the bit-packed header. Sizing against inline
            // when the stream cannot write it would over-favour runs on short-value blocks.
            final long alternative;
            if (layouts == Layouts.ANY && pendingLength < pendingCount * INLINE_MEAN_LENGTH) {
                alternative = pendingLength + pendingCount; // inline: ~one byte of vint per value
            } else {
                alternative = pendingLength + 1L + ByteArrayInts.bitPackedLength(pendingCount, bits); // packed
            }
            // Two vints a run against whatever the other form costs, plus the bytes each actually stores.
            return runBytes + 2L * runCount < alternative;
        }

        /** Each distinct value once, preceded by its length and how many documents in a row hold it. */
        private void writeRuns(int runCount) throws IOException {
            scratch = ArrayUtil.growNoCopy(scratch, 1 + 2 * runCount * ByteArrayInts.MAX_VINT_BYTES + pendingLength);
            scratch[0] = BlockLayout.RUNS.id;
            int header = 1;
            header += ByteArrayInts.writeVInt(runCount, scratch, header);
            for (int r = 0; r < runCount; r++) {
                header += ByteArrayInts.writeVInt(runLens[r], scratch, header);
                header += ByteArrayInts.writeVInt(runReps[r], scratch, header);
            }
            // The header is sized before the bytes are known, so the values are laid down after it.
            for (int r = 0; r < runCount; r++) {
                System.arraycopy(pendingBytes, runStarts[r], scratch, header, runLens[r]);
                header += runLens[r];
            }
            chunks.append(scratch, 0, header);
        }

        private void writeInline() throws IOException {
            // Each length stays in front of its own value rather than leading the block. A value is then
            // self-describing at its own address and can be handed over without consulting anything else,
            // which is the shape the binary surface speaks. The two are also what repeats, so a compressor
            // matches them as one token where separating them would leave it matching the shorter halves.
            //
            // The block is assembled whole and handed over once. Appending a length and then a value for
            // every one of them costs two calls and two bounds checks per value, to move a handful of bytes.
            scratch = ArrayUtil.growNoCopy(scratch, 1 + pendingCount * ByteArrayInts.MAX_VINT_BYTES + pendingLength);
            scratch[0] = BlockLayout.INLINE.id;
            int at = 1;
            int from = 0;
            for (int i = 0; i < pendingCount; i++) {
                final int length = pending[i];
                at += ByteArrayInts.writeVInt(length, scratch, at);
                System.arraycopy(pendingBytes, from, scratch, at, length);
                at += length;
                from += length;
            }
            chunks.append(scratch, 0, at);
        }

        public Metadata finish() throws IOException {
            if (count == 0) {
                return Metadata.empty();
            }
            if (pendingCount > 0) {
                flushBlock();
            }
            offsets.add(chunks.uncompressedLength());
            final ChunkIndexMetadata index = ChunkIndexMetadata.of(chunks.finish());
            return new Metadata(count, valueBytes, valuesPerBlock, index, offsets.finish(data));
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                chunks.close();
            } finally {
                offsets.close();
            }
        }
    }

    /** Random access by value address; a block is decoded once and its value bounds kept for the next lookup. */
    public static final class Reader {

        private final ChunkedBytesReader chunks;
        private final LongValues offsets;
        private final long numValues;
        private final int valuesPerBlock;

        private final BytesRef block = new BytesRef();
        private long cachedBlock = -1;
        private int[] runLengths = new int[0];
        private int[] runRepeats = new int[0];
        private final int blockShift;
        private final int blockMask;
        private int[] starts;
        private int[] lengths;
        // Cursor for the readVInt(byte[], int[]) overload, reused across block decodes to avoid allocation.
        private final int[] cursor = new int[1];

        Reader(ChunkedBytesReader chunks, LongValues offsets, long numValues, int valuesPerBlock) {
            this.chunks = chunks;
            this.offsets = offsets;
            this.numValues = numValues;
            this.valuesPerBlock = valuesPerBlock;
            // A block holds a power of two values, so the block a value is in and where it sits inside it are
            // a shift and a mask rather than a division and a multiplication, on a path taken once a value.
            assert valuesPerBlock > 0 && (valuesPerBlock & (valuesPerBlock - 1)) == 0
                : "values per block must be a power of two, got " + valuesPerBlock;
            this.blockShift = Integer.numberOfTrailingZeros(valuesPerBlock);
            this.blockMask = valuesPerBlock - 1;
            this.starts = new int[valuesPerBlock];
            this.lengths = new int[valuesPerBlock];
        }

        public long numValues() {
            return numValues;
        }

        /** Points {@code dst} at the value at {@code valueAddress}; the bytes are valid until the next call. */
        public void get(long valueAddress, BytesRef dst) throws IOException {
            read(valueAddress, dst);
        }

        /**
         * Places the value's bytes in {@code dst} and returns where they begin, as a token comparable only
         * within one reader. Two addresses hold the same stored bytes when this and {@code dst.length} both
         * match: a run is stored once and every value of it points at that one copy, so a caller tells a
         * repeat from a new value without comparing any bytes. The length is part of it because a value of
         * no bytes begins where the value stored after it does.
         *
         * <p>Two equal values stored apart - in different blocks, or in a block that did not take the run
         * form - answer differently, so a caller comparing them treats a repeat as new: correct, slower.
         */
        public long read(long valueAddress, BytesRef dst) throws IOException {
            assert valueAddress >= 0 && valueAddress < numValues : valueAddress + " out of [0, " + numValues + ")";
            final long blockIndex = valueAddress >>> blockShift;
            ensureBlock(blockIndex);
            final int within = (int) (valueAddress & blockMask);
            dst.bytes = block.bytes;
            dst.offset = starts[within];
            dst.length = lengths[within];
            return (blockIndex << 32) | Integer.toUnsignedLong(starts[within]);
        }

        private void ensureBlock(long blockIndex) throws IOException {
            if (blockIndex == cachedBlock) {
                return;
            }
            decodeBlock(blockIndex);
            cachedBlock = blockIndex;
        }

        /** Reads a block's lengths, then walks them to place its values; no value's bytes are examined. */
        private int decodeBlock(long blockIndex) throws IOException {
            final long start = offsets.get(blockIndex);
            final int span = (int) (offsets.get(blockIndex + 1) - start);
            chunks.span(start, span, block);
            final byte[] bytes = block.bytes;
            final BlockLayout layout = BlockLayout.fromId(bytes[block.offset]);
            if (layout == null) {
                throw new CorruptIndexException("unknown block layout [" + (bytes[block.offset] & 0xFF) + "]", chunks.toString());
            }
            final long first = blockIndex << blockShift;
            final int count = (int) Math.min(valuesPerBlock, numValues - first);
            return switch (layout) {
                case RUNS -> {
                    cursor[0] = block.offset + 1;
                    final int runCount = ByteArrayInts.readVInt(bytes, cursor);
                    if (runLengths.length < runCount) {
                        runLengths = new int[runCount];
                        runRepeats = new int[runCount];
                    }
                    for (int r = 0; r < runCount; r++) {
                        runLengths[r] = ByteArrayInts.readVInt(bytes, cursor);
                        runRepeats[r] = ByteArrayInts.readVInt(bytes, cursor);
                    }
                    int at = cursor[0];
                    // Every value of a run points at the one copy of its bytes, so the run is expanded without
                    // the bytes being duplicated.
                    int position = at;
                    int value = 0;
                    for (int r = 0; r < runCount; r++) {
                        for (int k = 0; k < runRepeats[r]; k++) {
                            starts[value] = position;
                            lengths[value] = runLengths[r];
                            value++;
                        }
                        position += runLengths[r];
                    }
                    yield count;
                }
                case INLINE -> {
                    cursor[0] = block.offset + 1;
                    for (int i = 0; i < count; i++) {
                        final int length = ByteArrayInts.readVInt(bytes, cursor);
                        starts[i] = cursor[0];
                        lengths[i] = length;
                        cursor[0] += length;
                    }
                    yield count;
                }
                case PACKED -> {
                    // [bitsPerValue] [bit-packed lengths] [value bytes]
                    final int bits = bytes[block.offset + 1] & 0xFF;
                    final int headerBytes = ByteArrayInts.bitPackedLength(count, bits);
                    ByteArrayInts.readBitPacked(bytes, block.offset + 2, count, bits, lengths);
                    int position = block.offset + 2 + headerBytes;
                    for (int i = 0; i < count; i++) {
                        starts[i] = position;
                        position += lengths[i];
                    }
                    yield count;
                }
            };
        }
    }

    private ValueStream() {}
}
