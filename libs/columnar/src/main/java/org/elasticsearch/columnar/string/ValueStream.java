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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.substrate.ByteArrayInts;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ChunkIndexMetadata;
import org.elasticsearch.columnar.substrate.ChunkedBytesReader;
import org.elasticsearch.columnar.substrate.ChunkedBytesWriter;
import org.elasticsearch.columnar.substrate.MonotonicReader;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.Closeable;
import java.io.IOException;

/**
 * An indexed sequence of byte values, addressed in blocks of {@link #VALUES_PER_BLOCK} values and compressed
 * in chunks of a fixed number of bytes. One offset is recorded per block rather than per value, so reading
 * value {@code i} reads its block and walks the lengths within it — which keeps the offset table a fraction
 * of the size a per-value table would be.
 *
 * <p>A block holds its lengths one of two ways, chosen by how long its values are: beside each value, where
 * a length and its value compress together as one pattern, or packed at the block's head, where walking past
 * a long value to reach the next one would cost more than the packing saves.
 *
 * <p>Blocks and chunks are separate on purpose. A block of long values and a block of short ones are the same
 * count of values and nothing like the same number of bytes, so the unit that is addressed cannot also be the
 * unit that is compressed. A chunk closes only on a block boundary, so no value spans two of them.
 */
public final class ValueStream {

    /** Values behind one offset. Larger trades a longer walk on random access for a smaller offset table. */
    public static final int VALUES_PER_BLOCK = 128;

    /** Marks a block whose lengths sit in front of their own values rather than together at its head. */
    private static final byte INLINE = 0;

    /** Mean value length below which a block keeps its lengths inline. */
    private static final int INLINE_MEAN_LENGTH = 32;

    /** Where a stream's bytes and offsets landed. */
    public record Metadata(long numValues, int valuesPerBlock, ChunkIndexMetadata chunks, MonotonicWriter.Table offsets) {

        public static Metadata empty() {
            return new Metadata(0, VALUES_PER_BLOCK, ChunkIndexMetadata.empty(), MonotonicWriter.Table.NONE);
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeVLong(numValues);
            if (numValues == 0) {
                return;
            }
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
            final int valuesPerBlock = in.readVInt();
            final ChunkIndexMetadata chunks = ChunkIndexMetadata.readFrom(in);
            final long dataOffset = in.readVLong();
            final long dataLength = in.readVLong();
            final byte[] meta = new byte[in.readVInt()];
            in.readBytes(meta, 0, meta.length);
            return new Metadata(numValues, valuesPerBlock, chunks, new MonotonicWriter.Table(dataOffset, dataLength, meta));
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
        private long count = 0;
        private boolean closed = false;
        // A block's lengths are written ahead of its bytes, so the block is buffered until it is full. It
        // holds valuesPerBlock values, which is bounded and independent of the column.
        private final int[] pending;
        private byte[] pendingBytes = new byte[1024];
        // Holds a block's length header, or one value's length as a vint, so neither is allocated per block.
        private byte[] scratch = new byte[0];
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
            IndexOutput data
        ) throws IOException {
            this.valuesPerBlock = valuesPerBlock;
            this.data = data;
            this.chunks = new ChunkedBytesWriter(codec, targetChunkBytes, dir, ctx, prefix, data);
            this.pending = new int[valuesPerBlock];
            final long blocks = (numValues + valuesPerBlock - 1) / valuesPerBlock;
            this.offsets = new MonotonicWriter(dir, ctx, prefix, blocks + 1L);
        }

        public void add(BytesRef value) throws IOException {
            if (pendingBytes.length < pendingLength + value.length) {
                pendingBytes = ArrayUtil.grow(pendingBytes, pendingLength + value.length);
            }
            System.arraycopy(value.bytes, value.offset, pendingBytes, pendingLength, value.length);
            pending[pendingCount++] = value.length;
            pendingLength += value.length;
            count++;
            if (pendingCount == valuesPerBlock) {
                flushBlock();
            }
        }

        /**
         * Emits the buffered block in whichever of the two layouts is smaller for it.
         *
         * <p><b>Packed</b> puts the lengths at one fixed width ahead of the bytes, so the block's values are
         * placed by a running sum of numbers already in hand rather than by a walk that reads each value to
         * find where the next one's length begins. It suits long values.
         *
         * <p><b>Inline</b> keeps each length in front of its own value. It suits short repeated values,
         * because the length and the value then repeat as one pattern that a compressor matches whole —
         * splitting them apart costs more than the walk saves.
         */
        private void flushBlock() throws IOException {
            chunks.boundary();
            offsets.add(chunks.uncompressedLength());
            int max = 0;
            for (int i = 0; i < pendingCount; i++) {
                max = Math.max(max, pending[i]);
            }
            final int width = ByteArrayInts.widthFor(max);
            // Which layout is smaller is decided after compression, so an uncompressed byte count cannot
            // choose between them. What separates them is how long the values are: short ones repeat
            // together with their length as a single pattern, and splitting the two apart costs more than
            // the walk saves. The threshold is where the measured shapes turn over.
            if (pendingLength < pendingCount * INLINE_MEAN_LENGTH) {
                writeInline();
            } else {
                writePacked(width);
            }
            pendingCount = 0;
            pendingLength = 0;
        }

        private void writePacked(int width) throws IOException {
            final int length = 1 + pendingCount * width;
            scratch = ArrayUtil.growNoCopy(scratch, length);
            scratch[0] = (byte) width;
            int at = 1;
            for (int i = 0; i < pendingCount; i++) {
                ByteArrayInts.writeIntLE(pending[i], width, scratch, at);
                at += width;
            }
            chunks.append(scratch, 0, length);
            chunks.append(pendingBytes, 0, pendingLength);
        }

        private void writeInline() throws IOException {
            scratch = ArrayUtil.growNoCopy(scratch, ByteArrayInts.MAX_VINT_BYTES);
            scratch[0] = INLINE;
            chunks.append(scratch, 0, 1);
            int at = 0;
            for (int i = 0; i < pendingCount; i++) {
                final int length = pending[i];
                // The marker is already appended, so scratch is free to hold this value's length.
                final int vintBytes = ByteArrayInts.writeVInt(length, scratch, 0);
                chunks.append(scratch, 0, vintBytes);
                chunks.append(pendingBytes, at, length);
                at += length;
            }
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
            return new Metadata(count, valuesPerBlock, index, offsets.finish(data));
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

    /** Random access by index; a block is decoded once and its value bounds kept for the next lookup. */
    public static final class Reader {

        private final ChunkedBytesReader chunks;
        private final LongValues offsets;
        private final long numValues;
        private final int valuesPerBlock;

        private final BytesRef block = new BytesRef();
        private long cachedBlock = -1;
        private int[] starts;
        private int[] lengths;

        Reader(ChunkedBytesReader chunks, LongValues offsets, long numValues, int valuesPerBlock) {
            this.chunks = chunks;
            this.offsets = offsets;
            this.numValues = numValues;
            this.valuesPerBlock = valuesPerBlock;
            this.starts = new int[valuesPerBlock];
            this.lengths = new int[valuesPerBlock];
        }

        public long numValues() {
            return numValues;
        }

        /** Points {@code dst} at value {@code index}; the bytes are valid until the next call. */
        public void get(long index, BytesRef dst) throws IOException {
            assert index >= 0 && index < numValues : index + " out of [0, " + numValues + ")";
            final long blockIndex = index / valuesPerBlock;
            ensureBlock(blockIndex);
            final int within = (int) (index - blockIndex * valuesPerBlock);
            dst.bytes = block.bytes;
            dst.offset = starts[within];
            dst.length = lengths[within];
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
            final int width = bytes[block.offset];
            final long first = blockIndex * valuesPerBlock;
            final int count = (int) Math.min(valuesPerBlock, numValues - first);
            if (width == INLINE) {
                int position = block.offset + 1;
                for (int i = 0; i < count; i++) {
                    final int length = ByteArrayInts.readVInt(bytes, position);
                    position += ByteArrayInts.vIntLength(length);
                    starts[i] = position;
                    lengths[i] = length;
                    position += length;
                }
                return count;
            }
            int at = block.offset + 1;
            int position = at + count * width;
            for (int i = 0; i < count; i++) {
                final int length = ByteArrayInts.readIntLE(bytes, at, width);
                at += width;
                starts[i] = position;
                lengths[i] = length;
                position += length;
            }
            return count;
        }
    }

    private ValueStream() {}
}
