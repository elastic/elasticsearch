/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;

/**
 * Compresses a block's packed bytes, through the same compressor a chunk of values goes through. Bit packing
 * prices a block by its widest value, so values that repeat still cost that width apiece; compressing the
 * packed bytes reaches the repetition the width cannot.
 *
 * <p>A chunk is told its uncompressed length by the chunk index, and a block has nowhere to be told, so one
 * is written ahead of the bytes. A block whose bytes do not compress is stored plain, and then costs only
 * the marker.
 *
 * <p>Holds scratch, so a reader and a writer each take their own rather than sharing one. The input a read
 * hands back views that scratch, so it is only good until the next block is read.
 */
final class ZstdBlockBytesCodec implements BlockBytesCodec {

    /** Which of the two shapes the stored bytes take, written ahead of them. */
    private static final byte STORED_PLAIN = 0;
    private static final byte STORED_COMPRESSED = 1;

    private final ChunkCompressor compressor = ChunkCodec.ZSTD.newCompressor();
    private final ChunkDecompressor decompressor = ChunkCodec.ZSTD.newDecompressor();
    private final Scratch block = new Scratch();
    private final Scratch compressed = new Scratch();
    private final ByteArrayDataInput decompressedInput = new ByteArrayDataInput();
    private byte[] decompressed = new byte[0];

    @Override
    public byte id() {
        return ZSTD_ID;
    }

    @Override
    public void write(BlockEncoder encoder, DataOutput out) throws IOException {
        block.reset();
        encoder.encode(block);
        final int length = block.length;

        compressed.reset();
        final int size = compressor.write(block.bytes, length, compressed);
        if (vIntBytes(length) + size >= length) {
            out.writeByte(STORED_PLAIN);
            out.writeBytes(block.bytes, 0, length);
            return;
        }
        out.writeByte(STORED_COMPRESSED);
        out.writeVInt(length);
        out.writeBytes(compressed.bytes, 0, size);
    }

    @Override
    public DataInput read(IndexInput in, int length) throws IOException {
        final long start = in.getFilePointer();
        if (in.readByte() == STORED_PLAIN) {
            // The rest of the stored bytes are the block itself, read straight from the input.
            return in;
        }
        final int uncompressed = in.readVInt();
        final int stored = length - Math.toIntExact(in.getFilePointer() - start);
        decompressed = ArrayUtil.growNoCopy(decompressed, uncompressed);
        decompressor.read(in, stored, decompressed, uncompressed);
        decompressedInput.reset(decompressed, 0, uncompressed);
        return decompressedInput;
    }

    private static int vIntBytes(int value) {
        return (Integer.SIZE - 1 - Integer.numberOfLeadingZeros(value | 1)) / 7 + 1;
    }

    /** A growable sink, reused across blocks so encoding one allocates nothing. */
    private static final class Scratch extends DataOutput {

        private byte[] bytes = new byte[0];
        private int length;

        void reset() {
            length = 0;
        }

        @Override
        public void writeByte(byte b) {
            bytes = ArrayUtil.grow(bytes, length + 1);
            bytes[length++] = b;
        }

        @Override
        public void writeBytes(byte[] src, int offset, int count) {
            bytes = ArrayUtil.grow(bytes, length + count);
            System.arraycopy(src, offset, bytes, length, count);
            length += count;
        }
    }
}
