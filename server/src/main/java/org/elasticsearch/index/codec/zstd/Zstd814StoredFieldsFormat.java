/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;

/**
 * {@link org.apache.lucene.codecs.StoredFieldsFormat} that compresses blocks of data using ZStandard.
 *
 * Unlike Lucene's default stored fields format, this format does not make use of dictionaries (even though ZStandard has great support for
 * dictionaries!). This is mostly due to the fact that LZ4/DEFLATE have short sliding windows that they can use to find duplicate strings
 * (64kB and 32kB respectively). In contrast, ZSTD doesn't have such a limitation and can better take advantage of large compression
 * buffers.
 */
public final class Zstd814StoredFieldsFormat extends Lucene90CompressingStoredFieldsFormat {

    // ZSTD has special optimizations for inputs that are less than 16kB and less than 256kB. So subtract a bit of memory from 16kB and
    // 256kB to make our inputs unlikely to grow beyond 16kB for BEST_SPEED and 256kB for BEST_COMPRESSION.
    private static final int BEST_SPEED_BLOCK_SIZE = (16 - 2) * 1_024;
    private static final int BEST_COMPRESSION_BLOCK_SIZE = (256 - 16) * 1_024;

    public enum Mode {
        BEST_SPEED(0, BEST_SPEED_BLOCK_SIZE, 128),
        BEST_COMPRESSION(3, BEST_COMPRESSION_BLOCK_SIZE, 2048);

        final int level, blockSizeInBytes, blockDocCount;

        private Mode(int level, int blockSizeInBytes, int blockDocCount) {
            this.level = level;
            this.blockSizeInBytes = blockSizeInBytes;
            this.blockDocCount = blockDocCount;
        }
    }

    public Zstd814StoredFieldsFormat(Mode mode) {
        this(mode.level, mode.blockSizeInBytes, mode.blockDocCount);
    }

    Zstd814StoredFieldsFormat(int level, int blockSizeInBytes, int blockDocCount) {
        super("ZstdStoredFields814", new ZstdCompressionMode(level), blockSizeInBytes, blockDocCount, 10);
    }

    private static class ZstdCompressionMode extends CompressionMode {
        private final int level;

        ZstdCompressionMode(int level) {
            this.level = level;
        }

        @Override
        public Compressor newCompressor() {
            return new ZstdCompressor(level);
        }

        @Override
        public Decompressor newDecompressor() {
            return new ZstdDecompressor();
        }

        @Override
        public String toString() {
            return "ZSTD(level=" + level + ")";
        }
    }

    private static final class ZstdDecompressor extends Decompressor {

        final byte[] copyBuffer = new byte[4096];

        ZstdDecompressor() {}

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            if (originalLength == 0) {
                bytes.offset = 0;
                bytes.length = 0;
                return;
            }

            final NativeAccess nativeAccess = NativeAccess.instance();
            final Zstd zstd = nativeAccess.getZstd();

            final int compressedLength = in.readVInt();

            try (
                CloseableByteBuffer src = NativeAccess.instance().newBuffer(compressedLength);
                CloseableByteBuffer dest = NativeAccess.instance().newBuffer(originalLength)
            ) {

                while (src.buffer().position() < compressedLength) {
                    final int numBytes = Math.min(copyBuffer.length, compressedLength - src.buffer().position());
                    in.readBytes(copyBuffer, 0, numBytes);
                    src.buffer().put(copyBuffer, 0, numBytes);
                }
                src.buffer().flip();

                final int decompressedLen = zstd.decompress(dest.buffer(), src.buffer());
                if (decompressedLen != originalLength) {
                    throw new CorruptIndexException("Expected " + originalLength + " decompressed bytes, got " + decompressedLen, in);
                }

                bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, length);
                dest.buffer().get(offset, bytes.bytes, 0, length);
                bytes.offset = 0;
                bytes.length = length;
            }
        }

        @Override
        public Decompressor clone() {
            return new ZstdDecompressor();
        }
    }

    private static class ZstdCompressor extends Compressor {

        final int level;
        final byte[] copyBuffer = new byte[4096];

        ZstdCompressor(int level) {
            this.level = level;
        }

        @Override
        public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
            final NativeAccess nativeAccess = NativeAccess.instance();
            final Zstd zstd = nativeAccess.getZstd();

            final int srcLen = Math.toIntExact(buffersInput.length());
            if (srcLen == 0) {
                return;
            }

            final int compressBound = zstd.compressBound(srcLen);

            try (
                CloseableByteBuffer src = NativeAccess.instance().newBuffer(srcLen);
                CloseableByteBuffer dest = NativeAccess.instance().newBuffer(compressBound)
            ) {

                while (buffersInput.position() < buffersInput.length()) {
                    final int numBytes = Math.min(copyBuffer.length, (int) (buffersInput.length() - buffersInput.position()));
                    buffersInput.readBytes(copyBuffer, 0, numBytes);
                    src.buffer().put(copyBuffer, 0, numBytes);
                }
                src.buffer().flip();

                final int compressedLen = zstd.compress(dest.buffer(), src.buffer(), level);
                out.writeVInt(compressedLen);

                for (int written = 0; written < compressedLen;) {
                    final int numBytes = Math.min(copyBuffer.length, compressedLen - written);
                    dest.buffer().get(copyBuffer, 0, numBytes);
                    out.writeBytes(copyBuffer, 0, numBytes);
                    written += numBytes;
                    assert written == dest.buffer().position();
                }
            }
        }

        @Override
        public void close() throws IOException {}
    }
}
