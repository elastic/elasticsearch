/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
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

    /** Attribute key for compression mode. */
    public static final String MODE_KEY = Zstd814StoredFieldsFormat.class.getSimpleName() + ".mode";

    public enum Mode {
        BEST_SPEED(0, BEST_SPEED_BLOCK_SIZE, 128),
        BEST_COMPRESSION(3, BEST_COMPRESSION_BLOCK_SIZE, 2048);

        final int level, blockSizeInBytes, blockDocCount;

        Mode(int level, int blockSizeInBytes, int blockDocCount) {
            this.level = level;
            this.blockSizeInBytes = blockSizeInBytes;
            this.blockDocCount = blockDocCount;
        }
    }

    private final Mode mode;

    public Zstd814StoredFieldsFormat(Mode mode) {
        super("ZstdStoredFields814", new ZstdCompressionMode(mode.level), mode.blockSizeInBytes, mode.blockDocCount, 10);
        this.mode = mode;
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        // Both modes are compatible, we only put an attribute for debug purposes.
        String previous = si.putAttribute(MODE_KEY, mode.name());
        if (previous != null && previous.equals(mode.name()) == false) {
            throw new IllegalStateException(
                "found existing value for " + MODE_KEY + " for segment: " + si.name + "old=" + previous + ", new=" + mode.name()
            );
        }
        return super.fieldsWriter(directory, si, context);
    }

    public Mode getMode() {
        return mode;
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

        // Buffer for copying between the DataInput and native memory. No hard science behind this number, it just tries to be high enough
        // to benefit from bulk copying and low enough to keep heap usage under control.
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
                CloseableByteBuffer src = nativeAccess.newBuffer(compressedLength);
                CloseableByteBuffer dest = nativeAccess.newBuffer(originalLength)
            ) {

                while (src.buffer().position() < compressedLength) {
                    final int numBytes = Math.min(copyBuffer.length, compressedLength - src.buffer().position());
                    in.readBytes(copyBuffer, 0, numBytes);
                    src.buffer().put(copyBuffer, 0, numBytes);
                }
                src.buffer().flip();

                final int decompressedLen = zstd.decompress(dest, src);
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
        // Buffer for copying between the DataInput and native memory. No hard science behind this number, it just tries to be high enough
        // to benefit from bulk copying and low enough to keep heap usage under control.
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

            // NOTE: We are allocating/deallocating native buffers on each call. We could save allocations by reusing these buffers, though
            // this would come at the expense of higher permanent memory usage. Benchmarks suggested that there is some performance to save
            // there, but it wouldn't be a game changer either.
            // Also note that calls to #compress implicitly allocate memory under the hood for e.g. hash tables and chain tables that help
            // identify duplicate strings. So if we wanted to avoid allocating memory on every compress call, we should also look into
            // reusing compression contexts, which are not small and would increase permanent memory usage as well.
            try (
                CloseableByteBuffer src = nativeAccess.newBuffer(srcLen);
                CloseableByteBuffer dest = nativeAccess.newBuffer(compressBound)
            ) {

                while (buffersInput.position() < buffersInput.length()) {
                    final int numBytes = Math.min(copyBuffer.length, (int) (buffersInput.length() - buffersInput.position()));
                    buffersInput.readBytes(copyBuffer, 0, numBytes);
                    src.buffer().put(copyBuffer, 0, numBytes);
                }
                src.buffer().flip();

                final int compressedLen = zstd.compress(dest, src, level);
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
