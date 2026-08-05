/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.foreign.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;
import org.elasticsearch.simdvec.IndexInputUtils;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

public class ZstdCompressionMode extends CompressionMode {
    private final int level;

    public ZstdCompressionMode(int level) {
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

    private static final class ZstdCompressor extends Compressor {

        final int level;
        // Buffer for copying between the DataInput and native memory. No hard science behind this number, it just tries to be high enough
        // to benefit from bulk copying and low enough to keep heap usage under control.
        final byte[] copyBuffer = new byte[4096];

        private ZstdCompressor(int level) {
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
                CloseableByteBuffer src = nativeAccess.newConfinedBuffer(srcLen);
                CloseableByteBuffer dest = nativeAccess.newConfinedBuffer(compressBound)
            ) {
                buffersInput.readBytes(src.buffer(), srcLen);
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

    static final class ZstdDecompressor extends Decompressor {

        // we can safely store and share a single Zstd instance, since we only use thread-safe decompress
        static final Zstd ZSTD = Objects.requireNonNull(NativeAccess.instance().getZstd());

        private ZstdDecompressor() {}

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            if (originalLength == 0) {
                bytes.offset = 0;
                bytes.length = 0;
                return;
            }
            final int compressedLength = in.readVInt();
            if (bytes.bytes.length >= originalLength || isFullDecompress(offset, length, originalLength)) {
                bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, originalLength);
                decompressDirect(in, compressedLength, originalLength, offset, length, bytes);
            } else {
                decompressSlice(in, compressedLength, originalLength, offset, length, bytes);
            }
        }

        private static boolean isFullDecompress(int offset, int length, int originalLength) {
            return offset == 0 && length == originalLength;
        }

        /** Decompress directly into bytes.bytes, then set offset/length view. */
        void decompressDirect(DataInput in, int cLen, int origLen, int off, int len, BytesRef bytes) throws IOException {
            MemorySegment dst = MemorySegment.ofArray(bytes.bytes).asSlice(0, origLen);
            int decompressedLen = decompressInput(in, cLen, dst);
            checkLength(decompressedLen, origLen, in);
            bytes.offset = off;
            bytes.length = len;
        }

        /** Decompress into a temporary off-heap buffer, copy only the needed range into bytes.bytes. */
        void decompressSlice(DataInput in, int cLen, int origLen, int off, int len, BytesRef bytes) throws IOException {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment tmp = arena.allocate(origLen);
                int decompressedLen = decompressInput(in, cLen, tmp);
                checkLength(decompressedLen, origLen, in);
                bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, len);
                MemorySegment.copy(tmp, ValueLayout.JAVA_BYTE, off, bytes.bytes, 0, len);
                bytes.offset = 0;
                bytes.length = len;
            }
        }

        static int decompressInput(DataInput in, int compressedLength, MemorySegment dst) throws IOException {
            if (in instanceof IndexInput indexIn && IndexInputUtils.canUseSegmentSlices(indexIn)) {
                try {
                    return IndexInputUtils.withSlice(indexIn, compressedLength, byte[]::new, src -> ZSTD.decompress(dst, src));
                } catch (@SuppressWarnings("unused") AlreadyClosedException e) {
                    // Region evicted mid-read — fall through to copy path
                }
            }
            return copyAndDecompress(in, compressedLength, dst);
        }

        static int copyAndDecompress(DataInput in, int compressedLength, MemorySegment dst) throws IOException {
            byte[] src = new byte[compressedLength];
            in.readBytes(src, 0, compressedLength);
            return ZSTD.decompress(dst, MemorySegment.ofArray(src));
        }

        static void checkLength(int decompressedLen, int originalLength, DataInput in) throws CorruptIndexException {
            if (decompressedLen != originalLength) {
                throw new CorruptIndexException("Expected " + originalLength + " decompressed bytes, got " + decompressedLen, in);
            }
        }

        @Override
        public Decompressor clone() {
            return new ZstdDecompressor();
        }
    }
}
