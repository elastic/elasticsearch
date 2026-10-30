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
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.zstd.Zstd;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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

        private ZstdCompressor(int level) {
            this.level = level;
        }

        @Override
        public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
            final Zstd zstd = Zstd.instance();

            final int srcLen = Math.toIntExact(buffersInput.length());
            if (srcLen == 0) {
                return;
            }

            final int compressBound = zstd.compressBound(srcLen);

            // NOTE: We are allocating/deallocating heap buffers on each call. We could save allocations by reusing these buffers, though
            // this would come at the expense of higher permanent memory usage. Benchmarks suggested that there is some performance to save
            // there, but it wouldn't be a game changer either.
            // Also note that calls to #compress implicitly allocate memory under the hood for e.g. hash tables and chain tables that help
            // identify duplicate strings. So if we wanted to avoid allocating memory on every compress call, we should also look into
            // reusing compression contexts, which are not small and would increase permanent memory usage as well.
            final byte[] src = new byte[srcLen];
            buffersInput.readBytes(src, 0, srcLen);
            final byte[] dest = new byte[compressBound];

            final int compressedLen = zstd.compress(dest, 0, compressBound, src, 0, srcLen, level);
            out.writeVInt(compressedLen);
            out.writeBytes(dest, 0, compressedLen);
        }

        @Override
        public void close() throws IOException {}
    }

    /**
     * Zstd can only decompress a whole block, while Lucene's stored fields reader asks for one document at a time, so reading
     * several documents of the same block would decompress it once per document. To avoid this, once an instance is asked for
     * part of the same block twice in a row, it keeps that block decompressed on heap and copies the following documents out of
     * it. From then on it keeps every block that it is asked for part of, reusing one buffer sized for the largest such block
     * (blocks are smaller than twice the chunk size of the stored fields format). An instance that is never asked for part of
     * the same block twice in a row, e.g. for a get or for scattered search hits, retains nothing and decompresses off-heap.
     * <p>
     * A block is identified by the input it is read from, its file pointer and its decompressed length. This assumes that the
     * bytes at a given file pointer of an input never change, which holds because Lucene files are write-once. An instance must
     * only be used by one thread at a time, like the other state of a stored fields reader: Lucene clones the decompressor
     * along with its input for each reader.
     */
    static final class ZstdDecompressor extends Decompressor {

        // we can safely store and share a single Zstd instance, since we only use thread-safe decompress
        static final Zstd ZSTD = Zstd.instance();

        // the last block decompressed on heap, valid only while cachedInput is not null
        private byte[] cachedBlock;
        private IndexInput cachedInput;
        private long cachedFilePointer;
        private int cachedLength;
        // the block of the last partial read that was decompressed off-heap
        private IndexInput lastInput;
        private long lastFilePointer;

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
            } else if (in instanceof IndexInput indexIn) {
                decompressCached(indexIn, compressedLength, originalLength, offset, length, bytes);
            } else {
                decompressSlice(in, compressedLength, originalLength, offset, length, bytes);
            }
        }

        /**
         * Copies {@code [off, off+len)} of the block whose compressed bytes {@code in} is positioned at into bytes.bytes,
         * decompressing the block on heap unless it is the one already held there. Falls back to {@link #decompressSlice} for
         * the first partial read of a block.
         */
        private void decompressCached(IndexInput in, int cLen, int origLen, int off, int len, BytesRef bytes) throws IOException {
            // identifies the block: the same block is always read from the same file pointer, and other blocks never are
            final long filePointer = in.getFilePointer();
            if (in == cachedInput && filePointer == cachedFilePointer && origLen == cachedLength) {
                in.seek(filePointer + cLen); // leave the input where decompressing the block would have
            } else if (cachedBlock != null || (in == lastInput && filePointer == lastFilePointer)) {
                cachedInput = null; // so that a failure cannot leave a partly overwritten block behind
                cachedBlock = ArrayUtil.growNoCopy(cachedBlock == null ? BytesRef.EMPTY_BYTES : cachedBlock, origLen);
                MemorySegment dst = MemorySegment.ofArray(cachedBlock).asSlice(0, origLen);
                int decompressedLen = decompressInput(in, cLen, dst);
                checkLength(decompressedLen, origLen, in);
                cachedInput = in;
                cachedFilePointer = filePointer;
                cachedLength = origLen;
            } else {
                lastInput = in;
                lastFilePointer = filePointer;
                decompressSlice(in, cLen, origLen, off, len, bytes);
                return;
            }
            bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, len);
            System.arraycopy(cachedBlock, off, bytes.bytes, 0, len);
            bytes.offset = 0;
            bytes.length = len;
        }

        // visible for testing
        boolean hasCachedBlock() {
            return cachedInput != null;
        }

        // visible for testing
        int retainedBytes() {
            return cachedBlock == null ? 0 : cachedBlock.length;
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
                    return IndexInputUtils.withSlice(
                        indexIn,
                        compressedLength,
                        len -> Arena.ofAuto().allocate(len, ValueLayout.JAVA_LONG.byteAlignment()),
                        src -> ZSTD.decompress(dst, src)
                    );
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
            return new ZstdDecompressor(); // starts without a cached block
        }
    }
}
