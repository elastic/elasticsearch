/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.zstd.Zstd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Compresses a chunk with Zstd through the native binding.
 *
 * <p>Both directions hand libzstd memory it addresses directly. A chunk is compressed out of, and
 * decompressed into, a heap {@code byte[]} passed as a {@link MemorySegment} through a critical downcall, so
 * neither direction stages the chunk in an off-heap buffer. On the read side the compressed bytes are taken
 * as a slice of the mapped file where the input can provide one, which leaves the decode with no copy on
 * either end; inputs that cannot (a compound file wrapper, an evicted blob-cache region) fall back to
 * reading the chunk into a buffer.
 *
 * <p>{@link Decompressor} decodes through a {@link Zstd.DCtx} it holds for its own lifetime rather than
 * libzstd's parameterless one-shot decompress, so the many chunks one reader decodes over its lifetime
 * reuse a single native context instead of paying libzstd's implicit alloc/free of a fresh one on every
 * chunk. See {@link Zstd.DCtx} for why that context does not need an explicit close from this class.
 *
 * <p>The binding itself is thread-safe for decompression and is shared. The buffers are not, so each
 * compressor and decompressor holds its own and belongs to a single writer or reader.
 */
final class ZstdChunkCodec {

    private static final int LEVEL = 1;

    private static final Zstd ZSTD = Zstd.instance();

    private ZstdChunkCodec() {}

    static final class Compressor implements ChunkCompressor {

        private byte[] scratch = new byte[0];

        @Override
        public int write(byte[] src, int length, DataOutput out) throws IOException {
            final int bound = ZSTD.compressBound(length);
            scratch = ArrayUtil.growNoCopy(scratch, bound);
            final int compressed = ZSTD.compress(scratch, 0, scratch.length, src, 0, length, LEVEL);
            out.writeBytes(scratch, 0, compressed);
            return compressed;
        }
    }

    static final class Decompressor implements ChunkDecompressor {

        private byte[] scratch = new byte[0];
        // Reused across every chunk this decompressor ever reads, instead of libzstd allocating and
        // freeing a fresh ZSTD_DCtx on every single decompress call (see Zstd.DCtx's javadoc). Needs no
        // explicit close: ChunkDecompressor has no close lifecycle of its own (a Decompressor is created
        // fresh per reader, and readers are created fresh per Lucene getBinary() call with nothing further
        // up that chain ever explicitly closed either), so this relies on Zstd.DCtx's own GC-driven release.
        private final Zstd.DCtx dctx = ZSTD.newDCtx();

        @Override
        public void read(IndexInput in, int storedLength, byte[] dst, int uncompressedLength) throws IOException {
            final MemorySegment target = MemorySegment.ofArray(dst).asSlice(0, uncompressedLength);
            final long start = in.getFilePointer();
            int decompressed = -1;
            if (IndexInputUtils.canUseSegmentSlices(in)) {
                try {
                    decompressed = IndexInputUtils.withSlice(in, storedLength, this::scratch, src -> dctx.decompress(target, src));
                } catch (@SuppressWarnings("unused") AlreadyClosedException e) {
                    // The region backing the slice was evicted mid-read; rewind and take the copying path.
                    in.seek(start);
                }
            }
            if (decompressed < 0) {
                scratch = ArrayUtil.growNoCopy(scratch, storedLength);
                in.readBytes(scratch, 0, storedLength);
                decompressed = dctx.decompress(target, MemorySegment.ofArray(scratch).asSlice(0, storedLength));
            }
            if (decompressed != uncompressedLength) {
                throw new IOException("chunk decompressed to " + decompressed + " bytes, expected " + uncompressedLength);
            }
        }

        /** Backs {@link IndexInputUtils#withSlice}'s copying path with the buffer this decompressor holds. */
        private byte[] scratch(int length) {
            scratch = ArrayUtil.growNoCopy(scratch, length);
            return scratch;
        }
    }
}
