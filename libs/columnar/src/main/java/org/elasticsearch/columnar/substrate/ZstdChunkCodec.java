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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Compresses a chunk with Zstd through the native binding.
 *
 * <p>Both directions hand libzstd memory it can address directly. A chunk is compressed out of, and
 * decompressed into, a heap {@code byte[]} passed as a {@link MemorySegment} through a critical downcall, so
 * neither direction stages the chunk in an off-heap buffer. On the read side the compressed bytes are taken
 * as a slice of the mapped file where the input can provide one, which leaves the decode with no copy on
 * either end; inputs that cannot (a compound file wrapper, an evicted blob-cache region) fall back to
 * reading the chunk into a reused array.
 */
final class ZstdChunkCodec implements ChunkCodec {

    static final ZstdChunkCodec INSTANCE = new ZstdChunkCodec();

    private static final int LEVEL = 3;

    // Decompression is thread-safe on this binding, so one instance is shared; the scratch buffers are not,
    // which is why a codec instance is only ever handed to one reader or writer at a time.
    private static final Zstd ZSTD = NativeAccess.instance().getZstd();

    private byte[] scratch = new byte[0];

    private ZstdChunkCodec() {}

    @Override
    public byte id() {
        return ZSTD_ID;
    }

    @Override
    public int write(byte[] src, int length, IndexOutput out) throws IOException {
        final int bound = ZSTD.compressBound(length);
        scratch = grow(scratch, bound);
        final int compressed = ZSTD.compress(scratch, 0, scratch.length, src, 0, length, LEVEL);
        out.writeBytes(scratch, 0, compressed);
        return compressed;
    }

    @Override
    public void read(IndexInput in, int storedLength, byte[] dst, int uncompressedLength) throws IOException {
        final MemorySegment target = MemorySegment.ofArray(dst).asSlice(0, uncompressedLength);
        final long start = in.getFilePointer();
        int decompressed = -1;
        if (IndexInputUtils.canUseSegmentSlices(in)) {
            try {
                decompressed = IndexInputUtils.withSlice(in, storedLength, this::scratch, src -> ZSTD.decompress(target, src));
            } catch (@SuppressWarnings("unused") AlreadyClosedException e) {
                // The region backing the slice was evicted mid-read; rewind and take the copying path.
                in.seek(start);
            }
        }
        if (decompressed < 0) {
            scratch = grow(scratch, storedLength);
            in.readBytes(scratch, 0, storedLength);
            decompressed = ZSTD.decompress(target, MemorySegment.ofArray(scratch).asSlice(0, storedLength));
        }
        if (decompressed != uncompressedLength) {
            throw new IOException("chunk decompressed to " + decompressed + " bytes, expected " + uncompressedLength);
        }
    }

    /** Backs {@link IndexInputUtils#withSlice}'s copying path with the buffer this codec already holds. */
    private byte[] scratch(int length) {
        scratch = grow(scratch, length);
        return scratch;
    }

    private static byte[] grow(byte[] buffer, int length) {
        return buffer.length < length ? new byte[ArrayUtil.oversize(length, Byte.BYTES)] : buffer;
    }
}
