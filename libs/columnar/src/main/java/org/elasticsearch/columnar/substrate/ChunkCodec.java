/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

/**
 * How a chunk of a column's byte stream is stored. A chunk is a byte-bounded unit of compression: it holds
 * whole values, and a value is located by its offset in the uncompressed stream plus the index that maps
 * such an offset to the chunk holding it.
 *
 * <p>The codec itself is stateless and shared. The buffers a codec needs belong to the compressor and
 * decompressor it hands out, one per writer and per reader, because a segment is read by many threads at
 * once and a buffer cannot be shared between them.
 */
public enum ChunkCodec {

    /** Chunk bytes are stored verbatim, which lets a reader take values straight from the mapped input. */
    IDENTITY((byte) 0) {
        @Override
        public ChunkCompressor newCompressor() {
            return IdentityChunkCodec.COMPRESSOR;
        }

        @Override
        public ChunkDecompressor newDecompressor() {
            return IdentityChunkCodec.DECOMPRESSOR;
        }
    },

    /** Chunk bytes are Zstd-compressed through the native binding. */
    ZSTD((byte) 1) {
        @Override
        public ChunkCompressor newCompressor() {
            return new ZstdChunkCodec.Compressor();
        }

        @Override
        public ChunkDecompressor newDecompressor() {
            return new ZstdChunkCodec.Decompressor();
        }
    };

    private final byte id;

    ChunkCodec(byte id) {
        this.id = id;
    }

    /** Frozen identifier persisted in column metadata. Never reuse or repurpose an id. */
    public byte id() {
        return id;
    }

    /** Whether a chunk is stored uncompressed, so its bytes can be read without decoding the chunk. */
    public boolean isIdentity() {
        return this == IDENTITY;
    }

    /** A compressor for one writer; not shared between them. */
    public abstract ChunkCompressor newCompressor();

    /** A decompressor for one reader; not shared between them. */
    public abstract ChunkDecompressor newDecompressor();

    public static ChunkCodec forId(byte id) {
        for (ChunkCodec codec : values()) {
            if (codec.id == id) {
                return codec;
            }
        }
        throw new IllegalArgumentException("Unknown chunk codec id: " + id);
    }
}
