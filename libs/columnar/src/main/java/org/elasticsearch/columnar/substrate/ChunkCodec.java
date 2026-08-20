/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * How a chunk of a column's byte stream is stored. A chunk is a byte-bounded unit of compression: it holds
 * whole values, and a value is located by its offset in the uncompressed stream plus the index that maps
 * such an offset to the chunk holding it.
 *
 * <p>Ids are frozen once shipped. The identity codec stores a chunk verbatim, which lets a reader serve
 * values straight from the mapped input with no chunk buffer at all.
 */
public interface ChunkCodec {

    /** Chunk bytes are stored verbatim. */
    byte IDENTITY_ID = 0;

    /** Chunk bytes are Zstd-compressed. */
    byte ZSTD_ID = 1;

    /** Frozen identifier persisted in column metadata. Never reuse or repurpose an id. */
    byte id();

    /** Whether a chunk is stored uncompressed, so its bytes can be read without decoding the chunk. */
    default boolean isIdentity() {
        return false;
    }

    /** Writes {@code src[0, length)} as one chunk and returns how many bytes it occupies in {@code out}. */
    int write(byte[] src, int length, IndexOutput out) throws IOException;

    /**
     * Reads a chunk written by {@link #write}, given {@code in} positioned at its first byte.
     *
     * @param storedLength       bytes the chunk occupies in the file
     * @param dst                buffer to hold the chunk, at least {@code uncompressedLength} long
     * @param uncompressedLength bytes the chunk holds once decoded
     */
    void read(IndexInput in, int storedLength, byte[] dst, int uncompressedLength) throws IOException;

    static ChunkCodec forId(byte id) {
        if (id == IDENTITY_ID) {
            return IdentityChunkCodec.INSTANCE;
        }
        if (id == ZSTD_ID) {
            return ZstdChunkCodec.INSTANCE;
        }
        throw new IllegalArgumentException("Unknown chunk codec id: " + id);
    }
}
