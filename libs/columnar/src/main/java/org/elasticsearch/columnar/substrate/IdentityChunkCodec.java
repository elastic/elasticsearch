/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

/** Stores a chunk's bytes verbatim. Holds no buffers, so one instance serves every reader and writer. */
final class IdentityChunkCodec {

    static final ChunkCompressor COMPRESSOR = (src, length, out) -> {
        out.writeBytes(src, 0, length);
        return length;
    };

    static final ChunkDecompressor DECOMPRESSOR = (in, storedLength, dst, uncompressedLength) -> {
        assert storedLength == uncompressedLength : storedLength + " != " + uncompressedLength;
        in.readBytes(dst, 0, uncompressedLength);
    };

    private IdentityChunkCodec() {}
}
