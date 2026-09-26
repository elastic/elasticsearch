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

import java.io.IOException;

/** Reads chunks for one {@link ChunkedBytesReader}; holds that reader's buffers and is not shared. */
public interface ChunkDecompressor {

    /**
     * Reads a chunk written by {@link ChunkCompressor#write}, given {@code in} positioned at its first byte.
     *
     * @param storedLength       bytes the chunk occupies in the file
     * @param dst                buffer to hold the chunk, at least {@code uncompressedLength} long
     * @param uncompressedLength bytes the chunk holds once decoded
     */
    void read(IndexInput in, int storedLength, byte[] dst, int uncompressedLength) throws IOException;
}
