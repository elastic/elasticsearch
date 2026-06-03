/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Writes the terms-dictionary blocks of a single SORTED or SORTED_SET field to a segment.
 *
 * <p>The {@link Encoder} is bound at construction by {@link TermsDictBlockCodec#createWriter},
 * which inspects the field and the segment-level {@link TermsDictWriteContext} to pick LZ4
 * ({@link LZ4TermsDictEncoder}) or raw ({@link RawTermsDictEncoder}). The terms-dict
 * serialization loop holds the same encoder for the lifetime of the field writer.
 */
public interface TermsDictFieldWriter {

    Encoder encoder();

    /**
     * Encodes one terms-dictionary block.
     *
     * <p>The TSDB doc-values format stores each block as a VInt length followed by a body. The
     * standard encoder LZ4-compresses the body against the first term of the block; the raw
     * encoder writes the body verbatim. The header and body are written through separate methods
     * so the wire-format header (a single VInt block length) stays invariant across
     * implementations while the body encoding may vary.
     */
    interface Encoder {

        /**
         * Writes the per-block header (the uncompressed block length, as a VInt).
         */
        void writeHeader(IndexOutput data, int blockLength) throws IOException;

        /**
         * Writes one block body to {@code data}.
         *
         * <p>{@code buffer} contains the dictionary prefix in positions
         * {@code [dictOffset, dictOffset + dictLength)} followed by {@code blockLength} body bytes.
         */
        void writeBody(IndexOutput data, byte[] buffer, int dictOffset, int dictLength, int blockLength) throws IOException;
    }
}
