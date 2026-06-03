/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Reads the terms-dictionary blocks of a single SORTED or SORTED_SET field from a segment.
 *
 * <p>The {@link Decoder} is bound at construction by {@link TermsDictBlockCodec#createReader},
 * which inspects the field and the segment-level {@link TermsDictReadContext} to pick LZ4
 * ({@link LZ4TermsDictDecoder}) or raw ({@link RawTermsDictDecoder}). The block-iteration code
 * holds the same decoder for the lifetime of the field reader.
 */
public interface TermsDictFieldReader {

    Decoder decoder();

    /**
     * Decodes one terms-dictionary block.
     *
     * <p>The TSDB doc-values format stores each block as a VInt length followed by a body. The
     * body is LZ4-compressed against the block's first term (the dictionary prefix) for most
     * fields and written raw for the {@code _tsid} field when LZ4 skip is enabled. The header
     * and body are read through separate methods so the wire-format header (a single VInt block
     * length) stays invariant across implementations while the body decoding may vary.
     */
    interface Decoder {

        /**
         * Reads the per-block header and returns the uncompressed body length.
         */
        int readHeader(IndexInput data) throws IOException;

        /**
         * Reads one block body from {@code data} into {@code outputBuffer} at {@code outputOffset}.
         *
         * <p>LZ4 implementations decompress using the first {@code outputOffset} bytes of
         * {@code outputBuffer} as the dictionary prefix; raw implementations copy the body bytes
         * verbatim. The dictionary prefix must already be present at
         * {@code outputBuffer[0..outputOffset)} when this method is called.
         */
        void readBody(IndexInput data, int blockLength, byte[] outputBuffer, int outputOffset) throws IOException;
    }
}
