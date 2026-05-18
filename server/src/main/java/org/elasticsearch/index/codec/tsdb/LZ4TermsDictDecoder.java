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
import org.apache.lucene.util.compress.LZ4;

import java.io.IOException;

/**
 * {@link TermsDictFieldReader.Decoder} that LZ4-decompresses each block body, using the block's
 * first term materialized at the start of the output buffer as the dictionary prefix. Stateless
 * and shared via {@link #INSTANCE}.
 */
public final class LZ4TermsDictDecoder implements TermsDictFieldReader.Decoder {

    public static final LZ4TermsDictDecoder INSTANCE = new LZ4TermsDictDecoder();

    private LZ4TermsDictDecoder() {}

    @Override
    public int readHeader(final IndexInput data) throws IOException {
        return data.readVInt();
    }

    @Override
    public void readBody(final IndexInput data, final int blockLength, final byte[] outputBuffer, final int outputOffset)
        throws IOException {
        LZ4.decompress(data, blockLength, outputBuffer, outputOffset);
    }
}
