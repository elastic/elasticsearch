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
 * {@link TermsDictFieldReader.Decoder} that reads each block body verbatim, matching the on-disk
 * format produced by {@link RawTermsDictEncoder}. Stateless and shared via {@link #INSTANCE}.
 */
public final class RawTermsDictDecoder implements TermsDictFieldReader.Decoder {

    public static final RawTermsDictDecoder INSTANCE = new RawTermsDictDecoder();

    private RawTermsDictDecoder() {}

    @Override
    public int readHeader(final IndexInput data) throws IOException {
        return data.readVInt();
    }

    @Override
    public void readBody(final IndexInput data, final int blockLength, final byte[] outputBuffer, final int outputOffset)
        throws IOException {
        data.readBytes(outputBuffer, outputOffset, blockLength);
    }
}
