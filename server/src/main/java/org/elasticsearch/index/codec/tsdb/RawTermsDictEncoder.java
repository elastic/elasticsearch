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
 * {@link TermsDictFieldWriter.Encoder} that writes the block body verbatim, bypassing LZ4 framing.
 * Used for fields whose terms dictionary is incompressible, such as the {@code _tsid} field whose
 * suffix bytes after prefix compression are uniformly random Murmur3 hash bytes. Stateless and
 * shared via {@link #INSTANCE}.
 */
public final class RawTermsDictEncoder implements TermsDictFieldWriter.Encoder {

    public static final RawTermsDictEncoder INSTANCE = new RawTermsDictEncoder();

    private RawTermsDictEncoder() {}

    @Override
    public void writeHeader(final IndexOutput data, final int blockLength) throws IOException {
        data.writeVInt(blockLength);
    }

    @Override
    public void writeBody(final IndexOutput data, final byte[] buffer, final int dictOffset, final int dictLength, final int blockLength)
        throws IOException {
        data.writeBytes(buffer, dictOffset + dictLength, blockLength);
    }
}
