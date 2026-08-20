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

/** Stores a chunk's bytes verbatim, so a reader can take values straight from the mapped input. */
final class IdentityChunkCodec implements ChunkCodec {

    static final IdentityChunkCodec INSTANCE = new IdentityChunkCodec();

    private IdentityChunkCodec() {}

    @Override
    public byte id() {
        return IDENTITY_ID;
    }

    @Override
    public boolean isIdentity() {
        return true;
    }

    @Override
    public int write(byte[] src, int length, IndexOutput out) throws IOException {
        out.writeBytes(src, 0, length);
        return length;
    }

    @Override
    public void read(IndexInput in, int storedLength, byte[] dst, int uncompressedLength) throws IOException {
        assert storedLength == uncompressedLength : storedLength + " != " + uncompressedLength;
        in.readBytes(dst, 0, uncompressedLength);
    }
}
