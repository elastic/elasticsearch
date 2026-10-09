/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * The pass-through {@link BlockBytesCodec}: block bytes are stored and read back verbatim. It is the
 * default terminal stage.
 */
final class IdentityBlockBytesCodec implements BlockBytesCodec {

    static final IdentityBlockBytesCodec INSTANCE = new IdentityBlockBytesCodec();

    private IdentityBlockBytesCodec() {}

    @Override
    public byte id() {
        return IDENTITY_ID;
    }

    @Override
    public void write(BlockEncoder encoder, DataOutput out) throws IOException {
        encoder.encode(out);
    }

    @Override
    public DataInput read(IndexInput in, int length) {
        // The stored bytes are already the raw block; the caller reads them straight from `in`.
        return in;
    }
}
