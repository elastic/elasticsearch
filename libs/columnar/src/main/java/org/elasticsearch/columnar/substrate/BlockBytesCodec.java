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
 * The terminal, type-agnostic byte-stream stage applied to a column's encoded blocks. The concrete codec
 * is chosen per column and recorded by {@link #id()} in the metadata; the identity codec (id {@code 0})
 * writes the block through unchanged. Ids are frozen once shipped.
 */
public interface BlockBytesCodec {

    /** Identity codec id: the block bytes are stored verbatim. */
    byte IDENTITY_ID = 0;

    /** Frozen identifier persisted in column metadata. Never reuse or repurpose an id. */
    byte id();

    /**
     * Emits one block's bytes by running {@code encoder} into {@code out}. The number of bytes written
     * is delimited by the column's block offsets, so no length prefix is required.
     */
    void write(BlockEncoder encoder, DataOutput out) throws IOException;

    /**
     * Returns a {@link DataInput} over one block's bytes, given {@code in} positioned at the block's
     * first byte and the block region's byte {@code length}.
     */
    DataInput read(IndexInput in, int length) throws IOException;

    /** Produces the raw bytes of a single block. */
    @FunctionalInterface
    interface BlockEncoder {
        void encode(DataOutput out) throws IOException;
    }

    /** Resolves a codec from its persisted id. */
    static BlockBytesCodec forId(byte id) {
        if (id == IDENTITY_ID) {
            return IdentityBlockBytesCodec.INSTANCE;
        }
        throw new IllegalArgumentException("Unknown block-bytes codec id: " + id);
    }
}
