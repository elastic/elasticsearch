/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;

import java.io.Closeable;
import java.io.IOException;

/**
 * Serializes values to bytes as the terminal stage of the encode pipeline.
 * Extends Closeable for stages that hold native resources (e.g., Zstd buffers).
 */
public interface PayloadEncoder extends Closeable {

    /** Returns the unique stage identifier. */
    byte id();

    /** Serializes values to the output stream. */
    void encode(long[] values, int valueCount, DataOutput out, EncodingContext context) throws IOException;

    /** Whether this stage holds resources that require explicit close. */
    default boolean requiresExplicitClose() {
        return false;
    }

    @Override
    default void close() throws IOException {}
}
