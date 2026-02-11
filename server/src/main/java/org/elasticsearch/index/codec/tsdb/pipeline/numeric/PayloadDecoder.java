/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.apache.lucene.store.DataInput;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;

import java.io.Closeable;
import java.io.IOException;

/**
 * Deserializes bytes back to values as the terminal stage of the decode pipeline.
 * Extends Closeable for stages that hold native resources (e.g., Zstd buffers).
 */
public interface PayloadDecoder extends Closeable {

    /** Returns the unique stage identifier. */
    byte id();

    /**
     * Deserializes bytes back to values.
     *
     * @return the value count
     */
    int decode(long[] values, DataInput in, DecodingContext context) throws IOException;

    /** Whether this stage holds resources that require explicit close. */
    default boolean requiresExplicitClose() {
        return false;
    }

    @Override
    default void close() throws IOException {}
}
