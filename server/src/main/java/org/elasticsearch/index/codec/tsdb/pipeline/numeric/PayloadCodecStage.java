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
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;

import java.io.Closeable;
import java.io.IOException;

/**
 * A terminal pipeline stage that serializes values to bytes during encoding
 * and deserializes bytes back to values during decoding.
 *
 * Unlike {@link NumericCodecStage} which transforms values in-place, this stage
 * writes to DataOutput during encode and reads from DataInput during decode.
 * Terminal stages write their metadata inline with the payload for sequential
 * reading during decode (they are the first stage to run when decoding).
 *
 * Extends Closeable with a default no-op for implementations that hold native
 * resources (e.g., ZstdCodecStage's direct ByteBuffers).
 */
public interface PayloadCodecStage extends Closeable {

    /** Returns the unique stage identifier. */
    byte id();

    /** Returns a human-readable name for the stage. */
    String name();

    /**
     * Serializes values to the output stream. Writes inline metadata followed
     * by payload bytes directly to the output.
     */
    void encode(long[] values, int valueCount, DataOutput out, EncodingContext context) throws IOException;

    /** Deserializes bytes back to values. Reads inline metadata and payload from the input. */
    int decode(long[] values, DataInput in, DecodingContext context) throws IOException;

    @Override
    default void close() throws IOException {}
}
