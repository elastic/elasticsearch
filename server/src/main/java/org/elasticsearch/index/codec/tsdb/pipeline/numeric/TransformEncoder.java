/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;

import java.io.IOException;

/**
 * Transforms numeric values in-place during the encode pipeline.
 * Writes transformation parameters to the encoding context metadata.
 */
public interface TransformEncoder {

    /** Returns the unique stage identifier. */
    byte id();

    /**
     * Transforms values in-place and writes metadata to the context.
     *
     * @return the new value count after transformation
     */
    int encode(long[] values, int valueCount, EncodingContext context) throws IOException;

    /**
     * Returns the worst-case metadata bytes this stage may write per block.
     * Used to right-size the metadata buffer. Return 0 for stages whose
     * metadata fits within the default buffer capacity (64 bytes).
     */
    default int maxMetadataBytes(int blockSize) {
        return 0;
    }
}
