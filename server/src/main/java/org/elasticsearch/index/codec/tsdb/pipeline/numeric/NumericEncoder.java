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
 * Transforms numeric values in-place during encoding.
 * Writes transformation parameters to the encoding context metadata.
 */
public interface NumericEncoder {

    /**
     * Returns the unique stage identifier.
     *
     * @return the stage ID byte
     */
    byte id();

    /**
     * Returns a human-readable name for the stage.
     *
     * @return the stage name
     */
    String name();

    /**
     * Transforms values in-place and writes metadata to the context.
     *
     * @param values the values to transform (modified in-place)
     * @param valueCount the number of values to process
     * @param context the encoding context for writing metadata
     * @return the new value count after transformation
     * @throws IOException if an I/O error occurs
     */
    int encode(long[] values, int valueCount, EncodingContext context) throws IOException;
}
