/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;

import java.io.IOException;

/**
 * Reverses numeric value transformations during decoding.
 * Reads transformation parameters from the decoding context metadata.
 */
public interface NumericDecoder {

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
     * Reverses the transformation in-place using metadata from the context.
     *
     * @param values the values to restore (modified in-place)
     * @param valueCount the number of values to process
     * @param context the decoding context for reading metadata
     * @return the new value count after restoration
     * @throws IOException if an I/O error occurs
     */
    int decode(long[] values, int valueCount, DecodingContext context) throws IOException;
}
