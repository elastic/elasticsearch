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
 * Reverses numeric value transformations during the decode pipeline.
 * Reads transformation parameters from the decoding context metadata.
 */
public interface TransformDecoder {

    /** Returns the unique stage identifier. */
    byte id();

    /**
     * Reverses the transformation in-place using metadata from the context.
     *
     * @return the new value count after restoration
     */
    int decode(long[] values, int valueCount, DecodingContext context) throws IOException;
}
