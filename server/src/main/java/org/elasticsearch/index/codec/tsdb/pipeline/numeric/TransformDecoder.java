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
 * Reverses an in-place transformation as a non-terminal stage of the decode pipeline.
 *
 * <p>Unlike {@link PayloadDecoder}, transform decoders do not read from a
 * {@link org.apache.lucene.store.DataInput} directly. They read metadata written
 * by the corresponding {@link TransformEncoder} via {@link DecodingContext#metadata()}.
 */
public interface TransformDecoder {

    /**
     * Returns the unique stage identifier.
     *
     * @return the stage ID byte
     */
    byte id();

    /**
     * Reverses the transformation on values in-place using metadata from the context.
     *
     * @param values     the values to reverse-transform in-place
     * @param valueCount the number of valid values in the array
     * @param context    the decoding context for reading stage metadata
     * @throws IOException if an I/O error occurs while reading metadata
     */
    void decode(long[] values, int valueCount, DecodingContext context) throws IOException;
}
