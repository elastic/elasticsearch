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

import java.io.IOException;

/**
 * Serializes values to bytes as the terminal stage of the encode pipeline.
 *
 * <p>Unlike {@link NumericEncoder}, payload stages write directly to a
 * {@link org.apache.lucene.store.DataOutput} rather than modifying values in-place.
 */
public interface PayloadEncoder {

    /**
     * Returns the unique stage identifier.
     *
     * @return the stage ID byte
     */
    byte id();

    /**
     * Serializes values to the output stream.
     *
     * @param values the values to encode
     * @param valueCount the number of valid values in the array
     * @param out the data output stream
     * @param context the encoding context with block metadata
     * @throws IOException if an I/O error occurs
     */
    void encode(long[] values, int valueCount, DataOutput out, EncodingContext context) throws IOException;
}
