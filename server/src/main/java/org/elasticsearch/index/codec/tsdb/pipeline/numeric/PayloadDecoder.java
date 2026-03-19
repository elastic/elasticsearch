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

import java.io.IOException;

/**
 * Deserializes bytes back to values as the terminal stage of the decode pipeline.
 *
 * <p>Unlike {@link NumericDecoder}, payload stages read directly from a
 * {@link org.apache.lucene.store.DataInput} rather than reading metadata from the context.
 */
public interface PayloadDecoder {

    /**
     * Returns the unique stage identifier.
     *
     * @return the stage ID byte
     */
    byte id();

    /**
     * Deserializes bytes from the input stream back to values.
     *
     * @param values the output array to populate
     * @param in the data input stream
     * @param context the decoding context with block metadata
     * @return the number of values decoded
     * @throws IOException if an I/O error occurs
     */
    int decode(long[] values, DataInput in, DecodingContext context) throws IOException;
}
