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

/**
 * Transforms values in-place as a non-terminal stage of the encode pipeline.
 *
 * <p>Unlike {@link PayloadEncoder}, transform stages do not write to a
 * {@link org.apache.lucene.store.DataOutput}. They modify the {@code long[]}
 * array in-place and record any metadata needed for decoding via
 * {@link EncodingContext#metadata()}.
 */
public interface TransformEncoder {

    /**
     * Returns the unique stage identifier.
     *
     * @return the stage ID byte
     */
    byte id();

    /**
     * Transforms values in-place and writes any metadata to the context.
     *
     * <p>If the stage determines that the transformation would not be effective,
     * it may return without modifying the values or writing metadata. The pipeline
     * checks {@link EncodingContext#isStageApplied(int)} to detect this.
     *
     * @param values     the values to transform in-place
     * @param valueCount the number of valid values in the array
     * @param context    the encoding context for metadata and stage tracking
     */
    void encode(long[] values, int valueCount, EncodingContext context);
}
