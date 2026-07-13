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
 * Per-field block encoder owning a mutable {@link EncodingContext}.
 *
 * <p>A single instance encodes all blocks for one field. The context holds
 * per-block mutable state (stage bitmap, metadata buffer, value count) that is
 * cleared via {@link EncodingContext#clear()} at the start of each block and
 * reused for the next. This zero-allocation-per-block design avoids GC pressure
 * on the encode hot path, but makes the instance NOT thread-safe: concurrent
 * callers must each obtain their own instance via
 * {@link NumericEncoder#newBlockEncoder()}.
 */
public final class NumericBlockEncoder {

    private final NumericEncodePipeline pipeline;
    private final EncodingContext encodingContext;

    NumericBlockEncoder(final NumericEncodePipeline pipeline) {
        this.pipeline = pipeline;
        this.encodingContext = new EncodingContext(pipeline.blockSize(), pipeline.size());
    }

    /**
     * Encodes a block of values through the pipeline.
     *
     * @param values     the values to encode (modified in-place by transform stages)
     * @param valueCount the number of valid values
     * @param out        the data output to write the encoded block to
     * @throws IOException if an I/O error occurs
     */
    public void encode(final long[] values, int valueCount, final DataOutput out) throws IOException {
        encodingContext.clear();
        pipeline.encode(values, valueCount, out, encodingContext);
    }

    /**
     * Returns the number of values per block.
     *
     * @return the number of values per block
     */
    public int blockSize() {
        return pipeline.blockSize();
    }
}
