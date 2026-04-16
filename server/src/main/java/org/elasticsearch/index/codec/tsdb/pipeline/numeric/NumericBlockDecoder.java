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
 * Per-field block decoder owning a mutable {@link DecodingContext}.
 *
 * <p>A single instance decodes all blocks for one field. The context holds
 * per-block mutable state (stage bitmap, metadata buffer, data input reference)
 * that is cleared via {@link DecodingContext#clear()} at the start of each
 * block and reused for the next. This zero-allocation-per-block design avoids
 * GC pressure on the decode hot path, but makes the instance NOT thread-safe:
 * concurrent callers must each obtain their own instance via
 * {@link NumericDecoder#newBlockDecoder()}.
 */
public final class NumericBlockDecoder {

    private final NumericDecodePipeline pipeline;
    private final DecodingContext decodingContext;

    NumericBlockDecoder(final NumericDecodePipeline pipeline) {
        this.pipeline = pipeline;
        this.decodingContext = new DecodingContext(pipeline.blockSize(), pipeline.size());
    }

    /**
     * Decodes a block of values by reading the payload and reversing transforms.
     *
     * @param values the output array to populate
     * @param count  the expected number of values
     * @param in     the data input to read from
     * @throws IOException if an I/O error occurs
     */
    public void decode(final long[] values, int count, final DataInput in) throws IOException {
        decodingContext.clear();
        pipeline.decode(values, count, in, decodingContext);
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
