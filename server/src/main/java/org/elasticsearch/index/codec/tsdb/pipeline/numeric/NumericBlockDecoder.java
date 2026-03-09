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

// NOTE: NumericBlockDecoder is NOT thread-safe. Each instance owns a DecodingContext
// with mutable per-call state (dataInput, positionBitmap). Callers that may run
// concurrently must each hold their own decoder instance.
public final class NumericBlockDecoder {

    private final NumericDecodePipeline pipeline;
    private final DecodingContext decodingContext;

    NumericBlockDecoder(final NumericDecodePipeline pipeline) {
        this.pipeline = pipeline;
        this.decodingContext = new DecodingContext(pipeline.blockSize(), pipeline.size());
    }

    public int decode(final long[] values, final DataInput in) throws IOException {
        decodingContext.clear();
        return pipeline.decode(values, in, decodingContext);
    }

    public int blockSize() {
        return pipeline.blockSize();
    }
}
