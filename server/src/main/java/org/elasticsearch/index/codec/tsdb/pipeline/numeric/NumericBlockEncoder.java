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

// NOTE: NumericBlockEncoder is NOT thread-safe. Each instance owns an EncodingContext
// with mutable per-call state (positionBitmap, metadataBuffer, valueCount). Callers that
// may run concurrently must each hold their own encoder instance.
public final class NumericBlockEncoder {

    private final NumericEncodePipeline pipeline;
    private final EncodingContext encodingContext;

    NumericBlockEncoder(final NumericEncodePipeline pipeline) {
        this.pipeline = pipeline;
        this.encodingContext = new EncodingContext(pipeline.blockSize(), pipeline.size(), pipeline.metadataCapacity());
    }

    public void encode(final long[] values, int valueCount, final DataOutput out) throws IOException {
        encodingContext.clear();
        pipeline.encode(values, valueCount, out, encodingContext);
    }

    public int blockSize() {
        return pipeline.blockSize();
    }
}
