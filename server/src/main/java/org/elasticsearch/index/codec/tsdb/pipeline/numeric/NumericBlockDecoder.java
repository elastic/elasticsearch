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

import java.io.IOException;

public final class NumericBlockDecoder {

    private final NumericPipeline pipeline;

    NumericBlockDecoder(final NumericPipeline pipeline) {
        this.pipeline = pipeline;
    }

    public int decode(long[] values, final DataInput in) throws IOException {
        return pipeline.decode(values, in);
    }
}
