/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

public class ExponentialHistogramBlockAccessor {

    private final ExponentialHistogramBlock block;
    private BytesRef tempBytesRef;
    private CompressedExponentialHistogram reusedHistogram;

    public ExponentialHistogramBlockAccessor(ExponentialHistogramBlock block) {
        this.block = block;
    }

    public ExponentialHistogram get(int position) {
        if (block.isNull(position)) {
            return null;
        }
        ExponentialHistogramArrayBlock arrayBlock = (ExponentialHistogramArrayBlock) block;
        if (reusedHistogram == null) {
            tempBytesRef = new BytesRef();
            reusedHistogram = new CompressedExponentialHistogram();
        }
        arrayBlock.loadAtPostion(position, reusedHistogram, tempBytesRef);
        return reusedHistogram;
    }

}
