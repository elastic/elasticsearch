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

/**
 * Provides access to the values stored in an {@link ExponentialHistogramBlock} as {@link ExponentialHistogram}s.
 */
public class ExponentialHistogramBlockAccessor {

    private final ExponentialHistogramBlock block;
    private BytesRef tempBytesRef;
    private CompressedExponentialHistogram reusedHistogram;

    public ExponentialHistogramBlockAccessor(ExponentialHistogramBlock block) {
        this.block = block;
    }

    /**
     * Returns the {@link ExponentialHistogram} at the given value index.
     * The return value of this method is reused across invocations, so callers should
     * not retain a reference to it.
     * In addition, the returned histogram must not be used after the block is released.
     *
     * @param valueIndex, should be obtained via {@link ExponentialHistogramBlock#getFirstValueIndex(int)}.
     * @return null if the the value stored in the block
     */
    public ExponentialHistogram get(int valueIndex) {
        assert block.isNull(valueIndex) == false;
        assert block.isReleased() == false;
        ExponentialHistogramArrayBlock arrayBlock = (ExponentialHistogramArrayBlock) block;
        if (reusedHistogram == null) {
            reusedHistogram = new CompressedExponentialHistogram();
        }
        if (tempBytesRef == null) {
            tempBytesRef = new BytesRef();
        }
        arrayBlock.loadValue(valueIndex, reusedHistogram, tempBytesRef);
        return reusedHistogram;
    }

    /**
     * Encodes and appends a histogram value, so that it can be later deserialized
     * via {@link ExponentialHistogramBlockBuilder#deserializeAndAppend(ExponentialHistogramBlock.SerializedInput)}.
     *
     * @param output the output to deserialize into
     */
    public void serializeValue(int valueIndex, ExponentialHistogramBlock.SerializedOutput output) {
        assert block.isNull(valueIndex) == false;
        assert block.isReleased() == false;
        ExponentialHistogramArrayBlock arrayBlock = (ExponentialHistogramArrayBlock) block;
        if (tempBytesRef == null) {
            tempBytesRef = new BytesRef();
        }
        arrayBlock.serializeValue(valueIndex, output, tempBytesRef);
    }

}
