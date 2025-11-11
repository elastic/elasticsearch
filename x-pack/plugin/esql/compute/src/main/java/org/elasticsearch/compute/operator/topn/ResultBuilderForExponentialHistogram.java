/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlockBuilder;

public class ResultBuilderForExponentialHistogram implements ResultBuilder {

    private final ExponentialHistogramBlockBuilder builder;
    private final ReusableTopNEncoderInput reusableInput = new ReusableTopNEncoderInput();

    ResultBuilderForExponentialHistogram(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newExponentialHistogramBlockBuilder(positions);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        throw new AssertionError("ExponentialHistogramBlock can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        assert count == 1 : "ExponentialHistogramBlock does not support multi values";
        reusableInput.inputValues = values;
        builder.deserializeAndAppend(reusableInput);
    }

    @Override
    public Block build() {
        return builder.build();
    }

    @Override
    public String toString() {
        return "ResultBuilderForExponentialHistogram";
    }

    @Override
    public void close() {
        builder.close();
    }

    private static final class ReusableTopNEncoderInput implements ExponentialHistogramBlock.SerializedInput {
        BytesRef inputValues;

        @Override
        public double readDouble() {
            return TopNEncoder.DEFAULT_UNSORTABLE.decodeDouble(inputValues);
        }

        @Override
        public long readLong() {
            return TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(inputValues);
        }

        @Override
        public BytesRef readBytesRef(BytesRef scratch) {
            return TopNEncoder.DEFAULT_UNSORTABLE.decodeBytesRef(inputValues, scratch);
        }
    }
}
