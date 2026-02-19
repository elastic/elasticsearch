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
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestBlockBuilder;

public class ResultBuilderForTDigest implements ResultBuilder {
    private final TDigestBlockBuilder builder;
    private final ResultBuilderForTDigest.ReusableTopNEncoderInput reusableInput = new ReusableTopNEncoderInput();

    ResultBuilderForTDigest(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newTDigestBlockBuilder(positions);
    }

    @Override
    public void decodeKey(BytesRef keys, boolean asc) {
        throw new AssertionError("TDigest can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        assert count == 1 : "TDigest does not support multi values";
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

    private static final class ReusableTopNEncoderInput implements TDigestBlock.SerializedTDigestInput {
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
