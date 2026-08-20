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
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;

public class ResultBuilderForDoubleRange implements ResultBuilder {

    private final DoubleRangeBlockBuilder builder;

    ResultBuilderForDoubleRange(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newDoubleRangeBlockBuilder(positions);
    }

    @Override
    public void decodeKey(BytesRef keys, boolean asc) {
        throw new AssertionError("DoubleRangeBlock can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        switch (count) {
            case 0 -> builder.appendNull();
            case 1 -> decodeOneDoubleRange(values);
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    decodeOneDoubleRange(values);
                }
                builder.endPositionEntry();
            }
        }
    }

    private void decodeOneDoubleRange(BytesRef values) {
        double from = TopNEncoder.DEFAULT_UNSORTABLE.decodeDouble(values);
        double to = TopNEncoder.DEFAULT_UNSORTABLE.decodeDouble(values);
        builder.appendDoubleRange(from, to);
    }

    @Override
    public Block build() {
        return builder.build();
    }

    @Override
    public long estimatedBytes() {
        return builder.estimatedBytes();
    }

    @Override
    public String toString() {
        return "ResultBuilderForDoubleRange";
    }

    @Override
    public void close() {
        builder.close();
    }
}
