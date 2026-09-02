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
import org.elasticsearch.compute.data.LongRangeBlockBuilder;

public class ResultBuilderForLongRange implements ResultBuilder {

    private final LongRangeBlockBuilder builder;

    ResultBuilderForLongRange(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newLongRangeBlockBuilder(positions);
    }

    @Override
    public void decodeKey(BytesRef keys, boolean asc) {
        throw new AssertionError("LongRangeBlock can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        switch (count) {
            case 0 -> builder.appendNull();
            case 1 -> decodeOneLongRange(values);
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    decodeOneLongRange(values);
                }
                builder.endPositionEntry();
            }
        }
    }

    private void decodeOneLongRange(BytesRef values) {
        long from = TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(values);
        long to = TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(values);
        builder.appendLongRange(from, to);
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
        return "ResultBuilderForLongRange";
    }

    @Override
    public void close() {
        builder.close();
    }
}
