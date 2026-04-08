/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.index.mapper.BlockLoader;

import java.util.List;

public class ResultBuilderForAggregateMetricDouble implements ResultBuilder {

    private final AggregateMetricDoubleBlockBuilder builder;

    ResultBuilderForAggregateMetricDouble(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newAggregateMetricDoubleBlockBuilder(positions);
    }

    @Override
    public void decodeKey(PagedBytesCursor keys, boolean asc) {
        throw new AssertionError("AggregateMetricDoubleBlock can't be a key");
    }

    @Override
    public void decodeValue(PagedBytesCursor cursor) {
        int count = cursor.readVInt();
        if (count == 0) {
            builder.appendNull();
            return;
        }
        for (BlockLoader.DoubleBuilder subBuilder : List.of(builder.min(), builder.max(), builder.sum())) {
            if (TopNEncoder.DEFAULT_UNSORTABLE.decodeBoolean(cursor)) {
                subBuilder.appendDouble(TopNEncoder.DEFAULT_UNSORTABLE.decodeDouble(cursor));
            } else {
                subBuilder.appendNull();
            }
        }
        if (TopNEncoder.DEFAULT_UNSORTABLE.decodeBoolean(cursor)) {
            builder.count().appendInt(TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(cursor));
        } else {
            builder.count().appendNull();
        }
    }

    @Override
    public void appendNull() {
        builder.appendNull();
    }

    @Override
    public void appendFromKey() {
        throw new AssertionError("AggregateMetricDoubleBlock can't be a key");
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
        return "ResultBuilderForAggregateMetricDouble";
    }

    @Override
    public void close() {
        builder.close();
    }
}
