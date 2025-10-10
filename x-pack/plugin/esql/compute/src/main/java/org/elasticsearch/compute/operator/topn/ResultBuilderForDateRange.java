/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.DateRangeBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.index.mapper.BlockLoader;

import java.util.List;

public class ResultBuilderForDateRange implements ResultBuilder {

    private final DateRangeBlockBuilder builder;

    ResultBuilderForDateRange(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newDateRangeBlockBuilder(positions);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        throw new AssertionError("DateRangeBlock can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        if (count == 0) {
            builder.appendNull();
        } else {
            builder.from().appendLong(TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(values));
            builder.to().appendLong(TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(values));
        }
    }

    @Override
    public Block build() {
        return builder.build();
    }

    @Override
    public String toString() {
        return "ResultBuilderForDateRange";
    }

    @Override
    public void close() {
        builder.close();
    }
}
