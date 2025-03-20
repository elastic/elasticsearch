/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;

public class ResultBuilderForComposite implements ResultBuilder {

    private final AggregateMetricDoubleBlockBuilder builder;

    ResultBuilderForComposite(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newAggregateMetricDoubleBlockBuilder(positions);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        // TODO this is not a clear message (user doesn't need to know that this is a composite block they just think it's an agg metric)
        throw new AssertionError("Composite Block can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        // TODO what do if null
        builder.min().appendDouble(TopNEncoder.DEFAULT_UNSORTABLE.decodeDouble(values));
        builder.max().appendDouble(TopNEncoder.DEFAULT_UNSORTABLE.decodeDouble(values));
        builder.sum().appendDouble(TopNEncoder.DEFAULT_UNSORTABLE.decodeDouble(values));
        builder.count().appendInt(TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(values));
    }

    @Override
    public Block build() {
        return builder.build();
    }

    @Override
    public String toString() {
        return "ValueExtractorForComposite";
    }

    @Override
    public void close() {
        builder.close();
    }
}
