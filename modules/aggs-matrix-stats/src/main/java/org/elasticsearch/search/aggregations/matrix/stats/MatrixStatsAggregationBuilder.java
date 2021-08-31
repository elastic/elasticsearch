/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ArrayValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class MatrixStatsAggregationBuilder extends ArrayValuesSourceAggregationBuilder.LeafOnly<MatrixStatsAggregationBuilder> {
    public static final String NAME = "matrix_stats";

    private MultiValueMode multiValueMode = MultiValueMode.AVG;

    public MatrixStatsAggregationBuilder(String name) {
        super(name);
    }

    protected MatrixStatsAggregationBuilder(
        MatrixStatsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.multiValueMode = clone.multiValueMode;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MatrixStatsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public MatrixStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    public MatrixStatsAggregationBuilder multiValueMode(MultiValueMode multiValueMode) {
        this.multiValueMode = multiValueMode;
        return this;
    }

    public MultiValueMode multiValueMode() {
        return this.multiValueMode;
    }

    @Override
    protected MatrixStatsAggregatorFactory innerBuild(
        AggregationContext context,
        Map<String, ValuesSourceConfig> configs,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new MatrixStatsAggregatorFactory(name, configs, multiValueMode, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(MULTIVALUE_MODE_FIELD.getPreferredName(), multiValueMode);
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
