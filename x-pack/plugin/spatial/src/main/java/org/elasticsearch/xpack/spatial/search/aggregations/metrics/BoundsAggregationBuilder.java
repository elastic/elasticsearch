/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.PointValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class BoundsAggregationBuilder extends ValuesSourceAggregationBuilder<BoundsAggregationBuilder> {

    public static final String NAME = "bounds";

    public static final ObjectParser<BoundsAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, BoundsAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
    }

    public BoundsAggregationBuilder(String name) {
        super(name);
    }

    protected BoundsAggregationBuilder(BoundsAggregationBuilder clone,
                                       AggregatorFactories.Builder factoriesBuilder,
                                       Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new BoundsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public BoundsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return PointValuesSourceType.instance();
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected BoundsAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config,
                                                    AggregatorFactory parent,
                                                    AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new BoundsAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        return super.equals(obj);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
