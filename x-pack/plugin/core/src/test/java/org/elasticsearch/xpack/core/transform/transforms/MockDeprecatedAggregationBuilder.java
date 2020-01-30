/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class MockDeprecatedAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource, MockDeprecatedAggregationBuilder> {

    public static final String NAME = "deprecated_agg";
    public static final String DEPRECATION_MESSAGE = "expected deprecation message from MockDeprecatedAggregationBuilder";

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(MockDeprecatedAggregationBuilder.class));

    protected MockDeprecatedAggregationBuilder(MockDeprecatedAggregationBuilder clone, Builder factoriesBuilder,
            Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new MockDeprecatedAggregationBuilder(this, factoriesBuilder, metaData);
    }

    public MockDeprecatedAggregationBuilder() {
        super(NAME, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
    }

    /**
     * Read from a stream.
     */
    protected MockDeprecatedAggregationBuilder(StreamInput in) throws IOException {
        super(in, null, null);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource> innerBuild(QueryShardContext queryShardContext,
                                                                        ValuesSourceConfig<ValuesSource> config,
                                                                        AggregatorFactory parent,
                                                                        Builder subFactoriesBuilder) throws IOException {
        return null;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    public static MockDeprecatedAggregationBuilder fromXContent(XContentParser p) {
        deprecationLogger.deprecatedAndMaybeLog("deprecated_mock", DEPRECATION_MESSAGE);
        return new MockDeprecatedAggregationBuilder();
    }
}
