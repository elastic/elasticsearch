/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

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
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class MockDeprecatedAggregationBuilder extends ValuesSourceAggregationBuilder<MockDeprecatedAggregationBuilder> {

    public static final String NAME = "deprecated_agg";
    public static final String DEPRECATION_MESSAGE = "expected deprecation message from MockDeprecatedAggregationBuilder";

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MockDeprecatedAggregationBuilder.class);

    protected MockDeprecatedAggregationBuilder(MockDeprecatedAggregationBuilder clone, Builder factoriesBuilder,
            Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MockDeprecatedAggregationBuilder(this, factoriesBuilder, metadata);
    }

    public MockDeprecatedAggregationBuilder() {
        super(NAME);
    }

    /**
     * Read from a stream.
     */
    protected MockDeprecatedAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return ValuesSourceRegistry.UNREGISTERED_KEY;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       Builder subFactoriesBuilder) throws IOException {
        return null;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    public static MockDeprecatedAggregationBuilder fromXContent(XContentParser p) {
        deprecationLogger.deprecate("deprecated_mock", DEPRECATION_MESSAGE);
        return new MockDeprecatedAggregationBuilder();
    }
}
