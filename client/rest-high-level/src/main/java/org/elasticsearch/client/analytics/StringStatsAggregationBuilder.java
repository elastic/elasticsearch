/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.analytics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Builds the {@code string_stats} aggregation request.
 * <p>
 * NOTE: This extends {@linkplain AbstractAggregationBuilder} for compatibility
 * with {@link SearchSourceBuilder#aggregation(AggregationBuilder)} but it
 * doesn't support any "server" side things like
 * {@linkplain Writeable#writeTo(StreamOutput)},
 * {@linkplain AggregationBuilder#rewrite(QueryRewriteContext)}, or
 * {@linkplain AbstractAggregationBuilder#build(AggregationContext, AggregatorFactory)}.
 */
public class StringStatsAggregationBuilder extends ValuesSourceAggregationBuilder<StringStatsAggregationBuilder> {
    public static final String NAME = "string_stats";
    private static final ParseField SHOW_DISTRIBUTION_FIELD = new ParseField("show_distribution");

    private boolean showDistribution = false;

    public StringStatsAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Compute the distribution of each character. Disabled by default.
     * @return this for chaining
     */
    public StringStatsAggregationBuilder showDistribution(boolean showDistribution) {
        this.showDistribution = showDistribution;
        return this;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.KEYWORD;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        // This would be called from the same thing that calls innerBuild, which also throws. So it's "safe" to throw here.
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(StringStatsAggregationBuilder.SHOW_DISTRIBUTION_FIELD.getPreferredName(), showDistribution);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        Builder subFactoriesBuilder
    ) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), showDistribution);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (false == super.equals(obj)) {
            return false;
        }
        StringStatsAggregationBuilder other = (StringStatsAggregationBuilder) obj;
        return showDistribution == other.showDistribution;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}
