/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ExtendedStatsAggregationBuilder extends ValuesSourceAggregationBuilder.MetricsAggregationBuilder<
    ExtendedStatsAggregationBuilder> {
    public static final String NAME = "extended_stats";
    public static final ValuesSourceRegistry.RegistryKey<ExtendedStatsAggregatorProvider> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, ExtendedStatsAggregatorProvider.class);

    public static final ObjectParser<ExtendedStatsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        ExtendedStatsAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareDouble(ExtendedStatsAggregationBuilder::sigma, ExtendedStatsAggregator.SIGMA_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        ExtendedStatsAggregatorFactory.registerAggregators(builder);
    }

    private double sigma = 2.0;

    public ExtendedStatsAggregationBuilder(String name) {
        super(name);
    }

    protected ExtendedStatsAggregationBuilder(
        ExtendedStatsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.sigma = clone.sigma;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ExtendedStatsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public ExtendedStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        sigma = in.readDouble();
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    public Set<String> metricNames() {
        return InternalExtendedStats.METRIC_NAMES;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(sigma);
    }

    public ExtendedStatsAggregationBuilder sigma(double sigma) {
        if (sigma < 0.0) {
            throw new IllegalArgumentException("[sigma] must be greater than or equal to 0. Found [" + sigma + "] in [" + name + "]");
        }
        this.sigma = sigma;
        return this;
    }

    @Override
    protected ExtendedStatsAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        ExtendedStatsAggregatorProvider aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new ExtendedStatsAggregatorFactory(name, config, sigma, context, parent, subFactoriesBuilder, metadata, aggregatorSupplier);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ExtendedStatsAggregator.SIGMA_FIELD.getPreferredName(), sigma);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sigma);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ExtendedStatsAggregationBuilder other = (ExtendedStatsAggregationBuilder) obj;
        return Objects.equals(sigma, other.sigma);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
