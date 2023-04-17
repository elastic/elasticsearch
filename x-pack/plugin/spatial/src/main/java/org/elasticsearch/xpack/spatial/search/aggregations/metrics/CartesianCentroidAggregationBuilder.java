/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class CartesianCentroidAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<CartesianCentroidAggregationBuilder> {
    public static final String NAME = "cartesian_centroid";
    public static final ValuesSourceRegistry.RegistryKey<MetricAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        MetricAggregatorSupplier.class
    );

    public static final ObjectParser<CartesianCentroidAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        CartesianCentroidAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, false, false);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        CartesianCentroidAggregatorFactory.registerAggregators(builder);
    }

    public CartesianCentroidAggregationBuilder(String name) {
        super(name);
    }

    protected CartesianCentroidAggregationBuilder(
        CartesianCentroidAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CartesianPointValuesSourceType.instance();
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new CartesianCentroidAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public CartesianCentroidAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected CartesianCentroidAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        MetricAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new CartesianCentroidAggregatorFactory(name, config, context, parent, subFactoriesBuilder, metadata, aggregatorSupplier);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_6_0;
    }
}
