/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
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
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.search.aggregations.metrics.PercentilesMethod.COMPRESSION_FIELD;

public class BoxplotAggregationBuilder extends ValuesSourceAggregationBuilder.MetricsAggregationBuilder<BoxplotAggregationBuilder> {
    public static final String NAME = "boxplot";
    public static final ValuesSourceRegistry.RegistryKey<BoxplotAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        BoxplotAggregatorSupplier.class
    );

    public static final ObjectParser<BoxplotAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        BoxplotAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareDouble(BoxplotAggregationBuilder::compression, COMPRESSION_FIELD);
    }

    private double compression = 100.0;

    public BoxplotAggregationBuilder(String name) {
        super(name);
    }

    protected BoxplotAggregationBuilder(
        BoxplotAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.compression = clone.compression;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        BoxplotAggregatorFactory.registerAggregators(builder);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new BoxplotAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public BoxplotAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        compression = in.readDouble();
    }

    @Override
    public Set<String> metricNames() {
        return InternalBoxplot.METRIC_NAMES;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(compression);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    /**
     * Expert: set the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     */
    public BoxplotAggregationBuilder compression(double compression) {
        if (compression < 0.0) {
            throw new IllegalArgumentException(
                "[compression] must be greater than or equal to 0. Found [" + compression + "] in [" + name + "]"
            );
        }
        this.compression = compression;
        return this;
    }

    /**
     * Expert: get the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     */
    public double compression() {
        return compression;
    }

    @Override
    protected BoxplotAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        BoxplotAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);

        return new BoxplotAggregatorFactory(name, config, compression, context, parent, subFactoriesBuilder, metadata, aggregatorSupplier);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(COMPRESSION_FIELD.getPreferredName(), compression);
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        BoxplotAggregationBuilder other = (BoxplotAggregationBuilder) obj;
        return Objects.equals(compression, other.compression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), compression);
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
    public Optional<Set<String>> getOutputFieldNames() {
        return Optional.of(InternalBoxplot.METRIC_NAMES);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_7_0;
    }
}
