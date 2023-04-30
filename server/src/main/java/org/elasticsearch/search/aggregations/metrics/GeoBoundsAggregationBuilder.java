/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.TransportVersion;
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

public class GeoBoundsAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<GeoBoundsAggregationBuilder> {
    public static final String NAME = "geo_bounds";
    public static final ValuesSourceRegistry.RegistryKey<GeoBoundsAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        GeoBoundsAggregatorSupplier.class
    );

    public static final ObjectParser<GeoBoundsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        GeoBoundsAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
        PARSER.declareBoolean(GeoBoundsAggregationBuilder::wrapLongitude, GeoBoundsAggregator.WRAP_LONGITUDE_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        GeoBoundsAggregatorFactory.registerAggregators(builder);
    }

    private boolean wrapLongitude = true;

    public GeoBoundsAggregationBuilder(String name) {
        super(name);
    }

    protected GeoBoundsAggregationBuilder(
        GeoBoundsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.wrapLongitude = clone.wrapLongitude;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new GeoBoundsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    /**
     * Read from a stream.
     */
    public GeoBoundsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        wrapLongitude = in.readBoolean();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(wrapLongitude);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.GEOPOINT;
    }

    /**
     * Set whether to wrap longitudes. Defaults to true.
     */
    public GeoBoundsAggregationBuilder wrapLongitude(boolean wrapLongitude) {
        this.wrapLongitude = wrapLongitude;
        return this;
    }

    /**
     * Get whether to wrap longitudes.
     */
    public boolean wrapLongitude() {
        return wrapLongitude;
    }

    @Override
    protected GeoBoundsAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        GeoBoundsAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new GeoBoundsAggregatorFactory(
            name,
            config,
            wrapLongitude,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(GeoBoundsAggregator.WRAP_LONGITUDE_FIELD.getPreferredName(), wrapLongitude);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), wrapLongitude);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        GeoBoundsAggregationBuilder other = (GeoBoundsAggregationBuilder) obj;
        return Objects.equals(wrapLongitude, other.wrapLongitude);
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
        return TransportVersion.ZERO;
    }
}
