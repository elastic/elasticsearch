/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.GeoGridAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;

public class GeoHashGridAggregationBuilder extends GeoGridAggregationBuilder {
    public static final String NAME = "geohash_grid";
    public static final int DEFAULT_PRECISION = 5;
    public static final int DEFAULT_MAX_NUM_CELLS = 10000;
    public static final ValuesSourceRegistry.RegistryKey<GeoGridAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        GeoGridAggregatorSupplier.class
    );

    public static final ObjectParser<GeoHashGridAggregationBuilder, String> PARSER = createParser(
        NAME,
        GeoUtils::parsePrecision,
        GeoHashGridAggregationBuilder::new
    );

    public GeoHashGridAggregationBuilder(String name) {
        super(name);
        precision(DEFAULT_PRECISION);
        size(DEFAULT_MAX_NUM_CELLS);
        shardSize = -1;
    }

    public GeoHashGridAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        GeoHashGridAggregatorFactory.registerAggregators(builder);
    }

    @Override
    public GeoGridAggregationBuilder precision(int precision) {
        this.precision = GeoUtils.checkPrecisionRange(precision);
        return this;
    }

    @Override
    protected ValuesSourceAggregatorFactory createFactory(
        String name,
        ValuesSourceConfig config,
        int precision,
        int requiredSize,
        int shardSize,
        GeoBoundingBox geoBoundingBox,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {

        GeoGridAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new GeoHashGridAggregatorFactory(
            name,
            config,
            precision,
            requiredSize,
            shardSize,
            geoBoundingBox,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

    private GeoHashGridAggregationBuilder(
        GeoHashGridAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new GeoHashGridAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }
}
