/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.h3.H3;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoGridAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public final class GeoHexGridAggregationBuilder extends GeoGridAggregationBuilder {
    public static final String NAME = "geohex_grid";
    private static final int DEFAULT_PRECISION = 5;
    private static final int DEFAULT_MAX_NUM_CELLS = 10000;
    public static final ValuesSourceRegistry.RegistryKey<GeoGridAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        GeoGridAggregatorSupplier.class
    );

    public static final ObjectParser<GeoHexGridAggregationBuilder, String> PARSER = createParser(
        NAME,
        GeoHexGridAggregationBuilder::parsePrecision,
        GeoHexGridAggregationBuilder::new
    );

    static int parsePrecision(XContentParser parser) throws IOException, ElasticsearchParseException {
        final Object node = parser.currentToken().equals(XContentParser.Token.VALUE_NUMBER)
            ? Integer.valueOf(parser.intValue())
            : parser.text();
        return XContentMapValues.nodeIntegerValue(node);
    }

    public GeoHexGridAggregationBuilder(String name) {
        super(name);
        precision(DEFAULT_PRECISION);
        size(DEFAULT_MAX_NUM_CELLS);
        shardSize = -1;
    }

    public GeoHexGridAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public GeoGridAggregationBuilder precision(int precision) {
        if (precision < 0 || precision > H3.MAX_H3_RES) {
            throw new IllegalArgumentException(
                "Invalid geohex aggregation precision of " + precision + "" + ". Must be between 0 and " + H3.MAX_H3_RES
            );
        }
        this.precision = precision;
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
        return new GeoHexGridAggregatorFactory(
            name,
            config,
            precision,
            requiredSize,
            shardSize,
            geoBoundingBox,
            context,
            parent,
            subFactoriesBuilder,
            metadata
        );
    }

    private GeoHexGridAggregationBuilder(
        GeoHexGridAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new GeoHexGridAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_1_0;
    }
}
