/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregates geo data in a vector tile.It currently only supports to be o the top-level (e.g  cannot have a parent)
 * but it can change.
 */
public class VectorTileAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, VectorTileAggregationBuilder> {
    public static final String NAME = "vector-tile";
    public static final ParseField ZOOM_FIELD = new ParseField("z");
    public static final ParseField X_FIELD = new ParseField("x");
    public static final ParseField Y_FIELD = new ParseField("y");
    public static final ValuesSourceRegistry.RegistryKey<VectorTileAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        VectorTileAggregatorSupplier.class
    );
    public static final ObjectParser<VectorTileAggregationBuilder, String> PARSER =
        ObjectParser.fromBuilder(NAME, VectorTileAggregationBuilder::new);

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false, false);
        PARSER.declareInt(VectorTileAggregationBuilder::z, ZOOM_FIELD);
        PARSER.declareInt(VectorTileAggregationBuilder::x, X_FIELD);
        PARSER.declareInt(VectorTileAggregationBuilder::x, Y_FIELD);
    }

    private int z;
    private int x;
    private int y;

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            VectorTileAggregationBuilder.REGISTRY_KEY,
            Collections.singletonList(CoreValuesSourceType.GEOPOINT),
            VectorTileGeoPointAggregator::new,
            true
        );
        builder.register(
            VectorTileAggregationBuilder.REGISTRY_KEY,
            Collections.singletonList(GeoShapeValuesSourceType.instance()),
            VectorTileGeoShapeAggregator::new,
            true
        );
    }

    public VectorTileAggregationBuilder(String name) {
        super(name);
    }

    protected VectorTileAggregationBuilder(
        VectorTileAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.x = clone.x;
        this.y = clone.y;
        this.z = clone.z;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new VectorTileAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public VectorTileAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        z = in.readVInt();
        x = in.readVInt();
        y = in.readVInt();
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.GEOPOINT;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(z);
        out.writeVInt(x);
        out.writeVInt(y);
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    @Override
    protected VectorTileAggregatorFactory innerBuild(
            AggregationContext context,
            ValuesSourceConfig config,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        if (parent != null) {
            // we don't allow vector-tile aggregations to be the child of a bucketing aggregation
            throw new IllegalArgumentException(NAME + " aggregation must be at the top level");
        }
        VectorTileAggregatorSupplier aggregatorSupplier =
            context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new VectorTileAggregatorFactory(name, config, z, x, y, context, parent,
                                         subFactoriesBuilder, metadata, aggregatorSupplier);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ZOOM_FIELD.getPreferredName(), z);
        builder.field(X_FIELD.getPreferredName(), x);
        builder.field(Y_FIELD.getPreferredName(), y);
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    public VectorTileAggregationBuilder z(int z) {
        this.z = z;
        return this;
    }

    public VectorTileAggregationBuilder x(int x) {
        this.x = x;
        return this;
    }

    public VectorTileAggregationBuilder y(int y) {
        this.y = y;
        return this;
    }

    @Override
    protected ValuesSourceConfig resolveConfig(AggregationContext context) {
        // TODO: make this behavior right
        if (field() == null && script() == null) {
            return new ValuesSourceConfig(CoreValuesSourceType.GEOPOINT, null, true, null, null, 1.0, null, DocValueFormat.RAW, context);
        } else {
            return super.resolveConfig(context);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        VectorTileAggregationBuilder that = (VectorTileAggregationBuilder) o;
        return z == that.z && x == that.x && y == that.y;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), z, x, y);
    }
}
