/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public abstract class GeoGridAggregationBuilder extends ValuesSourceAggregationBuilder<GeoGridAggregationBuilder> {
    /* recognized field names in JSON */
    static final ParseField FIELD_PRECISION = new ParseField("precision");
    static final ParseField FIELD_SIZE = new ParseField("size");
    static final ParseField FIELD_SHARD_SIZE = new ParseField("shard_size");

    protected int precision;
    protected int requiredSize;
    protected int shardSize;
    private GeoBoundingBox geoBoundingBox = new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN));

    @FunctionalInterface
    protected interface PrecisionParser {
        int parse(XContentParser parser) throws IOException;
    }

    public static <T extends GeoGridAggregationBuilder> ObjectParser<T, String> createParser(
        String name,
        PrecisionParser precisionParser,
        Function<String, T> ctor
    ) {
        ObjectParser<T, String> parser = ObjectParser.fromBuilder(name, ctor);
        ValuesSourceAggregationBuilder.declareFields(parser, false, false, false);
        parser.declareField(
            (p, builder, context) -> builder.precision(precisionParser.parse(p)),
            FIELD_PRECISION,
            org.elasticsearch.xcontent.ObjectParser.ValueType.INT
        );
        parser.declareInt(GeoGridAggregationBuilder::size, FIELD_SIZE);
        parser.declareInt(GeoGridAggregationBuilder::shardSize, FIELD_SHARD_SIZE);
        parser.declareField(
            (p, builder, context) -> builder.setGeoBoundingBox(GeoBoundingBox.parseBoundingBox(p)),
            GeoBoundingBox.BOUNDS_FIELD,
            org.elasticsearch.xcontent.ObjectParser.ValueType.OBJECT
        );
        return parser;
    }

    public GeoGridAggregationBuilder(String name) {
        super(name);
    }

    protected GeoGridAggregationBuilder(GeoGridAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.precision = clone.precision;
        this.requiredSize = clone.requiredSize;
        this.shardSize = clone.shardSize;
        this.geoBoundingBox = clone.geoBoundingBox;
    }

    /**
     * Read from a stream.
     */
    public GeoGridAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        precision = in.readVInt();
        requiredSize = in.readVInt();
        shardSize = in.readVInt();
        geoBoundingBox = new GeoBoundingBox(in);

    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.GEOPOINT;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(precision);
        out.writeVInt(requiredSize);
        out.writeVInt(shardSize);
        geoBoundingBox.writeTo(out);
    }

    /**
     * method to validate and set the precision value
     * @param precision the precision to set for the aggregation
     * @return the {@link GeoGridAggregationBuilder} builder
     */
    public abstract GeoGridAggregationBuilder precision(int precision);

    /**
     * Creates a new instance of the {@link ValuesSourceAggregatorFactory}-derived class specific to the geo aggregation.
     */
    protected abstract ValuesSourceAggregatorFactory createFactory(
        String name,
        ValuesSourceConfig config,
        int precision,
        int requiredSize,
        int shardSize,
        GeoBoundingBox geoBoundingBox,
        AggregationContext context,
        AggregatorFactory parent,
        Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException;

    public int precision() {
        return precision;
    }

    public GeoGridAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        this.requiredSize = size;
        return this;
    }

    public GeoGridAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException("[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        this.shardSize = shardSize;
        return this;
    }

    public GeoGridAggregationBuilder setGeoBoundingBox(GeoBoundingBox geoBoundingBox) {
        this.geoBoundingBox = geoBoundingBox;
        // no validation done here, similar to geo_bounding_box query behavior.
        return this;
    }

    public GeoBoundingBox geoBoundingBox() {
        return geoBoundingBox;
    }

    @Override
    public final BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        Builder subFactoriesBuilder
    ) throws IOException {
        int shardSize = this.shardSize;

        int requiredSize = this.requiredSize;

        if (shardSize < 0) {
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize);
        }

        if (requiredSize <= 0 || shardSize <= 0) {
            throw new ElasticsearchException(
                "parameters [required_size] and [shard_size] must be > 0 in " + getType() + " aggregation [" + name + "]."
            );
        }

        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }
        return createFactory(
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

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_PRECISION.getPreferredName(), precision);
        builder.field(FIELD_SIZE.getPreferredName(), requiredSize);
        if (shardSize > -1) {
            builder.field(FIELD_SHARD_SIZE.getPreferredName(), shardSize);
        }
        if (geoBoundingBox.isUnbounded() == false) {
            builder.startObject(GeoBoundingBox.BOUNDS_FIELD.getPreferredName());
            geoBoundingBox.toXContentFragment(builder);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        GeoGridAggregationBuilder other = (GeoGridAggregationBuilder) obj;
        return precision == other.precision
            && requiredSize == other.requiredSize
            && shardSize == other.shardSize
            && Objects.equals(geoBoundingBox, other.geoBoundingBox);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, requiredSize, shardSize, geoBoundingBox);
    }
}
