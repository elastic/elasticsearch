/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileCellIdSource;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

public class GeoTileGridValuesSourceBuilder extends CompositeValuesSourceBuilder<GeoTileGridValuesSourceBuilder> {
    @FunctionalInterface
    public interface GeoTileCompositeSuppier {
        CompositeValuesSourceConfig apply(
            ValuesSourceConfig config,
            int precision,
            GeoBoundingBox boundingBox,
            String name,
            boolean hasScript, // probably redundant with the config, but currently we check this two different ways...
            String format,
            boolean missingBucket,
            SortOrder order
        );
    }

    static final String TYPE = "geotile_grid";
    static final ValuesSourceRegistry.RegistryKey<GeoTileCompositeSuppier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        TYPE,
        GeoTileCompositeSuppier.class
    );

    private static final ObjectParser<GeoTileGridValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(GeoTileGridValuesSourceBuilder.TYPE);
        PARSER.declareInt(GeoTileGridValuesSourceBuilder::precision, new ParseField("precision"));
        PARSER.declareField(((p, builder, context) -> builder.geoBoundingBox(GeoBoundingBox.parseBoundingBox(p))),
            GeoBoundingBox.BOUNDS_FIELD, ObjectParser.ValueType.OBJECT);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER);
    }

    static GeoTileGridValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new GeoTileGridValuesSourceBuilder(name), null);
    }

    static void register(ValuesSourceRegistry.Builder builder) {

        builder.register(
            REGISTRY_KEY,
            CoreValuesSourceType.GEOPOINT,
            (valuesSourceConfig, precision, boundingBox, name, hasScript, format, missingBucket, order) -> {
                ValuesSource.GeoPoint geoPoint = (ValuesSource.GeoPoint) valuesSourceConfig.getValuesSource();
                // is specified in the builder.
                final MappedFieldType fieldType = valuesSourceConfig.fieldType();
                GeoTileCellIdSource cellIdSource = new GeoTileCellIdSource(
                    geoPoint,
                    precision,
                    boundingBox
                );
                return new CompositeValuesSourceConfig(
                    name,
                    fieldType,
                    cellIdSource,
                    DocValueFormat.GEOTILE,
                    order,
                    missingBucket,
                    hasScript,
                    (
                        BigArrays bigArrays,
                        IndexReader reader,
                        int size,
                        LongConsumer addRequestCircuitBreakerBytes,
                        CompositeValuesSourceConfig compositeValuesSourceConfig

                    ) -> {
                        final ValuesSource.Numeric cis = (ValuesSource.Numeric) compositeValuesSourceConfig.valuesSource();
                        return new GeoTileValuesSource(
                            bigArrays,
                            compositeValuesSourceConfig.fieldType(),
                            cis::longValues,
                            LongUnaryOperator.identity(),
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            size,
                            compositeValuesSourceConfig.reverseMul()
                        );
                    }
                );
            },
            false);
    }

    private int precision = GeoTileGridAggregationBuilder.DEFAULT_PRECISION;
    private GeoBoundingBox geoBoundingBox = new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN));

    public GeoTileGridValuesSourceBuilder(String name) {
        super(name);
    }

    GeoTileGridValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        this.precision = in.readInt();
        this.geoBoundingBox = new GeoBoundingBox(in);
    }

    public GeoTileGridValuesSourceBuilder precision(int precision) {
        this.precision = GeoTileUtils.checkPrecisionRange(precision);
        return this;
    }

    public GeoTileGridValuesSourceBuilder geoBoundingBox(GeoBoundingBox geoBoundingBox) {
        this.geoBoundingBox = geoBoundingBox;
        return this;
    }

    @Override
    public GeoTileGridValuesSourceBuilder format(String format) {
        throw new IllegalArgumentException("[format] is not supported for [" + TYPE + "]");
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeInt(precision);
        geoBoundingBox.writeTo(out);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("precision", precision);
        if (geoBoundingBox.isUnbounded() == false) {
            builder.startObject(GeoBoundingBox.BOUNDS_FIELD.getPreferredName());
            geoBoundingBox.toXContentFragment(builder, true);
            builder.endObject();
        }
    }

    @Override
    String type() {
        return TYPE;
    }

    GeoBoundingBox geoBoundingBox() {
        return geoBoundingBox;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, geoBoundingBox);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        GeoTileGridValuesSourceBuilder other = (GeoTileGridValuesSourceBuilder) obj;
        return Objects.equals(precision,other.precision)
            && Objects.equals(geoBoundingBox, other.geoBoundingBox);
    }

    @Override
    protected ValuesSourceType getDefaultValuesSourceType() {
        return CoreValuesSourceType.GEOPOINT;
    }

    @Override
    protected CompositeValuesSourceConfig innerBuild(ValuesSourceRegistry registry, ValuesSourceConfig config) throws IOException {
        return registry.getAggregator(REGISTRY_KEY, config)
            .apply(config, precision, geoBoundingBox(), name, script() != null, format(), missingBucket(), order());
    }

}
