/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.geogrid.CellIdSource;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Objects;

public class GeoTileGridValuesSourceBuilder extends CompositeValuesSourceBuilder<GeoTileGridValuesSourceBuilder> {
    static final String TYPE = "geotile_grid";

    private static final ObjectParser<GeoTileGridValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(GeoTileGridValuesSourceBuilder.TYPE);
        PARSER.declareInt(GeoTileGridValuesSourceBuilder::precision, new ParseField("precision"));
        PARSER.declareField(((p, builder, context) -> builder.geoBoundingBox(GeoBoundingBox.parseBoundingBox(p))),
            GeoBoundingBox.BOUNDS_FIELD, ObjectParser.ValueType.OBJECT);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER, ValueType.NUMERIC);
    }

    static GeoTileGridValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new GeoTileGridValuesSourceBuilder(name), null);
    }

    private int precision = GeoTileGridAggregationBuilder.DEFAULT_PRECISION;
    private GeoBoundingBox geoBoundingBox = new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN));

    GeoTileGridValuesSourceBuilder(String name) {
        super(name);
    }

    GeoTileGridValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        this.precision = in.readInt();
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            this.geoBoundingBox = new GeoBoundingBox(in);
        }
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
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            geoBoundingBox.writeTo(out);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("precision", precision);
        if (geoBoundingBox.isUnbounded() == false) {
            geoBoundingBox.toXContent(builder, params);
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
    protected CompositeValuesSourceConfig innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config) throws IOException {
        ValuesSource orig = config.hasValues() ? config.getValuesSource() : null;
        if (orig == null) {
            orig = ValuesSource.GeoPoint.EMPTY;
        }
        if (orig instanceof ValuesSource.GeoPoint) {
            ValuesSource.GeoPoint geoPoint = (ValuesSource.GeoPoint) orig;
            // is specified in the builder.
            final MappedFieldType fieldType = config.fieldType();
            CellIdSource cellIdSource = new CellIdSource(geoPoint, precision, geoBoundingBox, GeoTileUtils::longEncode);
            return new CompositeValuesSourceConfig(name, fieldType, cellIdSource, DocValueFormat.GEOTILE, order(),
                missingBucket(), script() != null);
        } else {
            throw new IllegalArgumentException("invalid source, expected geo_point, got " + orig.getClass().getSimpleName());
        }
    }

}
