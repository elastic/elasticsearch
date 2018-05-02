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

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GeoGridAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.GeoPoint, GeoGridAggregationBuilder>
        implements MultiBucketAggregationBuilder {
    public static final String NAME = "geohash_grid";
    public static final GeoHashType DEFAULT_TYPE = GeoHashType.GEOHASH;
    public static final int DEFAULT_MAX_NUM_CELLS = 10000;

    private static final ObjectParser<GeoGridAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(GeoGridAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareGeoFields(PARSER, false, false);
        PARSER.declareString(GeoGridAggregationBuilder::type, GeoHashGridParams.FIELD_TYPE);
        // ValueType.INT supports both numbers and strings
        PARSER.declareField(GeoGridAggregationBuilder::parsePrecision, GeoHashGridParams.FIELD_PRECISION, ObjectParser.ValueType.INT);
        PARSER.declareInt(GeoGridAggregationBuilder::size, GeoHashGridParams.FIELD_SIZE);
        PARSER.declareInt(GeoGridAggregationBuilder::shardSize, GeoHashGridParams.FIELD_SHARD_SIZE);
    }

    private static void parsePrecision(XContentParser parser, GeoGridAggregationBuilder builder, Void context)
            throws IOException {
        // In some cases, this value cannot be fully parsed until after we know the type
        // Declare precision as an ObjectParser.ValueType.STRING, and convert it to String if needed
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            builder.precision(Integer.toString(parser.intValue()));
        } else {
            builder.precision(parser.text());
        }
    }

    public static GeoGridAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new GeoGridAggregationBuilder(aggregationName), null);
    }

    private GeoHashType type = DEFAULT_TYPE;
    private String precision = null;
    private int requiredSize = DEFAULT_MAX_NUM_CELLS;
    private int shardSize = -1;

    public GeoGridAggregationBuilder(String name) {
        super(name, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
    }

    protected GeoGridAggregationBuilder(GeoGridAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.type = clone.type;
        this.precision = clone.precision;
        this.requiredSize = clone.requiredSize;
        this.shardSize = clone.shardSize;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new GeoGridAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public GeoGridAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        type = GeoHashType.readFromStream(in);
        precision = in.readOptionalString();
        requiredSize = in.readVInt();
        shardSize = in.readVInt();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        type.writeTo(out);
        out.writeOptionalString(precision);
        out.writeVInt(requiredSize);
        out.writeVInt(shardSize);
    }

    public GeoGridAggregationBuilder type(String type) {
        try {
            this.type = GeoHashType.forString(type);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "[type] is not valid. Allowed values: " +
                    Stream.of(GeoHashType.values())
                        .map(GeoHashType::toString)
                        .collect(Collectors.joining(", ")) +
                    ". Found [" + type + "] in [" + name + "]");
        }
        return this;
    }

    public GeoHashType type() {
        return type;
    }

    public GeoGridAggregationBuilder precision(String precision) {
        // validation depends on the type, will be checked in innerBuild
        this.precision = precision;
        return this;
    }

    public String precision() {
        return precision;
    }

    public GeoGridAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException(
                    "[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        this.requiredSize = size;
        return this;
    }

    public int size() {
        return requiredSize;
    }

    public GeoGridAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException(
                    "[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        this.shardSize = shardSize;
        return this;
    }

    public int shardSize() {
        return shardSize;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource.GeoPoint, ?> innerBuild(SearchContext context,
            ValuesSourceConfig<ValuesSource.GeoPoint> config, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
                    throws IOException {

        // delayed precision validation and defaults
        int precision;
        switch (type) {
            case GEOHASH:
                if (this.precision != null) {
                    precision = GeoUtils.parsePrecisionString(this.precision);
                } else {
                    precision = 5;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown type " + type.toString());
        }

        int shardSize = this.shardSize;

        int requiredSize = this.requiredSize;

        if (shardSize < 0) {
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize, context.numberOfShards());
        }

        if (requiredSize <= 0 || shardSize <= 0) {
            throw new ElasticsearchException(
                    "parameters [required_size] and [shard_size] must be >0 in geohash_grid aggregation [" + name + "].");
        }

        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }
        return new GeoHashGridAggregatorFactory(name, config, type, precision, requiredSize, shardSize, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(GeoHashGridParams.FIELD_TYPE.getPreferredName(), type);
        builder.field(GeoHashGridParams.FIELD_PRECISION.getPreferredName(), precision);
        builder.field(GeoHashGridParams.FIELD_SIZE.getPreferredName(), requiredSize);
        if (shardSize > -1) {
            builder.field(GeoHashGridParams.FIELD_SHARD_SIZE.getPreferredName(), shardSize);
        }
        return builder;
    }

    @Override
    protected boolean innerEquals(Object obj) {
        GeoGridAggregationBuilder other = (GeoGridAggregationBuilder) obj;
        if (type != other.type) {
            return false;
        }
        if (!Objects.equals(precision, other.precision)) {
            return false;
        }
        if (requiredSize != other.requiredSize) {
            return false;
        }
        if (shardSize != other.shardSize) {
            return false;
        }
        return true;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(type, precision, requiredSize, shardSize);
    }

    @Override
    public String getType() {
        return NAME;
    }

    private static class CellValues extends AbstractSortingNumericDocValues {
        private MultiGeoPointValues geoValues;
        private GeoHashType type;
        private int precision;

        protected CellValues(MultiGeoPointValues geoValues, GeoHashType type, int precision) {
            this.geoValues = geoValues;
            this.type = type;
            this.precision = precision;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                resize(geoValues.docValueCount());
                for (int i = 0; i < docValueCount(); ++i) {
                    GeoPoint target = geoValues.nextValue();

                    switch (type) {
                        case GEOHASH:
                            values[i] = GeoHashUtils.longEncode(target.getLon(), target.getLat(), precision);
                            break;
                        default:
                            throw new IllegalArgumentException();
                    }
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }

    static class CellIdSource extends ValuesSource.Numeric {
        private final ValuesSource.GeoPoint valuesSource;
        private final GeoHashType type;
        private final int precision;

        CellIdSource(ValuesSource.GeoPoint valuesSource, GeoHashType type, int precision) {
            this.valuesSource = valuesSource;
            //different GeoPoints could map to the same or different geohash cells.
            this.type = type;
            this.precision = precision;
        }

        public GeoHashType type() {
            return type;
        }

        public int precision() {
            return precision;
        }

        @Override
        public boolean isFloatingPoint() {
            return false;
        }

        @Override
        public SortedNumericDocValues longValues(LeafReaderContext ctx) {
            return new CellValues(valuesSource.geoPointValues(ctx), type, precision);
        }

        @Override
        public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
            throw new UnsupportedOperationException();
        }

    }
}
