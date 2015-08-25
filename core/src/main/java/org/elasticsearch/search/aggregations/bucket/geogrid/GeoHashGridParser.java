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
import org.apache.lucene.util.GeoHashUtils;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingNumericDocValues;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.GeoPointValuesSourceParser;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregates Geo information into cells determined by geohashes of a given precision.
 * WARNING - for high-precision geohashes it may prove necessary to use a {@link GeoBoundingBoxQueryBuilder}
 * aggregation to focus in on a smaller area to avoid generating too many buckets and using too much RAM
 */
public class GeoHashGridParser extends GeoPointValuesSourceParser {

    public static final int DEFAULT_PRECISION = 5;
    public static final int DEFAULT_MAX_NUM_CELLS = 10000;

    public GeoHashGridParser() {
        super(false, false);
    }

    @Override
    public String type() {
        return InternalGeoHashGrid.TYPE.name();
    }
    @Override
    public AggregatorFactory getFactoryPrototype() {
        return new GeoGridFactory(null);
    }

    @Override
    protected ValuesSourceAggregatorFactory<org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint> createFactory(
            String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        GeoGridFactory factory = new GeoGridFactory(aggregationName);
        Integer precision = (Integer) otherOptions.get(GeoHashGridParams.FIELD_PRECISION);
        if (precision != null) {
            factory.precision(precision);
                }
        Integer size = (Integer) otherOptions.get(GeoHashGridParams.FIELD_SIZE);
        if (size != null) {
            factory.size(size);
        }
        Integer shardSize = (Integer) otherOptions.get(GeoHashGridParams.FIELD_SHARD_SIZE);
        if (shardSize != null) {
            factory.shardSize(shardSize);
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER || token == XContentParser.Token.VALUE_STRING) {
            if (parseFieldMatcher.match(currentFieldName, GeoHashGridParams.FIELD_PRECISION)) {
                otherOptions.put(GeoHashGridParams.FIELD_PRECISION, parser.intValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, GeoHashGridParams.FIELD_SIZE)) {
                otherOptions.put(GeoHashGridParams.FIELD_SIZE, parser.intValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, GeoHashGridParams.FIELD_SHARD_SIZE)) {
                otherOptions.put(GeoHashGridParams.FIELD_SHARD_SIZE, parser.intValue());
                return true;
        }
        }
        return false;
        }

    public static class GeoGridFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {

        private int precision = DEFAULT_PRECISION;
        private int requiredSize = DEFAULT_MAX_NUM_CELLS;
        private int shardSize = -1;

        public GeoGridFactory(String name) {
            super(name, InternalGeoHashGrid.TYPE.name(), ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
    }

        public void precision(int precision) {
            this.precision = GeoHashGridParams.checkPrecision(precision);
        }

        public void size(int size) {
            this.requiredSize = size;
        }

        public void shardSize(int shardSize) {
            this.shardSize = shardSize;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) throws IOException {
            final InternalAggregation aggregation = new InternalGeoHashGrid(name, requiredSize,
                    Collections.<InternalGeoHashGrid.Bucket> emptyList(), pipelineAggregators, metaData);
            return new NonCollectingAggregator(name, aggregationContext, parent, pipelineAggregators, metaData) {
                @Override
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

        @Override
        protected Aggregator doCreateInternal(final ValuesSource.GeoPoint valuesSource, AggregationContext aggregationContext,
                Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException {
            if (shardSize == 0) {
                shardSize = Integer.MAX_VALUE;
            }

            if (requiredSize == 0) {
                requiredSize = Integer.MAX_VALUE;
            }

            if (shardSize < 0) {
                // Use default heuristic to avoid any wrong-ranking caused by
                // distributed counting
                shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize, aggregationContext.searchContext().numberOfShards());
            }

            if (shardSize < requiredSize) {
                shardSize = requiredSize;
            }
            if (collectsFromSingleBucket == false) {
                return asMultiBucketAggregator(this, aggregationContext, parent);
            }
            CellIdSource cellIdSource = new CellIdSource(valuesSource, precision);
            return new GeoHashGridAggregator(name, factories, cellIdSource, requiredSize, shardSize, aggregationContext, parent, pipelineAggregators,
                    metaData);

        }

        @Override
        protected ValuesSourceAggregatorFactory<org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint> innerReadFrom(
                String name, ValuesSourceType valuesSourceType,
                ValueType targetValueType, StreamInput in) throws IOException {
            GeoGridFactory factory = new GeoGridFactory(name);
            factory.precision = in.readVInt();
            factory.requiredSize = in.readVInt();
            factory.shardSize = in.readVInt();
            return factory;
        }

        @Override
        protected void innerWriteTo(StreamOutput out) throws IOException {
            out.writeVInt(precision);
            out.writeVInt(requiredSize);
            out.writeVInt(shardSize);
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(GeoHashGridParams.FIELD_PRECISION.getPreferredName(), precision);
            builder.field(GeoHashGridParams.FIELD_SIZE.getPreferredName(), requiredSize);
            builder.field(GeoHashGridParams.FIELD_SHARD_SIZE.getPreferredName(), shardSize);
            return builder;
        }

        @Override
        protected boolean innerEquals(Object obj) {
            GeoGridFactory other = (GeoGridFactory) obj;
            if (precision != other.precision) {
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
            return Objects.hash(precision, requiredSize, shardSize);
        }

        private static class CellValues extends SortingNumericDocValues {
            private MultiGeoPointValues geoValues;
            private int precision;

            protected CellValues(MultiGeoPointValues geoValues, int precision) {
                this.geoValues = geoValues;
                this.precision = precision;
            }

            @Override
            public void setDocument(int docId) {
                geoValues.setDocument(docId);
                resize(geoValues.count());
                for (int i = 0; i < count(); ++i) {
                    GeoPoint target = geoValues.valueAt(i);
                    values[i] = GeoHashUtils.longEncode(target.getLon(), target.getLat(), precision);
                }
                sort();
            }
        }

        static class CellIdSource extends ValuesSource.Numeric {
            private final ValuesSource.GeoPoint valuesSource;
            private final int precision;

            public CellIdSource(ValuesSource.GeoPoint valuesSource, int precision) {
                this.valuesSource = valuesSource;
                //different GeoPoints could map to the same or different geohash cells.
                this.precision = precision;
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
                return new CellValues(valuesSource.geoPointValues(ctx), precision);
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
}