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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingNumericDocValues;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Objects;

public class GeoGridAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.GeoPoint, GeoGridAggregationBuilder> {
    public static final String NAME = InternalGeoHashGrid.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private int precision = GeoHashGridParser.DEFAULT_PRECISION;
    private int requiredSize = GeoHashGridParser.DEFAULT_MAX_NUM_CELLS;
    private int shardSize = -1;

    public GeoGridAggregationBuilder(String name) {
        super(name, InternalGeoHashGrid.TYPE, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
    }

    /**
     * Read from a stream.
     */
    public GeoGridAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalGeoHashGrid.TYPE, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        precision = in.readVInt();
        requiredSize = in.readVInt();
        shardSize = in.readVInt();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(precision);
        out.writeVInt(requiredSize);
        out.writeVInt(shardSize);
    }

    public GeoGridAggregationBuilder precision(int precision) {
        this.precision = GeoHashGridParams.checkPrecision(precision);
        return this;
    }

    public int precision() {
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
    protected ValuesSourceAggregatorFactory<ValuesSource.GeoPoint, ?> innerBuild(AggregationContext context,
            ValuesSourceConfig<ValuesSource.GeoPoint> config, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
                    throws IOException {
        int shardSize = this.shardSize;

        int requiredSize = this.requiredSize;

        if (shardSize < 0) {
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize, context.searchContext().numberOfShards());
        }

        if (requiredSize <= 0 || shardSize <= 0) {
            throw new ElasticsearchException(
                    "parameters [required_size] and [shard_size] must be >0 in geohash_grid aggregation [" + name + "].");
        }

        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }
        return new GeoHashGridAggregatorFactory(name, type, config, precision, requiredSize, shardSize, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
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

    @Override
    public String getWriteableName() {
        return NAME;
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
