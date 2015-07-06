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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingNumericDocValues;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates Geo information into cells determined by geohashes of a given precision.
 * WARNING - for high-precision geohashes it may prove necessary to use a {@link GeoBoundingBoxQueryBuilder}
 * aggregation to focus in on a smaller area to avoid generating too many buckets and using too much RAM
 */
public class GeoHashGridParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalGeoHashGrid.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser<ValuesSource.GeoPoint> vsParser = ValuesSourceParser
                .geoPoint(aggregationName, InternalGeoHashGrid.TYPE, context).build();

        int precision = GeoHashGridParams.DEFAULT_PRECISION;
        int requiredSize = GeoHashGridParams.DEFAULT_MAX_NUM_CELLS;
        int shardSize = -1;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_NUMBER ||
                    token == XContentParser.Token.VALUE_STRING) { //Be lenient and also allow numbers enclosed in quotes
                if (context.parseFieldMatcher().match(currentFieldName, GeoHashGridParams.FIELD_PRECISION)) {
                    precision = GeoHashGridParams.checkPrecision(parser.intValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, GeoHashGridParams.FIELD_SIZE)) {
                    requiredSize = parser.intValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, GeoHashGridParams.FIELD_SHARD_SIZE)) {
                    shardSize = parser.intValue();
                }
            } else if (token != XContentParser.Token.START_OBJECT) {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].",
                        parser.getTokenLocation());
            }
        }

        if (shardSize == 0) {
            shardSize = Integer.MAX_VALUE;
        }

        if (requiredSize == 0) {
            requiredSize = Integer.MAX_VALUE;
        }

        if (shardSize < 0) {
            //Use default heuristic to avoid any wrong-ranking caused by distributed counting
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize, context.numberOfShards());
        }

        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }

        return new GeoGridFactory(aggregationName, vsParser.input(), precision, requiredSize, shardSize);

    }


    static class GeoGridFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {

        private final int precision;
        private final int requiredSize;
        private final int shardSize;

        public GeoGridFactory(String name, ValuesSourceParser.Input<ValuesSource.GeoPoint> input, int precision, int requiredSize,
                int shardSize) {
            super(name, InternalGeoHashGrid.TYPE.name(), input);
            this.precision = precision;
            this.requiredSize = requiredSize;
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
            if (collectsFromSingleBucket == false) {
                return asMultiBucketAggregator(this, aggregationContext, parent);
            }
            CellIdSource cellIdSource = new CellIdSource(valuesSource, precision);
            return new GeoHashGridAggregator(name, factories, cellIdSource, requiredSize, shardSize, aggregationContext, parent, pipelineAggregators,
                    metaData);

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