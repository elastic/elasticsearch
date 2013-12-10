/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.aggregations.support.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *  Aggregates Geo information into cells determined by geohashes of a given precision.
 *  WARNING - for high-precision geohashes it may prove necessary to use a {@link GeoBoundingBoxFilterBuilder}
 *  aggregation to focus in on a smaller area to avoid generating too many buckets and using too much RAM 
 *  
 */
public class GeoHashGridParser implements Aggregator.Parser {

    public static final String aggTypeName="geohashgrid";
    @Override
    public String type() {
        return aggTypeName;
    }
    
   
    public static final int DEFAULT_PRECISION=5;
    public static final int DEFAULT_MAX_NUM_CELLS=10000;
    

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        int precision=DEFAULT_PRECISION;
        int requiredSize=DEFAULT_MAX_NUM_CELLS;
        int shardSize=0;
        

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } 
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("precision".equals(currentFieldName)) {
                    precision = parser.intValue();
                }else  if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                }else  if ("shard_size".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                }else  if ("shardSize".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                }  
                
            } 
        }
        if(shardSize==0)
        {
            //Use default heuristic of each shard returning double the final number of cells
            //required in order to try avoid any wrong-ranking caused by distributed counting            
            shardSize=requiredSize*2;
        }

        ValuesSourceConfig<GeoPointValuesSource> config = new ValuesSourceConfig<GeoPointValuesSource>(GeoPointValuesSource.class);
        if (field == null) {
            return new GeoGridFactory(aggregationName, config,precision,requiredSize,shardSize);
        }

        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            config.unmapped(true);
            return new GeoGridFactory(aggregationName, config,precision,requiredSize,shardSize);
        }

        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);
        config.fieldContext(new FieldContext(field, indexFieldData));
        return new GeoGridFactory(aggregationName, config,precision,requiredSize,shardSize);
    }

    private static class GeoGridFactory extends ValueSourceAggregatorFactory<GeoPointValuesSource> {

        private int precision;
        private int requiredSize;
        private int shardSize;

        public GeoGridFactory(String name, ValuesSourceConfig<GeoPointValuesSource> valueSourceConfig,
                int precision,int requiredSize,int shardSize) {
            super(name, StringTerms.TYPE.name(), valueSourceConfig);
            this.precision=precision;
            this.requiredSize=requiredSize;
            this.shardSize=shardSize;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            InternalOrder order = (InternalOrder) GeoHashCells.Order.COUNT_DESC;            
            return new GeoHashCellsAggregator.Unmapped(name, order, requiredSize, aggregationContext, parent);
        }
        
        @Override
        protected Aggregator create(final GeoPointValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            final CellValues cellIdValues = new CellValues(valuesSource,precision);
            FieldDataSource.Numeric cellIdSource = new CellIdSource(cellIdValues);
            //TODO "sortedAndUnique" line below is following the example in GeoDistanceParser which assumes
            // that docs always have multi-points. There is an open question as to if we can detect 
            // if any multi-point exists in a segment and if so, avoid this needless wrapper  
            cellIdSource = new FieldDataSource.Numeric.SortedAndUnique(cellIdSource);
            final NumericValuesSource geohashIdSource = new NumericValuesSource(cellIdSource,null,null);
            InternalOrder order = (InternalOrder) GeoHashCells.Order.COUNT_DESC;
            return new GeoHashCellsAggregator(name, factories, geohashIdSource, order, requiredSize,
                    shardSize, aggregationContext, parent);

        }

        private static class CellValues extends LongValues {

            private GeoPointValuesSource geoPointValues;
            private GeoPointValues geoValues;
            private int precision;

            protected CellValues(GeoPointValuesSource geoPointValues, int precision ) {
                super(true);
                this.geoPointValues = geoPointValues;
                this.precision=precision;
            }            
            @Override
            public int setDocument(int docId) {
                geoValues = geoPointValues.values();
                return geoValues.setDocument(docId);
            }

            @Override
            public long nextValue() {
                GeoPoint target = geoValues.nextValue();
                return GeoHashUtils.encodeAsLong(target.getLat(), target.getLon(), precision);
            }
            
        }

        private static class CellIdSource extends FieldDataSource.Numeric {
            private final LongValues values;

            public CellIdSource(LongValues values) {
                this.values = values;
            }

            @Override
            public boolean isFloatingPoint() {
                return false;
            }

            @Override
            public LongValues longValues() {
                return values;
            }

            @Override
            public DoubleValues doubleValues() {
               throw new UnsupportedOperationException();
            }

            @Override
            public BytesValues bytesValues() {
                throw new UnsupportedOperationException();
            }

        }
    }

}