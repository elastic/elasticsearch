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

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class GeoTileGridAggregationBuilder extends GeoGridAggregationBuilder {
    public static final String NAME = "geotile_grid";
    public static final int DEFAULT_PRECISION = 7;
    private static final int DEFAULT_MAX_NUM_CELLS = 10000;

    private static final ObjectParser<GeoGridAggregationBuilder, Void> PARSER = createParser(NAME, GeoTileUtils::parsePrecision);

    public GeoTileGridAggregationBuilder(String name) {
        super(name);
        precision(DEFAULT_PRECISION);
        size(DEFAULT_MAX_NUM_CELLS);
        shardSize = -1;
    }

    public GeoTileGridAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public GeoGridAggregationBuilder precision(int precision) {
        this.precision = GeoTileUtils.checkPrecisionRange(precision);
        return this;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> createFactory(
            String name, ValuesSourceConfig<ValuesSource.GeoPoint> config, int precision, int requiredSize, int shardSize,
            GeoBoundingBox geoBoundingBox, QueryShardContext queryShardContext, AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData ) throws IOException {
        return new GeoTileGridAggregatorFactory(name, config, precision, requiredSize, shardSize, geoBoundingBox,
            queryShardContext, parent, subFactoriesBuilder, metaData);
    }

    private GeoTileGridAggregationBuilder(GeoTileGridAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder,
                                          Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new GeoTileGridAggregationBuilder(this, factoriesBuilder, metaData);
    }

    public static GeoGridAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new GeoTileGridAggregationBuilder(aggregationName), null);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
