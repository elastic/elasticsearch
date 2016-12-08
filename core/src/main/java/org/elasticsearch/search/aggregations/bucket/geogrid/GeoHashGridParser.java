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

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.GeoPointValuesSourceParser;

import java.io.IOException;

/**
 * Aggregates Geo information into cells determined by geohashes of a given precision.
 * WARNING - for high-precision geohashes it may prove necessary to use a {@link GeoBoundingBoxQueryBuilder}
 * aggregation to focus in on a smaller area to avoid generating too many buckets and using too much RAM
 */
public class GeoHashGridParser extends GeoPointValuesSourceParser {

    public static final int DEFAULT_PRECISION = 5;
    public static final int DEFAULT_MAX_NUM_CELLS = 10000;

    private final ObjectParser<GeoGridAggregationBuilder, QueryParseContext> parser;

    public GeoHashGridParser() {
        parser = new ObjectParser<>(GeoGridAggregationBuilder.NAME);
        addFields(parser, false, false);
        parser.declareInt(GeoGridAggregationBuilder::precision, GeoHashGridParams.FIELD_PRECISION);
        parser.declareInt(GeoGridAggregationBuilder::size, GeoHashGridParams.FIELD_SIZE);
        parser.declareInt(GeoGridAggregationBuilder::shardSize, GeoHashGridParams.FIELD_SHARD_SIZE);
    }

    @Override
    public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return parser.parse(context.parser(), new GeoGridAggregationBuilder(aggregationName), context);
    }

}
