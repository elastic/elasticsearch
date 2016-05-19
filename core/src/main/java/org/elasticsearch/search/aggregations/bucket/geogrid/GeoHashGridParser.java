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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.GeoPointValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

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
    protected GeoGridAggregationBuilder createFactory(
            String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        GeoGridAggregationBuilder factory = new GeoGridAggregationBuilder(aggregationName);
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
}
