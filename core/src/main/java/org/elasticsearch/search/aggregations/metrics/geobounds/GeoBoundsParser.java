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

package org.elasticsearch.search.aggregations.metrics.geobounds;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.GeoPointValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class GeoBoundsParser extends GeoPointValuesSourceParser {

    public GeoBoundsParser() {
        super(false, false);
    }

    @Override
    protected GeoBoundsAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                        ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        GeoBoundsAggregationBuilder factory = new GeoBoundsAggregationBuilder(aggregationName);
        Boolean wrapLongitude = (Boolean) otherOptions.get(GeoBoundsAggregator.WRAP_LONGITUDE_FIELD);
        if (wrapLongitude != null) {
            factory.wrapLongitude(wrapLongitude);
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token == XContentParser.Token.VALUE_BOOLEAN) {
            if (parseFieldMatcher.match(currentFieldName, GeoBoundsAggregator.WRAP_LONGITUDE_FIELD)) {
                otherOptions.put(GeoBoundsAggregator.WRAP_LONGITUDE_FIELD, parser.booleanValue());
                return true;
            }
        }
        return false;
    }
}
