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
package org.elasticsearch.search.aggregations.bucket.range.geodistance;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.GeoPointValuesSourceParser;
import org.elasticsearch.search.aggregations.support.GeoPointParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class GeoDistanceParser extends GeoPointValuesSourceParser {

    static final ParseField ORIGIN_FIELD = new ParseField("origin", "center", "point", "por");
    static final ParseField UNIT_FIELD = new ParseField("unit");
    static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");

    private GeoPointParser geoPointParser = new GeoPointParser(InternalGeoDistance.TYPE, ORIGIN_FIELD);

    public GeoDistanceParser() {
        super(true, false);
    }

    public static class Range extends RangeAggregator.Range {
        public Range(String key, Double from, Double to) {
            super(key(key, from, to), from == null ? 0 : from, to);
        }

        /**
         * Read from a stream.
         */
        public Range(StreamInput in) throws IOException {
            super(in.readOptionalString(), in.readDouble(), in.readDouble());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeDouble(from);
            out.writeDouble(to);
        }

        private static String key(String key, Double from, Double to) {
            if (key != null) {
                return key;
            }
            StringBuilder sb = new StringBuilder();
            sb.append((from == null || from == 0) ? "*" : from);
            sb.append("-");
            sb.append((to == null || Double.isInfinite(to)) ? "*" : to);
            return sb.toString();
        }
    }

    @Override
    protected GeoDistanceAggregationBuilder createFactory(
            String aggregationName, ValuesSourceType valuesSourceType, ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        GeoPoint origin = (GeoPoint) otherOptions.get(ORIGIN_FIELD);
        GeoDistanceAggregationBuilder factory = new GeoDistanceAggregationBuilder(aggregationName, origin);
        @SuppressWarnings("unchecked")
        List<Range> ranges = (List<Range>) otherOptions.get(RangeAggregator.RANGES_FIELD);
        for (Range range : ranges) {
            factory.addRange(range);
        }
        Boolean keyed = (Boolean) otherOptions.get(RangeAggregator.KEYED_FIELD);
        if (keyed != null) {
            factory.keyed(keyed);
        }
        DistanceUnit unit = (DistanceUnit) otherOptions.get(UNIT_FIELD);
        if (unit != null) {
            factory.unit(unit);
        }
        GeoDistance distanceType = (GeoDistance) otherOptions.get(DISTANCE_TYPE_FIELD);
        if (distanceType != null) {
            factory.distanceType(distanceType);
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (geoPointParser.token(aggregationName, currentFieldName, token, parser, parseFieldMatcher, otherOptions)) {
            return true;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            if (parseFieldMatcher.match(currentFieldName, UNIT_FIELD)) {
                DistanceUnit unit = DistanceUnit.fromString(parser.text());
                otherOptions.put(UNIT_FIELD, unit);
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, DISTANCE_TYPE_FIELD)) {
                GeoDistance distanceType = GeoDistance.fromString(parser.text());
                otherOptions.put(DISTANCE_TYPE_FIELD, distanceType);
                return true;
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            if (parseFieldMatcher.match(currentFieldName, RangeAggregator.KEYED_FIELD)) {
                boolean keyed = parser.booleanValue();
                otherOptions.put(RangeAggregator.KEYED_FIELD, keyed);
                return true;
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            if (parseFieldMatcher.match(currentFieldName, RangeAggregator.RANGES_FIELD)) {
                List<Range> ranges = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    String fromAsStr = null;
                    String toAsStr = null;
                    double from = 0.0;
                    double to = Double.POSITIVE_INFINITY;
                    String key = null;
                    String toOrFromOrKey = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            toOrFromOrKey = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if (parseFieldMatcher.match(toOrFromOrKey, Range.FROM_FIELD)) {
                                from = parser.doubleValue();
                            } else if (parseFieldMatcher.match(toOrFromOrKey, Range.TO_FIELD)) {
                                to = parser.doubleValue();
                            }
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if (parseFieldMatcher.match(toOrFromOrKey, Range.KEY_FIELD)) {
                                key = parser.text();
                            } else if (parseFieldMatcher.match(toOrFromOrKey, Range.FROM_FIELD)) {
                                fromAsStr = parser.text();
                            } else if (parseFieldMatcher.match(toOrFromOrKey, Range.TO_FIELD)) {
                                toAsStr = parser.text();
                            }
                        }
                    }
                    if (fromAsStr != null || toAsStr != null) {
                        ranges.add(new Range(key, Double.parseDouble(fromAsStr), Double.parseDouble(toAsStr)));
                    } else {
                        ranges.add(new Range(key, from, to));
                    }
                }
                otherOptions.put(RangeAggregator.RANGES_FIELD, ranges);
                return true;
            }
        }
        return false;
    }
}
