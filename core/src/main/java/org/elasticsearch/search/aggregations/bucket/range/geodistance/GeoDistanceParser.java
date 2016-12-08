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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.GeoPointValuesSourceParser;

import java.io.IOException;

public class GeoDistanceParser extends GeoPointValuesSourceParser {

    static final ParseField ORIGIN_FIELD = new ParseField("origin", "center", "point", "por");
    static final ParseField UNIT_FIELD = new ParseField("unit");
    static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");

    private final ObjectParser<GeoDistanceAggregationBuilder, QueryParseContext> parser;

    public GeoDistanceParser() {
        parser = new ObjectParser<>(GeoDistanceAggregationBuilder.NAME);
        addFields(parser, true, false);

        parser.declareBoolean(GeoDistanceAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        parser.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges) agg.addRange(range);
        }, GeoDistanceParser::parseRange, RangeAggregator.RANGES_FIELD);

        parser.declareField(GeoDistanceAggregationBuilder::unit, p -> DistanceUnit.fromString(p.text()),
                UNIT_FIELD, ObjectParser.ValueType.STRING);

        parser.declareField(GeoDistanceAggregationBuilder::distanceType, p -> GeoDistance.fromString(p.text()),
                DISTANCE_TYPE_FIELD, ObjectParser.ValueType.STRING);

        parser.declareField(GeoDistanceAggregationBuilder::origin, GeoDistanceParser::parseGeoPoint,
                ORIGIN_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);
    }

    @Override
    public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        GeoDistanceAggregationBuilder builder = parser.parse(context.parser(), new GeoDistanceAggregationBuilder(aggregationName), context);
        if (builder.origin() == null) {
            throw new IllegalArgumentException("Aggregation [" + aggregationName + "] must define an [origin].");
        }
        return builder;
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

    private static GeoPoint parseGeoPoint(XContentParser parser, QueryParseContext context) throws IOException {
        Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            GeoPoint point = new GeoPoint();
            point.resetFromString(parser.text());
            return point;
        }
        if (token == XContentParser.Token.START_ARRAY) {
            double lat = Double.NaN;
            double lon = Double.NaN;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (Double.isNaN(lon)) {
                    lon = parser.doubleValue();
                } else if (Double.isNaN(lat)) {
                    lat = parser.doubleValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "malformed [" + ORIGIN_FIELD.getPreferredName()
                        + "]: a geo point array must be of the form [lon, lat]");
                }
            }
            return new GeoPoint(lat, lon);
        }
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            double lat = Double.NaN;
            double lon = Double.NaN;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("lat".equals(currentFieldName)) {
                        lat = parser.doubleValue();
                    } else if ("lon".equals(currentFieldName)) {
                        lon = parser.doubleValue();
                    }
                }
            }
            if (Double.isNaN(lat) || Double.isNaN(lon)) {
                throw new ParsingException(parser.getTokenLocation(),
                        "malformed [" + currentFieldName + "] geo point object. either [lat] or [lon] (or both) are " + "missing");
            }
            return new GeoPoint(lat, lon);
        }

        // should not happen since we only parse geo points when we encounter a string, an object or an array
        throw new IllegalArgumentException("Unexpected token [" + token + "] while parsing geo point");
    }

    private static Range parseRange(XContentParser parser, QueryParseContext context) throws IOException {
        ParseFieldMatcher parseFieldMatcher = context.getParseFieldMatcher();
        String fromAsStr = null;
        String toAsStr = null;
        double from = 0.0;
        double to = Double.POSITIVE_INFINITY;
        String key = null;
        String toOrFromOrKey = null;
        Token token;
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
            return new Range(key, Double.parseDouble(fromAsStr), Double.parseDouble(toAsStr));
        } else {
            return new Range(key, from, to);
        }
    }
}
