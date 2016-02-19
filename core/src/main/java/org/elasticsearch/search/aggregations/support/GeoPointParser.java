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

package org.elasticsearch.search.aggregations.support;


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class GeoPointParser {

    private final InternalAggregation.Type aggType;
    private final ParseField field;

    public GeoPointParser(InternalAggregation.Type aggType, ParseField field) {
        this.aggType = aggType;
        this.field = field;
    }

    public boolean token(String aggName, String currentFieldName, XContentParser.Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (!parseFieldMatcher.match(currentFieldName, field)) {
            return false;
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            GeoPoint point = new GeoPoint();
            point.resetFromString(parser.text());
            otherOptions.put(field, point);
            return true;
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
                    throw new ParsingException(parser.getTokenLocation(), "malformed [" + currentFieldName + "] geo point array in ["
                            + aggName + "] " + aggType + " aggregation. a geo point array must be of the form [lon, lat]");
                }
            }
            GeoPoint point = new GeoPoint(lat, lon);
            otherOptions.put(field, point);
            return true;
        }
        if (token == XContentParser.Token.START_OBJECT) {
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
                        "malformed [" + currentFieldName + "] geo point object. either [lat] or [lon] (or both) are " + "missing in ["
                                + aggName + "] " + aggType + " aggregation");
            }
            GeoPoint point = new GeoPoint(lat, lon);
            otherOptions.put(field, point);
            return true;
        }
        return false;
    }

}
