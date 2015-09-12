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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class GeoPointParser {

    private final String aggName;
    private final InternalAggregation.Type aggType;
    private final SearchContext context;
    private final ParseField field;

    GeoPoint point;

    public GeoPointParser(String aggName, InternalAggregation.Type aggType, SearchContext context, ParseField field) {
        this.aggName = aggName;
        this.aggType = aggType;
        this.context = context;
        this.field = field;
    }

    public boolean token(String currentFieldName, XContentParser.Token token, XContentParser parser) throws IOException {
        if (!context.parseFieldMatcher().match(currentFieldName, field)) {
            return false;
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            point = new GeoPoint();
            point.resetFromString(parser.text());
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
                    throw new SearchParseException(context, "malformed [" + currentFieldName + "] geo point array in [" +
                            aggName + "] " + aggType + " aggregation. a geo point array must be of the form [lon, lat]", 
                            parser.getTokenLocation());
                }
            }
            point = new GeoPoint(lat, lon);
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
                throw new SearchParseException(context, "malformed [" + currentFieldName + "] geo point object. either [lat] or [lon] (or both) are " +
                        "missing in [" + aggName + "] " + aggType + " aggregation", parser.getTokenLocation());
            }
            point = new GeoPoint(lat, lon);
            return true;
        }
        return false;
    }

    public GeoPoint geoPoint() {
        return point;
    }

}
