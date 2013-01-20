/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.sort;

import org.apache.lucene.search.SortField;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.GeoDistanceComparatorSource;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.index.search.geo.GeoHashUtils;
import org.elasticsearch.index.search.geo.GeoUtils;
import org.elasticsearch.index.search.geo.Point;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class GeoDistanceSortParser implements SortParser {

    @Override
    public String[] names() {
        return new String[]{"_geo_distance", "_geoDistance"};
    }

    @Override
    public SortField parse(XContentParser parser, SearchContext context) throws Exception {
        String fieldName = null;
        double lat = Double.NaN;
        double lon = Double.NaN;
        DistanceUnit unit = DistanceUnit.KILOMETERS;
        GeoDistance geoDistance = GeoDistance.ARC;
        boolean reverse = false;

        boolean normalizeLon = true;
        boolean normalizeLat = true;

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                token = parser.nextToken();
                lon = parser.doubleValue();
                token = parser.nextToken();
                lat = parser.doubleValue();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {

                }
                fieldName = currentName;
            } else if (token == XContentParser.Token.START_OBJECT) {
                // the json in the format of -> field : { lat : 30, lon : 12 }
                fieldName = currentName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentName = parser.currentName();
                    } else if (token.isValue()) {
                        if (currentName.equals(GeoPointFieldMapper.Names.LAT)) {
                            lat = parser.doubleValue();
                        } else if (currentName.equals(GeoPointFieldMapper.Names.LON)) {
                            lon = parser.doubleValue();
                        } else if (currentName.equals(GeoPointFieldMapper.Names.GEOHASH)) {
                            double[] values = GeoHashUtils.decode(parser.text());
                            lat = values[0];
                            lon = values[1];
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("reverse".equals(currentName)) {
                    reverse = parser.booleanValue();
                } else if ("order".equals(currentName)) {
                    reverse = "desc".equals(parser.text());
                } else if (currentName.equals("unit")) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (currentName.equals("distance_type") || currentName.equals("distanceType")) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if ("normalize".equals(currentName)) {
                    normalizeLat = parser.booleanValue();
                    normalizeLon = parser.booleanValue();
                } else {
                    // assume the value is the actual value
                    String value = parser.text();
                    int comma = value.indexOf(',');
                    if (comma != -1) {
                        lat = Double.parseDouble(value.substring(0, comma).trim());
                        lon = Double.parseDouble(value.substring(comma + 1).trim());
                    } else {
                        double[] values = GeoHashUtils.decode(value);
                        lat = values[0];
                        lon = values[1];
                    }

                    fieldName = currentName;
                }
            }
        }

        if (normalizeLat || normalizeLon) {
            Point point = new Point(lat, lon);
            GeoUtils.normalizePoint(point, normalizeLat, normalizeLon);
            lat = point.lat;
            lon = point.lon;
        }

        FieldMapper mapper = context.smartNameFieldMapper(fieldName);
        if (mapper == null) {
            throw new ElasticSearchIllegalArgumentException("failed to find mapper for [" + fieldName + "] for geo distance based sort");
        }
        IndexGeoPointFieldData indexFieldData = context.fieldData().getForField(mapper);

        return new SortField(fieldName, new GeoDistanceComparatorSource(indexFieldData, lat, lon, unit, geoDistance), reverse);
    }
}
