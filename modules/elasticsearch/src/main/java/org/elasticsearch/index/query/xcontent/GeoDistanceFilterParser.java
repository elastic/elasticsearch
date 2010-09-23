/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.geo.GeoDistance;
import org.elasticsearch.common.lucene.geo.GeoDistanceFilter;
import org.elasticsearch.common.lucene.geo.GeoHashUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.xcontent.XContentGeoPointFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * <pre>
 * {
 *     "name.lat" : 1.1,
 *     "name.lon" : 1.2,
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class GeoDistanceFilterParser extends AbstractIndexComponent implements XContentFilterParser {

    public static final String NAME = "geo_distance";

    @Inject public GeoDistanceFilterParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String[] names() {
        return new String[]{NAME, "geoDistance"};
    }

    @Override public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token;

        String filterName = null;
        String currentFieldName = null;
        double lat = 0;
        double lon = 0;
        String latFieldName = null;
        String lonFieldName = null;
        double distance = 0;
        DistanceUnit unit = null;
        GeoDistance geoDistance = GeoDistance.ARC;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                token = parser.nextToken();
                lat = parser.doubleValue();
                token = parser.nextToken();
                lon = parser.doubleValue();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {

                }
                latFieldName = currentFieldName + "." + XContentGeoPointFieldMapper.Names.LAT;
                lonFieldName = currentFieldName + "." + XContentGeoPointFieldMapper.Names.LON;
            } else if (token == XContentParser.Token.START_OBJECT) {
                // the json in the format of -> field : { lat : 30, lon : 12 }
                String currentName = parser.currentName();
                latFieldName = currentFieldName + XContentGeoPointFieldMapper.Names.LAT_SUFFIX;
                lonFieldName = currentFieldName + XContentGeoPointFieldMapper.Names.LON_SUFFIX;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentName = parser.currentName();
                    } else if (token.isValue()) {
                        if (currentName.equals(XContentGeoPointFieldMapper.Names.LAT)) {
                            lat = parser.doubleValue();
                        } else if (currentName.equals(XContentGeoPointFieldMapper.Names.LON)) {
                            lon = parser.doubleValue();
                        } else if (currentName.equals(XContentGeoPointFieldMapper.Names.GEOHASH)) {
                            double[] values = GeoHashUtils.decode(parser.text());
                            lat = values[0];
                            lon = values[1];
                        }
                    }
                }
            } else if (token.isValue()) {
                if (currentFieldName.equals("distance")) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        distance = DistanceUnit.parse(parser.text(), DistanceUnit.KILOMETERS, DistanceUnit.MILES);
                    } else {
                        distance = parser.doubleValue();
                    }
                } else if (currentFieldName.equals("unit")) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (currentFieldName.equals("distance_type") || currentFieldName.equals("distanceType")) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if (currentFieldName.endsWith(XContentGeoPointFieldMapper.Names.LAT_SUFFIX)) {
                    lat = parser.doubleValue();
                    latFieldName = currentFieldName;
                } else if (currentFieldName.endsWith(XContentGeoPointFieldMapper.Names.LON_SUFFIX)) {
                    lon = parser.doubleValue();
                    lonFieldName = currentFieldName;
                } else if (currentFieldName.endsWith(XContentGeoPointFieldMapper.Names.GEOHASH_SUFFIX)) {
                    double[] values = GeoHashUtils.decode(parser.text());
                    lat = values[0];
                    lon = values[1];
                    latFieldName = currentFieldName.substring(0, currentFieldName.length() - XContentGeoPointFieldMapper.Names.GEOHASH_SUFFIX.length()) + XContentGeoPointFieldMapper.Names.LAT_SUFFIX;
                    lonFieldName = currentFieldName.substring(0, currentFieldName.length() - XContentGeoPointFieldMapper.Names.GEOHASH_SUFFIX.length()) + XContentGeoPointFieldMapper.Names.LON_SUFFIX;
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
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

                    latFieldName = currentFieldName + XContentGeoPointFieldMapper.Names.LAT_SUFFIX;
                    lonFieldName = currentFieldName + XContentGeoPointFieldMapper.Names.LON_SUFFIX;
                }
            }
        }

        if (unit != null) {
            distance = unit.toMiles(distance);
        }

        MapperService mapperService = parseContext.mapperService();
        FieldMapper mapper = mapperService.smartNameFieldMapper(latFieldName);
        if (mapper == null) {
            throw new QueryParsingException(index, "failed to find lat field [" + latFieldName + "]");
        }
        latFieldName = mapper.names().indexName();

        mapper = mapperService.smartNameFieldMapper(lonFieldName);
        if (mapper == null) {
            throw new QueryParsingException(index, "failed to find lon field [" + lonFieldName + "]");
        }
        lonFieldName = mapper.names().indexName();

        Filter filter = new GeoDistanceFilter(lat, lon, distance, geoDistance, latFieldName, lonFieldName, mapper.fieldDataType(), parseContext.indexCache().fieldData());
        filter = wrapSmartNameFilter(filter, parseContext.smartFieldMappers(latFieldName), parseContext);
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}
