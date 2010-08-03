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
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.geo.GeoHashUtils;
import org.elasticsearch.common.lucene.geo.GeoPolygonFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.xcontent.XContentGeoPointFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.List;

/**
 * <pre>
 * {
 *     "pin.location" : {
 *         "points" : [
 *              { "lat" : 12, "lon" : 40},
 *              {}
 *         ]
 *     }
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class GeoPolygonFilterParser extends AbstractIndexComponent implements XContentFilterParser {

    public static final String NAME = "geo_polygon";

    @Inject public GeoPolygonFilterParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String[] names() {
        return new String[]{NAME, "geoPolygon"};
    }

    @Override public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        String latFieldName = parser.currentName() + XContentGeoPointFieldMapper.Names.LAT_SUFFIX;
        String lonFieldName = parser.currentName() + XContentGeoPointFieldMapper.Names.LON_SUFFIX;

        // now, we move after the field name, which starts the object
        token = parser.nextToken();
        assert token == XContentParser.Token.START_OBJECT;

        List<GeoPolygonFilter.Point> points = Lists.newArrayList();


        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("points".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            GeoPolygonFilter.Point point = new GeoPolygonFilter.Point();
                            token = parser.nextToken();
                            point.lat = parser.doubleValue();
                            token = parser.nextToken();
                            point.lon = parser.doubleValue();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {

                            }
                            points.add(point);
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            GeoPolygonFilter.Point point = new GeoPolygonFilter.Point();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (currentFieldName.equals(XContentGeoPointFieldMapper.Names.LAT)) {
                                        point.lat = parser.doubleValue();
                                    } else if (currentFieldName.equals(XContentGeoPointFieldMapper.Names.LON)) {
                                        point.lon = parser.doubleValue();
                                    } else if (currentFieldName.equals(XContentGeoPointFieldMapper.Names.GEOHASH)) {
                                        double[] values = GeoHashUtils.decode(parser.text());
                                        point.lat = values[0];
                                        point.lon = values[1];
                                    }
                                }
                            }
                            points.add(point);
                        } else if (token.isValue()) {
                            GeoPolygonFilter.Point point = new GeoPolygonFilter.Point();
                            String value = parser.text();
                            int comma = value.indexOf(',');
                            if (comma != -1) {
                                point.lat = Double.parseDouble(value.substring(0, comma).trim());
                                point.lon = Double.parseDouble(value.substring(comma + 1).trim());
                            } else {
                                double[] values = GeoHashUtils.decode(value);
                                point.lat = values[0];
                                point.lon = values[1];
                            }
                            points.add(point);
                        }
                    }
                }
            }
        }

        if (points.isEmpty()) {
            throw new QueryParsingException(index, "no points defined for geo_polygon filter");
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

        return new GeoPolygonFilter(points.toArray(new GeoPolygonFilter.Point[points.size()]), latFieldName, lonFieldName, mapper.fieldDataType(), parseContext.indexCache().fieldData());
    }
}
