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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldDataType;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.search.geo.GeoBoundingBoxFilter;
import org.elasticsearch.index.search.geo.GeoHashUtils;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class GeoBoundingBoxFilterParser extends AbstractIndexComponent implements XContentFilterParser {

    public static final String NAME = "geo_bbox";

    @Inject public GeoBoundingBoxFilterParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String[] names() {
        return new String[]{NAME, "geoBbox", "geo_bounding_box", "geoBoundingBox"};
    }

    @Override public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean cache = false;
        String fieldName = null;
        GeoBoundingBoxFilter.Point topLeft = new GeoBoundingBoxFilter.Point();
        GeoBoundingBoxFilter.Point bottomRight = new GeoBoundingBoxFilter.Point();

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        GeoBoundingBoxFilter.Point point = null;
                        if ("top_left".equals(currentFieldName) || "topLeft".equals(currentFieldName)) {
                            point = topLeft;
                        } else if ("bottom_right".equals(currentFieldName) || "bottomRight".equals(currentFieldName)) {
                            point = bottomRight;
                        }

                        if (point != null) {
                            token = parser.nextToken();
                            point.lon = parser.doubleValue();
                            token = parser.nextToken();
                            point.lat = parser.doubleValue();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {

                            }
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        GeoBoundingBoxFilter.Point point = null;
                        if ("top_left".equals(currentFieldName) || "topLeft".equals(currentFieldName)) {
                            point = topLeft;
                        } else if ("bottom_right".equals(currentFieldName) || "bottomRight".equals(currentFieldName)) {
                            point = bottomRight;
                        }

                        if (point != null) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (currentFieldName.equals(GeoPointFieldMapper.Names.LAT)) {
                                        point.lat = parser.doubleValue();
                                    } else if (currentFieldName.equals(GeoPointFieldMapper.Names.LON)) {
                                        point.lon = parser.doubleValue();
                                    } else if (currentFieldName.equals(GeoPointFieldMapper.Names.GEOHASH)) {
                                        double[] values = GeoHashUtils.decode(parser.text());
                                        point.lat = values[0];
                                        point.lon = values[1];
                                    }
                                }
                            }
                        }
                    } else if (token.isValue()) {
                        if ("field".equals(currentFieldName)) {
                            fieldName = parser.text();
                        } else {
                            GeoBoundingBoxFilter.Point point = null;
                            if ("top_left".equals(currentFieldName) || "topLeft".equals(currentFieldName)) {
                                point = topLeft;
                            } else if ("bottom_right".equals(currentFieldName) || "bottomRight".equals(currentFieldName)) {
                                point = bottomRight;
                            }

                            if (point != null) {
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
                            }
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                }
            }
        }

        MapperService mapperService = parseContext.mapperService();

        FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName);
        if (mapper == null) {
            throw new QueryParsingException(index, "failed to find geo_point field [" + fieldName + "]");
        }
        if (mapper.fieldDataType() != GeoPointFieldDataType.TYPE) {
            throw new QueryParsingException(index, "field [" + fieldName + "] is not a geo_point field");
        }
        fieldName = mapper.names().indexName();


        Filter filter = new GeoBoundingBoxFilter(topLeft, bottomRight, fieldName, parseContext.indexCache().fieldData());
        if (cache) {
            filter = parseContext.cacheFilter(filter);
        }
        filter = wrapSmartNameFilter(filter, parseContext.smartFieldMappers(fieldName), parseContext);
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}
