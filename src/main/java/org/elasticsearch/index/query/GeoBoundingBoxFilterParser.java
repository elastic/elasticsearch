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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.InMemoryGeoBoundingBoxFilter;
import org.elasticsearch.index.search.geo.IndexedGeoBoundingBoxFilter;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

/**
 *
 */
public class GeoBoundingBoxFilterParser implements FilterParser {

    public static final String NAME = "geo_bbox";

    @Inject
    public GeoBoundingBoxFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "geoBbox", "geo_bounding_box", "geoBoundingBox"};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean cache = false;
        CacheKeyFilter.Key cacheKey = null;
        String fieldName = null;
        GeoPoint topLeft = new GeoPoint();
        GeoPoint bottomRight = new GeoPoint();

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean normalize = true;

        String type = "memory";

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        GeoPoint point = null;
                        if ("top_left".equals(currentFieldName) || "topLeft".equals(currentFieldName)) {
                            point = topLeft;
                        } else if ("bottom_right".equals(currentFieldName) || "bottomRight".equals(currentFieldName)) {
                            point = bottomRight;
                        }

                        if (point != null) {
                            token = parser.nextToken();
                            point.resetLon(parser.doubleValue());
                            token = parser.nextToken();
                            point.resetLat(parser.doubleValue());
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {

                            }
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        GeoPoint point = null;
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
                                        point.resetLat(parser.doubleValue());
                                    } else if (currentFieldName.equals(GeoPointFieldMapper.Names.LON)) {
                                        point.resetLon(parser.doubleValue());
                                    } else if (currentFieldName.equals(GeoPointFieldMapper.Names.GEOHASH)) {
                                        GeoHashUtils.decode(parser.text(), point);
                                    }
                                }
                            }
                        }
                    } else if (token.isValue()) {
                        if ("field".equals(currentFieldName)) {
                            fieldName = parser.text();
                        } else {
                            GeoPoint point = null;
                            if ("top_left".equals(currentFieldName) || "topLeft".equals(currentFieldName)) {
                                point = topLeft;
                            } else if ("bottom_right".equals(currentFieldName) || "bottomRight".equals(currentFieldName)) {
                                point = bottomRight;
                            }

                            if (point != null) {
                                String value = parser.text();
                                point.resetFromString(value);
                            }
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new CacheKeyFilter.Key(parser.text());
                } else if ("normalize".equals(currentFieldName)) {
                    normalize = parser.booleanValue();
                } else if ("type".equals(currentFieldName)) {
                    type = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[geo_bbox] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (normalize) {
            GeoUtils.normalizePoint(topLeft);
            GeoUtils.normalizePoint(bottomRight);
        }

        MapperService.SmartNameFieldMappers smartMappers = parseContext.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new QueryParsingException(parseContext.index(), "failed to find geo_point field [" + fieldName + "]");
        }
        FieldMapper<?> mapper = smartMappers.mapper();
        if (!(mapper instanceof GeoPointFieldMapper.GeoStringFieldMapper)) {
            throw new QueryParsingException(parseContext.index(), "field [" + fieldName + "] is not a geo_point field");
        }
        GeoPointFieldMapper geoMapper = ((GeoPointFieldMapper.GeoStringFieldMapper) mapper).geoMapper();

        Filter filter;
        if ("indexed".equals(type)) {
            filter = IndexedGeoBoundingBoxFilter.create(topLeft, bottomRight, geoMapper);
        } else if ("memory".equals(type)) {
            IndexGeoPointFieldData<?> indexFieldData = parseContext.fieldData().getForField(mapper);
            filter = new InMemoryGeoBoundingBoxFilter(topLeft, bottomRight, indexFieldData);
        } else {
            throw new QueryParsingException(parseContext.index(), "geo bounding box type [" + type + "] not supported, either 'indexed' or 'memory' are allowed");
        }

        if (cache) {
            filter = parseContext.cacheFilter(filter, cacheKey);
        }
        filter = wrapSmartNameFilter(filter, smartMappers, parseContext);
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}
