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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.InMemoryGeoBoundingBoxFilter;
import org.elasticsearch.index.search.geo.IndexedGeoBoundingBoxFilter;

import java.io.IOException;

/**
 *
 */
public class GeoBoundingBoxFilterParser implements FilterParser {

    public static final String TOP = "top";
    public static final String LEFT = "left";
    public static final String RIGHT = "right";
    public static final String BOTTOM = "bottom";

    public static final String TOP_LEFT = TOP + "_" + LEFT;
    public static final String TOP_RIGHT = TOP + "_" + RIGHT;
    public static final String BOTTOM_LEFT = BOTTOM + "_" + LEFT;
    public static final String BOTTOM_RIGHT = BOTTOM + "_" + RIGHT;

    public static final String TOPLEFT = "topLeft";
    public static final String TOPRIGHT = "topRight";
    public static final String BOTTOMLEFT = "bottomLeft";
    public static final String BOTTOMRIGHT = "bottomRight";

    public static final String NAME = "geo_bbox";
    public static final String FIELD = "field";

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

        QueryCachingPolicy cache = parseContext.autoFilterCachePolicy();
        HashedBytesRef cacheKey = null;
        String fieldName = null;

        double top = Double.NaN;
        double bottom = Double.NaN;
        double left = Double.NaN;
        double right = Double.NaN;
        
        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean normalize = true;

        GeoPoint sparse = new GeoPoint();
        
        String type = "memory";

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (FIELD.equals(currentFieldName)) {
                            fieldName = parser.text();
                        } else if (TOP.equals(currentFieldName)) {
                            top = parser.doubleValue();
                        } else if (BOTTOM.equals(currentFieldName)) {
                            bottom = parser.doubleValue();
                        } else if (LEFT.equals(currentFieldName)) {
                            left = parser.doubleValue();
                        } else if (RIGHT.equals(currentFieldName)) {
                            right = parser.doubleValue();
                        } else {
                            if (TOP_LEFT.equals(currentFieldName) || TOPLEFT.equals(currentFieldName)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                top = sparse.getLat();
                                left = sparse.getLon();
                            } else if (BOTTOM_RIGHT.equals(currentFieldName) || BOTTOMRIGHT.equals(currentFieldName)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                bottom = sparse.getLat();
                                right = sparse.getLon();
                            } else if (TOP_RIGHT.equals(currentFieldName) || TOPRIGHT.equals(currentFieldName)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                top = sparse.getLat();
                                right = sparse.getLon();
                            } else if (BOTTOM_LEFT.equals(currentFieldName) || BOTTOMLEFT.equals(currentFieldName)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                bottom = sparse.getLat();
                                left = sparse.getLon();
                            } else {
                                throw new ElasticsearchParseException("Unexpected field [" + currentFieldName + "]");
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException("fieldname expected but [" + token + "] found");
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parseContext.parseFilterCachePolicy();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else if ("normalize".equals(currentFieldName)) {
                    normalize = parser.booleanValue();
                } else if ("type".equals(currentFieldName)) {
                    type = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[geo_bbox] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        final GeoPoint topLeft = sparse.reset(top, left);  //just keep the object
        final GeoPoint bottomRight = new GeoPoint(bottom, right);

        if (normalize) {
            // Special case: if the difference bettween the left and right is 360 and the right is greater than the left, we are asking for 
            // the complete longitude range so need to set longitude to the complete longditude range
            boolean completeLonRange = ((right - left) % 360 == 0 && right > left);
            GeoUtils.normalizePoint(topLeft, true, !completeLonRange);
            GeoUtils.normalizePoint(bottomRight, true, !completeLonRange);
            if (completeLonRange) {
                topLeft.resetLon(-180);
                bottomRight.resetLon(180);
            }
        }

        MapperService.SmartNameFieldMappers smartMappers = parseContext.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new QueryParsingException(parseContext.index(), "failed to find geo_point field [" + fieldName + "]");
        }
        FieldMapper<?> mapper = smartMappers.mapper();
        if (!(mapper instanceof GeoPointFieldMapper)) {
            throw new QueryParsingException(parseContext.index(), "field [" + fieldName + "] is not a geo_point field");
        }
        GeoPointFieldMapper geoMapper = ((GeoPointFieldMapper) mapper);

        Filter filter;
        if ("indexed".equals(type)) {
            filter = IndexedGeoBoundingBoxFilter.create(topLeft, bottomRight, geoMapper);
        } else if ("memory".equals(type)) {
            IndexGeoPointFieldData indexFieldData = parseContext.getForField(mapper);
            filter = new InMemoryGeoBoundingBoxFilter(topLeft, bottomRight, indexFieldData);
        } else {
            throw new QueryParsingException(parseContext.index(), "geo bounding box type [" + type + "] not supported, either 'indexed' or 'memory' are allowed");
        }

        if (cache != null) {
            filter = parseContext.cacheFilter(filter, cacheKey, cache);
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }    
}
