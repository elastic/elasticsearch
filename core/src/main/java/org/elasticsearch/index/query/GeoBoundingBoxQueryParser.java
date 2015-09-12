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

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.InMemoryGeoBoundingBoxQuery;
import org.elasticsearch.index.search.geo.IndexedGeoBoundingBoxQuery;

import java.io.IOException;

/**
 *
 */
public class GeoBoundingBoxQueryParser implements QueryParser {

    public static final String NAME = "geo_bbox";

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

    public static final String FIELD = "field";

    @Inject
    public GeoBoundingBoxQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "geoBbox", "geo_bounding_box", "geoBoundingBox"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;

        double top = Double.NaN;
        double bottom = Double.NaN;
        double left = Double.NaN;
        double right = Double.NaN;
        
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        final boolean indexCreatedBeforeV2_0 = parseContext.indexVersionCreated().before(Version.V_2_0_0);
        boolean coerce = false;
        boolean ignoreMalformed = false;

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
                        if (parseContext.isDeprecatedSetting(currentFieldName)) {
                            // skip
                        } else if (FIELD.equals(currentFieldName)) {
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
                                throw new ElasticsearchParseException("failed to parse [{}] query. unexpected field [{}]", NAME, currentFieldName);
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException("failed to parse [{}] query. field name expected but [{}] found", NAME, token);
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("coerce".equals(currentFieldName) || (indexCreatedBeforeV2_0 && "normalize".equals(currentFieldName))) {
                    coerce = parser.booleanValue();
                    if (coerce == true) {
                        ignoreMalformed = true;
                    }
                } else if ("type".equals(currentFieldName)) {
                    type = parser.text();
                } else if ("ignore_malformed".equals(currentFieldName) && coerce == false) {
                    ignoreMalformed = parser.booleanValue();
                } else {
                    throw new QueryParsingException(parseContext, "failed to parse [{}] query. unexpected field [{}]", NAME, currentFieldName);
                }
            }
        }

        final GeoPoint topLeft = sparse.reset(top, left);  //just keep the object
        final GeoPoint bottomRight = new GeoPoint(bottom, right);

        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed on 2.x created indexes
        if (!indexCreatedBeforeV2_0 && !ignoreMalformed) {
            if (topLeft.lat() > 90.0 || topLeft.lat() < -90.0) {
                throw new QueryParsingException(parseContext, "illegal latitude value [{}] for [{}]", topLeft.lat(), NAME);
            }
            if (topLeft.lon() > 180.0 || topLeft.lon() < -180) {
                throw new QueryParsingException(parseContext, "illegal longitude value [{}] for [{}]", topLeft.lon(), NAME);
            }
            if (bottomRight.lat() > 90.0 || bottomRight.lat() < -90.0) {
                throw new QueryParsingException(parseContext, "illegal latitude value [{}] for [{}]", bottomRight.lat(), NAME);
            }
            if (bottomRight.lon() > 180.0 || bottomRight.lon() < -180) {
                throw new QueryParsingException(parseContext, "illegal longitude value [{}] for [{}]", bottomRight.lon(), NAME);
            }
        }

        if (coerce) {
            // Special case: if the difference between the left and right is 360 and the right is greater than the left, we are asking for
            // the complete longitude range so need to set longitude to the complete longditude range
            boolean completeLonRange = ((right - left) % 360 == 0 && right > left);
            GeoUtils.normalizePoint(topLeft, true, !completeLonRange);
            GeoUtils.normalizePoint(bottomRight, true, !completeLonRange);
            if (completeLonRange) {
                topLeft.resetLon(-180);
                bottomRight.resetLon(180);
            }
        }

        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryParsingException(parseContext, "failed to parse [{}] query. could not find [{}] field [{}]", NAME, GeoPointFieldMapper.CONTENT_TYPE, fieldName);
        }
        if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryParsingException(parseContext, "failed to parse [{}] query. field [{}] is expected to be of type [{}], but is of [{}] type instead", NAME, fieldName, GeoPointFieldMapper.CONTENT_TYPE, fieldType.typeName());
        }
        GeoPointFieldMapper.GeoPointFieldType geoFieldType = ((GeoPointFieldMapper.GeoPointFieldType) fieldType);

        Query filter;
        if ("indexed".equals(type)) {
            filter = IndexedGeoBoundingBoxQuery.create(topLeft, bottomRight, geoFieldType);
        } else if ("memory".equals(type)) {
            IndexGeoPointFieldData indexFieldData = parseContext.getForField(fieldType);
            filter = new InMemoryGeoBoundingBoxQuery(topLeft, bottomRight, indexFieldData);
        } else {
            throw new QueryParsingException(parseContext, "failed to parse [{}] query. geo bounding box type [{}] is not supported. either [indexed] or [memory] are allowed", NAME, type);
        }

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, filter);
        }
        return filter;
    }    
}
