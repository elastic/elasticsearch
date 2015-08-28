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
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.GeoPolygonQuery;

import java.io.IOException;
import java.util.ArrayList;
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
 */
public class GeoPolygonQueryParser implements QueryParser {

    public static final String NAME = "geo_polygon";
    public static final String POINTS = "points";

    @Inject
    public GeoPolygonQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "geoPolygon"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;

        List<GeoPoint> shell = new ArrayList<>();

        final boolean indexCreatedBeforeV2_0 = parseContext.indexVersionCreated().before(Version.V_2_0_0);
        boolean coerce = false;
        boolean ignoreMalformed = false;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (POINTS.equals(currentFieldName)) {
                            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                shell.add(GeoUtils.parseGeoPoint(parser));
                            }
                            if (!shell.get(shell.size()-1).equals(shell.get(0))) {
                                shell.add(shell.get(0));
                            }
                        } else {
                            throw new QueryParsingException(parseContext, "[geo_polygon] query does not support [" + currentFieldName
                                    + "]");
                        }
                    } else {
                        throw new QueryParsingException(parseContext, "[geo_polygon] query does not support token type [" + token.name()
                                + "] under [" + currentFieldName + "]");
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
                } else if ("ignore_malformed".equals(currentFieldName) && coerce == false) {
                    ignoreMalformed = parser.booleanValue();
                } else {
                    throw new QueryParsingException(parseContext, "[geo_polygon] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new QueryParsingException(parseContext, "[geo_polygon] unexpected token type [" + token.name() + "]");
            }
        }

        if (shell.isEmpty()) {
            throw new QueryParsingException(parseContext, "no points defined for geo_polygon query");
        } else {
            if (shell.size() < 3) {
                throw new QueryParsingException(parseContext, "too few points defined for geo_polygon query");
            }
            GeoPoint start = shell.get(0);
            if (!start.equals(shell.get(shell.size() - 1))) {
                shell.add(start);
            }
            if (shell.size() < 4) {
                throw new QueryParsingException(parseContext, "too few points defined for geo_polygon query");
            }
        }

        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed on 2.x created indexes
        if (!indexCreatedBeforeV2_0 && !ignoreMalformed) {
            for (GeoPoint point : shell) {
                if (point.lat() > 90.0 || point.lat() < -90.0) {
                    throw new QueryParsingException(parseContext, "illegal latitude value [{}] for [{}]", point.lat(), NAME);
                }
                if (point.lon() > 180.0 || point.lon() < -180) {
                    throw new QueryParsingException(parseContext, "illegal longitude value [{}] for [{}]", point.lon(), NAME);
                }
            }
        }

        if (coerce) {
            for (GeoPoint point : shell) {
                GeoUtils.normalizePoint(point, coerce, coerce);
            }
        }

        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryParsingException(parseContext, "failed to find geo_point field [" + fieldName + "]");
        }
        if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryParsingException(parseContext, "field [" + fieldName + "] is not a geo_point field");
        }

        IndexGeoPointFieldData indexFieldData = parseContext.getForField(fieldType);
        Query query = new GeoPolygonQuery(indexFieldData, shell.toArray(new GeoPoint[shell.size()]));
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
