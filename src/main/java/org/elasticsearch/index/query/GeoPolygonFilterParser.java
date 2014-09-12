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

import com.google.common.collect.Lists;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.GeoPolygonFilter;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

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
public class GeoPolygonFilterParser implements FilterParser {

    public static final String NAME = "geo_polygon";
    public static final String POINTS = "points";

    @Inject
    public GeoPolygonFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "geoPolygon"};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean cache = false;
        CacheKeyFilter.Key cacheKey = null;
        String fieldName = null;

        List<GeoPoint> shell = Lists.newArrayList();

        boolean normalizeLon = true;
        boolean normalizeLat = true;

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
                        if (POINTS.equals(currentFieldName)) {
                            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                shell.add(GeoUtils.parseGeoPoint(parser));
                            }
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[geo_polygon] filter does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[geo_polygon] filter does not support token type [" + token.name() + "] under [" + currentFieldName + "]");
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
                    normalizeLat = parser.booleanValue();
                    normalizeLon = parser.booleanValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[geo_polygon] filter does not support [" + currentFieldName + "]");
                }
            } else {
                throw new QueryParsingException(parseContext.index(), "[geo_polygon] unexpected token type [" + token.name() + "]");
            }
        }

        if (shell.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "no points defined for geo_polygon filter");
        } else {
            if (shell.size() < 3) {
                throw new QueryParsingException(parseContext.index(), "to few points defined for geo_polygon filter");
            }
            GeoPoint start = shell.get(0);
            if (!start.equals(shell.get(shell.size() - 1))) {
                shell.add(start);
            }
            if (shell.size() < 4) {
                throw new QueryParsingException(parseContext.index(), "to few points defined for geo_polygon filter");
            }
        }

        if (normalizeLat || normalizeLon) {
            for (GeoPoint point : shell) {
                GeoUtils.normalizePoint(point, normalizeLat, normalizeLon);
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

        IndexGeoPointFieldData indexFieldData = parseContext.getForField(mapper);
        Filter filter = new GeoPolygonFilter(indexFieldData, shell.toArray(new GeoPoint[shell.size()]));
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
