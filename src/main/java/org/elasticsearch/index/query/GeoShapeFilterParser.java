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

import com.spatial4j.core.shape.Shape;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.search.shape.ShapeFetchService;

import java.io.IOException;

/**
 * {@link FilterParser} for filtering Documents based on {@link Shape}s.
 * <p/>
 * Only those fields mapped using {@link GeoShapeFieldMapper} can be filtered
 * using this parser.
 * <p/>
 * Format supported:
 * <p/>
 * <pre>
 * "field" : {
 *     "relation" : "intersects",
 *     "shape" : {
 *         "type" : "polygon",
 *         "coordinates" : [
 *              [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 *         ]
 *     }
 * }
 * </pre>
 */
public class GeoShapeFilterParser implements FilterParser {

    public static final String NAME = "geo_shape";

    private ShapeFetchService fetchService;

    public static class DEFAULTS {
        public static final String INDEX_NAME = "shapes";
        public static final String SHAPE_FIELD_NAME = "shape";
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "geoShape"};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        ShapeRelation shapeRelation = ShapeRelation.INTERSECTS;
        String strategyName = null;
        ShapeBuilder shape = null;
        QueryCachingPolicy cache = parseContext.autoFilterCachePolicy();
        HashedBytesRef cacheKey = null;
        String filterName = null;

        String id = null;
        String type = null;
        String index = DEFAULTS.INDEX_NAME;
        String shapePath = DEFAULTS.SHAPE_FIELD_NAME;

        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();

                        token = parser.nextToken();
                        if ("shape".equals(currentFieldName)) {
                            shape = ShapeBuilder.parse(parser);
                        } else if ("relation".equals(currentFieldName)) {
                            shapeRelation = ShapeRelation.getRelationByName(parser.text());
                            if (shapeRelation == null) {
                                throw new QueryParsingException(parseContext.index(), "Unknown shape operation [" + parser.text() + "]");
                            }
                        } else if ("strategy".equals(currentFieldName)) {
                            strategyName = parser.text();
                        } else if ("indexed_shape".equals(currentFieldName) || "indexedShape".equals(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if ("id".equals(currentFieldName)) {
                                        id = parser.text();
                                    } else if ("type".equals(currentFieldName)) {
                                        type = parser.text();
                                    } else if ("index".equals(currentFieldName)) {
                                        index = parser.text();
                                    } else if ("path".equals(currentFieldName)) {
                                        shapePath = parser.text();
                                    }
                                }
                            }
                            if (id == null) {
                                throw new QueryParsingException(parseContext.index(), "ID for indexed shape not provided");
                            } else if (type == null) {
                                throw new QueryParsingException(parseContext.index(), "Type for indexed shape not provided");
                            }
                            shape = fetchService.fetch(id, type, index, shapePath);
                        }  else {
                            throw new QueryParsingException(parseContext.index(), "[geo_shape] filter does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parseContext.parseFilterCachePolicy();
                } else if ("_cache_key".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[geo_shape] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (shape == null) {
            throw new QueryParsingException(parseContext.index(), "No Shape defined");
        } else if (shapeRelation == null) {
            throw new QueryParsingException(parseContext.index(), "No Shape Relation defined");
        }

        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers == null || !smartNameFieldMappers.hasMapper()) {
            throw new QueryParsingException(parseContext.index(), "Failed to find geo_shape field [" + fieldName + "]");
        }

        FieldMapper fieldMapper = smartNameFieldMappers.mapper();
        // TODO: This isn't the nicest way to check this
        if (!(fieldMapper instanceof GeoShapeFieldMapper)) {
            throw new QueryParsingException(parseContext.index(), "Field [" + fieldName + "] is not a geo_shape");
        }

        GeoShapeFieldMapper shapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = shapeFieldMapper.defaultStrategy();
        if (strategyName != null) {
            strategy = shapeFieldMapper.resolveStrategy(strategyName);
        }
        
        Filter filter;
        if (strategy instanceof RecursivePrefixTreeStrategy && shapeRelation == ShapeRelation.DISJOINT) {
            // this strategy doesn't support disjoint anymore: but it did before, including creating lucene fieldcache (!)
            // in this case, execute disjoint as exists && !intersects
            BooleanQuery bool = new BooleanQuery();
            Filter exists = ExistsFilterParser.newFilter(parseContext, fieldName, null);
            Filter intersects = strategy.makeFilter(GeoShapeQueryParser.getArgs(shape, ShapeRelation.INTERSECTS));
            bool.add(exists, BooleanClause.Occur.MUST);
            bool.add(intersects, BooleanClause.Occur.MUST_NOT);
            filter = Queries.wrap(bool);
        } else {
            filter = strategy.makeFilter(GeoShapeQueryParser.getArgs(shape, shapeRelation));
        }

        if (cache != null) {
            filter = parseContext.cacheFilter(filter, cacheKey, cache);
        }

        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }

        return filter;
    }

    @Inject(optional = true)
    public void setFetchService(@Nullable ShapeFetchService fetchService) {
        this.fetchService = fetchService;
    }
}
