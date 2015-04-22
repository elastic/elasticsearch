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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.search.shape.ShapeFetchService;

import java.io.IOException;

public class GeoShapeQueryParser implements QueryParser {

    public static final String NAME = "geo_shape";

    private ShapeFetchService fetchService;

    public static class DEFAULTS {
        public static final String INDEX_NAME = "shapes";
        public static final String SHAPE_FIELD_NAME = "shape";
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        ShapeRelation shapeRelation = ShapeRelation.INTERSECTS;
        String strategyName = null;
        ShapeBuilder shape = null;

        String id = null;
        String type = null;
        String index = DEFAULTS.INDEX_NAME;
        String shapePath = DEFAULTS.SHAPE_FIELD_NAME;

        XContentParser.Token token;
        String currentFieldName = null;
        float boost = 1f;
        String queryName = null;

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
                        } else if ("strategy".equals(currentFieldName)) {
                            strategyName = parser.text();
                        } else if ("relation".equals(currentFieldName)) {
                            shapeRelation = ShapeRelation.getRelationByName(parser.text());
                            if (shapeRelation == null) {
                                throw new QueryParsingException(parseContext.index(), "Unknown shape operation [" + parser.text() + " ]");
                            }
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
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[geo_shape] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[geo_shape] query does not support [" + currentFieldName + "]");
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
        Query query;
        if (strategy instanceof RecursivePrefixTreeStrategy && shapeRelation == ShapeRelation.DISJOINT) {
            // this strategy doesn't support disjoint anymore: but it did before, including creating lucene fieldcache (!)
            // in this case, execute disjoint as exists && !intersects
            BooleanQuery bool = new BooleanQuery();
            Filter exists = ExistsFilterParser.newFilter(parseContext, fieldName, null);
            Filter intersects = strategy.makeFilter(getArgs(shape, ShapeRelation.INTERSECTS));
            bool.add(exists, BooleanClause.Occur.MUST);
            bool.add(intersects, BooleanClause.Occur.MUST_NOT);
            query = new ConstantScoreQuery(bool);
        } else {
            query = strategy.makeQuery(getArgs(shape, shapeRelation));
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Inject(optional = true)
    public void setFetchService(@Nullable ShapeFetchService fetchService) {
        this.fetchService = fetchService;
    }
    
    public static SpatialArgs getArgs(ShapeBuilder shape, ShapeRelation relation) {
        switch(relation) {
        case DISJOINT:
            return new SpatialArgs(SpatialOperation.IsDisjointTo, shape.build());
        case INTERSECTS:
            return new SpatialArgs(SpatialOperation.Intersects, shape.build());
        case WITHIN:
            return new SpatialArgs(SpatialOperation.IsWithin, shape.build());
        default:
            throw new ElasticsearchIllegalArgumentException("");
        
        }
    }
}
