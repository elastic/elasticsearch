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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class GeoShapeQueryParser implements QueryParser<GeoShapeQueryBuilder> {

    public static final ParseField SHAPE_FIELD = new ParseField("shape");
    public static final ParseField STRATEGY_FIELD = new ParseField("strategy");
    public static final ParseField RELATION_FIELD = new ParseField("relation");
    public static final ParseField INDEXED_SHAPE_FIELD = new ParseField("indexed_shape");
    public static final ParseField SHAPE_ID_FIELD = new ParseField("id");
    public static final ParseField SHAPE_TYPE_FIELD = new ParseField("type");
    public static final ParseField SHAPE_INDEX_FIELD = new ParseField("index");
    public static final ParseField SHAPE_PATH_FIELD = new ParseField("path");

    @Override
    public String[] names() {
        return new String[]{GeoShapeQueryBuilder.NAME, Strings.toCamelCase(GeoShapeQueryBuilder.NAME)};
    }

    @Override
    public GeoShapeQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        ShapeRelation shapeRelation = null;
        SpatialStrategy strategy = null;
        ShapeBuilder shape = null;

        String id = null;
        String type = null;
        String index = null;
        String shapePath = null;

        XContentParser.Token token;
        String currentFieldName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(), "[" + GeoShapeQueryBuilder.NAME + "] point specified twice. [" + currentFieldName + "]");
                }
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (parseContext.parseFieldMatcher().match(currentFieldName, SHAPE_FIELD)) {
                            shape = ShapeBuilder.parse(parser);
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, STRATEGY_FIELD)) {
                            String strategyName = parser.text();
                            strategy = SpatialStrategy.fromString(strategyName);
                            if (strategy == null) {
                                throw new ParsingException(parser.getTokenLocation(), "Unknown strategy [" + strategyName + " ]");
                            }
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, RELATION_FIELD)) {
                            shapeRelation = ShapeRelation.getRelationByName(parser.text());
                            if (shapeRelation == null) {
                                throw new ParsingException(parser.getTokenLocation(), "Unknown shape operation [" + parser.text() + " ]");
                            }
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, INDEXED_SHAPE_FIELD)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (parseContext.parseFieldMatcher().match(currentFieldName, SHAPE_ID_FIELD)) {
                                        id = parser.text();
                                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, SHAPE_TYPE_FIELD)) {
                                        type = parser.text();
                                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, SHAPE_INDEX_FIELD)) {
                                        index = parser.text();
                                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, SHAPE_PATH_FIELD)) {
                                        shapePath = parser.text();
                                    }
                                } else {
                                    throw new ParsingException(parser.getTokenLocation(), "[" + GeoShapeQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
                                }
                            }
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[" + GeoShapeQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + GeoShapeQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            }
        }
        GeoShapeQueryBuilder builder;
        if (shape != null) {
            builder = new GeoShapeQueryBuilder(fieldName, shape);
        } else {
            builder = new GeoShapeQueryBuilder(fieldName, id, type);
        }
        if (index != null) {
            builder.indexedShapeIndex(index);
        }
        if (shapePath != null) {
            builder.indexedShapePath(shapePath);
        }
        if (shapeRelation != null) {
            builder.relation(shapeRelation);
        }
        if (strategy != null) {
            builder.strategy(strategy);
        }
        if (queryName != null) {
            builder.queryName(queryName);
        }
            builder.boost(boost);
        return builder;
    }

    @Override
    public GeoShapeQueryBuilder getBuilderPrototype() {
        return GeoShapeQueryBuilder.PROTOTYPE;
    }
}
