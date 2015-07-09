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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class SpanNearQueryParser extends BaseQueryParser {

    @Inject
    public SpanNearQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{SpanNearQueryBuilder.NAME, Strings.toCamelCase(SpanNearQueryBuilder.NAME)};
    }

    @Override
    public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        Integer slop = null;
        boolean inOrder = SpanNearQueryBuilder.DEFAULT_IN_ORDER;
        boolean collectPayloads = SpanNearQueryBuilder.DEFAULT_COLLECT_PAYLOADS;
        String queryName = null;

        List<SpanQueryBuilder> clauses = newArrayList();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("clauses".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        QueryBuilder query = parseContext.parseInnerQueryBuilder();
                        if (!(query instanceof SpanQueryBuilder)) {
                            throw new QueryParsingException(parseContext, "spanNear [clauses] must be of type span query");
                        }
                        clauses.add((SpanQueryBuilder) query);
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[span_near] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("in_order".equals(currentFieldName) || "inOrder".equals(currentFieldName)) {
                    inOrder = parser.booleanValue();
                } else if ("collect_payloads".equals(currentFieldName) || "collectPayloads".equals(currentFieldName)) {
                    collectPayloads = parser.booleanValue();
                } else if ("slop".equals(currentFieldName)) {
                    slop = Integer.valueOf(parser.intValue());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[span_near] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new QueryParsingException(parseContext, "[span_near] query does not support [" + currentFieldName + "]");
            }
        }

        if (slop == null) {
            throw new QueryParsingException(parseContext, "span_near must include [slop]");
        }

        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(slop);
        for (SpanQueryBuilder subQuery : clauses) {
            queryBuilder.clause(subQuery);
        }
        queryBuilder.inOrder(inOrder);
        queryBuilder.collectPayloads(collectPayloads);
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    @Override
    public SpanNearQueryBuilder getBuilderPrototype() {
        return SpanNearQueryBuilder.PROTOTYPE;
    }
}
