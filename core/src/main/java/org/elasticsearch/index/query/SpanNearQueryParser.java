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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for span_near query
 */
public class SpanNearQueryParser extends BaseQueryParser<SpanNearQueryBuilder> {

    @Override
    public String[] names() {
        return new String[]{SpanNearQueryBuilder.NAME, Strings.toCamelCase(SpanNearQueryBuilder.NAME)};
    }

    @Override
    public SpanNearQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        Integer slop = null;
        boolean inOrder = SpanNearQueryBuilder.DEFAULT_IN_ORDER;
        boolean collectPayloads = SpanNearQueryBuilder.DEFAULT_COLLECT_PAYLOADS;
        String queryName = null;

        List<SpanQueryBuilder> clauses = new ArrayList<>();

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
                            throw new ParsingException(parser.getTokenLocation(), "spanNear [clauses] must be of type span query");
                        }
                        clauses.add((SpanQueryBuilder) query);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_near] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("in_order".equals(currentFieldName) || "inOrder".equals(currentFieldName)) {
                    inOrder = parser.booleanValue();
                } else if ("collect_payloads".equals(currentFieldName) || "collectPayloads".equals(currentFieldName)) {
                    collectPayloads = parser.booleanValue();
                } else if ("slop".equals(currentFieldName)) {
                    slop = parser.intValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_near] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[span_near] query does not support [" + currentFieldName + "]");
            }
        }

        if (clauses.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "span_near must include [clauses]");
        }

        if (slop == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_near must include [slop]");
        }

        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(clauses.get(0), slop);
        for (int i = 1; i < clauses.size(); i++) {
            queryBuilder.clause(clauses.get(i));
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
