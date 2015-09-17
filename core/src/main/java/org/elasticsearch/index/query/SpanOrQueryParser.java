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
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for span_or query
 */
public class SpanOrQueryParser extends BaseQueryParser<SpanOrQueryBuilder> {

    @Override
    public String[] names() {
        return new String[]{SpanOrQueryBuilder.NAME, Strings.toCamelCase(SpanOrQueryBuilder.NAME)};
    }

    @Override
    public SpanOrQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
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
                            throw new QueryParsingException(parseContext, "spanOr [clauses] must be of type span query");
                        }
                        clauses.add((SpanQueryBuilder) query);
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[span_or] query does not support [" + currentFieldName + "]");
                }
            } else {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[span_or] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (clauses.isEmpty()) {
            throw new QueryParsingException(parseContext, "spanOr must include [clauses]");
        }

        SpanOrQueryBuilder queryBuilder = new SpanOrQueryBuilder(clauses.get(0));
        for (int i = 1; i < clauses.size(); i++) {
            queryBuilder.clause(clauses.get(i));
        }
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    @Override
    public SpanOrQueryBuilder getBuilderPrototype() {
        return SpanOrQueryBuilder.PROTOTYPE;
    }
}
