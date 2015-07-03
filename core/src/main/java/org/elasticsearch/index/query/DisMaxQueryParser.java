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
public class DisMaxQueryParser extends BaseQueryParser {

    @Inject
    public DisMaxQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{DisMaxQueryBuilder.NAME, Strings.toCamelCase(DisMaxQueryBuilder.NAME)};
    }

    @Override
    public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        float tieBreaker = DisMaxQueryBuilder.DEFAULT_TIE_BREAKER;

        final List<QueryBuilder> queries = newArrayList();
        boolean queriesFound = false;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("queries".equals(currentFieldName)) {
                    queriesFound = true;
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    queries.add(query);
                } else {
                    throw new QueryParsingException(parseContext, "[dis_max] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("queries".equals(currentFieldName)) {
                    queriesFound = true;
                    while (token != XContentParser.Token.END_ARRAY) {
                        QueryBuilder query = parseContext.parseInnerQueryBuilder();
                        queries.add(query);
                        token = parser.nextToken();
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[dis_max] query does not support [" + currentFieldName + "]");
                }
            } else {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    tieBreaker = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[dis_max] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!queriesFound) {
            throw new QueryParsingException(parseContext, "[dis_max] requires 'queries' field");
        }

        DisMaxQueryBuilder disMaxQuery = new DisMaxQueryBuilder();
        disMaxQuery.tieBreaker(tieBreaker);
        disMaxQuery.queryName(queryName);
        disMaxQuery.boost(boost);
        for (QueryBuilder query : queries) {
            disMaxQuery.add(query);
        }
        return disMaxQuery;
    }

    @Override
    public DisMaxQueryBuilder getBuilderPrototype() {
        return DisMaxQueryBuilder.PROTOTYPE;
    }
}
