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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

/**
 *
 */
public class BoolQueryParser implements QueryParser {

    public static final String NAME = "bool";

    @Inject
    public BoolQueryParser(Settings settings) {
        BooleanQuery.setMaxClauseCount(settings.getAsInt("index.query.bool.max_clause_count", settings.getAsInt("indices.query.bool.max_clause_count", BooleanQuery.getMaxClauseCount())));
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean disableCoord = false;
        float boost = 1.0f;
        String minimumShouldMatch = null;

        List<BooleanClause> clauses = newArrayList();
        boolean adjustPureNegative = true;
        String queryName = null;
        
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("must".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (query != null) {
                        clauses.add(new BooleanClause(query, BooleanClause.Occur.MUST));
                    }
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (query != null) {
                        clauses.add(new BooleanClause(query, BooleanClause.Occur.MUST_NOT));
                    }
                } else if ("should".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (query != null) {
                        clauses.add(new BooleanClause(query, BooleanClause.Occur.SHOULD));
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("must".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Query query = parseContext.parseInnerQuery();
                        if (query != null) {
                            clauses.add(new BooleanClause(query, BooleanClause.Occur.MUST));
                        }
                    }
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Query query = parseContext.parseInnerQuery();
                        if (query != null) {
                            clauses.add(new BooleanClause(query, BooleanClause.Occur.MUST_NOT));
                        }
                    }
                } else if ("should".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Query query = parseContext.parseInnerQuery();
                        if (query != null) {
                            clauses.add(new BooleanClause(query, BooleanClause.Occur.SHOULD));
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "bool query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                    disableCoord = parser.booleanValue();
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("minimum_number_should_match".equals(currentFieldName) || "minimumNumberShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("adjust_pure_negative".equals(currentFieldName) || "adjustPureNegative".equals(currentFieldName)) {
                    adjustPureNegative = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (clauses.isEmpty()) {
            return new MatchAllDocsQuery();
        }

        BooleanQuery booleanQuery = new BooleanQuery(disableCoord);
        for (BooleanClause clause : clauses) {
            booleanQuery.add(clause);
        }
        booleanQuery.setBoost(boost);
        Queries.applyMinimumShouldMatch(booleanQuery, minimumShouldMatch);
        Query query = adjustPureNegative ? fixNegativeQueryIfNeeded(booleanQuery) : booleanQuery;
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
