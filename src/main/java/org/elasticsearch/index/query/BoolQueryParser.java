/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;
import static org.elasticsearch.common.lucene.search.Queries.optimizeQuery;

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
        int minimumNumberShouldMatch = -1;

        List<BooleanClause> clauses = newArrayList();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("must".equals(currentFieldName)) {
                    clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.MUST));
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.MUST_NOT));
                } else if ("should".equals(currentFieldName)) {
                    clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.SHOULD));
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("must".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.MUST));
                    }
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.MUST_NOT));
                    }
                } else if ("should".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.SHOULD));
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "bool query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                    disableCoord = parser.booleanValue();
                } else if ("minimum_number_should_match".equals(currentFieldName) || "minimumNumberShouldMatch".equals(currentFieldName)) {
                    minimumNumberShouldMatch = parser.intValue();
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    minimumNumberShouldMatch = parser.intValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (clauses.size() == 1) {
            BooleanClause clause = clauses.get(0);
            if (clause.getOccur() == BooleanClause.Occur.MUST) {
                Query query = clause.getQuery();
                query.setBoost(boost * query.getBoost());
                return query;
            }
            if (clause.getOccur() == BooleanClause.Occur.SHOULD && minimumNumberShouldMatch > 0) {
                Query query = clause.getQuery();
                query.setBoost(boost * query.getBoost());
                return query;
            }
        }

        BooleanQuery query = new BooleanQuery(disableCoord);
        for (BooleanClause clause : clauses) {
            query.add(clause);
        }
        query.setBoost(boost);
        if (minimumNumberShouldMatch != -1) {
            query.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
        }
        return optimizeQuery(fixNegativeQueryIfNeeded(query));
    }
}
