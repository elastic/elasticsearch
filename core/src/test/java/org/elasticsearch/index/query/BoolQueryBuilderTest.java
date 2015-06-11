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
import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticsearch.common.lucene.search.Queries;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

public class BoolQueryBuilderTest extends BaseQueryTestCase<BoolQueryBuilder> {

    @Override
    protected void assertLuceneQuery(BoolQueryBuilder queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            if (queryBuilder.hasClauses()) {
                assertThat(namedQuery, equalTo(query));
            } else {
                assertNull(namedQuery);
            }
        }
    }

    @Override
    protected Query createExpectedQuery(BoolQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        if (!queryBuilder.hasClauses()) {
            return new MatchAllDocsQuery();
        }

        BooleanQuery boolQuery = new BooleanQuery(queryBuilder.disableCoord());
        boolQuery.setBoost(queryBuilder.boost());
        addBooleanClauses(context, boolQuery, queryBuilder.must(), BooleanClause.Occur.MUST);
        addBooleanClauses(context, boolQuery, queryBuilder.mustNot(), BooleanClause.Occur.MUST_NOT);
        addBooleanClauses(context, boolQuery, queryBuilder.should(), BooleanClause.Occur.SHOULD);
        addBooleanClauses(context, boolQuery, queryBuilder.filter(), BooleanClause.Occur.FILTER);

        Queries.applyMinimumShouldMatch(boolQuery, queryBuilder.minimumNumberShouldMatch());
        return queryBuilder.adjustPureNegative() ? fixNegativeQueryIfNeeded(boolQuery) : boolQuery;
    }

    private static void addBooleanClauses(QueryParseContext parseContext, BooleanQuery booleanQuery, List<QueryBuilder> clauses, Occur occurs)
            throws IOException {
        for (QueryBuilder query : clauses) {
            booleanQuery.add(new BooleanClause(query.toQuery(parseContext), occurs));
        }
    }

    @Override
    protected RandomQueryBuilder<BoolQueryBuilder> getRandomQueryBuilder() {
        return new RandomBoolQueryBuilder();
    }
}
