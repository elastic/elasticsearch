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
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

public class BoolQueryBuilderTest extends BaseQueryTestCase<BoolQueryBuilder> {

    @Override
    protected BoolQueryBuilder doCreateTestQueryBuilder() {
        BoolQueryBuilder query = new BoolQueryBuilder();
        if (randomBoolean()) {
            query.adjustPureNegative(randomBoolean());
        }
        if (randomBoolean()) {
            query.disableCoord(randomBoolean());
        }
        if (randomBoolean()) {
            query.minimumNumberShouldMatch(randomIntBetween(1, 10));
        }
        int mustClauses = randomIntBetween(0, 3);
        for (int i = 0; i < mustClauses; i++) {
            query.must(RandomQueryBuilder.createQuery(random()));
        }
        int mustNotClauses = randomIntBetween(0, 3);
        for (int i = 0; i < mustNotClauses; i++) {
            query.mustNot(RandomQueryBuilder.createQuery(random()));
        }
        int shouldClauses = randomIntBetween(0, 3);
        for (int i = 0; i < shouldClauses; i++) {
            query.should(RandomQueryBuilder.createQuery(random()));
        }
        int filterClauses = randomIntBetween(0, 3);
        for (int i = 0; i < filterClauses; i++) {
            query.filter(RandomQueryBuilder.createQuery(random()));
        }
        return query;
    }

    @Override
    protected Query doCreateExpectedQuery(BoolQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        if (!queryBuilder.hasClauses()) {
            return new MatchAllDocsQuery();
        }

        BooleanQuery boolQuery = new BooleanQuery(queryBuilder.disableCoord());
        addBooleanClauses(context, boolQuery, queryBuilder.must(), BooleanClause.Occur.MUST);
        addBooleanClauses(context, boolQuery, queryBuilder.mustNot(), BooleanClause.Occur.MUST_NOT);
        addBooleanClauses(context, boolQuery, queryBuilder.should(), BooleanClause.Occur.SHOULD);
        addBooleanClauses(context, boolQuery, queryBuilder.filter(), BooleanClause.Occur.FILTER);

        if (boolQuery.clauses().isEmpty()) {
            return new MatchAllDocsQuery();
        }
        Queries.applyMinimumShouldMatch(boolQuery, queryBuilder.minimumNumberShouldMatch());
        return queryBuilder.adjustPureNegative() ? fixNegativeQueryIfNeeded(boolQuery) : boolQuery;
    }

    private static void addBooleanClauses(QueryParseContext parseContext, BooleanQuery booleanQuery, List<QueryBuilder> clauses, Occur occurs)
            throws IOException {
        for (QueryBuilder query : clauses) {
            Query innerQuery = query.toQuery(parseContext);
            if (innerQuery != null) {
                booleanQuery.add(new BooleanClause(innerQuery, occurs));
            }
        }
    }

    @Test
    public void testValidate() {
        BoolQueryBuilder booleanQuery = new BoolQueryBuilder();
        int iters = randomIntBetween(0, 3);
        int totalExpectedErrors = 0;
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                booleanQuery.must(RandomQueryBuilder.createInvalidQuery(random()));
                totalExpectedErrors++;
            } else {
                booleanQuery.must(RandomQueryBuilder.createQuery(random()));
            }
        }
        iters = randomIntBetween(0, 3);
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                booleanQuery.should(RandomQueryBuilder.createInvalidQuery(random()));
                totalExpectedErrors++;
            } else {
                booleanQuery.should(RandomQueryBuilder.createQuery(random()));
            }
        }
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                booleanQuery.mustNot(RandomQueryBuilder.createInvalidQuery(random()));
                totalExpectedErrors++;
            } else {
                booleanQuery.mustNot(RandomQueryBuilder.createQuery(random()));
            }
        }
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                booleanQuery.filter(RandomQueryBuilder.createInvalidQuery(random()));
                totalExpectedErrors++;
            } else {
                booleanQuery.filter(RandomQueryBuilder.createQuery(random()));
            }
        }
        assertValidate(booleanQuery, totalExpectedErrors);
    }

    @Test(expected=NullPointerException.class)
    public void testAddNullMust() {
        new BoolQueryBuilder().must(null);
    }

    @Test(expected=NullPointerException.class)
    public void testAddNullMustNot() {
        new BoolQueryBuilder().mustNot(null);
    }

    @Test(expected=NullPointerException.class)
    public void testAddNullShould() {
        new BoolQueryBuilder().should(null);
    }

    @Test(expected=NullPointerException.class)
    public void testAddNullFilter() {
        new BoolQueryBuilder().filter(null);
    }
}
