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
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

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
    protected void doAssertLuceneQuery(BoolQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (!queryBuilder.hasClauses()) {
            assertThat(query, instanceOf(MatchAllDocsQuery.class));
        } else {
            List<BooleanClause> clauses = new ArrayList<>();
            clauses.addAll(getBooleanClauses(queryBuilder.must(), BooleanClause.Occur.MUST, context));
            clauses.addAll(getBooleanClauses(queryBuilder.mustNot(), BooleanClause.Occur.MUST_NOT, context));
            clauses.addAll(getBooleanClauses(queryBuilder.should(), BooleanClause.Occur.SHOULD, context));
            clauses.addAll(getBooleanClauses(queryBuilder.filter(), BooleanClause.Occur.FILTER, context));

            if (clauses.isEmpty()) {
                assertThat(query, instanceOf(MatchAllDocsQuery.class));
            } else {
                assertThat(query, instanceOf(BooleanQuery.class));
                BooleanQuery booleanQuery = (BooleanQuery) query;
                if (queryBuilder.adjustPureNegative()) {
                    boolean isNegative = true;
                    for (BooleanClause clause : clauses) {
                        if (clause.isProhibited() == false) {
                            isNegative = false;
                            break;
                        }
                    }
                    if (isNegative) {
                        clauses.add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.MUST));
                    }
                }
                assertThat(booleanQuery.clauses().size(), equalTo(clauses.size()));
                Iterator<BooleanClause> clauseIterator = clauses.iterator();
                for (BooleanClause booleanClause : booleanQuery.getClauses()) {
                    assertThat(booleanClause, equalTo(clauseIterator.next()));
                }
            }
        }
    }

    private static List<BooleanClause> getBooleanClauses(List<QueryBuilder> queryBuilders, BooleanClause.Occur occur, QueryShardContext context) throws IOException {
        List<BooleanClause> clauses = new ArrayList<>();
        for (QueryBuilder query : queryBuilders) {
            Query innerQuery = query.toQuery(context);
            if (innerQuery != null) {
                clauses.add(new BooleanClause(innerQuery, occur));
            }
        }
        return clauses;
    }

    @Test
    public void testValidate() {
        BoolQueryBuilder booleanQuery = new BoolQueryBuilder();
        int iters = randomIntBetween(0, 3);
        int totalExpectedErrors = 0;
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    booleanQuery.must(RandomQueryBuilder.createInvalidQuery(random()));
                } else {
                    booleanQuery.must(null);
                }
                totalExpectedErrors++;
            } else {
                booleanQuery.must(RandomQueryBuilder.createQuery(random()));
            }
        }
        iters = randomIntBetween(0, 3);
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    booleanQuery.should(RandomQueryBuilder.createInvalidQuery(random()));
                } else {
                    booleanQuery.should(null);
                }
                totalExpectedErrors++;
            } else {
                booleanQuery.should(RandomQueryBuilder.createQuery(random()));
            }
        }
        iters = randomIntBetween(0, 3);
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    booleanQuery.mustNot(RandomQueryBuilder.createInvalidQuery(random()));
                } else {
                    booleanQuery.mustNot(null);
                }
                totalExpectedErrors++;
            } else {
                booleanQuery.mustNot(RandomQueryBuilder.createQuery(random()));
            }
        }
        iters = randomIntBetween(0, 3);
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    booleanQuery.filter(RandomQueryBuilder.createInvalidQuery(random()));
                } else {
                    booleanQuery.filter(null);
                }
                totalExpectedErrors++;
            } else {
                booleanQuery.filter(RandomQueryBuilder.createQuery(random()));
            }
        }
        assertValidate(booleanQuery, totalExpectedErrors);
    }
}
