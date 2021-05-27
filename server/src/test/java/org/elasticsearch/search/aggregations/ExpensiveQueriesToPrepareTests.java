/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExpensiveQueriesToPrepareTests extends ESTestCase {
    /**
     * Check that the expensive checker returned the right result.
     */
    interface Check {
        void check(Set<TestQueries> queries, boolean result);
    }

    /**
     * What queries did we send to the expensive checker?
     */
    enum TestQueries {
        TERM,
        SMALL_TERM_IN_SET,
        LARGE_TERM_IN_SET,
        WILDCARD,
        BOOL,
        BOOSTING
    }

    private void test(ExpensiveQueriesToPrepare expensive, Check check) {
        check.check(EnumSet.of(TestQueries.TERM), expensive.isExpensive(termQuery()));
        check.check(EnumSet.of(TestQueries.SMALL_TERM_IN_SET), expensive.isExpensive(smallTermInSetQuery()));
        check.check(EnumSet.of(TestQueries.LARGE_TERM_IN_SET), expensive.isExpensive(largeTermInSetQuery()));
        check.check(EnumSet.of(TestQueries.WILDCARD), expensive.isExpensive(wildcardQuery()));
        for (Occur occur : Occur.values()) {
            check.check(
                EnumSet.of(TestQueries.BOOL, TestQueries.TERM),
                expensive.isExpensive(new BooleanQuery.Builder().add(termQuery(), occur).add(termQuery(), occur).build())
            );
        }
        for (Occur occur : Occur.values()) {
            check.check(
                EnumSet.of(TestQueries.BOOL, TestQueries.TERM, TestQueries.SMALL_TERM_IN_SET),
                expensive.isExpensive(new BooleanQuery.Builder().add(termQuery(), occur).add(smallTermInSetQuery(), occur).build())
            );
        }
        for (Occur occur : Occur.values()) {
            check.check(
                EnumSet.of(TestQueries.BOOL, TestQueries.TERM, TestQueries.LARGE_TERM_IN_SET),
                expensive.isExpensive(new BooleanQuery.Builder().add(termQuery(), occur).add(largeTermInSetQuery(), occur).build())
            );
        }
        for (Occur occur : Occur.values()) {
            check.check(
                EnumSet.of(TestQueries.BOOL, TestQueries.TERM, TestQueries.WILDCARD),
                expensive.isExpensive(new BooleanQuery.Builder().add(termQuery(), occur).add(wildcardQuery(), occur).build())
            );
        }
        check.check(
            EnumSet.of(TestQueries.BOOSTING, TestQueries.SMALL_TERM_IN_SET),
            expensive.isExpensive(new BoostQuery(smallTermInSetQuery(), 2.0f))
        );
        check.check(
            EnumSet.of(TestQueries.BOOSTING, TestQueries.LARGE_TERM_IN_SET),
            expensive.isExpensive(new BoostQuery(largeTermInSetQuery(), 2.0f))
        );
        check.check(
            EnumSet.of(TestQueries.BOOSTING, TestQueries.WILDCARD),
            expensive.isExpensive(new BoostQuery(new WildcardQuery(new Term(randomAlphaOfLength(5), randomAlphaOfLength(5))), 2.0f))
        );
    }

    private Query termQuery() {
        return new TermQuery(new Term(randomAlphaOfLength(5), randomAlphaOfLength(5)));
    }

    private Query wildcardQuery() {
        return new WildcardQuery(new Term(randomAlphaOfLength(5), randomAlphaOfLength(5)));
    }

    private Query smallTermInSetQuery() {
        return termInSetQuery(between(10, 50));
    }

    private Query largeTermInSetQuery() {
        return termInSetQuery(between(1000, 5000));
    }

    private Query termInSetQuery(int count) {
        Set<BytesRef> terms = new HashSet<>(count);
        while (terms.size() < count) {
            terms.add(new BytesRef(randomAlphaOfLength(10)));
        }
        return new TermInSetQuery(randomAlphaOfLength(3), terms);
    }

    public void testAllQueries() {
        test(new ExpensiveQueriesToPrepare(List.of(Query.class)), new Check() {
            @Override
            public void check(Set<TestQueries> queries, boolean result) {
                assertTrue(result);
            }
        });
    }

    public void testWildcard() {
        test(new ExpensiveQueriesToPrepare(List.of(WildcardQuery.class)), new Check() {
            @Override
            public void check(Set<TestQueries> queries, boolean result) {
                if (queries.contains(TestQueries.WILDCARD)) {
                    assertTrue(result);
                } else {
                    assertFalse(result);
                }
            }
        });
    }

    public void testTermInSet() {
        test(new ExpensiveQueriesToPrepare(List.of(TermInSetQuery.class)), new Check() {
            @Override
            public void check(Set<TestQueries> queries, boolean result) {
                if (queries.contains(TestQueries.SMALL_TERM_IN_SET) || queries.contains(TestQueries.LARGE_TERM_IN_SET)) {
                    assertTrue(result);
                } else {
                    assertFalse(result);
                }
            }
        });
    }

    public void testWildcardAndTermInSet() {
        test(new ExpensiveQueriesToPrepare(List.of(WildcardQuery.class, TermInSetQuery.class)), new Check() {
            @Override
            public void check(Set<TestQueries> queries, boolean result) {
                if (queries.contains(TestQueries.WILDCARD)
                    || queries.contains(TestQueries.SMALL_TERM_IN_SET)
                    || queries.contains(TestQueries.LARGE_TERM_IN_SET)) {

                    assertTrue(result);
                } else {
                    assertFalse(result);
                }
            }
        });
    }
}
