/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DisMaxQueryBuilderTests extends AbstractQueryTestCase<DisMaxQueryBuilder> {
    /**
     * @return a {@link DisMaxQueryBuilder} with random inner queries
     */
    @Override
    protected DisMaxQueryBuilder doCreateTestQueryBuilder() {
        DisMaxQueryBuilder dismax = new DisMaxQueryBuilder();
        int clauses = randomIntBetween(1, 5);
        for (int i = 0; i < clauses; i++) {
            dismax.add(RandomQueryBuilder.createQuery(random()));
        }
        if (randomBoolean()) {
            dismax.tieBreaker((float) randomDoubleBetween(0d, 1d, true));
        }
        return dismax;
    }

    @Override
    protected DisMaxQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        DisMaxQueryBuilder disMaxQueryBuilder = new DisMaxQueryBuilder();
        disMaxQueryBuilder.add(queryBuilder);
        int innerQueries = randomIntBetween(0, 2);
        for (int i = 0; i < innerQueries; i++) {
            disMaxQueryBuilder.add(randomBoolean() ? queryBuilder : new MatchAllQueryBuilder());
        }
        return disMaxQueryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(DisMaxQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        Collection<Query> queries = AbstractQueryBuilder.toQueries(
            queryBuilder.innerQueries(),
            context,
            new MaxClauseCountQueryVisitor(Integer.MAX_VALUE)
        );
        Query expected = new DisjunctionMaxQuery(queries, queryBuilder.tieBreaker());
        assertEquals(expected, query);
    }

    @Override
    protected Map<String, DisMaxQueryBuilder> getAlternateVersions() {
        Map<String, DisMaxQueryBuilder> alternateVersions = new HashMap<>();
        QueryBuilder innerQuery = createTestQueryBuilder().innerQueries().get(0);
        DisMaxQueryBuilder expectedQuery = new DisMaxQueryBuilder();
        expectedQuery.add(innerQuery);
        String contentString = Strings.format("""
            {
              "dis_max": {
                "queries": %s
              }
            }""", innerQuery.toString());
        alternateVersions.put(contentString, expectedQuery);
        return alternateVersions;
    }

    public void testIllegalArguments() {
        DisMaxQueryBuilder disMaxQuery = new DisMaxQueryBuilder();
        expectThrows(IllegalArgumentException.class, () -> disMaxQuery.add(null));
    }

    public void testToQueryInnerPrefixQuery() throws Exception {
        String queryAsString = Strings.format("""
            {
              "dis_max": {
                "queries": [
                  {
                    "prefix": {
                      "%s": {
                        "value": "sh",
                        "boost": 1.2
                      }
                    }
                  }
                ]
              }
            }""", TEXT_FIELD_NAME);
        Query query = parseQuery(queryAsString).toQuery(createSearchExecutionContext());
        Query expected = new DisjunctionMaxQuery(List.of(new BoostQuery(new PrefixQuery(new Term(TEXT_FIELD_NAME, "sh")), 1.2f)), 0);
        assertEquals(expected, query);
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "dis_max" : {
                "tie_breaker" : 0.7,
                "queries" : [ {
                  "term" : {
                    "age" : {
                      "value" : 34
                    }
                  }
                }, {
                  "term" : {
                    "age" : {
                      "value" : 35
                    }
                  }
                } ],
                "boost" : 1.2
              }
            }""";

        DisMaxQueryBuilder parsed = (DisMaxQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 1.2, parsed.boost(), 0.0001);
        assertEquals(json, 0.7, parsed.tieBreaker(), 0.0001);
        assertEquals(json, 2, parsed.innerQueries().size());
    }

    public void testRewriteMultipleTimes() throws IOException {
        DisMaxQueryBuilder dismax = new DisMaxQueryBuilder();
        dismax.add(new WrapperQueryBuilder(new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()).toString()));
        QueryBuilder rewritten = dismax.rewrite(createSearchExecutionContext());
        DisMaxQueryBuilder expected = new DisMaxQueryBuilder();
        expected.add(new MatchAllQueryBuilder());
        assertEquals(expected, rewritten);

        expected = new DisMaxQueryBuilder();
        expected.add(new MatchAllQueryBuilder());
        QueryBuilder rewrittenAgain = rewritten.rewrite(createSearchExecutionContext());
        assertEquals(rewrittenAgain, expected);
        assertEquals(Rewriteable.rewrite(dismax, createSearchExecutionContext()), expected);
    }

    public void testToQueryChargesEntireTreeOnceMatchingVisitorTotal() throws IOException {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            long before = cb.getUsed();
            Query query = new DisMaxQueryBuilder().add(new TermQueryBuilder(KEYWORD_FIELD_NAME, "value"))
                .add(new TermQueryBuilder(TEXT_FIELD_NAME, "other"))
                .toQuery(context);
            long delta = cb.getUsed() - before;

            assertTrue("a dis_max query must contribute strictly positive bytes to the breaker", delta > 0);
            assertEquals(
                "dis_max must charge the breaker exactly once with the estimate of the query tree it produced",
                estimateBytesOf(query),
                delta
            );
            assertEquals(delta, context.getQueryConstructionMemoryUsed());
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    public void testNestedDisMaxDoesNotCompoundTheCharge() throws IOException {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            long before = cb.getUsed();
            Query query = new DisMaxQueryBuilder().add(new DisMaxQueryBuilder().add(new TermQueryBuilder(KEYWORD_FIELD_NAME, "value")))
                .toQuery(context);
            long delta = cb.getUsed() - before;

            assertTrue("a nested dis_max query must contribute strictly positive bytes to the breaker", delta > 0);
            assertEquals("each dis_max nesting level must not re-charge the clauses below it", estimateBytesOf(query), delta);
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    /**
     * Walks the final Lucene query with a fresh visitor to obtain the estimate the breaker is expected to
     * have been charged, independently of how the builders accumulated it while constructing the query.
     */
    private static long estimateBytesOf(Query query) {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(Integer.MAX_VALUE);
        query.visit(visitor);
        return visitor.getEstimatedBytes();
    }
}
