/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.test.AbstractBuilderTestCase;

import java.io.IOException;

/**
 * Compound queries must charge the request breaker once for each of their sub-queries, however deep they are nested.
 */
public class CompoundQueryCircuitBreakerTests extends AbstractBuilderTestCase {

    private static SpanTermQueryBuilder spanTerm(String value) {
        return new SpanTermQueryBuilder(TEXT_FIELD_NAME, value);
    }

    private static SpanNearQueryBuilder spanNear(SpanQueryBuilder first, SpanQueryBuilder second) {
        return new SpanNearQueryBuilder(first, 1).addClause(second);
    }

    public void testSpanQueriesChargeTheirTreeOnce() throws IOException {
        assertChargesTreeOnce(spanNear(spanTerm("a"), spanTerm("b")));
        assertChargesTreeOnce(spanNear(spanNear(spanTerm("a"), spanTerm("b")), spanTerm("c")));
        assertChargesTreeOnce(new SpanOrQueryBuilder(spanTerm("a")).addClause(spanTerm("b")));
        assertChargesTreeOnce(
            new SpanOrQueryBuilder(new SpanOrQueryBuilder(spanTerm("a")).addClause(spanTerm("b"))).addClause(spanTerm("c"))
        );
        assertChargesTreeOnce(new SpanNotQueryBuilder(spanTerm("a"), spanTerm("b")));
        assertChargesTreeOnce(new SpanFirstQueryBuilder(spanTerm("a"), 3));
        assertChargesTreeOnce(new SpanWithinQueryBuilder(spanTerm("a"), spanTerm("b")));
        assertChargesTreeOnce(new SpanContainingQueryBuilder(spanTerm("a"), spanTerm("b")));
        assertChargesTreeOnce(new FieldMaskingSpanQueryBuilder(spanTerm("a"), TEXT_FIELD_NAME));
    }

    public void testSpanNearChargesGapsOnce() throws IOException {
        assertChargesTreeOnce(spanNear(spanTerm("a"), new SpanNearQueryBuilder.SpanGapQueryBuilder(TEXT_FIELD_NAME, 2)));
    }

    public void testBoostingChargesBothClausesOnce() throws IOException {
        QueryBuilder positive = new TermQueryBuilder(KEYWORD_FIELD_NAME, "value");
        QueryBuilder negative = new TermQueryBuilder(TEXT_FIELD_NAME, "other");
        long leaf = charge(positive);
        assertEquals(charge(negative), leaf);

        assertEquals(2 * leaf, charge(new BoostingQueryBuilder(positive, negative)));
        assertEquals(3 * leaf, charge(new BoostingQueryBuilder(new BoostingQueryBuilder(positive, negative), negative)));
    }

    private void assertChargesTreeOnce(QueryBuilder builder) throws IOException {
        CircuitBreaker breaker = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), breaker);
        try {
            Query query = builder.toQuery(context);
            MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(Integer.MAX_VALUE);
            query.visit(visitor);
            assertTrue(visitor.getEstimatedBytes() > 0);
            assertEquals(builder.toString(), visitor.getEstimatedBytes(), breaker.getUsed());
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    private long charge(QueryBuilder builder) throws IOException {
        CircuitBreaker breaker = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), breaker);
        try {
            builder.toQuery(context);
            return breaker.getUsed();
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }
}
