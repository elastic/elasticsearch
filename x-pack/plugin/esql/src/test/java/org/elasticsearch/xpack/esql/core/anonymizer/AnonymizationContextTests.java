/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.anonymizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

public class AnonymizationContextTests extends ESTestCase {

    public void testColumnTokenStableAcrossCallsInOneContext() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        String first = ctx.mapper().column("user_email");
        String second = ctx.mapper().column("user_email");
        assertEquals("same name maps to same token within one context", first, second);
        assertTrue("column token has expected col_ prefix", first.startsWith("col_"));
    }

    public void testColumnTokenStableAcrossContextsOnSameCluster() {
        String clusterUuid = randomUUID();
        String tokenA = AnonymizationContext.forSubmission(clusterUuid).mapper().column("salary");
        String tokenB = AnonymizationContext.forSubmission(clusterUuid).mapper().column("salary");
        assertEquals("same name + same cluster → same token across contexts", tokenA, tokenB);
    }

    public void testColumnTokenDiffersAcrossClusters() {
        String tokenA = AnonymizationContext.forSubmission(randomUUID()).mapper().column("salary");
        String tokenB = AnonymizationContext.forSubmission(randomUUID()).mapper().column("salary");
        assertNotEquals("different cluster UUIDs produce disjoint token spaces", tokenA, tokenB);
    }

    public void testIndexTokenStableAndPrefixed() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        String first = ctx.mapper().index("customer-orders-2026");
        String second = ctx.mapper().index("customer-orders-2026");
        assertEquals(first, second);
        assertTrue("index token has expected idx_ prefix", first.startsWith("idx_"));
    }

    public void testLiteralIdentityWithinContext() {
        // The (f==5) && (bar==5) requirement: same value+type maps to same token within one context.
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        String first = ctx.mapper().literal(5L, DataType.LONG);
        String second = ctx.mapper().literal(5L, DataType.LONG);
        assertEquals(first, second);
        assertEquals("0", first);
    }

    public void testLiteralIdsAreMonotonicAcrossDistinctValues() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        assertEquals("0", ctx.mapper().literal(5L, DataType.LONG));
        assertEquals("1", ctx.mapper().literal(42, DataType.INTEGER));
        assertEquals("L2", ctx.mapper().literal(new BytesRef("alice"), DataType.KEYWORD));
    }

    public void testLiteralRegeneratesAcrossSubmissions() {
        // Same value, fresh context — fresh interning id. Stable literal tokens would buy no
        // telemetry value and would widen frequency-analysis attack surface on common values.
        String clusterUuid = randomUUID();
        String first = AnonymizationContext.forSubmission(clusterUuid).mapper().literal(5L, DataType.LONG);
        var second = AnonymizationContext.forSubmission(clusterUuid);
        second.mapper().literal(42L, DataType.LONG); // shift the interning rank
        assertNotEquals(first, second.mapper().literal(5L, DataType.LONG));
    }

    public void testNullLiteralValueTaggedExplicitly() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        assertEquals("null", ctx.mapper().literal(null, DataType.KEYWORD));
    }

    public void testStringLiteralKeyedByContentNotInstance() {
        // Two distinct BytesRef instances with the same utf-8 content must intern to the same id.
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        String first = ctx.mapper().literal(new BytesRef("alice"), DataType.KEYWORD);
        String second = ctx.mapper().literal(new BytesRef("alice"), DataType.KEYWORD);
        assertEquals(first, second);
    }

    public void testNullClusterUuidDoesNotNpe() {
        // Test fixtures sometimes pass a null cluster identifier — fall back to a deterministic key.
        var ctx = AnonymizationContext.forSubmission(null);
        assertNotNull(ctx.mapper().column("foo"));
        assertNotNull(ctx.mapper().index("bar"));
        assertNotNull(ctx.mapper().literal(1, DataType.INTEGER));
    }
}
