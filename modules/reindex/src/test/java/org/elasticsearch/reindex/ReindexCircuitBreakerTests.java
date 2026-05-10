/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end check that reindex reserves heap budget against the REQUEST circuit breaker before each batch's
 * bulk request is built, and surfaces a {@link CircuitBreakingException} to the client when the breaker trips
 * — without sending the oversized bulk request that would have pushed the node toward OOM.
 *
 * <p>The reservation/release lifecycle of the hooks themselves is covered by unit tests in
 * {@code AsyncBulkByScrollActionTests}; this class verifies the production wiring (CircuitBreakerService →
 * Reindexer → AsyncIndexBySearchAction) actually fires against a real breaker. The breaker limit and document
 * sizes here are chosen so the trip happens on the per-batch ratchet rather than on the upfront seed in
 * {@code start()}; otherwise the test would short-circuit before any scroll response was processed.
 */
public class ReindexCircuitBreakerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            // Sized to fit the upfront seed (batchSize=5 × 1000 × 2 = 10 000 bytes) but trip when the
            // per-batch ratchet measures the real document sizes from the first scroll response.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "50kb")
            .build();
    }

    public void testReindexFailsWhenPerBatchRatchetExceedsRequestBreakerLimit() {
        // Pre-create dest so we can search it after the failure even though no bulk write reaches it.
        assertAcked(indicesAdmin().prepareCreate("dest"));

        // Five docs × ~8 000 bytes of source ⇒ batch estimate ≈ 5 × (8 000 + 50) = 40 250 bytes ⇒ ratchet
        // target ≈ 80 500 bytes ⇒ exceeds the 50 KiB breaker. The 10 KiB upfront seed at start() fits
        // comfortably under 50 KiB, so the trip we observe is from the per-batch ratchet in
        // prepareBulkRequest(), not from start().
        int batchSize = 5;
        int docCount = batchSize;
        int sourceBytes = 8_000;
        for (int i = 0; i < docCount; i++) {
            prepareIndex("source").setId(Integer.toString(i)).setSource("data", "x".repeat(sourceBytes)).get();
        }
        indicesAdmin().prepareRefresh("source").get();

        ReindexRequest request = new ReindexRequest().setSourceIndices("source");
        request.setDestIndex("dest");
        request.getSearchRequest().source().size(batchSize);

        ExecutionException thrown = expectThrows(ExecutionException.class, () -> client().execute(ReindexAction.INSTANCE, request).get());
        Throwable circuitBreakingCause = ExceptionsHelper.unwrap(thrown, CircuitBreakingException.class);
        assertThat("expected CircuitBreakingException in cause chain, got: " + thrown, circuitBreakingCause, notNullValue());
        // The label is set by Reindexer.AsyncIndexBySearchAction#reserveBatchAllocation and identifies the source.
        assertThat(circuitBreakingCause.getMessage(), containsString("reindex_bulk_batch"));

        // No bulk request should have been issued — destination remains empty.
        assertHitCount(client().prepareSearch("dest").setSize(0), 0);
    }
}
