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
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end check that update-by-query reserves heap budget against the REQUEST circuit breaker before
 * each batch's bulk request is built, and surfaces a {@link CircuitBreakingException} to the client when the
 * breaker trips — without sending the oversized bulk request that would have pushed the node toward OOM.
 *
 * <p>This is the UBQ companion to {@link ReindexCircuitBreakerTests}; the two paths share the lifecycle in
 * {@link AbstractAsyncBulkByScrollAction} but each concrete action has its own breaker wiring with a distinct
 * label, so each needs its own end-to-end coverage to guard against wiring drift. The breaker limit and
 * document sizes are chosen so the trip happens on the per-batch ratchet rather than on the upfront seed.
 */
public class UpdateByQueryCircuitBreakerTests extends ESSingleNodeTestCase {

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

    public void testUpdateByQueryFailsWhenPerBatchRatchetExceedsRequestBreakerLimit() {
        // Five docs × ~8 000 bytes of source ⇒ batch estimate ≈ 5 × (8 000 + 50) = 40 250 bytes ⇒ ratchet
        // target ≈ 80 500 bytes ⇒ exceeds the 50 KiB breaker. The 10 KiB upfront seed fits comfortably
        // under 50 KiB, so the trip we observe is from the per-batch ratchet in prepareBulkRequest().
        int batchSize = 5;
        int docCount = batchSize;
        int sourceBytes = 8_000;
        for (int i = 0; i < docCount; i++) {
            prepareIndex("source").setId(Integer.toString(i)).setSource("data", "x".repeat(sourceBytes)).get();
        }
        indicesAdmin().prepareRefresh("source").get();

        UpdateByQueryRequest request = new UpdateByQueryRequest("source");
        request.getSearchRequest().source().size(batchSize);

        ExecutionException thrown = expectThrows(
            ExecutionException.class,
            () -> client().execute(UpdateByQueryAction.INSTANCE, request).get()
        );
        Throwable circuitBreakingCause = ExceptionsHelper.unwrap(thrown, CircuitBreakingException.class);
        assertThat("expected CircuitBreakingException in cause chain, got: " + thrown, circuitBreakingCause, notNullValue());
        // The label is set by TransportUpdateByQueryAction.AsyncIndexBySearchAction#reserveBatchAllocation.
        assertThat(circuitBreakingCause.getMessage(), containsString("update_by_query_bulk_batch"));

        // Source documents should remain unchanged — no bulk request was issued.
        // assertHitCount handles SearchResponse refcount release; calling .get() and dropping the response
        // would leak it and trip the test framework's LeakTracker on the next GC.
        assertHitCount(client().prepareSearch("source").setSize(0), docCount);
    }
}
