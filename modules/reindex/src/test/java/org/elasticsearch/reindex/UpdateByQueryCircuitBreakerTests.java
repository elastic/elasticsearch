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
 * End-to-end check that update-by-query surfaces a {@link CircuitBreakingException} to the client when
 * the REQUEST circuit breaker limit is too small to accommodate the fetched source data, without issuing
 * any bulk request that would push the node toward OOM.
 *
 * <p>This is the UBQ companion to {@link ReindexCircuitBreakerTests}; the two paths share the lifecycle in
 * {@link AbstractAsyncBulkByPaginatedSearchAction}. Since the fetch circuit breaker now charges
 * {@code fetch[source]} bytes to the REQUEST breaker for each batch, a low limit trips during the fetch
 * phase rather than at bulk-batch reservation time.
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
            // Sized below the fetch-source charge UBQ will accumulate for one batch (≈ 40–42 KiB for
            // 5 docs × ~8 KiB source each) so the breaker trips during FetchPhase.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "30kb")
            .build();
    }

    public void testUpdateByQueryFailsWhenBulkRequestSizeExceedsRequestBreakerLimit() {
        // Five docs × ~8 000-byte source ⇒ FetchPhase accumulates ≈ 5 × (8 000 + metadata overhead) ≈ 42 KiB
        // of fetch[source] charges, which exceeds the 30 KiB breaker limit configured above.
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
        // The fetch circuit breaker trips during FetchPhase before the bulk-batch reservation is reached;
        // the label is set by FetchPhase's source accounting.
        assertThat(circuitBreakingCause.getMessage(), containsString("fetch[source]"));

        // Source documents should remain unchanged — no bulk request was issued.
        // assertHitCount handles SearchResponse refcount release; calling .get() and dropping the response
        // would leak it and trip the test framework's LeakTracker on the next GC.
        assertHitCount(client().prepareSearch("source").setSize(0), docCount);
    }
}
