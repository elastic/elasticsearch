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
 * End-to-end check that update-by-query reserves heap against the REQUEST circuit breaker for the {@link
 * org.elasticsearch.action.bulk.BulkRequest} it is about to send, and surfaces a {@link
 * CircuitBreakingException} to the client when that reservation can't fit.
 *
 * <p>This is the UBQ companion to {@link ReindexCircuitBreakerTests}; the two paths share the lifecycle in
 * {@link AbstractAsyncBulkByPaginatedSearchAction} but each concrete action has its own breaker wiring with a distinct
 * label, so each needs its own end-to-end coverage to guard against wiring drift.
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
            // Sized below the BulkRequest reservation UBQ will attempt (≈ 40 KiB) so the breaker trips
            // when the action calls reserveBatchAllocation in prepareBulkRequest.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "30kb")
            .build();
    }

    public void testUpdateByQueryFailsWhenBulkRequestSizeExceedsRequestBreakerLimit() {
        // Five docs × ~8 000-byte source ⇒ BulkRequest.estimatedSizeInBytes() ≈ 5 × (8 000 + 50) ≈ 40 250
        // bytes, which exceeds the 30 KiB breaker limit configured above.
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
