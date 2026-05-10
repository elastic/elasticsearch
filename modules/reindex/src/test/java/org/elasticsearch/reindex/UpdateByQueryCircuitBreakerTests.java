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
            // Starve the REQUEST breaker so the per-batch reservation update-by-query makes will trip it.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1kb")
            .build();
    }

    public void testUpdateByQueryFailsWithCircuitBreakingExceptionWhenBatchExceedsRequestBreakerLimit() {
        final int docCount = 5;
        for (int i = 0; i < docCount; i++) {
            prepareIndex("source").setId(Integer.toString(i)).setSource("data", "x".repeat(500)).get();
        }
        indicesAdmin().prepareRefresh("source").get();

        final long sourceCountBefore = client().prepareSearch("source").setSize(0).get().getHits().getTotalHits().value();
        assertEquals(docCount, sourceCountBefore);

        UpdateByQueryRequest request = new UpdateByQueryRequest("source");

        ExecutionException thrown = expectThrows(
            ExecutionException.class,
            () -> client().execute(UpdateByQueryAction.INSTANCE, request).get()
        );
        Throwable circuitBreakingCause = ExceptionsHelper.unwrap(thrown, CircuitBreakingException.class);
        assertThat("expected CircuitBreakingException in cause chain, got: " + thrown, circuitBreakingCause, notNullValue());
        // The label is set by TransportUpdateByQueryAction.AsyncIndexBySearchAction#reserveBatchAllocation.
        assertThat(circuitBreakingCause.getMessage(), containsString("update_by_query_bulk_batch"));

        // Source documents should remain unchanged — no bulk request was issued.
        assertHitCount(client().prepareSearch("source").setSize(0), docCount);
    }
}
