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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
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
 * End-to-end check that delete-by-query reserves heap budget against the REQUEST circuit breaker and surfaces
 * a {@link CircuitBreakingException} to the client when the breaker trips. Unlike reindex and update-by-query,
 * DBQ deliberately disables {@code _source} fetching (see {@link DeleteByQueryRequest}) and emits
 * {@code DeleteRequest}s (no body) in the bulk, so the per-batch ratchet's byte estimate is small enough that
 * realistically-sized batches cannot trip the breaker on the ratchet path. This test instead verifies the
 * upfront-reservation path in {@code start()} — the only path that meaningfully trips for DBQ — by setting
 * the breaker limit below the upfront seed.
 *
 * <p>This is the DBQ companion to {@link ReindexCircuitBreakerTests} and {@link UpdateByQueryCircuitBreakerTests};
 * each concrete action has its own breaker wiring with a distinct label so each needs its own end-to-end
 * coverage to guard against wiring drift.
 */
public class DeleteByQueryCircuitBreakerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            // Sized to trip on the upfront seed at start() rather than the per-batch ratchet, since DBQ's
            // ratchet estimate is dominated by 50-byte-per-doc REQUEST_OVERHEAD and never reaches a
            // realistic breaker limit. Default scroll_size = 1000 × 1000 × 2 = 2 MiB upfront ⇒ trips the
            // 1 KiB limit.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1kb")
            .build();
    }

    public void testDeleteByQueryFailsWhenUpfrontReservationExceedsRequestBreakerLimit() {
        final int docCount = 5;
        for (int i = 0; i < docCount; i++) {
            prepareIndex("source").setId(Integer.toString(i)).setSource("data", "x".repeat(500)).get();
        }
        indicesAdmin().prepareRefresh("source").get();

        DeleteByQueryRequest request = new DeleteByQueryRequest("source").setQuery(QueryBuilders.matchAllQuery());

        ExecutionException thrown = expectThrows(
            ExecutionException.class,
            () -> client().execute(DeleteByQueryAction.INSTANCE, request).get()
        );
        Throwable circuitBreakingCause = ExceptionsHelper.unwrap(thrown, CircuitBreakingException.class);
        assertThat("expected CircuitBreakingException in cause chain, got: " + thrown, circuitBreakingCause, notNullValue());
        // The label is set by AsyncDeleteByQueryAction#reserveBatchAllocation.
        assertThat(circuitBreakingCause.getMessage(), containsString("delete_by_query_bulk_batch"));

        // No documents should have been deleted — no bulk request was issued.
        assertHitCount(client().prepareSearch("source").setSize(0), docCount);
    }
}
