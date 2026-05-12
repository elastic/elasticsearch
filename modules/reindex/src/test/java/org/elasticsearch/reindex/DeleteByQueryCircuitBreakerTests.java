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
 * End-to-end check that delete-by-query reserves heap against the REQUEST circuit breaker for the {@link
 * org.elasticsearch.action.bulk.BulkRequest} it is about to send, and surfaces a {@link
 * CircuitBreakingException} to the client when that reservation can't fit.
 *
 * <p>DBQ's bulk request is unusually small — only {@code DeleteRequest}s with no body (DBQ disables
 * {@code _source} fetching, see {@link DeleteByQueryRequest}), so each request contributes just the per-doc
 * {@code BulkRequest.REQUEST_OVERHEAD} (~50 bytes). To trip the breaker on a realistic-sized batch we
 * configure an unusually small breaker limit; in production the small reservation is correct precisely
 * because DBQ's heap pressure is minimal.
 *
 * <p>Companion to {@link ReindexCircuitBreakerTests} and {@link UpdateByQueryCircuitBreakerTests}; each
 * concrete action has its own breaker wiring with a distinct label so each needs its own end-to-end coverage
 * to guard against wiring drift.
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
            // DBQ's BulkRequest for 5 DeleteRequests is ≈ 5 × 50 = 250 bytes (no source bodies). Set the
            // breaker just under that so the reservation in prepareBulkRequest trips.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "200b")
            .build();
    }

    public void testDeleteByQueryFailsWhenBulkRequestSizeExceedsRequestBreakerLimit() {
        int docCount = 5;
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
