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
 * End-to-end check that reindex reserves heap against the REQUEST circuit breaker for the {@link
 * org.elasticsearch.action.bulk.BulkRequest} it is about to send, and surfaces a {@link
 * CircuitBreakingException} to the client when that reservation can't fit — without sending the oversized
 * bulk request that would have pushed the node toward OOM.
 *
 * <p>The reservation/release lifecycle of the hooks themselves is covered by unit tests in
 * {@code AsyncBulkByPaginatedSearchActionTests}; this class verifies the production wiring (CircuitBreakerService →
 * Reindexer → AsyncIndexBySearchAction) actually fires against a real breaker.
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
            // Sized below the BulkRequest reservation the reindex will attempt (≈ 40 KiB) so the breaker
            // trips when the action calls reserveBatchAllocation in prepareBulkRequest. Local search-side
            // accounting for these 5 small-ish hits stays well under this limit.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "30kb")
            .build();
    }

    public void testReindexFailsWhenBulkRequestSizeExceedsRequestBreakerLimit() {
        // Pre-create dest so we can search it after the failure even though no bulk write reaches it.
        assertAcked(indicesAdmin().prepareCreate("dest"));

        // Five docs × ~8 000-byte source ⇒ BulkRequest.estimatedSizeInBytes() ≈ 5 × (8 000 + 50) ≈ 40 250
        // bytes, which exceeds the 30 KiB breaker limit configured above.
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
