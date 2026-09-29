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
 * End-to-end check that delete-by-query surfaces a {@link CircuitBreakingException} to the client when
 * the REQUEST circuit breaker limit is too small to accommodate the fetched document-field data, without
 * issuing any bulk request that would push the node toward OOM.
 *
 * <p>DBQ disables {@code _source} fetching (see {@link DeleteByQueryRequest}), so the circuit breaker
 * trips on {@code fetch[document_fields]} — the RAM estimate of the per-hit metadata fields
 * ({@code _id}, {@code _seq_no}, {@code _primary_term}, etc.) accumulated across the batch.  For 100 docs
 * that charge is well above a 2 KiB limit.  The fetch circuit breaker now charges these bytes to the
 * REQUEST breaker during FetchPhase, before the (much smaller) bulk-batch reservation is ever reached.
 *
 * <p>Companion to {@link ReindexCircuitBreakerTests} and {@link UpdateByQueryCircuitBreakerTests}.
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
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "2kb")
            .build();
    }

    public void testDeleteByQueryFailsWhenBulkRequestSizeExceedsRequestBreakerLimit() {
        int docCount = 100;
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
        // The fetch circuit breaker trips during FetchPhase (on document-field metadata) before the
        // bulk-batch reservation is reached; the label is set by FetchPhase's document-fields accounting.
        assertThat(circuitBreakingCause.getMessage(), containsString("fetch[document_fields]"));

        // No documents should have been deleted — no bulk request was issued.
        assertHitCount(client().prepareSearch("source").setSize(0), docCount);
    }
}
