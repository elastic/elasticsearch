/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * A page of hits held by a local reindex, update-by-query or delete-by-query worker is charged to the REQUEST breaker until the
 * worker releases it.
 */
public class SearchHitsCircuitBreakerTests extends ESSingleNodeTestCase {

    private static final int BATCH_SIZE = 5;
    // Big enough for a page to reach the minimum cluster.reindex.memory_accounting_threshold
    private static final int SOURCE_BYTES = 256_000;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ReindexPlugin.class);
    }

    @After
    public void resetSettings() {
        updateClusterSettings(Settings.builder().putNull(ReindexSettings.REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING.getKey()));
    }

    public void testPageIsChargedWhileHeld() throws Exception {
        // A page is charged the source bytes of its hits, which all have the same source
        assertChargedWhileHeld(() -> BATCH_SIZE * client().prepareGet("source", "0").get().getSourceAsBytesRef().length());
    }

    public void testHighThresholdTurnsAccountingOff() throws Exception {
        updateClusterSettings(Settings.builder().put(ReindexSettings.REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING.getKey(), "1pb"));
        assertChargedWhileHeld(() -> 0L);
    }

    /**
     * Runs a throttled reindex (PIT) or update-by-query (scroll), which holds its second page without building a bulk from it, and
     * checks what the REQUEST breaker holds for it. The page is charged before the worker throttles on it, so once the task reports
     * being throttled the charge is in place, and it stays for as long as the throttle. Checks the charge is released on cancel.
     * <p>
     * On a multi-shard PIT search the coordinator may have charged the hits already ({@code fetch[coordinator]}, by its own
     * estimate), and that charge is taken over instead. Whether it did depends on how many shards the page came from.
     */
    private void assertChargedWhileHeld(LongSupplier expectedBytes) throws Exception {
        int numShards = between(1, 3);
        createIndex("source", indexSettings(numShards, 0).build());
        indexSource(2 * BATCH_SIZE);
        long expected = expectedBytes.getAsLong();
        CircuitBreaker requestBreaker = requestBreaker();
        long baseline = requestBreaker.getUsed();

        final String action;
        final ActionFuture<BulkByPaginatedSearchResponse> response;
        boolean reindex = randomBoolean();
        if (reindex) {
            ReindexRequest request = new ReindexRequest().setSourceIndices("source");
            request.setDestIndex("dest");
            request.getSearchRequest().source().size(BATCH_SIZE);
            request.setRequestsPerSecond(.000001f);
            action = ReindexAction.NAME;
            response = client().execute(ReindexAction.INSTANCE, request);
        } else {
            UpdateByQueryRequest request = new UpdateByQueryRequest("source");
            request.getSearchRequest().source().size(BATCH_SIZE);
            request.setRequestsPerSecond(.000001f);
            action = UpdateByQueryAction.NAME;
            response = client().execute(UpdateByQueryAction.INSTANCE, request);
        }

        try {
            assertBusy(() -> {
                ListTasksResponse tasks = clusterAdmin().prepareListTasks().setActions(action).setDetailed(true).get();
                assertThat(tasks.getTasks().size(), greaterThan(0));
                var status = (BulkByPaginatedSearchTask.Status) tasks.getTasks().get(0).status();
                assertThat(status.getBatches(), greaterThanOrEqualTo(1));
                assertThat(status.getThrottledUntil(), greaterThan(TimeValue.ZERO));
            });
            // Busy only until the search response that delivered the page is released, with any search-side charge on it
            boolean mayTakeOverCoordinatorCharge = reindex && numShards > 1 && expected > 0;
            assertBusy(() -> {
                long held = requestBreaker.getUsed() - baseline;
                if (mayTakeOverCoordinatorCharge) {
                    assertThat(held, greaterThanOrEqualTo(expected));
                } else {
                    assertEquals(expected, held);
                }
            });
        } finally {
            clusterAdmin().prepareCancelTasks().setActions(action).get();
            response.get(30, TimeUnit.SECONDS);
        }
        assertBusy(() -> assertEquals(baseline, requestBreaker.getUsed()));
    }

    private void indexSource(int docs) {
        for (int i = 0; i < docs; i++) {
            prepareIndex("source").setId(Integer.toString(i)).setSource("data", "x".repeat(SOURCE_BYTES)).get();
        }
        indicesAdmin().prepareRefresh("source").get();
    }

    private void updateClusterSettings(Settings.Builder settings) {
        updateClusterSettings(settings.build());
    }

    private CircuitBreaker requestBreaker() {
        return getInstanceFromNode(CircuitBreakerService.class).getBreaker(CircuitBreaker.REQUEST);
    }
}
