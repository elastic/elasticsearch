/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockIndexEventListener;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for {@link org.elasticsearch.index.shard.IndexShard#waitForSearchReady}: the
 * gate that parks search requests while a shard is in RECOVERING state.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchReadyGateIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockIndexEventListener.TestPlugin.class);
    }

    /**
     * A search against a recovering shard parks in the gate until the shard completes recovery,
     * then completes successfully.
     */
    public void testSearchCompletesAfterShardBecomesReady() throws Exception {
        final String node = internalCluster().startNode();
        final String indexName = "test";

        final var recoveryStarted = new CountDownLatch(1);
        final var allowRecovery = new CountDownLatch(1);

        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node).setNewDelegate(new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                if (indexShard.shardId().getIndexName().equals(indexName)) {
                    recoveryStarted.countDown();
                    safeAwait(allowRecovery);
                }
                listener.onResponse(null);
            }
        });

        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).setWaitForActiveShards(ActiveShardCount.NONE));
        safeAwait(recoveryStarted); // shard is RECOVERING; gate is armed

        ActionFuture<SearchResponse> searchFuture = prepareSearch(indexName).execute();

        // wait for the shard-level task: confirms the request has reached the data node while the
        // shard is still RECOVERING, so the search will park in the gate
        assertBusy(() -> {
            List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(SearchTransportService.QUERY_ACTION_NAME).get().getTasks();
            assertThat(tasks, hasSize(greaterThan(0)));
        });

        allowRecovery.countDown(); // unblock → shard completes recovery → gate fires

        assertResponse(searchFuture, response -> {
            assertThat(response.getSuccessfulShards(), equalTo(1));
            assertThat(response.getFailedShards(), equalTo(0));
        });
    }

    /**
     * Cancelling a search task while it is parked on a recovering shard delivers a failure
     * promptly, without waiting for recovery to complete.
     */
    public void testCancelledSearchFailsPromptlyWhileShardIsRecovering() throws Exception {
        final String node = internalCluster().startNode();
        final String indexName = "test";

        final var recoveryStarted = new CountDownLatch(1);
        final var allowRecovery = new CountDownLatch(1);

        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node).setNewDelegate(new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                if (indexShard.shardId().getIndexName().equals(indexName)) {
                    recoveryStarted.countDown();
                    safeAwait(allowRecovery);
                }
                listener.onResponse(null);
            }
        });

        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).setWaitForActiveShards(ActiveShardCount.NONE));
        safeAwait(recoveryStarted);

        ActionFuture<SearchResponse> searchFuture = prepareSearch(indexName).execute();

        // wait for the task and capture its ID; hasSize(1) is intentional — the task ID must be
        // unambiguous so we cancel the right task
        final var taskId = new AtomicReference<TaskId>();
        assertBusy(() -> {
            List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(TransportSearchAction.TYPE.name()).get().getTasks();
            assertThat(tasks, hasSize(1));
            taskId.set(tasks.getFirst().taskId());
        });

        clusterAdmin().prepareCancelTasks().setTargetTaskId(taskId.get()).get();

        try {
            // 1 primary, 0 replicas: all shards fail on cancellation, so SearchPhaseExecutionException is always thrown
            SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, searchFuture::actionGet);
            assertThat(ExceptionsHelper.unwrap(ex, TaskCancelledException.class), notNullValue());
        } finally {
            allowRecovery.countDown(); // let recovery finish so the node shuts down cleanly
        }
    }
}
