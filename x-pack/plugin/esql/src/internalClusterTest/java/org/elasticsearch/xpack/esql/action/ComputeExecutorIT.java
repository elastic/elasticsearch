/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * With a single esql_worker thread, a driver woken by its exchange runs before the drivers of queries that queued up
 * while it was asleep.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ComputeExecutorIT extends AbstractPausableIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EsqlPlugin.ESQL_WORKER_THREAD_POOL_SIZE.getKey(), 1)
            .build();
    }

    public void testWokenDriverRunsBeforeQueuedDrivers() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        // the data driver of the first query parks on the only worker thread at its first doc
        String firstQuery = "from test | keep pause_me";
        ActionFuture<EsqlQueryResponse> first = runQuery(firstQuery);
        assertTrue(scriptWaits.tryAcquire(30, TimeUnit.SECONDS));
        // the coordinator and data drivers of the other queries can only queue behind it
        int others = between(2, 5);
        List<ActionFuture<EsqlQueryResponse>> rest = new ArrayList<>();
        for (int i = 0; i < others; i++) {
            rest.add(runQuery("from test | stats sum(pause_me)"));
        }
        assertBusy(() -> assertThat(esqlWorkerQueue(), equalTo(2 * others)));
        // the first page reaches the coordinator, whose driver is woken while the thread is still busy
        scriptPermits.release(pageSize());
        assertBusy(() -> assertThat(esqlWorkerQueue(), equalTo(2 * others + 1)));
        // the data driver finishes and frees the thread; the woken driver runs before any queued one,
        // which would park on the script again with no permits left
        scriptPermits.release(numberOfDocs() - pageSize());
        assertBusy(() -> {
            DriverStatus status = coordinatorDriverStatus(firstQuery);
            assertTrue(status == null || status.iterations() > 1);
        });

        scriptPermits.release(others * numberOfDocs());
        try (EsqlQueryResponse response = first.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(getValuesList(response), hasSize(numberOfDocs()));
        }
        for (ActionFuture<EsqlQueryResponse> future : rest) {
            try (EsqlQueryResponse response = future.actionGet(30, TimeUnit.SECONDS)) {
                assertThat(getValuesList(response).get(0).get(0), equalTo((long) numberOfDocs()));
            }
        }
    }

    private ActionFuture<EsqlQueryResponse> runQuery(String query) {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.PAGE_SIZE.getKey(), pageSize()).build());
        return client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query).pragmas(pragmas));
    }

    /**
     * Status of the coordinator driver of {@code query}, or {@code null} once the query has completed.
     */
    private DriverStatus coordinatorDriverStatus(String query) {
        List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions("*esql*").setDetailed(true).get().getTasks();
        for (TaskInfo root : tasks) {
            if (root.action().equals(EsqlQueryAction.NAME) && root.description().equals(query)) {
                for (TaskInfo task : tasks) {
                    if (task.action().equals(DriverTaskRunner.ACTION_NAME) && task.parentTaskId().equals(root.taskId())) {
                        return (DriverStatus) task.status();
                    }
                }
            }
        }
        return null;
    }

    private int esqlWorkerQueue() {
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getRandomNodeName());
        for (ThreadPoolStats.Stats stats : threadPool.stats()) {
            if (stats.name().equals(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME)) {
                return stats.queue();
            }
        }
        throw new AssertionError("no " + EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME + " pool");
    }
}
