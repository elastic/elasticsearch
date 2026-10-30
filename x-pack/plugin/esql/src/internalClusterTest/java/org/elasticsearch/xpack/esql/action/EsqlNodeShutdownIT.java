/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that graceful shutdown of a data node waits for the ES|QL tasks it runs on behalf of a coordinator on another node.
 * <p>
 * The query is started on one node, acting as coordinator, against an index allocated on a separate data-only node. The data node
 * work is paused on the {@code pause_me} runtime field while the data node prepares for shutdown.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlNodeShutdownIT extends AbstractPausableIntegTestCase {

    private static final String QUERY = "FROM test | STATS SUM(pause_me)";

    private String coordinator;
    private String dataNode;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), EsqlAsyncActionIT.LocalStateEsqlAsync.class);
    }

    /**
     * Invoked once, from the base class' {@code @Before}, to create the test index.
     */
    @Override
    protected Settings.Builder testIndexSettings() {
        // the node the cluster starts with acts as coordinator, the index lives on a separate data node
        coordinator = internalCluster().getMasterName();
        dataNode = internalCluster().startDataOnlyNode();
        return super.testIndexSettings().put("index.routing.allocation.include._name", dataNode);
    }

    /**
     * Lets a query left paused by a failed assertion finish, so that the leak checks of the base class don't mask the failure.
     */
    @After
    public void releasePausedQuery() {
        scriptPermits.release(numberOfDocs());
    }

    public void testShutdownWaitsForDataNodeTasks() throws Exception {
        ActionFuture<EsqlQueryResponse> query = startQuery();
        awaitDataNodeTask();

        PlainActionFuture<Void> shutdown = prepareForShutdownAsync(dataNode);
        // the data node task cannot complete until the script permits are released, so the shutdown must not either
        expectThrows(TimeoutException.class, () -> shutdown.get(1, TimeUnit.SECONDS));

        scriptPermits.release(numberOfDocs());
        assertQueryResult(query);
        safeGet(shutdown);
        internalCluster().stopNode(dataNode);
    }

    /**
     * The async query outlives the request that submitted it, so the coordinator must wait for it to complete and store its result.
     */
    public void testShutdownWaitsForAsyncQueryOnCoordinator() throws Exception {
        scriptPermits.drainPermits();
        String id;
        try (
            EsqlQueryResponse initialResponse = client(coordinator).execute(
                EsqlQueryAction.INSTANCE,
                EsqlQueryRequest.asyncEsqlQueryRequest(QUERY)
                    .waitForCompletionTimeout(TimeValue.timeValueMillis(1))
                    .keepOnCompletion(true)
                    .keepAlive(TimeValue.timeValueMinutes(10))
            ).actionGet(30, TimeUnit.SECONDS)
        ) {
            assertTrue(initialResponse.isRunning());
            id = initialResponse.asyncExecutionId().orElseThrow();
        }

        PlainActionFuture<Void> shutdown = prepareForShutdownAsync(coordinator);
        // the async query cannot complete until the script permits are released, so the shutdown must not either
        expectThrows(TimeoutException.class, () -> shutdown.get(1, TimeUnit.SECONDS));

        scriptPermits.release(numberOfDocs());
        safeGet(shutdown);
        var coordinatorTasks = internalCluster().getInstance(TransportService.class, coordinator).getTaskManager().getTasks().values();
        String asyncAction = EsqlQueryAction.NAME + AsyncTaskManagementService.ASYNC_ACTION_SUFFIX;
        assertFalse(coordinatorTasks.stream().anyMatch(t -> asyncAction.equals(t.getAction())));

        var getResultRequest = new GetAsyncResultRequest(id);
        getResultRequest.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30));
        assertQueryResult(client(dataNode).execute(EsqlAsyncGetResultAction.INSTANCE, getResultRequest));
    }

    private ActionFuture<EsqlQueryResponse> startQuery() {
        scriptPermits.drainPermits();
        return client(coordinator).execute(EsqlQueryAction.INSTANCE, EsqlQueryRequest.syncEsqlQueryRequest(QUERY));
    }

    private void awaitDataNodeTask() throws Exception {
        var taskManager = internalCluster().getInstance(TransportService.class, dataNode).getTaskManager();
        assertBusy(
            () -> assertTrue(taskManager.getTasks().values().stream().anyMatch(t -> ComputeService.DATA_ACTION_NAME.equals(t.getAction())))
        );
    }

    private PlainActionFuture<Void> prepareForShutdownAsync(String nodeName) {
        var shutdownPrepareService = internalCluster().getInstance(ShutdownPrepareService.class, nodeName);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        new Thread(() -> {
            try {
                shutdownPrepareService.prepareForShutdown();
                future.onResponse(null);
            } catch (Exception e) {
                future.onFailure(e);
            }
        }, "prepare-for-shutdown-" + nodeName).start();
        return future;
    }

    private void assertQueryResult(ActionFuture<EsqlQueryResponse> query) {
        try (EsqlQueryResponse response = safeGet(query)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs()))));
        }
    }
}
