/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(AbstractEsqlIntegTestCase.InternalExchangePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // long enough that within a test only an explicit close removes a sink that no data node request claimed
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMinutes(10))
            .build();
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

    /**
     * A query cancelled between the open and the creation of its compute group task leaves the target with a sink that no data node
     * request will claim. The coordinator removes it, so it does not stay until the inactive-sink reaper.
     */
    public void testCancelBeforeGroupTaskRemovesOpenedSink() throws Exception {
        assertCancelledQueryRemovesSink(CancelPoint.BEFORE_GROUP_TASK);
    }

    /**
     * Without partial results the data node request is a child of the query's task, so once that task is cancelled the request is not
     * sent. Closing the exchange still reaches the target, because the closing request carries no parent task.
     */
    public void testCancelBeforeDataRequestRemovesOpenedSink() throws Exception {
        assertCancelledQueryRemovesSink(CancelPoint.BEFORE_DATA_REQUEST);
    }

    /**
     * The ban of the cancelled query reaches the target before the data node request does, so the target rejects the request before
     * its handler runs. Closing the exchange still reaches the target, because the closing request carries no parent task.
     */
    public void testDataRequestRejectedByBanRemovesOpenedSink() throws Exception {
        assertCancelledQueryRemovesSink(CancelPoint.DATA_REQUEST_BANNED);
    }

    /**
     * Where the query is when it is cancelled. In each case the target has opened the exchange, and no data node request task ever
     * runs there to finish the sink.
     */
    private enum CancelPoint {
        /** The reply to the open is held until the cancellation is in place, with partial results, so the group task is next. */
        BEFORE_GROUP_TASK,
        /** The reply to the open is held until the cancellation is in place, without partial results, so the data request is next. */
        BEFORE_DATA_REQUEST,
        /** The data request is held on the coordinator, after it was registered as a child request, until the target has the ban. */
        DATA_REQUEST_BANNED
    }

    private void assertCancelledQueryRemovesSink(CancelPoint cancelPoint) throws Exception {
        boolean allowPartialResults = switch (cancelPoint) {
            // only a query with partial results creates a group task
            case BEFORE_GROUP_TASK -> true;
            case BEFORE_DATA_REQUEST -> false;
            case DATA_REQUEST_BANNED -> randomBoolean();
        };
        TransportService coordinatorTransport = internalCluster().getInstance(TransportService.class, coordinator);
        TransportService dataNodeTransport = internalCluster().getInstance(TransportService.class, dataNode);
        ExchangeService dataNodeExchanges = internalCluster().getInstance(ExchangeService.class, dataNode);
        AtomicInteger dataRequestsOnDataNode = countRequests(dataNode, ComputeService.DATA_ACTION_NAME);

        // the sink exists once the reply to the open is ready
        CountDownLatch sinkOpened = new CountDownLatch(1);
        SubscribableListener<Void> releaseOpenReply = new SubscribableListener<>();
        holdReplies(dataNode, ExchangeService.OPEN_EXCHANGE_ACTION_NAME, sinkOpened, releaseOpenReply);
        if (cancelPoint == CancelPoint.DATA_REQUEST_BANNED) {
            releaseOpenReply.onResponse(null);
        }

        // createGroupTask only fails while the coordinator holds a ban on the query's task, and the coordinator only bans the task on
        // itself while a child request of the task to itself is outstanding. The coordinator's driver is such a request: holding its
        // reply keeps the ban in place.
        AtomicInteger driverRequests = countRequests(coordinator, DriverTaskRunner.ACTION_NAME);
        SubscribableListener<Void> releaseDriverReplies = new SubscribableListener<>();
        if (cancelPoint == CancelPoint.BEFORE_GROUP_TASK) {
            holdReplies(coordinator, DriverTaskRunner.ACTION_NAME, new CountDownLatch(1), releaseDriverReplies);
        } else {
            releaseDriverReplies.onResponse(null);
        }

        CountDownLatch dataRequestHeld = new CountDownLatch(1);
        CountDownLatch releaseDataRequest = new CountDownLatch(1);
        AtomicReference<TaskId> dataRequestParent = new AtomicReference<>();
        if (cancelPoint == CancelPoint.DATA_REQUEST_BANNED) {
            MockTransportService.getInstance(coordinator)
                .addSendBehavior(dataNodeTransport, (connection, requestId, action, request, options) -> {
                    if (action.equals(ComputeService.DATA_ACTION_NAME)) {
                        // the request is registered as a child of its parent task by now, so a cancellation bans that task here
                        dataRequestParent.set(request.getParentTask());
                        dataRequestHeld.countDown();
                        safeAwait(releaseDataRequest, TimeValue.timeValueSeconds(30));
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
        }

        ActionFuture<EsqlQueryResponse> query = client(coordinator).execute(
            EsqlQueryAction.INSTANCE,
            EsqlQueryRequest.syncEsqlQueryRequest(QUERY).allowPartialResults(allowPartialResults)
        );
        try {
            safeAwait(sinkOpened);
            if (cancelPoint == CancelPoint.DATA_REQUEST_BANNED) {
                safeAwait(dataRequestHeld);
            }
            if (cancelPoint == CancelPoint.BEFORE_GROUP_TASK) {
                assertBusy(() -> assertThat(driverRequests.get(), greaterThan(0)));
            }
            assertFalse("the sink is there before the query is cancelled", dataNodeExchanges.isEmpty());

            CancellableTask queryTask = coordinatorTransport.getTaskManager()
                .getCancellableTasks()
                .values()
                .stream()
                .filter(t -> EsqlQueryAction.NAME.equals(t.getAction()))
                .findFirst()
                .orElseThrow();
            TaskId queryTaskId = new TaskId(coordinatorTransport.getLocalNode().getId(), queryTask.getId());
            client(coordinator).admin().cluster().prepareCancelTasks().setTargetTaskId(queryTaskId).get();
            assertBusy(() -> assertTrue(queryTask.isCancelled()));
            switch (cancelPoint) {
                case BEFORE_GROUP_TASK -> {
                    assertBusy(() -> assertTrue(coordinatorTransport.getTaskManager().getBannedTaskIds().contains(queryTaskId)));
                    releaseOpenReply.onResponse(null);
                }
                case BEFORE_DATA_REQUEST -> releaseOpenReply.onResponse(null);
                case DATA_REQUEST_BANNED -> {
                    assertBusy(() -> assertTrue(dataNodeTransport.getTaskManager().getBannedTaskIds().contains(dataRequestParent.get())));
                    releaseDataRequest.countDown();
                }
            }

            // Within the assertBusy timeout, far less than the inactive-sink interval. The target answers the request that removes the
            // sink before it removes it, hence the wait.
            assertBusy(() -> assertTrue(dataNodeExchanges.isEmpty()));
            assertThat("no data node request runs on the target", dataRequestsOnDataNode.get(), equalTo(0));
        } finally {
            // A failed assertion must not leave anything held: the query could not end, and the teardown would hang or fail on top.
            releaseOpenReply.onResponse(null);
            releaseDriverReplies.onResponse(null);
            releaseDataRequest.countDown();
        }
        Exception e = expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS));
        assertNotNull(e.toString(), ExceptionsHelper.unwrap(e, TaskCancelledException.class));
    }

    /**
     * Only a fetch is cancelled, not its query, before the data node request has claimed the sink. The sink is removed, and the data
     * node request that arrives afterwards fails with the cancellation. If it completed empty, as it does for an exchange that the
     * coordinator finished, the query would succeed without the rows of the shards.
     */
    public void testFetchCancelledAloneFailsLateDataRequest() throws Exception {
        boolean allowPartialResults = randomBoolean();
        TransportService dataNodeTransport = internalCluster().getInstance(TransportService.class, dataNode);
        ExchangeService dataNodeExchanges = internalCluster().getInstance(ExchangeService.class, dataNode);

        CountDownLatch dataRequestHeld = new CountDownLatch(1);
        CountDownLatch releaseDataRequest = new CountDownLatch(1);
        MockTransportService.getInstance(dataNode)
            .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                dataRequestHeld.countDown();
                safeAwait(releaseDataRequest, TimeValue.timeValueSeconds(30));
                handler.messageReceived(request, channel, task);
            });

        scriptPermits.drainPermits();
        scriptPermits.release(numberOfDocs());
        ActionFuture<EsqlQueryResponse> query = client(coordinator).execute(
            EsqlQueryAction.INSTANCE,
            EsqlQueryRequest.syncEsqlQueryRequest(QUERY).allowPartialResults(allowPartialResults)
        );
        try {
            safeAwait(dataRequestHeld);
            // the coordinator starts to fetch once it has sent the data node request
            var dataNodeTasks = dataNodeTransport.getTaskManager();
            AtomicReference<CancellableTask> fetch = new AtomicReference<>();
            assertBusy(() -> {
                fetch.set(
                    dataNodeTasks.getCancellableTasks()
                        .values()
                        .stream()
                        .filter(t -> ExchangeService.EXCHANGE_ACTION_NAME.equals(t.getAction()))
                        .findFirst()
                        .orElse(null)
                );
                assertNotNull(fetch.get());
            });
            dataNodeTasks.cancelTaskAndDescendants(fetch.get(), "test", false, ActionListener.noop());
            assertBusy(() -> assertTrue(dataNodeExchanges.isEmpty()));
        } finally {
            releaseDataRequest.countDown();
        }
        if (allowPartialResults) {
            try (EsqlQueryResponse response = safeGet(query)) {
                assertTrue("the shards of the data node are reported as failed", response.isPartial());
            }
        } else {
            Exception e = expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS));
            assertNotNull(e.toString(), ExceptionsHelper.unwrap(e, TaskCancelledException.class));
        }
    }

    /**
     * Counts the requests for the action that reach the node's handler, which a request rejected at task registration does not.
     */
    private static AtomicInteger countRequests(String nodeName, String action) {
        AtomicInteger requests = new AtomicInteger();
        MockTransportService.getInstance(nodeName).addRequestHandlingBehavior(action, (handler, request, channel, task) -> {
            requests.incrementAndGet();
            handler.messageReceived(request, channel, task);
        });
        return requests;
    }

    /**
     * Holds every reply of the node's handler for the action until {@code release} completes, and counts {@code replyReady} down as
     * each one is ready. Nothing blocks while a reply is held.
     */
    private static void holdReplies(String nodeName, String action, CountDownLatch replyReady, SubscribableListener<Void> release) {
        MockTransportService.getInstance(nodeName)
            .addRequestHandlingBehavior(
                action,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        response.mustIncRef();
                        replyReady.countDown();
                        release.addListener(ActionListener.running(() -> {
                            try {
                                channel.sendResponse(response);
                            } finally {
                                response.decRef();
                            }
                        }));
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        replyReady.countDown();
                        release.addListener(ActionListener.running(() -> channel.sendResponse(exception)));
                    }
                }, task)
            );
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
