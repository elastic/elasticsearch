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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.NodeNotConnectedException;
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
 * <p>
 * The nodes of an internal cluster have no termination handler, so their shutdown takes the stateful path and never drains. The drain
 * tests build their own {@link ShutdownPrepareService} with a handler that reports {@link TerminationHandler.Outcome#READY}, see
 * {@link #shutdownServiceWithReadyHandler}.
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
     * An open admitted before the cutoff holds the drain back, and the data node request that follows it after the cutoff still runs on
     * this node. The reply to the open is held until admission is closed, so the coordinator only sends the data node request then.
     */
    public void testDrainWaitsForExchangeAdmittedBeforeCutoff() throws Exception {
        // the sink exists and the exchange is recorded as admitted once the reply is ready
        CountDownLatch openAdmitted = new CountDownLatch(1);
        SubscribableListener<Void> releaseOpenReply = new SubscribableListener<>();
        holdReplies(dataNode, ExchangeService.OPEN_EXCHANGE_ACTION_NAME, openAdmitted, releaseOpenReply);

        ActionFuture<EsqlQueryResponse> query = startQuery();
        safeAwait(openAdmitted);

        PlainActionFuture<Void> shutdown = prepareForShutdownAsync(shutdownServiceWithReadyHandler(dataNode), dataNode);
        ExchangeService exchangeService = internalCluster().getInstance(ExchangeService.class, dataNode);
        assertBusy(() -> assertTrue(exchangeService.isAdmissionClosed()));
        // the admitted exchange has not finished, so neither can the drain
        expectThrows(TimeoutException.class, () -> shutdown.get(1, TimeUnit.SECONDS));

        releaseOpenReply.onResponse(null);
        // the data node request arrives after the cutoff and runs here, paused on the script
        awaitDataNodeTask();
        expectThrows(TimeoutException.class, () -> shutdown.get(1, TimeUnit.SECONDS));

        scriptPermits.release(numberOfDocs());
        assertQueryResult(query);
        safeGet(shutdown);
        assertTrue(exchangeService.isEmpty());
    }

    /**
     * A node that stopped admitting exchanges refuses an open; the coordinator resolves the shard again, leaves that node out, and gets
     * the result from the node the shard moved to.
     */
    public void testRefusedOpenIsRetriedOnTheNodeTheShardMovedTo() throws Exception {
        assertOpenRetriedOnTheNodeTheShardMovedTo(true);
    }

    /**
     * Like {@link #testRefusedOpenIsRetriedOnTheNodeTheShardMovedTo}, for a node the coordinator cannot reach any more.
     */
    public void testOpenToUnreachableNodeIsRetriedOnTheNodeTheShardMovedTo() throws Exception {
        assertOpenRetriedOnTheNodeTheShardMovedTo(false);
    }

    /**
     * Holds the coordinator's open to the data node until the shard has moved to a new node, so the coordinator still targets the data
     * node, then lets the data node refuse the open or fails it as if the node could not be reached.
     */
    private void assertOpenRetriedOnTheNodeTheShardMovedTo(boolean refuse) throws Exception {
        String replacement = internalCluster().startDataOnlyNode();
        ExchangeService dataNodeExchanges = internalCluster().getInstance(ExchangeService.class, dataNode);
        if (refuse) {
            // nothing is admitted yet, so the drain completes right away and the node stays up with admission closed
            safeGet(prepareForShutdownAsync(shutdownServiceWithReadyHandler(dataNode), dataNode));
            assertTrue(dataNodeExchanges.isAdmissionClosed());
        }
        AtomicInteger opensOnDataNode = countOpens(dataNode);
        AtomicInteger opensOnReplacement = countOpens(replacement);

        CountDownLatch openSent = new CountDownLatch(1);
        CountDownLatch shardMoved = new CountDownLatch(1);
        MockTransportService.getInstance(coordinator)
            .addSendBehavior(
                internalCluster().getInstance(TransportService.class, dataNode),
                (connection, requestId, action, request, options) -> {
                    if (action.equals(ExchangeService.OPEN_EXCHANGE_ACTION_NAME)) {
                        openSent.countDown();
                        safeAwait(shardMoved, TimeValue.timeValueSeconds(30));
                        if (refuse == false) {
                            throw new NodeNotConnectedException(connection.getNode(), "simulated");
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                }
            );

        scriptPermits.drainPermits();
        scriptPermits.release(numberOfDocs());
        ActionFuture<EsqlQueryResponse> query = client(coordinator).execute(
            EsqlQueryAction.INSTANCE,
            EsqlQueryRequest.syncEsqlQueryRequest(QUERY).allowPartialResults(randomBoolean())
        );
        safeAwait(openSent);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include._name", replacement), "test");
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, "test").setWaitForGreenStatus().setWaitForNoRelocatingShards(true).get();
        shardMoved.countDown();

        try (EsqlQueryResponse response = safeGet(query)) {
            assertFalse(response.isPartial());
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs()))));
        }
        assertThat(opensOnDataNode.get(), equalTo(refuse ? 1 : 0));
        assertThat(opensOnReplacement.get(), equalTo(1));
        assertTrue("a refused open leaves no sink", dataNodeExchanges.isEmpty());
    }

    /**
     * A query cancelled between the open and the creation of its compute group task leaves the target with an admitted sink that no
     * data node request will claim. The coordinator removes it, so the target's drain does not wait for the inactive-sink reaper.
     */
    public void testCancelBeforeGroupTaskRemovesOpenedSink() throws Exception {
        assertCancelledQueryRemovesAdmittedSink(CancelPoint.BEFORE_GROUP_TASK);
    }

    /**
     * Without partial results the data node request is a child of the query's task, so once that task is cancelled the request is not
     * sent. Closing the exchange still reaches the target, because the closing request carries no parent task.
     */
    public void testCancelBeforeDataRequestRemovesOpenedSink() throws Exception {
        assertCancelledQueryRemovesAdmittedSink(CancelPoint.BEFORE_DATA_REQUEST);
    }

    /**
     * The ban of the cancelled query reaches the target before the data node request does, so the target rejects the request before
     * its handler runs. Closing the exchange still reaches the target, because the closing request carries no parent task.
     */
    public void testDataRequestRejectedByBanRemovesOpenedSink() throws Exception {
        assertCancelledQueryRemovesAdmittedSink(CancelPoint.DATA_REQUEST_BANNED);
    }

    /**
     * Where the query is when it is cancelled. In each case the target has admitted the exchange, and no data node request task ever
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

    private void assertCancelledQueryRemovesAdmittedSink(CancelPoint cancelPoint) throws Exception {
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

        CountDownLatch openAdmitted = new CountDownLatch(1);
        SubscribableListener<Void> releaseOpenReply = new SubscribableListener<>();
        holdReplies(dataNode, ExchangeService.OPEN_EXCHANGE_ACTION_NAME, openAdmitted, releaseOpenReply);
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
        safeAwait(openAdmitted);
        if (cancelPoint == CancelPoint.DATA_REQUEST_BANNED) {
            safeAwait(dataRequestHeld);
        }
        if (cancelPoint == CancelPoint.BEFORE_GROUP_TASK) {
            assertBusy(() -> assertThat(driverRequests.get(), greaterThan(0)));
        }
        SubscribableListener<Void> drained = dataNodeExchanges.closeAdmission();
        assertFalse(drained.isDone());

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

        // within the safe-await timeout, far less than the inactive-sink interval
        safeAwait(drained);
        assertTrue(dataNodeExchanges.isEmpty());
        assertThat("no data node request runs on the target", dataRequestsOnDataNode.get(), equalTo(0));

        releaseDriverReplies.onResponse(null);
        Exception e = expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS));
        assertNotNull(e.toString(), ExceptionsHelper.unwrap(e, TaskCancelledException.class));
    }

    /**
     * The data node request to the node a shard is leaving cannot be sent. The coordinator closes the exchange it opened there,
     * resolves the shard again, and gets the result from the node the shard moved to.
     */
    public void testDataRequestFailedOnSendIsRetriedOnTheNodeTheShardMovedTo() throws Exception {
        assertDataRequestCutOffBeforeFirstPage(false, true);
    }

    /**
     * Like {@link #testDataRequestFailedOnSendIsRetriedOnTheNodeTheShardMovedTo}, for a data node request whose connection closes
     * before the old node answered or sent a page. The closed connection cancels the request's task on the old node, which finishes
     * the sink there.
     */
    public void testDataRequestDisconnectedInFlightIsRetriedOnTheNodeTheShardMovedTo() throws Exception {
        assertDataRequestCutOffBeforeFirstPage(true, true);
    }

    /**
     * Without partial results, a data node request cut off before its first page fails the query, as before.
     */
    public void testDataRequestCutOffWithoutPartialResultsFailsTheQuery() throws Exception {
        assertDataRequestCutOffBeforeFirstPage(randomBoolean(), false);
    }

    /**
     * Lets the open to the data node through, then cuts its data node request off once the shard has moved to a new node: either the
     * request fails to send, or the coordinator drops its connection to the data node while the request is held there. The query runs
     * from a coordinating-only node, so dropping that connection does not make the master remove the data node from the cluster.
     */
    private void assertDataRequestCutOffBeforeFirstPage(boolean inFlight, boolean allowPartialResults) throws Exception {
        String queryCoordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        String replacement = internalCluster().startDataOnlyNode();
        TransportService coordinatorTransport = internalCluster().getInstance(TransportService.class, queryCoordinator);
        TransportService dataNodeTransport = internalCluster().getInstance(TransportService.class, dataNode);
        ExchangeService dataNodeExchanges = internalCluster().getInstance(ExchangeService.class, dataNode);
        AtomicInteger opensOnDataNode = countOpens(dataNode);
        AtomicInteger opensOnReplacement = countOpens(replacement);
        AtomicInteger dataAnsweredOnDataNode = countAnswered(dataNode, ComputeService.DATA_ACTION_NAME);
        AtomicInteger dataAnsweredOnReplacement = countAnswered(replacement, ComputeService.DATA_ACTION_NAME);

        CountDownLatch dataRequestSent = new CountDownLatch(1);
        CountDownLatch cutOff = new CountDownLatch(1);
        if (inFlight) {
            MockTransportService.getInstance(dataNode)
                .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    dataRequestSent.countDown();
                    // the real handler, which adds the cancellation listener that finishes the sink, only runs after the disconnect
                    safeAwait(cutOff, TimeValue.timeValueSeconds(30));
                    handler.messageReceived(request, channel, task);
                });
        } else {
            MockTransportService.getInstance(queryCoordinator)
                .addSendBehavior(dataNodeTransport, (connection, requestId, action, request, options) -> {
                    if (action.equals(ComputeService.DATA_ACTION_NAME)) {
                        dataRequestSent.countDown();
                        safeAwait(cutOff, TimeValue.timeValueSeconds(30));
                        throw new NodeNotConnectedException(connection.getNode(), "simulated");
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
        }

        scriptPermits.drainPermits();
        scriptPermits.release(numberOfDocs());
        ActionFuture<EsqlQueryResponse> query = client(queryCoordinator).execute(
            EsqlQueryAction.INSTANCE,
            EsqlQueryRequest.syncEsqlQueryRequest(QUERY).allowPartialResults(allowPartialResults)
        );
        safeAwait(dataRequestSent);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include._name", replacement), "test");
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, "test").setWaitForGreenStatus().setWaitForNoRelocatingShards(true).get();
        if (inFlight) {
            coordinatorTransport.disconnectFromNode(dataNodeTransport.getLocalNode());
            var dataNodeTasks = dataNodeTransport.getTaskManager();
            assertBusy(
                () -> assertTrue(
                    dataNodeTasks.getCancellableTasks()
                        .values()
                        .stream()
                        .anyMatch(t -> ComputeService.DATA_ACTION_NAME.equals(t.getAction()) && t.isCancelled())
                )
            );
        }
        cutOff.countDown();

        if (allowPartialResults) {
            try (EsqlQueryResponse response = safeGet(query)) {
                assertFalse(response.isPartial());
                assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs()))));
            }
            assertThat(opensOnDataNode.get(), equalTo(1));
            assertThat(opensOnReplacement.get(), equalTo(1));
            assertThat(dataAnsweredOnDataNode.get(), equalTo(0));
            assertThat(dataAnsweredOnReplacement.get(), equalTo(1));
        } else {
            expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS));
        }
        // within the assertBusy timeout, far less than the inactive-sink interval
        assertBusy(() -> assertTrue(dataNodeExchanges.isEmpty()));
    }

    private static AtomicInteger countOpens(String nodeName) {
        return countRequests(nodeName, ExchangeService.OPEN_EXCHANGE_ACTION_NAME);
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
     * Counts the requests for the action that the node answered with a response rather than a failure.
     */
    private static AtomicInteger countAnswered(String nodeName, String action) {
        AtomicInteger answered = new AtomicInteger();
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
                        answered.incrementAndGet();
                        channel.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        channel.sendResponse(exception);
                    }
                }, task)
            );
        return answered;
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

    /**
     * A shutdown service for the node like the one it is built with, but with a termination handler that confirms the node is safe to
     * shut down, so the plugin drains run. A node only gets a termination handler from a service provider, and one on the test
     * classpath would be found through every plugin of every node of this source set.
     */
    private static ShutdownPrepareService shutdownServiceWithReadyHandler(String nodeName) {
        List<ShutdownAwarePlugin> plugins = internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(ShutdownAwarePlugin.class)
            .toList();
        return new ShutdownPrepareService(
            internalCluster().getInstance(ClusterService.class, nodeName).getSettings(),
            internalCluster().getInstance(HttpServerTransport.class, nodeName),
            internalCluster().getInstance(TransportService.class, nodeName),
            () -> TerminationHandler.Outcome.READY,
            plugins.stream().flatMap(p -> p.getActionsToAwaitOnShutdown().stream()).toList(),
            plugins.stream().map(p -> new ShutdownPrepareService.PluginDrain(p.getClass().getName(), p::startDrainOnShutdown)).toList()
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
        return prepareForShutdownAsync(internalCluster().getInstance(ShutdownPrepareService.class, nodeName), nodeName);
    }

    private static PlainActionFuture<Void> prepareForShutdownAsync(ShutdownPrepareService shutdownPrepareService, String nodeName) {
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
