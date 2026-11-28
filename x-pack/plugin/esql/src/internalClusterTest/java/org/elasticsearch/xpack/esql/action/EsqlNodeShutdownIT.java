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
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests that graceful shutdown of a data node waits for the ES|QL tasks it runs on behalf of a coordinator on another node.
 * <p>
 * The query is started on one node, acting as coordinator, against an index allocated on a separate data-only node. The data node
 * work is paused on the {@code pause_me} runtime field while the data node prepares for shutdown.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlNodeShutdownIT extends AbstractPausableIntegTestCase {

    private static final String QUERY = "FROM test | STATS SUM(pause_me)";

    /**
     * How long a send behaviour holds a request on the thread that sends it, where it has to stay to fail there as a request that
     * cannot be sent does. It has to cover every wait the test makes before it releases the request, which is the move of the test
     * index, see {@link #moveTestIndexTo}: the settings update, which waits up to 30 seconds for the master and then up to 30 more
     * for the acknowledgement, the health call and the wait for the coordinator to apply the routing, of up to 30 seconds each. That
     * is about two minutes in the worst case, and this leaves room above it. The test releases the request also when one of these
     * fails, so the timeout only ends a hold that nothing else would.
     */
    private static final TimeValue HELD_SEND_TIMEOUT = TimeValue.timeValueMinutes(3);

    private String coordinator;
    private String dataNode;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
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
     * The coordinator cannot reach the node a shard is leaving any more. Its open to that node is held until the shard has moved to a
     * new node, so the coordinator still targets the data node, and then fails as if the node could not be reached. The coordinator
     * resolves the shard again and gets the result from the node the shard moved to.
     */
    public void testOpenToUnreachableNodeIsRetriedOnTheNodeTheShardMovedTo() throws Exception {
        String replacement = internalCluster().startDataOnlyNode();
        ExchangeService dataNodeExchanges = internalCluster().getInstance(ExchangeService.class, dataNode);
        AtomicInteger opensOnDataNode = countOpens(dataNode);
        AtomicInteger opensOnReplacement = countOpens(replacement);
        AtomicInteger dataRequestsOnDataNode = countRequests(dataNode, ComputeService.DATA_ACTION_NAME);

        CountDownLatch openSent = new CountDownLatch(1);
        CountDownLatch shardMoved = new CountDownLatch(1);
        MockTransportService.getInstance(coordinator)
            .addSendBehavior(
                internalCluster().getInstance(TransportService.class, dataNode),
                (connection, requestId, action, request, options) -> {
                    if (action.equals(ExchangeService.OPEN_EXCHANGE_ACTION_NAME)) {
                        openSent.countDown();
                        safeAwait(shardMoved, HELD_SEND_TIMEOUT);
                        throw new NodeNotConnectedException(connection.getNode(), "simulated");
                    }
                    connection.sendRequest(requestId, action, request, options);
                }
            );

        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(QUERY).allowPartialResults(randomBoolean());
        scriptPermits.drainPermits();
        scriptPermits.release(numberOfDocs());
        final ActionFuture<EsqlQueryResponse> query = client(coordinator).execute(EsqlQueryAction.INSTANCE, request);
        boolean shardMovedOn = false;
        try {
            safeAwait(openSent);
            moveTestIndexTo(replacement, coordinator);
            shardMovedOn = true;
        } finally {
            // also when a step above failed, so that the open is not left held
            shardMoved.countDown();
            if (shardMovedOn == false) {
                closeResponseOf(query);
            }
        }

        try (EsqlQueryResponse response = safeGet(query)) {
            assertFalse(response.isPartial());
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs()))));
        }
        assertThat(opensOnDataNode.get(), equalTo(0));
        assertThat(opensOnReplacement.get(), equalTo(1));
        assertThat("no data node request reaches the node the shard left", dataRequestsOnDataNode.get(), equalTo(0));
        assertTrue("an open that was not sent leaves no sink", dataNodeExchanges.isEmpty());
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
     * Without partial results, a data node request that cannot be sent fails the query with that failure, as before, and nothing
     * is opened on the node the shard moved to: the failure has cancelled the query. This does not tell whether the shard was
     * resolved again, since a request to the node it moved to is skipped once the query is cancelled. That the failure is not read
     * as one of the node is checked in {@code DataNodeRequestSenderTests}.
     */
    public void testDataRequestFailedOnSendWithoutPartialResultsFailsTheQuery() throws Exception {
        assertDataRequestCutOffBeforeFirstPage(false, false);
    }

    /**
     * Like {@link #testDataRequestFailedOnSendWithoutPartialResultsFailsTheQuery}, for a data node request whose connection closes
     * before the old node answered or sent a page.
     */
    public void testDataRequestDisconnectedInFlightWithoutPartialResultsFailsTheQuery() throws Exception {
        assertDataRequestCutOffBeforeFirstPage(true, false);
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
        SubscribableListener<Void> cutOff = new SubscribableListener<>();
        if (inFlight) {
            MockTransportService.getInstance(dataNode)
                .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    dataRequestSent.countDown();
                    // the real handler, which adds the cancellation listener that finishes the sink, only runs after the disconnect
                    handleDataRequestOnceReleased(cutOff, dataNodeTransport, handler, request, channel, task);
                });
        } else {
            MockTransportService.getInstance(queryCoordinator)
                .addSendBehavior(dataNodeTransport, (connection, requestId, action, request, options) -> {
                    if (action.equals(ComputeService.DATA_ACTION_NAME)) {
                        dataRequestSent.countDown();
                        safeAwait(cutOff, HELD_SEND_TIMEOUT);
                        throw new NodeNotConnectedException(connection.getNode(), "simulated");
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
        }

        scriptPermits.drainPermits();
        scriptPermits.release(numberOfDocs());
        final ActionFuture<EsqlQueryResponse> query = client(queryCoordinator).execute(
            EsqlQueryAction.INSTANCE,
            EsqlQueryRequest.syncEsqlQueryRequest(QUERY).allowPartialResults(allowPartialResults)
        );
        boolean cutOffDone = false;
        try {
            safeAwait(dataRequestSent);
            moveTestIndexTo(replacement, queryCoordinator);
            if (inFlight) {
                coordinatorTransport.disconnectFromNode(dataNodeTransport.getLocalNode());
                awaitDataRequestCancelledOn(dataNodeTransport);
            }
            cutOffDone = true;
        } finally {
            // also when a step above failed, so that the data node request is not left held
            cutOff.onResponse(null);
            if (cutOffDone == false) {
                closeResponseOf(query);
            }
        }

        if (allowPartialResults) {
            try (EsqlQueryResponse response = safeGet(query)) {
                assertFalse(response.isPartial());
                assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs()))));
            }
            assertThat(opensOnDataNode.get(), equalTo(1));
            assertThat(opensOnReplacement.get(), equalTo(1));
            // A request that was held on the old node is only handled there once the connection is gone and its task is cancelled.
            // Whatever the node answers then has no connection to go back on, and the result above shows that every row was
            // counted once.
            if (inFlight == false) {
                assertThat(dataAnsweredOnDataNode.get(), equalTo(0));
            }
            assertThat(dataAnsweredOnReplacement.get(), equalTo(1));
        } else {
            Exception e = expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS));
            // the failure of the request, and not a timeout of the wait above
            assertNotNull(e.toString(), ExceptionsHelper.unwrap(e, ConnectTransportException.class));
            assertThat(opensOnDataNode.get(), equalTo(1));
            // the index has one copy, so the node it moved to is only known by resolving the shard again
            assertThat("the shard is not retried", opensOnReplacement.get(), equalTo(0));
        }
        // within the assertBusy timeout, far less than the inactive-sink interval
        assertBusy(() -> assertTrue(dataNodeExchanges.isEmpty()));
    }

    /**
     * The data node rejects the page fetches of the coordinator before it returned a page, and the data node request then loses its
     * connection. The rejection is a failure of the node's own, so the lost connection does not make the node count as unreachable:
     * the shard is not resolved again, although it has moved to a node that could serve it, and the query reports the rejection.
     * <p>
     * The coordinator learns of the rejection through the remote sink it links for the data node, which is what this test covers
     * and the unit tests cannot: they build that sink themselves.
     */
    public void testRejectedFetchBeforeDataRequestDisconnectedIsNotRetriedOnTheNodeTheShardMovedTo() throws Exception {
        String queryCoordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        String replacement = internalCluster().startDataOnlyNode();
        TransportService coordinatorTransport = internalCluster().getInstance(TransportService.class, queryCoordinator);
        MockTransportService dataNodeTransport = MockTransportService.getInstance(dataNode);
        ExchangeService dataNodeExchanges = internalCluster().getInstance(ExchangeService.class, dataNode);
        AtomicInteger opensOnDataNode = countOpens(dataNode);
        AtomicInteger opensOnReplacement = countOpens(replacement);

        SubscribableListener<Void> dataRequestArrived = new SubscribableListener<>();
        SubscribableListener<Void> cutOff = new SubscribableListener<>();
        dataNodeTransport.addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
            dataRequestArrived.onResponse(null);
            // held, so that the request has neither been answered nor failed when its connection is dropped
            handleDataRequestOnceReleased(cutOff, dataNodeTransport, handler, request, channel, task);
        });
        String rejection = "simulated rejection of a page fetch";
        dataNodeTransport.addRequestHandlingBehavior(
            ExchangeService.EXCHANGE_ACTION_NAME,
            // Not before the data node request is here: the rejection has it cancelled, which must find its task. The fetch does
            // not wait for that on its thread: the data node request needs one of the same pool to get here.
            (handler, request, channel, task) -> dataRequestArrived.addListener(
                ActionListener.running(() -> channel.sendResponse(new EsRejectedExecutionException(rejection)))
            )
        );

        scriptPermits.drainPermits();
        scriptPermits.release(numberOfDocs());
        final ActionFuture<EsqlQueryResponse> query = client(queryCoordinator).execute(
            EsqlQueryAction.INSTANCE,
            EsqlQueryRequest.syncEsqlQueryRequest(QUERY).allowPartialResults(true)
        );
        boolean cutOffDone = false;
        try {
            safeAwait(dataRequestArrived);
            // The rejection fails the compute of the data node on the coordinator, which then cancels the data node request. Once
            // the cancellation shows on the data node, the coordinator has taken note of the rejection, and only then is the
            // connection dropped.
            awaitDataRequestCancelledOn(dataNodeTransport);
            moveTestIndexTo(replacement, queryCoordinator);
            // In this order: the held request is released once its connection is gone, so that whatever the data node answers for
            // its cancelled task cannot reach the coordinator, and the request fails there on the lost connection.
            coordinatorTransport.disconnectFromNode(dataNodeTransport.getLocalNode());
            cutOffDone = true;
        } finally {
            // also when a step above failed, so that the data node request is not left held
            cutOff.onResponse(null);
            if (cutOffDone == false) {
                closeResponseOf(query);
            }
        }

        try (EsqlQueryResponse response = safeGet(query)) {
            // The query does not fail although its only shard did: a STATS without groups has a row also when it got no input
            assertTrue("the shard was served although its node failed for a reason of its own", response.isPartial());
            assertThat(getValuesList(response), equalTo(List.of(Collections.singletonList(null))));
            List<ShardSearchFailure> failures = response.getExecutionInfo()
                .getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY)
                .getFailures();
            assertThat(failures, hasSize(1));
            // the rejection of the fetch, and not the lost connection of the data node request
            Throwable reported = ExceptionsHelper.unwrap(failures.get(0).getCause(), EsRejectedExecutionException.class);
            assertNotNull(failures.toString(), reported);
            assertThat(reported.getMessage(), equalTo(rejection));
            // which the data node request did fail on: the compute of the node hands it on next to the failure it picked
            assertThat(reported.getSuppressed(), hasItemInArray(instanceOf(ConnectTransportException.class)));
        }
        assertThat(opensOnDataNode.get(), equalTo(1));
        // the index has one copy, so the node it moved to is only known by resolving the shard again
        assertThat("the shard is not retried", opensOnReplacement.get(), equalTo(0));
        // within the assertBusy timeout, far less than the inactive-sink interval
        assertBusy(() -> assertTrue(dataNodeExchanges.isEmpty()));
    }

    /**
     * Hands a data node request to its handler once the listener is completed, and holds no thread until then. The handler then
     * runs in the thread context the request arrived in and on the search pool of its node: it is registered on that pool and
     * asserts further down that it runs there, so the thread that completes the listener, one of the test, will not do. A failure
     * of the handler, or of the pool to take it, is sent to the channel.
     */
    private static <R extends TransportRequest> void handleDataRequestOnceReleased(
        SubscribableListener<Void> released,
        TransportService dataNodeTransport,
        TransportRequestHandler<R> handler,
        R request,
        TransportChannel channel,
        Task task
    ) {
        ThreadPool threadPool = dataNodeTransport.getThreadPool();
        released.addListener(
            ActionListener.wrap(ignored -> handler.messageReceived(request, channel, task), channel::sendResponse),
            threadPool.executor(ThreadPool.Names.SEARCH),
            threadPool.getThreadContext()
        );
    }

    /**
     * Waits until the task of a data node request shows as cancelled on the node that got the request.
     */
    private static void awaitDataRequestCancelledOn(TransportService dataNodeTransport) throws Exception {
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

    /**
     * Moves the test index to the given node, and waits until the node that coordinates the query knows, see
     * {@link #awaitTestIndexAppliedOn}.
     */
    private static void moveTestIndexTo(String movedTo, String queryCoordinator) {
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include._name", movedTo), "test");
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, "test").setWaitForGreenStatus().setWaitForNoRelocatingShards(true).get();
        awaitTestIndexAppliedOn(queryCoordinator, movedTo);
    }

    /**
     * Waits until the node that coordinates the query has itself applied the routing that shows the test index started on the node it
     * moved to. A health call waits on the master. The query resolves the shard again on the routing its coordinator has applied,
     * which can lag behind the master's, and does not wait for newer routing: on the old routing it could use up its resolution
     * attempts before the move shows.
     */
    private static void awaitTestIndexAppliedOn(String queryCoordinator, String movedTo) {
        String movedToId = getNodeId(movedTo);
        awaitClusterState(queryCoordinator, state -> {
            var index = state.routingTable().index("test");
            return index != null
                && index.allShards()
                    .allMatch(shard -> shard.primaryShard().started() && movedToId.equals(shard.primaryShard().currentNodeId()));
        });
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

    /**
     * For a test that failed in its setup, after it started a query: once the held request is released the query may still complete
     * normally, and its response must be closed, or the blocks it holds are reported as leaked on top of the failure of the test.
     * Never throws, so that it does not mask that failure.
     */
    private static void closeResponseOf(ActionFuture<EsqlQueryResponse> query) {
        try {
            query.actionGet(30, TimeUnit.SECONDS).close();
        } catch (Exception e) {
            // the query failed, so there is no response to close
        }
    }

    private void assertQueryResult(ActionFuture<EsqlQueryResponse> query) {
        try (EsqlQueryResponse response = safeGet(query)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs()))));
        }
    }
}
