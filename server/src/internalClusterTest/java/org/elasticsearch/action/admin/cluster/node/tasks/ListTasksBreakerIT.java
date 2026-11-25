/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Lists the tasks through a real HTTP connection, to check that the replies of the other nodes are charged to the in-flight requests
 * breaker of the coordinating node while they arrive and are held, that a reply too large for it fails only that node, and that a client
 * which closes its connection releases what is held before the other nodes answer.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ListTasksBreakerIT extends ESIntegTestCase {

    private static final String NODE_TASKS_ACTION = TransportListTasksAction.TYPE.name() + "[n]";
    private static final ByteSizeValue IN_FLIGHT_LIMIT = ByteSizeValue.ofMb(2);

    private final AtomicReference<SubscribableListener<Void>> hold = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> held = new AtomicReference<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(getTestTransportPlugin(), MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(ordinal, otherSettings))
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), IN_FLIGHT_LIMIT)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    private List<String> startCluster() {
        final List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);
        ((MockTransportService) internalCluster().getInstance(TransportService.class, nodes.get(1))).addRequestHandlingBehavior(
            NODE_TASKS_ACTION,
            (handler, request, channel, task) -> {
                final SubscribableListener<Void> holdListener = hold.get();
                if (holdListener == null) {
                    handler.messageReceived(request, channel, task);
                } else {
                    held.get().countDown();
                    holdListener.addListener(ActionListener.wrap(ignored -> handler.messageReceived(request, channel, task), e -> {
                        throw new AssertionError("unexpected", e);
                    }));
                }
            }
        );
        return nodes;
    }

    private void sendRestRequestsTo(String node) {
        final TransportAddress address = internalCluster().getInstance(HttpServerTransport.class, node).boundAddress().publishAddress();
        getRestClient().setNodes(
            List.of(
                new org.elasticsearch.client.Node(
                    new HttpHost(NetworkAddress.format(address.address().getAddress()), address.address().getPort(), "http")
                )
            )
        );
    }

    private CircuitBreaker inFlightBreaker(String node) {
        return internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    private long[] usedBytes(List<String> nodes) {
        return nodes.stream().mapToLong(node -> inFlightBreaker(node).getUsed()).toArray();
    }

    private void assertUsedBytesAre(List<String> nodes, long[] expected) throws Exception {
        assertBusy(() -> assertThat(usedBytes(nodes), equalTo(expected)));
    }

    private Task registerTask(String node, String action, int descriptionLength) {
        final String description = "x".repeat(descriptionLength);
        final TaskManager taskManager = internalCluster().getInstance(TransportService.class, node).getTaskManager();
        return taskManager.register("test", action, new TaskAwareRequest() {
            private TaskId parentTask = TaskId.EMPTY_TASK_ID;

            @Override
            public void setParentTask(TaskId taskId) {
                parentTask = taskId;
            }

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return parentTask;
            }

            @Override
            public String getDescription() {
                return description;
            }
        });
    }

    private void unregisterTask(String node, Task task) {
        internalCluster().getInstance(TransportService.class, node).getTaskManager().unregister(task);
    }

    public void testReplyTooLargeForInFlightBreakerFailsOnlyThatNode() throws Exception {
        final List<String> nodes = startCluster();
        final String coordinator = nodes.get(0);
        final String smallNode = nodes.get(1);
        final String bigNode = nodes.get(2);
        sendRestRequestsTo(coordinator);
        final long[] usedBefore = usedBytes(nodes);
        final long trippedBefore = inFlightBreaker(coordinator).getTrippedCount();

        final Task small = registerTask(smallNode, "internal:test/list_tasks_breaker", 1024);
        final Task big = registerTask(bigNode, "internal:test/list_tasks_breaker", Math.toIntExact(IN_FLIGHT_LIMIT.getBytes()) * 2);
        try {
            final Request request = new Request("GET", "/_tasks");
            request.addParameter("detailed", "true");
            request.addParameter("actions", "internal:test/list_tasks_breaker");
            final ObjectPath response = ObjectPath.createFromResponse(getRestClient().performRequest(request));

            final List<Object> nodeFailures = response.evaluate("node_failures");
            assertThat(nodeFailures, hasSize(1));
            assertThat(response.evaluate("node_failures.0.node_id"), equalTo(getNodeId(bigNode)));
            assertThat(response.evaluate("node_failures.0.caused_by.type"), equalTo("circuit_breaking_exception"));
            assertThat(response.evaluate("nodes." + getNodeId(smallNode) + ".tasks"), aMapWithSize(1));
            assertThat(inFlightBreaker(coordinator).getTrippedCount(), equalTo(trippedBefore + 1));
            assertUsedBytesAre(nodes, usedBefore);
        } finally {
            unregisterTask(smallNode, small);
            unregisterTask(bigNode, big);
        }
    }

    private void assertNoListTasksRunning(List<String> nodes) throws Exception {
        assertBusy(() -> {
            for (String node : nodes) {
                final var tasks = internalCluster().getInstance(TransportService.class, node)
                    .getTaskManager()
                    .getTasks()
                    .values()
                    .stream()
                    .filter(t -> t.getAction().startsWith(TransportListTasksAction.TYPE.name()))
                    .toList();
                assertThat(tasks.toString(), tasks, hasSize(0));
            }
        });
    }

    private void holdNodeRequests() {
        held.set(new CountDownLatch(1));
        hold.set(new SubscribableListener<>());
    }

    private void releaseNodeRequests() {
        hold.getAndSet(null).onResponse(null);
    }

    private void listTasksAndDisconnect(String coordinator, List<String> nodes, long[] usedBefore) throws Exception {
        holdNodeRequests();
        final Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        try {
            final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

            safeAwait(held.get());
            assertBusy(() -> assertThat(inFlightBreaker(coordinator).getUsed(), greaterThan(usedBefore[nodes.indexOf(coordinator)])));

            cancellable.cancel();
            assertAllCancellableTasksAreCancelled(TransportListTasksAction.TYPE.name());
            assertBusy(() -> assertThat(inFlightBreaker(coordinator).getUsed(), equalTo(usedBefore[nodes.indexOf(coordinator)])));
        } finally {
            releaseNodeRequests();
        }
        expectThrows(CancellationException.class, future::actionGet);
        assertNoListTasksRunning(nodes);
        assertUsedBytesAre(nodes, usedBefore);
    }

    public void testClientDisconnectCancelsTasksAndReleasesReplyBytesBeforeNodesAnswer() throws Exception {
        final List<String> nodes = startCluster();
        final String coordinator = nodes.get(0);
        sendRestRequestsTo(coordinator);
        final long[] usedBefore = usedBytes(nodes);

        final Task small = registerTask(nodes.get(2), "internal:test/list_tasks_breaker", 256 * 1024);
        try {
            listTasksAndDisconnect(coordinator, nodes, usedBefore);
        } finally {
            unregisterTask(nodes.get(2), small);
        }
    }

    public void testRepeatedListTasksDoNotAccumulateBreakerBytes() throws Exception {
        final List<String> nodes = startCluster();
        final String coordinator = nodes.get(0);
        sendRestRequestsTo(coordinator);
        final long[] usedBefore = usedBytes(nodes);

        final Task small = registerTask(nodes.get(2), "internal:test/list_tasks_breaker", 256 * 1024);
        try {
            for (int round = 0; round < 10; round++) {
                listTasksAndDisconnect(coordinator, nodes, usedBefore);

                final Request request = new Request("GET", "/_tasks");
                request.addParameter("detailed", "true");
                final Response response = getRestClient().performRequest(request);
                assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
                assertUsedBytesAre(nodes, usedBefore);
            }
        } finally {
            unregisterTask(nodes.get(2), small);
        }
    }
}
