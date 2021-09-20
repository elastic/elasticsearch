/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessResponse;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.hamcrest.CustomMatcher;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.transport.MockTransportService.createNewService;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class TransportClientNodesServiceTests extends ESTestCase {

    private static class TestIteration implements Closeable {
        private final ThreadPool threadPool;
        private final FailAndRetryMockTransport<TestResponse> transport;
        private final MockTransportService transportService;
        private final TransportClientNodesService transportClientNodesService;
        private final int listNodesCount;
        private final int sniffNodesCount;
        private TransportAddress livenessAddress = buildNewFakeTransportAddress();
        final List<TransportAddress> listNodeAddresses;
        // map for each address of the nodes a cluster state request should respond with
        final Map<TransportAddress, DiscoveryNodes> nodeMap;

        TestIteration() {
            this(Settings.EMPTY);
        }

        TestIteration(Settings extraSettings) {
            Settings settings = Settings.builder().put(extraSettings).put("cluster.name", "test").build();
            ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
            List<TransportAddress> listNodes = new ArrayList<>();
            Map<TransportAddress, DiscoveryNodes> nodeMap = new HashMap<>();
            this.listNodesCount = randomIntBetween(1, 10);
            int sniffNodesCount = 0;
            for (int i = 0; i < listNodesCount; i++) {
                TransportAddress transportAddress = buildNewFakeTransportAddress();
                listNodes.add(transportAddress);
                DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
                discoNodes.add(new DiscoveryNode("#list-node#-" + transportAddress, transportAddress, Version.CURRENT));

                if (TransportClient.CLIENT_TRANSPORT_SNIFF.get(settings)) {
                    final int numSniffNodes = randomIntBetween(0, 3);
                    for (int j = 0; j < numSniffNodes; ++j) {
                        TransportAddress sniffAddress = buildNewFakeTransportAddress();
                        DiscoveryNode sniffNode = new DiscoveryNode("#sniff-node#-" + sniffAddress, sniffAddress, Version.CURRENT);
                        discoNodes.add(sniffNode);
                        // also allow sniffing of the sniff node itself
                        nodeMap.put(sniffAddress, DiscoveryNodes.builder().add(sniffNode).build());
                        ++sniffNodesCount;
                    }
                }
                nodeMap.put(transportAddress, discoNodes.build());
            }
            listNodeAddresses = listNodes;
            this.nodeMap = nodeMap;
            this.sniffNodesCount = sniffNodesCount;

            threadPool = new TestThreadPool("transport-client-nodes-service-tests");
            transport = new FailAndRetryMockTransport<TestResponse>(random(), clusterName) {
                @Override
                public List<String> getDefaultSeedAddresses() {
                    return Collections.emptyList();
                }

                @Override
                protected TestResponse newResponse() {
                    return new TestResponse();
                }

                @Override
                protected ClusterState getMockClusterState(DiscoveryNode node) {
                    return ClusterState.builder(clusterName).nodes(TestIteration.this.nodeMap.get(node.getAddress())).build();
                }
            };

            transportService = new MockTransportService(settings, transport, threadPool, new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new AsyncSender() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action,
                                                                              TransportRequest request,
                                                                              TransportRequestOptions options,
                                                                              TransportResponseHandler<T> handler) {
                            if (TransportLivenessAction.NAME.equals(action)) {
                                sender.sendRequest(connection, action, request, options, wrapLivenessResponseHandler(handler,
                                    connection.getNode(), clusterName));
                            } else {
                                sender.sendRequest(connection, action, request, options, handler);
                            }
                        }
                    };
                }
            }, (addr) -> {
                assert addr == null : "boundAddress: " + addr;
                return DiscoveryNode.createLocal(settings, buildNewFakeTransportAddress(), UUIDs.randomBase64UUID());
            }, null, Collections.emptySet());
            transportService.addNodeConnectedBehavior((cm, dn) -> false);
            transportService.addGetConnectionBehavior((connectionManager, discoveryNode) -> {
                // The FailAndRetryTransport does not use the connection profile
                PlainActionFuture<Transport.Connection> future = PlainActionFuture.newFuture();
                transport.openConnection(discoveryNode, null, future);
                return future.actionGet();
            });
            transportService.start();
            transportService.acceptIncomingRequests();
            transportClientNodesService =
                new TransportClientNodesService(settings, transportService, threadPool, (a, b) -> {});
            transportClientNodesService.addTransportAddresses(listNodeAddresses.toArray(new TransportAddress[0]));
        }

        private <T extends TransportResponse> TransportResponseHandler<T> wrapLivenessResponseHandler(TransportResponseHandler<T> handler,
                                                                                                   DiscoveryNode node,
                                                                                                   ClusterName clusterName) {
            return new TransportResponseHandler<T>() {
                @Override
                public T read(StreamInput in) throws IOException {
                    return handler.read(in);
                }

                @Override
                @SuppressWarnings("unchecked")
                public void handleResponse(T response) {
                    LivenessResponse livenessResponse = new LivenessResponse(clusterName,
                            new DiscoveryNode(node.getName(), node.getId(), node.getEphemeralId(), "liveness-hostname" + node.getId(),
                                    "liveness-hostaddress" + node.getId(),
                                    livenessAddress, node.getAttributes(), node.getRoles(),
                                    node.getVersion()));
                    handler.handleResponse((T)livenessResponse);
                }

                @Override
                public void handleException(TransportException exp) {
                    handler.handleException(exp);
                }

                @Override
                public String executor() {
                    return handler.executor();
                }
            };
        }

        @Override
        public void close() {
            transport.endConnectMode();
            transportService.stop();
            transportClientNodesService.close();
            terminate(threadPool);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/37567")
    public void testListenerFailures() throws InterruptedException {
        int iters = iterations(10, 100);
        for (int i = 0; i <iters; i++) {
            try(TestIteration iteration = new TestIteration()) {
                iteration.transport.endConnectMode(); // stop transport from responding early
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger finalFailures = new AtomicInteger();
                final AtomicReference<Throwable> finalFailure = new AtomicReference<>();
                final AtomicReference<TestResponse> response = new AtomicReference<>();
                ActionListener<TestResponse> actionListener = new ActionListener<TestResponse>() {
                    @Override
                    public void onResponse(TestResponse testResponse) {
                        response.set(testResponse);
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        finalFailures.incrementAndGet();
                        finalFailure.set(e);
                        latch.countDown();
                    }
                };

                final AtomicInteger preSendFailures = new AtomicInteger();

                iteration.transportClientNodesService.execute((node, retryListener) -> {
                    if (rarely()) {
                        preSendFailures.incrementAndGet();
                        //throw whatever exception that is not a subclass of ConnectTransportException
                        throw new IllegalArgumentException();
                    }

                    iteration.transportService.sendRequest(node, "action", new TestRequest(),
                            TransportRequestOptions.EMPTY, new TransportResponseHandler<TestResponse>() {
                        @Override
                        public TestResponse read(StreamInput in) {
                            return new TestResponse(in);
                        }

                        @Override
                        public void handleResponse(TestResponse response1) {
                            retryListener.onResponse(response1);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            retryListener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return randomBoolean() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;
                        }
                    });
                }, actionListener);

                latch.await();

                //there can be only either one failure that causes the request to fail straightaway or success
                assertThat(preSendFailures.get() + iteration.transport.failures() + iteration.transport.successes(), lessThanOrEqualTo(1));

                if (iteration.transport.successes() == 1) {
                    assertThat(finalFailures.get(), equalTo(0));
                    assertThat(finalFailure.get(), nullValue());
                    assertThat(response.get(), notNullValue());
                } else {
                    assertThat(finalFailures.get(), equalTo(1));
                    assertThat(finalFailure.get(), notNullValue());
                    assertThat(response.get(), nullValue());
                    if (preSendFailures.get() == 0 && iteration.transport.failures() == 0) {
                        assertThat(finalFailure.get(), instanceOf(NoNodeAvailableException.class));
                    }
                }

                assertThat(iteration.transport.triedNodes().size(), lessThanOrEqualTo(iteration.listNodesCount));
                assertThat(iteration.transport.triedNodes().size(), equalTo(iteration.transport.connectTransportExceptions() +
                        iteration.transport.failures() + iteration.transport.successes()));
            }
        }
    }

    public void testConnectedNodes() {
        int iters = iterations(10, 100);
        for (int i = 0; i <iters; i++) {
            try(TestIteration iteration = new TestIteration()) {
                assertThat(iteration.transportClientNodesService.connectedNodes().size(), lessThanOrEqualTo(iteration.listNodesCount));
                for (DiscoveryNode discoveryNode : iteration.transportClientNodesService.connectedNodes()) {
                    assertThat(discoveryNode.getHostName(), startsWith("liveness-"));
                    assertThat(discoveryNode.getHostAddress(), startsWith("liveness-"));
                    assertNotEquals(discoveryNode.getAddress(), iteration.livenessAddress);
                    assertThat(iteration.listNodeAddresses, hasItem(discoveryNode.getAddress()));
                }
            }
        }
    }

    public void testRemoveAddressSniff() {
        checkRemoveAddress(true);
    }

    public void testRemoveAddressSimple() {
        checkRemoveAddress(false);
    }

    private void checkRemoveAddress(boolean sniff) {
        Settings extraSettings = Settings.builder().put(TransportClient.CLIENT_TRANSPORT_SNIFF.getKey(), sniff).build();
        try(TestIteration iteration = new TestIteration(extraSettings)) {
            final TransportClientNodesService service = iteration.transportClientNodesService;
            assertEquals(iteration.listNodesCount + iteration.sniffNodesCount, service.connectedNodes().size());
            final TransportAddress addressToRemove = randomFrom(iteration.listNodeAddresses);
            service.removeTransportAddress(addressToRemove);
            assertThat(service.connectedNodes(), everyItem(not(new CustomMatcher<DiscoveryNode>("removed address") {
                @Override
                public boolean matches(Object item) {
                    return item instanceof DiscoveryNode && ((DiscoveryNode)item).getAddress().equals(addressToRemove);
                }
            })));
            assertEquals(iteration.listNodesCount + iteration.sniffNodesCount - 1, service.connectedNodes().size());
        }
    }

    public void testSniffNodesSamplerClosesConnections() throws Exception {
        final TestThreadPool threadPool = new TestThreadPool("testSniffNodesSamplerClosesConnections");

        Settings remoteSettings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "remote").build();
        try (MockTransportService remoteService = createNewService(remoteSettings, Version.CURRENT, threadPool, null)) {
            final MockHandler handler = new MockHandler(remoteService);
            remoteService.registerRequestHandler(ClusterStateAction.NAME, ThreadPool.Names.SAME, ClusterStateRequest::new,  handler);
            remoteService.getTaskManager().setTaskCancellationService(new TaskCancellationService(remoteService));
            remoteService.start();
            remoteService.acceptIncomingRequests();

            Settings clientSettings = Settings.builder()
                    .put(TransportClient.CLIENT_TRANSPORT_SNIFF.getKey(), true)
                    .put(TransportClient.CLIENT_TRANSPORT_PING_TIMEOUT.getKey(), TimeValue.timeValueSeconds(1))
                    .put(TransportClient.CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL.getKey(), TimeValue.timeValueSeconds(30))
                    .build();

            try (MockTransportService clientService = createNewService(clientSettings, Version.CURRENT, threadPool, null)) {
                final List<Transport.Connection> establishedConnections = new CopyOnWriteArrayList<>();

                clientService.addConnectBehavior(remoteService, (transport, discoveryNode, profile, listener) ->
                    transport.openConnection(discoveryNode, profile, listener.delegateFailure((delegatedListener, connection) -> {
                        establishedConnections.add(connection);
                        delegatedListener.onResponse(connection);
                    })));

                clientService.start();
                clientService.acceptIncomingRequests();

                try (TransportClientNodesService transportClientNodesService =
                        new TransportClientNodesService(clientSettings, clientService, threadPool, (a, b) -> {})) {
                    assertEquals(0, transportClientNodesService.connectedNodes().size());
                    assertEquals(0, establishedConnections.size());

                    transportClientNodesService.addTransportAddresses(remoteService.getLocalDiscoNode().getAddress());
                    assertEquals(1, transportClientNodesService.connectedNodes().size());
                    assertEquals(1, clientService.connectionManager().size());

                    transportClientNodesService.doSample();
                    assertEquals(1, clientService.connectionManager().size());

                    establishedConnections.clear();
                    handler.failToRespond();
                    Thread thread = new Thread(transportClientNodesService::doSample);
                    thread.start();

                    assertBusy(() ->  assertTrue(establishedConnections.size() >= 1));
                    assertFalse("Temporary ping connection must be opened", establishedConnections.get(0).isClosed());

                    thread.join();

                    assertTrue(establishedConnections.get(0).isClosed());
                }
            }
        } finally {
            terminate(threadPool);
        }
    }

    class MockHandler implements TransportRequestHandler<ClusterStateRequest> {

        private final AtomicBoolean failToRespond = new AtomicBoolean(false);
        private final MockTransportService transportService;

        MockHandler(MockTransportService transportService) {
            this.transportService = transportService;
        }

        @Override
        public void messageReceived(ClusterStateRequest request, TransportChannel channel, Task task) throws Exception {
            if (failToRespond.get()) {
                return;
            }

            DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(transportService.getLocalDiscoNode()).build();
            ClusterState build = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();
            channel.sendResponse(new ClusterStateResponse(ClusterName.DEFAULT, build, false));
        }

        void failToRespond() {
            if (failToRespond.compareAndSet(false, true) == false) {
                throw new AssertionError("Request handler is already marked as failToRespond");
            }
        }
    }

    public static class TestRequest extends TransportRequest {

    }

    private static class TestResponse extends TransportResponse {

        private TestResponse() {}
        private TestResponse(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
