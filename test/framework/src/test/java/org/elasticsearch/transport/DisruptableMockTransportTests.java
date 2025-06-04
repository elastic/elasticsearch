/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.coordination.CleanableResponseHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableRef;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.DisruptableMockTransport.ConnectionStatus;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;

public class DisruptableMockTransportTests extends ESTestCase {

    private static final String TEST_ACTION = "internal:dummy";

    private DiscoveryNode node1;
    private DiscoveryNode node2;

    private TransportService service1;
    private TransportService service2;

    private DeterministicTaskQueue deterministicTaskQueue;

    private Runnable deliverBlackholedRequests;
    private Runnable blockTestAction;

    private Set<Tuple<DiscoveryNode, DiscoveryNode>> disconnectedLinks;
    private Set<Tuple<DiscoveryNode, DiscoveryNode>> blackholedLinks;
    private Set<Tuple<DiscoveryNode, DiscoveryNode>> blackholedRequestLinks;

    private long activeRequestCount;
    private final Set<DiscoveryNode> rebootedNodes = new HashSet<>();

    private ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination) {
        Tuple<DiscoveryNode, DiscoveryNode> link = Tuple.tuple(sender, destination);
        if (disconnectedLinks.contains(link)) {
            assert blackholedLinks.contains(link) == false;
            assert blackholedRequestLinks.contains(link) == false;
            return ConnectionStatus.DISCONNECTED;
        }
        if (blackholedLinks.contains(link)) {
            assert blackholedRequestLinks.contains(link) == false;
            return ConnectionStatus.BLACK_HOLE;
        }
        if (blackholedRequestLinks.contains(link)) {
            return ConnectionStatus.BLACK_HOLE_REQUESTS_ONLY;
        }
        return ConnectionStatus.CONNECTED;
    }

    @Before
    public void initTransports() {
        node1 = DiscoveryNodeUtils.create("node1");
        node2 = DiscoveryNodeUtils.create("node2");

        disconnectedLinks = new HashSet<>();
        blackholedLinks = new HashSet<>();
        blackholedRequestLinks = new HashSet<>();

        List<DisruptableMockTransport> transports = new ArrayList<>();

        deterministicTaskQueue = new DeterministicTaskQueue();

        final DisruptableMockTransport transport1 = new DisruptableMockTransport(node1, deterministicTaskQueue) {
            @Override
            protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                return DisruptableMockTransportTests.this.getConnectionStatus(getLocalNode(), destination);
            }

            @Override
            protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                return transports.stream().filter(t -> t.getLocalNode().getAddress().equals(address)).findAny();
            }

            @Override
            protected void execute(Runnable runnable) {
                deterministicTaskQueue.scheduleNow(unlessRebooted(node1, runnable));
            }
        };

        final DisruptableMockTransport transport2 = new DisruptableMockTransport(node2, deterministicTaskQueue) {
            @Override
            protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                return DisruptableMockTransportTests.this.getConnectionStatus(getLocalNode(), destination);
            }

            @Override
            protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                return transports.stream().filter(t -> t.getLocalNode().getAddress().equals(address)).findAny();
            }

            @Override
            protected void execute(Runnable runnable) {
                deterministicTaskQueue.scheduleNow(unlessRebooted(node2, runnable));
            }
        };

        transports.add(transport1);
        transports.add(transport2);

        service1 = transport1.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            NOOP_TRANSPORT_INTERCEPTOR,
            a -> node1,
            null,
            Collections.emptySet()
        );
        service2 = transport2.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            NOOP_TRANSPORT_INTERCEPTOR,
            a -> node2,
            null,
            Collections.emptySet()
        );

        service1.start();
        service2.start();

        final PlainActionFuture<Releasable> fut1 = new PlainActionFuture<>();
        service1.connectToNode(node2, fut1);
        final PlainActionFuture<Releasable> fut2 = new PlainActionFuture<>();
        service2.connectToNode(node1, fut2);
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(fut1.isDone());
        assertTrue(fut2.isDone());

        deliverBlackholedRequests = () -> transports.forEach(DisruptableMockTransport::deliverBlackholedRequests);

        blockTestAction = new Runnable() {
            @Override
            public void run() {
                transports.forEach(t -> t.addActionBlock(TEST_ACTION));
            }

            @Override
            public String toString() {
                return "add block for " + TEST_ACTION;
            }
        };

        activeRequestCount = 0;
        rebootedNodes.clear();
    }

    @After
    public void assertAllRequestsReleased() {
        assertEquals(0, activeRequestCount);
    }

    private Runnable reboot(DiscoveryNode discoveryNode) {
        return new Runnable() {
            @Override
            public void run() {
                rebootedNodes.add(discoveryNode);
            }

            @Override
            public String toString() {
                return "reboot " + discoveryNode;
            }
        };
    }

    private Runnable unlessRebooted(DiscoveryNode discoveryNode, Runnable runnable) {
        return new Runnable() {
            @Override
            public void run() {
                if (rebootedNodes.contains(discoveryNode)) {
                    if (runnable instanceof DisruptableMockTransport.RebootSensitiveRunnable rebootSensitiveRunnable) {
                        rebootSensitiveRunnable.ifRebooted();
                    }
                } else {
                    runnable.run();
                }
            }

            @Override
            public String toString() {
                return "unlessRebooted[" + discoveryNode.getId() + "/" + runnable + "]";
            }
        };
    }

    private TransportRequestHandler<TestRequest> requestHandlerShouldNotBeCalled() {
        return (request, channel, task) -> { throw new AssertionError("should not be called"); };
    }

    private TransportRequestHandler<TestRequest> requestHandlerRepliesNormally() {
        return (request, channel, task) -> {
            logger.debug("got a dummy request, replying normally...");
            try (var responseRef = ReleasableRef.of(new TestResponse())) {
                channel.sendResponse(responseRef.get());
            }
        };
    }

    private TransportRequestHandler<TestRequest> requestHandlerRepliesExceptionally(Exception e) {
        return (request, channel, task) -> {
            logger.debug("got a dummy request, replying exceptionally...");
            channel.sendResponse(e);
        };
    }

    private TransportRequestHandler<TestRequest> requestHandlerCaptures(Consumer<TransportChannel> channelConsumer) {
        return (request, channel, task) -> {
            logger.debug("got a dummy request...");
            channelConsumer.accept(channel);
        };
    }

    private <T extends TransportResponse> TransportResponseHandler<T> responseHandlerShouldNotBeCalled() {
        return new TransportResponseHandler<>() {
            @Override
            public T read(StreamInput in) {
                throw new AssertionError("should not be called");
            }

            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(T response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                throw new AssertionError("should not be called");
            }
        };
    }

    private TransportResponseHandler<TestResponse> responseHandlerShouldBeCalledNormally(Runnable onCalled) {
        return new TransportResponseHandler<>() {
            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }

            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(TestResponse response) {
                onCalled.run();
            }

            @Override
            public void handleException(TransportException exp) {
                throw new AssertionError("should not be called");
            }
        };
    }

    private <T extends TransportResponse> TransportResponseHandler<T> responseHandlerShouldBeCalledExceptionally(
        Consumer<TransportException> onCalled
    ) {
        return new TransportResponseHandler<>() {
            @Override
            public T read(StreamInput in) {
                throw new AssertionError("should not be called");
            }

            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(T response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                onCalled.accept(exp);
            }
        };
    }

    private void registerRequestHandler(TransportService transportService, TransportRequestHandler<TestRequest> handler) {
        transportService.registerRequestHandler(
            TEST_ACTION,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC),
            TestRequest::new,
            handler
        );
    }

    private void send(
        TransportService transportService,
        DiscoveryNode destinationNode,
        TransportResponseHandler<? extends TransportResponse> responseHandler
    ) {
        final var request = new TestRequest();
        try {
            transportService.sendRequest(destinationNode, TEST_ACTION, request, responseHandler);
        } finally {
            request.decRef();
        }
    }

    public void testSuccessfulResponse() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerRepliesNormally());
        AtomicBoolean responseHandlerCalled = new AtomicBoolean();
        send(service1, node2, responseHandlerShouldBeCalledNormally(() -> responseHandlerCalled.set(true)));
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(responseHandlerCalled.get());
    }

    public void testBlockedAction() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerRepliesNormally());
        blockTestAction.run();
        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerException.get());
        assertNotNull(responseHandlerException.get().getCause());
        assertThat(responseHandlerException.get().getCause().getMessage(), containsString("action [" + TEST_ACTION + "] is blocked"));
    }

    public void testExceptionalResponse() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        Exception e = new Exception("dummy exception");
        registerRequestHandler(service2, requestHandlerRepliesExceptionally(e));
        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerException.get());
        assertNotNull(responseHandlerException.get().getCause());
        assertThat(responseHandlerException.get().getCause().getMessage(), containsString("dummy exception"));
    }

    public void testDisconnectedOnRequest() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerShouldNotBeCalled());
        disconnectedLinks.add(Tuple.tuple(node1, node2));
        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerException.get());
        assertThat(responseHandlerException.get().getMessage(), containsString("disconnected"));
    }

    public void testUnavailableOnRequest() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerShouldNotBeCalled());
        blackholedLinks.add(Tuple.tuple(node1, node2));
        send(service1, node2, responseHandlerShouldNotBeCalled());
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testUnavailableOnRequestOnly() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerShouldNotBeCalled());
        blackholedRequestLinks.add(Tuple.tuple(node1, node2));
        send(service1, node2, responseHandlerShouldNotBeCalled());
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testDisconnectedOnSuccessfulResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerChannel.get());
        assertNull(responseHandlerException.get());

        disconnectedLinks.add(Tuple.tuple(node2, node1));
        try (var responseRef = ReleasableRef.of(new TestResponse())) {
            responseHandlerChannel.get().sendResponse(responseRef.get());
        }
        deterministicTaskQueue.runAllTasks();
        deliverBlackholedRequests.run();
        deterministicTaskQueue.runAllTasks();

        assertThat(responseHandlerException.get(), instanceOf(ConnectTransportException.class));
    }

    public void testDisconnectedOnExceptionalResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerChannel.get());
        assertNull(responseHandlerException.get());

        disconnectedLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(new Exception());
        deterministicTaskQueue.runAllTasks();
        deliverBlackholedRequests.run();
        deterministicTaskQueue.runAllTasks();

        assertThat(responseHandlerException.get(), instanceOf(ConnectTransportException.class));
    }

    public void testUnavailableOnSuccessfulResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        send(service1, node2, responseHandlerShouldNotBeCalled());
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerChannel.get());

        blackholedLinks.add(Tuple.tuple(node2, node1));
        try (var responseRef = ReleasableRef.of(new TestResponse())) {
            responseHandlerChannel.get().sendResponse(responseRef.get());
        }
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testUnavailableOnExceptionalResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        send(service1, node2, responseHandlerShouldNotBeCalled());
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerChannel.get());

        blackholedLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(new Exception());
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testUnavailableOnRequestOnlyReceivesSuccessfulResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        AtomicBoolean responseHandlerCalled = new AtomicBoolean();
        send(service1, node2, responseHandlerShouldBeCalledNormally(() -> responseHandlerCalled.set(true)));

        deterministicTaskQueue.runAllTasks();
        assertNotNull(responseHandlerChannel.get());
        assertFalse(responseHandlerCalled.get());

        blackholedRequestLinks.add(Tuple.tuple(node1, node2));
        blackholedRequestLinks.add(Tuple.tuple(node2, node1));
        try (var responseRef = ReleasableRef.of(new TestResponse())) {
            responseHandlerChannel.get().sendResponse(responseRef.get());
        }

        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(responseHandlerCalled.get());
    }

    public void testUnavailableOnRequestOnlyReceivesExceptionalResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        AtomicBoolean responseHandlerCalled = new AtomicBoolean();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(e -> responseHandlerCalled.set(true)));

        deterministicTaskQueue.runAllTasks();
        assertNotNull(responseHandlerChannel.get());
        assertFalse(responseHandlerCalled.get());

        blackholedRequestLinks.add(Tuple.tuple(node1, node2));
        blackholedRequestLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(new Exception());

        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(responseHandlerCalled.get());
    }

    public void testResponseWithReboots() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(
            service2,
            randomFrom(requestHandlerRepliesNormally(), requestHandlerRepliesExceptionally(new ElasticsearchException("simulated")))
        );

        final var linkDisruptions = List.of(
            Tuple.tuple("blackhole", blackholedLinks),
            Tuple.tuple("blackhole-request", blackholedRequestLinks),
            Tuple.tuple("disconnected", disconnectedLinks)
        );

        for (Runnable runnable : Stream.concat(
            Stream.of(reboot(node1), reboot(node2), blockTestAction),
            Stream.of(Tuple.tuple(node1, node2), Tuple.tuple(node2, node1)).map(link -> {
                final var disruption = randomFrom(linkDisruptions);
                return new Runnable() {
                    @Override
                    public void run() {
                        if (linkDisruptions.stream().noneMatch(otherDisruption -> otherDisruption.v2().contains(link))) {
                            disruption.v2().add(link);
                        }
                    }

                    @Override
                    public String toString() {
                        return disruption.v1() + ": " + link.v1().getId() + " to " + link.v2().getId();
                    }
                };
            })
        ).toList()) {
            if (randomBoolean()) {
                deterministicTaskQueue.scheduleNow(runnable);
            }
        }

        AtomicBoolean responseHandlerCalled = new AtomicBoolean();
        AtomicBoolean responseHandlerReleased = new AtomicBoolean();
        deterministicTaskQueue.scheduleNow(new Runnable() {
            @Override
            public void run() {
                DisruptableMockTransportTests.this.send(
                    service1,
                    node2,
                    new CleanableResponseHandler<>(
                        ActionListener.running(() -> assertFalse(responseHandlerCalled.getAndSet(true))),
                        TestResponse::new,
                        EsExecutors.DIRECT_EXECUTOR_SERVICE,
                        () -> assertFalse(responseHandlerReleased.getAndSet(true))
                    )
                );
            }

            @Override
            public String toString() {
                return "send test message";
            }
        });

        deterministicTaskQueue.runAllRunnableTasks();
        deliverBlackholedRequests.run();
        deterministicTaskQueue.runAllRunnableTasks();

        assertTrue(responseHandlerReleased.get());
        assertTrue(rebootedNodes.contains(node1) || responseHandlerCalled.get());
    }

    public void testBrokenLinkFailsToConnect() {
        service1.disconnectFromNode(node2);

        disconnectedLinks.add(Tuple.tuple(node1, node2));
        assertThat(
            AbstractSimpleTransportTestCase.connectToNodeExpectFailure(service1, node2, null).getMessage(),
            endsWith("is [DISCONNECTED] not [CONNECTED]")
        );
        disconnectedLinks.clear();

        blackholedLinks.add(Tuple.tuple(node1, node2));
        assertThat(
            AbstractSimpleTransportTestCase.connectToNodeExpectFailure(service1, node2, null).getMessage(),
            endsWith("is [BLACK_HOLE] not [CONNECTED]")
        );
        blackholedLinks.clear();

        blackholedRequestLinks.add(Tuple.tuple(node1, node2));
        assertThat(
            AbstractSimpleTransportTestCase.connectToNodeExpectFailure(service1, node2, null).getMessage(),
            endsWith("is [BLACK_HOLE_REQUESTS_ONLY] not [CONNECTED]")
        );
        blackholedRequestLinks.clear();

        final DiscoveryNode node3 = DiscoveryNodeUtils.create("node3");
        assertThat(
            AbstractSimpleTransportTestCase.connectToNodeExpectFailure(service1, node3, null).getMessage(),
            endsWith("does not exist")
        );
    }

    private class TestRequest extends AbstractTransportRequest {
        private final RefCounted refCounted;

        TestRequest() {
            activeRequestCount++;
            refCounted = AbstractRefCounted.of(() -> activeRequestCount--);
        }

        TestRequest(StreamInput in) throws IOException {
            super(in);
            activeRequestCount++;
            refCounted = AbstractRefCounted.of(() -> activeRequestCount--);
        }

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }
    }

    private class TestResponse extends TransportResponse {
        private final RefCounted refCounted;

        TestResponse() {
            activeRequestCount++;
            refCounted = AbstractRefCounted.of(() -> activeRequestCount--);
        }

        TestResponse(StreamInput in) {
            activeRequestCount++;
            refCounted = AbstractRefCounted.of(() -> activeRequestCount--);
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }
    }
}
