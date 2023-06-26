/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.NodeVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class TransportActionProxyTests extends ESTestCase {
    protected ThreadPool threadPool;
    // we use always a non-alpha or beta version here otherwise minimumCompatibilityVersion will be different for the two used versions
    private static final Version CURRENT_VERSION = Version.fromString(String.valueOf(Version.CURRENT.major) + ".0.0");
    protected static final NodeVersions version0 = NodeVersions.inferVersions(CURRENT_VERSION.minimumCompatibilityVersion());
    protected static final TransportVersion transportVersion0 = TransportVersion.MINIMUM_COMPATIBLE;

    protected DiscoveryNode nodeA;
    protected MockTransportService serviceA;

    protected static final NodeVersions version1 = NodeVersions.inferVersions(Version.fromId(CURRENT_VERSION.id + 1));
    protected static final TransportVersion transportVersion1 = TransportVersion.fromId(TransportVersion.current().id() + 1);
    protected DiscoveryNode nodeB;
    protected MockTransportService serviceB;

    protected DiscoveryNode nodeC;
    protected MockTransportService serviceC;

    protected DiscoveryNode nodeD;
    protected MockTransportService serviceD;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        serviceA = buildService(version0, transportVersion0); // this one supports dynamic tracer updates
        serviceA.taskManager.setTaskCancellationService(new TaskCancellationService(serviceA));
        nodeA = serviceA.getLocalDiscoNode();
        serviceB = buildService(version1, transportVersion1); // this one doesn't support dynamic tracer updates
        serviceB.taskManager.setTaskCancellationService(new TaskCancellationService(serviceB));
        nodeB = serviceB.getLocalDiscoNode();
        serviceC = buildService(version1, transportVersion1); // this one doesn't support dynamic tracer updates
        serviceC.taskManager.setTaskCancellationService(new TaskCancellationService(serviceC));
        nodeC = serviceC.getLocalDiscoNode();
        serviceD = buildService(version1, transportVersion1);
        nodeD = serviceD.getLocalDiscoNode();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(serviceA, serviceB, serviceC, serviceD, () -> { terminate(threadPool); });
    }

    private MockTransportService buildService(NodeVersions version, TransportVersion transportVersion) {
        MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, version, transportVersion, threadPool, null);
        service.start();
        service.acceptIncomingRequests();
        return service;

    }

    public void testSendMessage() throws InterruptedException {
        serviceA.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new, (request, channel, task) -> {
            assertEquals(request.sourceNode, "TS_A");
            final SimpleTestResponse response = new SimpleTestResponse("TS_A");
            channel.sendResponse(response);
            assertThat(response.hasReferences(), equalTo(false));
        });
        final boolean cancellable = randomBoolean();
        TransportActionProxy.registerProxyAction(serviceA, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceA, nodeB);

        serviceB.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new, (request, channel, task) -> {
            assertThat(task instanceof CancellableTask, equalTo(cancellable));
            assertEquals(request.sourceNode, "TS_A");
            final SimpleTestResponse response = new SimpleTestResponse("TS_B");
            channel.sendResponse(response);
            assertThat(response.hasReferences(), equalTo(false));
        });
        TransportActionProxy.registerProxyAction(serviceB, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceB, nodeC);
        serviceC.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new, (request, channel, task) -> {
            assertThat(task instanceof CancellableTask, equalTo(cancellable));
            assertEquals(request.sourceNode, "TS_A");
            final SimpleTestResponse response = new SimpleTestResponse("TS_C");
            channel.sendResponse(response);
            assertThat(response.hasReferences(), equalTo(false));
        });

        TransportActionProxy.registerProxyAction(serviceC, "internal:test", cancellable, SimpleTestResponse::new);
        // Node A -> Node B -> Node C: different versions - serialize the response
        {
            final List<TransportMessage> responses = Collections.synchronizedList(new ArrayList<>());
            final CountDownLatch latch = new CountDownLatch(1);
            serviceB.addRequestHandlingBehavior(
                TransportActionProxy.getProxyAction("internal:test"),
                (handler, request, channel, task) -> handler.messageReceived(
                    request,
                    new CapturingTransportChannel(channel, responses::add),
                    task
                )
            );
            serviceA.sendRequest(
                nodeB,
                TransportActionProxy.getProxyAction("internal:test"),
                TransportActionProxy.wrapRequest(nodeC, new SimpleTestRequest("TS_A", cancellable)),
                new TransportResponseHandler<SimpleTestResponse>() {
                    @Override
                    public SimpleTestResponse read(StreamInput in) throws IOException {
                        return new SimpleTestResponse(in);
                    }

                    @Override
                    public void handleResponse(SimpleTestResponse response) {
                        try {
                            assertEquals("TS_C", response.targetNode);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        try {
                            throw new AssertionError(exp);
                        } finally {
                            latch.countDown();
                        }
                    }
                }
            );
            latch.await();
            assertThat(responses, hasSize(1));
            assertThat(responses.get(0), instanceOf(SimpleTestResponse.class));
            serviceB.clearAllRules();
        }
        // Node D -> node B -> Node C: the same version - do not serialize the responses
        {
            AbstractSimpleTransportTestCase.connectToNode(serviceD, nodeB);
            final CountDownLatch latch = new CountDownLatch(1);
            final List<TransportMessage> responses = Collections.synchronizedList(new ArrayList<>());
            serviceB.addRequestHandlingBehavior(
                TransportActionProxy.getProxyAction("internal:test"),
                (handler, request, channel, task) -> handler.messageReceived(
                    request,
                    new CapturingTransportChannel(channel, responses::add),
                    task
                )
            );
            serviceD.sendRequest(
                nodeB,
                TransportActionProxy.getProxyAction("internal:test"),
                TransportActionProxy.wrapRequest(nodeC, new SimpleTestRequest("TS_A", cancellable)),
                new TransportResponseHandler<SimpleTestResponse>() {
                    @Override
                    public SimpleTestResponse read(StreamInput in) throws IOException {
                        return new SimpleTestResponse(in);
                    }

                    @Override
                    public void handleResponse(SimpleTestResponse response) {
                        try {
                            assertEquals("TS_C", response.targetNode);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        try {
                            throw new AssertionError(exp);
                        } finally {
                            latch.countDown();
                        }
                    }
                }
            );
            latch.await();
            assertThat(responses, hasSize(1));
            assertThat(responses.get(0), instanceOf(TransportActionProxy.BytesTransportResponse.class));
            serviceB.clearAllRules();
        }
    }

    public void testSendLocalRequest() throws Exception {
        final AtomicReference<SimpleTestResponse> response = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(2);

        final boolean cancellable = randomBoolean();
        serviceB.registerRequestHandler(
            "internal:test",
            randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            SimpleTestRequest::new,
            (request, channel, task) -> {
                try {
                    assertThat(task instanceof CancellableTask, equalTo(cancellable));
                    assertEquals(request.sourceNode, "TS_A");
                    final SimpleTestResponse responseB = new SimpleTestResponse("TS_B");
                    channel.sendResponse(responseB);
                    response.set(responseB);
                } finally {
                    latch.countDown();
                }
            }
        );
        TransportActionProxy.registerProxyAction(serviceB, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceA, nodeB);

        // Node A -> Proxy Node B (Local execution)
        serviceA.sendRequest(
            nodeB,
            TransportActionProxy.getProxyAction("internal:test"),
            TransportActionProxy.wrapRequest(nodeB, new SimpleTestRequest("TS_A", cancellable)), // Request
            new TransportResponseHandler<SimpleTestResponse>() {
                @Override
                public SimpleTestResponse read(StreamInput in) throws IOException {
                    return new SimpleTestResponse(in);
                }

                @Override
                public void handleResponse(SimpleTestResponse response) {
                    try {
                        assertEquals("TS_B", response.targetNode);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    try {
                        throw new AssertionError(exp);
                    } finally {
                        latch.countDown();
                    }
                }
            }
        );
        latch.await();

        assertThat(response.get(), notNullValue());
        assertBusy(() -> assertThat(response.get().hasReferences(), equalTo(false)));
    }

    public void testException() throws InterruptedException {
        boolean cancellable = randomBoolean();
        serviceA.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new, (request, channel, task) -> {
            assertEquals(request.sourceNode, "TS_A");
            SimpleTestResponse response = new SimpleTestResponse("TS_A");
            channel.sendResponse(response);
        });
        TransportActionProxy.registerProxyAction(serviceA, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceA, nodeB);

        serviceB.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new, (request, channel, task) -> {
            assertEquals(request.sourceNode, "TS_A");
            SimpleTestResponse response = new SimpleTestResponse("TS_B");
            channel.sendResponse(response);
        });
        TransportActionProxy.registerProxyAction(serviceB, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceB, nodeC);
        serviceC.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new, (request, channel, task) -> {
            throw new ElasticsearchException("greetings from TS_C");
        });
        TransportActionProxy.registerProxyAction(serviceC, "internal:test", cancellable, SimpleTestResponse::new);

        CountDownLatch latch = new CountDownLatch(1);
        serviceA.sendRequest(
            nodeB,
            TransportActionProxy.getProxyAction("internal:test"),
            TransportActionProxy.wrapRequest(nodeC, new SimpleTestRequest("TS_A", cancellable)),
            new TransportResponseHandler<SimpleTestResponse>() {
                @Override
                public SimpleTestResponse read(StreamInput in) throws IOException {
                    return new SimpleTestResponse(in);
                }

                @Override
                public void handleResponse(SimpleTestResponse response) {
                    try {
                        fail("expected exception");
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    try {
                        Throwable cause = ExceptionsHelper.unwrapCause(exp);
                        assertEquals("greetings from TS_C", cause.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }
            }
        );
        latch.await();
    }

    public static class SimpleTestRequest extends TransportRequest {
        final boolean cancellable;
        final String sourceNode;

        public SimpleTestRequest(String sourceNode, boolean cancellable) {
            this.sourceNode = sourceNode;
            this.cancellable = cancellable;
        }

        public SimpleTestRequest(StreamInput in) throws IOException {
            super(in);
            sourceNode = in.readString();
            cancellable = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceNode);
            out.writeBoolean(cancellable);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            if (cancellable) {
                return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                    @Override
                    public boolean shouldCancelChildrenOnCancellation() {
                        return randomBoolean();
                    }
                };
            } else {
                return super.createTask(id, type, action, parentTaskId, headers);
            }
        }
    }

    public static class SimpleTestResponse extends TransportResponse {

        final String targetNode;
        final RefCounted refCounted = new AbstractRefCounted() {
            @Override
            protected void closeInternal() {}
        };

        SimpleTestResponse(String targetNode) {
            this.targetNode = targetNode;
        }

        SimpleTestResponse(StreamInput in) throws IOException {
            super(in);
            this.targetNode = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(targetNode);
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

    public void testGetAction() {
        String action = "foo/bar";
        String proxyAction = TransportActionProxy.getProxyAction(action);
        assertTrue(proxyAction.endsWith(action));
        assertEquals("internal:transport/proxy/foo/bar", proxyAction);
    }

    public void testUnwrap() {
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(nodeA, TransportService.HandshakeRequest.INSTANCE);
        assertTrue(transportRequest instanceof TransportActionProxy.ProxyRequest);
        assertSame(TransportService.HandshakeRequest.INSTANCE, TransportActionProxy.unwrapRequest(transportRequest));
    }

    public void testIsProxyAction() {
        String action = "foo/bar";
        String proxyAction = TransportActionProxy.getProxyAction(action);
        assertTrue(TransportActionProxy.isProxyAction(proxyAction));
        assertFalse(TransportActionProxy.isProxyAction(action));
    }

    public void testIsProxyRequest() {
        assertTrue(TransportActionProxy.isProxyRequest(new TransportActionProxy.ProxyRequest<>(TransportRequest.Empty.INSTANCE, null)));
        assertFalse(TransportActionProxy.isProxyRequest(TransportRequest.Empty.INSTANCE));
    }

    static class CapturingTransportChannel implements TransportChannel {
        final TransportChannel in;
        final Consumer<TransportResponse> onResponse;

        CapturingTransportChannel(TransportChannel in, Consumer<TransportResponse> onResponse) {
            this.in = in;
            this.onResponse = onResponse;
        }

        @Override
        public String getProfileName() {
            return in.getProfileName();
        }

        @Override
        public String getChannelType() {
            return in.getChannelType();
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            onResponse.accept(response);
            in.sendResponse(response);
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            in.sendResponse(exception);
        }

        @Override
        public TransportVersion getVersion() {
            return in.getVersion();
        }
    }
}
