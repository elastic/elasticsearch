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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;

public class TransportActionProxyTests extends ESTestCase {
    protected ThreadPool threadPool;
    // we use always a non-alpha or beta version here otherwise minimumCompatibilityVersion will be different for the two used versions
    private static final Version CURRENT_VERSION = Version.fromString(String.valueOf(Version.CURRENT.major) + ".0.0");
    protected static final Version version0 = CURRENT_VERSION.minimumCompatibilityVersion();

    protected DiscoveryNode nodeA;
    protected MockTransportService serviceA;

    protected static final Version version1 = Version.fromId(CURRENT_VERSION.id + 1);
    protected DiscoveryNode nodeB;
    protected MockTransportService serviceB;

    protected DiscoveryNode nodeC;
    protected MockTransportService serviceC;


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        serviceA = buildService(version0); // this one supports dynamic tracer updates
        nodeA = serviceA.getLocalDiscoNode();
        serviceB = buildService(version1); // this one doesn't support dynamic tracer updates
        nodeB = serviceB.getLocalDiscoNode();
        serviceC = buildService(version1); // this one doesn't support dynamic tracer updates
        nodeC = serviceC.getLocalDiscoNode();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(serviceA, serviceB, serviceC, () -> {
            terminate(threadPool);
        });
    }

    private MockTransportService buildService(final Version version) {
        MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, version, threadPool, null);
            service.start();
            service.acceptIncomingRequests();
        return service;

    }


    public void testSendMessage() throws InterruptedException {
        serviceA.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
            (request, channel, task) -> {
                assertEquals(request.sourceNode, "TS_A");
                SimpleTestResponse response = new SimpleTestResponse("TS_A");
                channel.sendResponse(response);
            });
        final boolean cancellable = randomBoolean();
        TransportActionProxy.registerProxyAction(serviceA, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceA, nodeB);

        serviceB.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
            (request, channel, task) -> {
                assertThat(task instanceof CancellableTask, equalTo(cancellable));
                assertEquals(request.sourceNode, "TS_A");
                SimpleTestResponse response = new SimpleTestResponse("TS_B");
                channel.sendResponse(response);
            });
        TransportActionProxy.registerProxyAction(serviceB, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceB, nodeC);
        serviceC.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
            (request, channel, task) -> {
                assertThat(task instanceof CancellableTask, equalTo(cancellable));
                assertEquals(request.sourceNode, "TS_A");
                SimpleTestResponse response = new SimpleTestResponse("TS_C");
                channel.sendResponse(response);
            });

        TransportActionProxy.registerProxyAction(serviceC, "internal:test", cancellable, SimpleTestResponse::new);

        CountDownLatch latch = new CountDownLatch(1);
        serviceA.sendRequest(nodeB, TransportActionProxy.getProxyAction("internal:test"), TransportActionProxy.wrapRequest(nodeC,
            new SimpleTestRequest("TS_A", cancellable)), new TransportResponseHandler<SimpleTestResponse>() {
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
            });
        latch.await();
    }

    public void testException() throws InterruptedException {
        boolean cancellable = randomBoolean();
        serviceA.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
            (request, channel, task) -> {
                assertEquals(request.sourceNode, "TS_A");
                SimpleTestResponse response = new SimpleTestResponse("TS_A");
                channel.sendResponse(response);
            });
        TransportActionProxy.registerProxyAction(serviceA, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceA, nodeB);

        serviceB.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
            (request, channel, task) -> {
                assertEquals(request.sourceNode, "TS_A");
                SimpleTestResponse response = new SimpleTestResponse("TS_B");
                channel.sendResponse(response);
            });
        TransportActionProxy.registerProxyAction(serviceB, "internal:test", cancellable, SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceB, nodeC);
        serviceC.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
            (request, channel, task) -> {
                throw new ElasticsearchException("greetings from TS_C");
            });
        TransportActionProxy.registerProxyAction(serviceC, "internal:test", cancellable, SimpleTestResponse::new);

        CountDownLatch latch = new CountDownLatch(1);
        serviceA.sendRequest(nodeB, TransportActionProxy.getProxyAction("internal:test"), TransportActionProxy.wrapRequest(nodeC,
            new SimpleTestRequest("TS_A", cancellable)), new TransportResponseHandler<SimpleTestResponse>() {
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
            });
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
}
