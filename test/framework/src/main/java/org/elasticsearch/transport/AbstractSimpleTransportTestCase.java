/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public abstract class AbstractSimpleTransportTestCase extends ESTestCase {

    protected ThreadPool threadPool;
    // we use always a non-alpha or beta version here otherwise minimumCompatibilityVersion will be different for the two used versions
    private static final Version CURRENT_VERSION = Version.fromString(String.valueOf(Version.CURRENT.major) + ".0.0");
    protected static final Version version0 = CURRENT_VERSION.minimumCompatibilityVersion();

    private ClusterSettings clusterSettings;

    protected volatile DiscoveryNode nodeA;
    protected volatile MockTransportService serviceA;

    protected static final Version version1 = Version.fromId(CURRENT_VERSION.id + 1);
    protected volatile DiscoveryNode nodeB;
    protected volatile MockTransportService serviceB;

    protected abstract MockTransportService build(Settings settings, Version version, ClusterSettings clusterSettings, boolean doHandshake);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        serviceA = buildService("TS_A", version0, clusterSettings); // this one supports dynamic tracer updates
        nodeA = serviceA.getLocalNode();
        serviceB = buildService("TS_B", version1, null); // this one doesn't support dynamic tracer updates
        nodeB = serviceB.getLocalNode();
        // wait till all nodes are properly connected and the event has been sent, so tests in this class
        // will not get this callback called on the connections done in this setup
        final CountDownLatch latch = new CountDownLatch(2);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                fail("disconnect should not be called " + node);
            }
        };
        serviceA.addConnectionListener(waitForConnection);
        serviceB.addConnectionListener(waitForConnection);
        int numHandshakes = 1;
        serviceA.connectToNode(nodeB);
        serviceB.connectToNode(nodeA);
        assertNumHandshakes(numHandshakes, serviceA.getOriginalTransport());
        assertNumHandshakes(numHandshakes, serviceB.getOriginalTransport());

        assertThat("failed to wait for all nodes to connect", latch.await(5, TimeUnit.SECONDS), equalTo(true));
        serviceA.removeConnectionListener(waitForConnection);
        serviceB.removeConnectionListener(waitForConnection);
    }

    private MockTransportService buildService(final String name, final Version version, ClusterSettings clusterSettings,
                                              Settings settings, boolean acceptRequests, boolean doHandshake) {
        MockTransportService service = build(
                Settings.builder()
                        .put(settings)
                        .put(Node.NODE_NAME_SETTING.getKey(), name)
                        .put(TransportService.TRACE_LOG_INCLUDE_SETTING.getKey(), "")
                        .put(TransportService.TRACE_LOG_EXCLUDE_SETTING.getKey(), "NOTHING")
                        .build(),
                version,
                clusterSettings, doHandshake);
        if (acceptRequests) {
            service.acceptIncomingRequests();
        }
        return service;
    }

    private MockTransportService buildService(final String name, final Version version, ClusterSettings clusterSettings) {
        return buildService(name, version, clusterSettings, Settings.EMPTY, true, true);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            assertNoPendingHandshakes(serviceA.getOriginalTransport());
            assertNoPendingHandshakes(serviceB.getOriginalTransport());
        } finally {
            IOUtils.close(serviceA, serviceB, () -> {
                try {
                    terminate(threadPool);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    public void assertNumHandshakes(long expected, Transport transport) {
        if (transport instanceof TcpTransport) {
            assertEquals(expected, ((TcpTransport) transport).getNumHandshakes());
        }
    }

    public void assertNoPendingHandshakes(Transport transport) {
        if (transport instanceof TcpTransport) {
            assertEquals(0, ((TcpTransport) transport).getNumPendingHandshakes());
        }
    }


    public void testHelloWorld() {
        serviceA.registerRequestHandler("sayHello", StringMessageRequest::new, ThreadPool.Names.GENERIC,
            (request, channel) -> {
                assertThat("moshe", equalTo(request.message));
                try {
                    channel.sendResponse(new StringMessageResponse("hello " + request.message));
                } catch (IOException e) {
                    logger.error("Unexpected failure", e);
                    fail(e.getMessage());
                }
            });

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHello",
            new StringMessageRequest("moshe"), new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse newInstance() {
                    return new StringMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {
                    assertThat("hello moshe", equalTo(response.message));
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }
            });

        try {
            StringMessageResponse message = res.get();
            assertThat("hello moshe", equalTo(message.message));
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }

        res = serviceB.submitRequest(nodeA, "sayHello", new StringMessageRequest("moshe"),
            TransportRequestOptions.builder().withCompress(true).build(), new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse newInstance() {
                    return new StringMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {
                    assertThat("hello moshe", equalTo(response.message));
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }
            });

        try {
            StringMessageResponse message = res.get();
            assertThat("hello moshe", equalTo(message.message));
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }
    }

    public void testThreadContext() throws ExecutionException, InterruptedException {

        serviceA.registerRequestHandler("ping_pong", StringMessageRequest::new, ThreadPool.Names.GENERIC, (request, channel) -> {
            assertEquals("ping_user", threadPool.getThreadContext().getHeader("test.ping.user"));
            assertNull(threadPool.getThreadContext().getTransient("my_private_context"));
            try {
                StringMessageResponse response = new StringMessageResponse("pong");
                threadPool.getThreadContext().putHeader("test.pong.user", "pong_user");
                channel.sendResponse(response);
            } catch (IOException e) {
                logger.error("Unexpected failure", e);
                fail(e.getMessage());
            }
        });
        final Object context = new Object();
        final String executor = randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet().toArray(new String[0]));
        TransportResponseHandler<StringMessageResponse> responseHandler = new TransportResponseHandler<StringMessageResponse>() {
            @Override
            public StringMessageResponse newInstance() {
                return new StringMessageResponse();
            }

            @Override
            public String executor() {
                return executor;
            }

            @Override
            public void handleResponse(StringMessageResponse response) {
                assertThat("pong", equalTo(response.message));
                assertEquals("ping_user", threadPool.getThreadContext().getHeader("test.ping.user"));
                assertNull(threadPool.getThreadContext().getHeader("test.pong.user"));
                assertSame(context, threadPool.getThreadContext().getTransient("my_private_context"));
                threadPool.getThreadContext().putHeader("some.temp.header", "booooom");
            }

            @Override
            public void handleException(TransportException exp) {
                logger.error("Unexpected failure", exp);
                fail("got exception instead of a response: " + exp.getMessage());
            }
        };
        StringMessageRequest ping = new StringMessageRequest("ping");
        threadPool.getThreadContext().putHeader("test.ping.user", "ping_user");
        threadPool.getThreadContext().putTransient("my_private_context", context);

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "ping_pong", ping, responseHandler);

        StringMessageResponse message = res.get();
        assertThat("pong", equalTo(message.message));
        assertEquals("ping_user", threadPool.getThreadContext().getHeader("test.ping.user"));
        assertSame(context, threadPool.getThreadContext().getTransient("my_private_context"));
        assertNull("this header is only visible in the handler context", threadPool.getThreadContext().getHeader("some.temp.header"));
    }

    public void testLocalNodeConnection() throws InterruptedException {
        assertTrue("serviceA is not connected to nodeA", serviceA.nodeConnected(nodeA));
        // this should be a noop
        serviceA.disconnectFromNode(nodeA);
        final AtomicReference<Exception> exception = new AtomicReference<>();
        serviceA.registerRequestHandler("localNode", StringMessageRequest::new, ThreadPool.Names.GENERIC,
            (request, channel) -> {
                try {
                    channel.sendResponse(new StringMessageResponse(request.message));
                } catch (IOException e) {
                    exception.set(e);
                }
            });
        final AtomicReference<String> responseString = new AtomicReference<>();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        serviceA.sendRequest(nodeA, "localNode", new StringMessageRequest("test"), new TransportResponseHandler<StringMessageResponse>() {
            @Override
            public StringMessageResponse newInstance() {
                return new StringMessageResponse();
            }

            @Override
            public void handleResponse(StringMessageResponse response) {
                responseString.set(response.message);
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                exception.set(exp);
                responseLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        });
        responseLatch.await();
        assertNull(exception.get());
        assertThat(responseString.get(), equalTo("test"));
    }

    public void testAdapterSendReceiveCallbacks() throws Exception {
        final TransportRequestHandler<TransportRequest.Empty> requestHandler = (request, channel) -> {
            try {
                if (randomBoolean()) {
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } else {
                    channel.sendResponse(new ElasticsearchException("simulated"));
                }
            } catch (IOException e) {
                logger.error("Unexpected failure", e);
                fail(e.getMessage());
            }
        };
        final String ACTION = "action";
        serviceA.registerRequestHandler(ACTION, TransportRequest.Empty::new, ThreadPool.Names.GENERIC,
            requestHandler);
        serviceB.registerRequestHandler(ACTION, TransportRequest.Empty::new, ThreadPool.Names.GENERIC,
            requestHandler);


        class CountingTracer extends MockTransportService.Tracer {
            AtomicInteger requestsReceived = new AtomicInteger();
            AtomicInteger requestsSent = new AtomicInteger();
            AtomicInteger responseReceived = new AtomicInteger();
            AtomicInteger responseSent = new AtomicInteger();

            @Override
            public void receivedRequest(long requestId, String action) {
                if (action.equals(ACTION)) {
                    requestsReceived.incrementAndGet();
                }
            }

            @Override
            public void responseSent(long requestId, String action) {
                if (action.equals(ACTION)) {
                    responseSent.incrementAndGet();
                }
            }

            @Override
            public void responseSent(long requestId, String action, Throwable t) {
                if (action.equals(ACTION)) {
                    responseSent.incrementAndGet();
                }
            }

            @Override
            public void receivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
                if (action.equals(ACTION)) {
                    responseReceived.incrementAndGet();
                }
            }

            @Override
            public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
                if (action.equals(ACTION)) {
                    requestsSent.incrementAndGet();
                }
            }
        }
        final CountingTracer tracerA = new CountingTracer();
        final CountingTracer tracerB = new CountingTracer();
        serviceA.addTracer(tracerA);
        serviceB.addTracer(tracerB);

        try {
            serviceA
                .submitRequest(nodeB, ACTION, TransportRequest.Empty.INSTANCE, EmptyTransportResponseHandler.INSTANCE_SAME).get();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
            assertThat(ExceptionsHelper.unwrapCause(e.getCause()).getMessage(), equalTo("simulated"));
        }

        // use assert busy as call backs are sometime called after the response have been sent
        assertBusy(() -> {
            assertThat(tracerA.requestsReceived.get(), equalTo(0));
            assertThat(tracerA.requestsSent.get(), equalTo(1));
            assertThat(tracerA.responseReceived.get(), equalTo(1));
            assertThat(tracerA.responseSent.get(), equalTo(0));
            assertThat(tracerB.requestsReceived.get(), equalTo(1));
            assertThat(tracerB.requestsSent.get(), equalTo(0));
            assertThat(tracerB.responseReceived.get(), equalTo(0));
            assertThat(tracerB.responseSent.get(), equalTo(1));
        });

        try {
            serviceA
                .submitRequest(nodeA, ACTION, TransportRequest.Empty.INSTANCE, EmptyTransportResponseHandler.INSTANCE_SAME).get();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
            assertThat(ExceptionsHelper.unwrapCause(e.getCause()).getMessage(), equalTo("simulated"));
        }

        // use assert busy as call backs are sometime called after the response have been sent
        assertBusy(() -> {
            assertThat(tracerA.requestsReceived.get(), equalTo(1));
            assertThat(tracerA.requestsSent.get(), equalTo(2));
            assertThat(tracerA.responseReceived.get(), equalTo(2));
            assertThat(tracerA.responseSent.get(), equalTo(1));
            assertThat(tracerB.requestsReceived.get(), equalTo(1));
            assertThat(tracerB.requestsSent.get(), equalTo(0));
            assertThat(tracerB.responseReceived.get(), equalTo(0));
            assertThat(tracerB.responseSent.get(), equalTo(1));
        });
    }

    public void testVoidMessageCompressed() {
        serviceA.registerRequestHandler("sayHello", TransportRequest.Empty::new, ThreadPool.Names.GENERIC,
            (request, channel) -> {
                try {
                    TransportResponseOptions responseOptions = TransportResponseOptions.builder().withCompress(true).build();
                    channel.sendResponse(TransportResponse.Empty.INSTANCE, responseOptions);
                } catch (IOException e) {
                    logger.error("Unexpected failure", e);
                    fail(e.getMessage());
                }
            });

        TransportFuture<TransportResponse.Empty> res = serviceB.submitRequest(nodeA, "sayHello",
            TransportRequest.Empty.INSTANCE, TransportRequestOptions.builder().withCompress(true).build(),
            new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty newInstance() {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }
            });

        try {
            TransportResponse.Empty message = res.get();
            assertThat(message, notNullValue());
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }
    }

    public void testHelloWorldCompressed() {
        serviceA.registerRequestHandler("sayHello", StringMessageRequest::new, ThreadPool.Names.GENERIC,
            new TransportRequestHandler<StringMessageRequest>() {
                @Override
                public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                    assertThat("moshe", equalTo(request.message));
                    try {
                        TransportResponseOptions responseOptions = TransportResponseOptions.builder().withCompress(true).build();
                        channel.sendResponse(new StringMessageResponse("hello " + request.message), responseOptions);
                    } catch (IOException e) {
                        logger.error("Unexpected failure", e);
                        fail(e.getMessage());
                    }
                }
            });

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHello",
            new StringMessageRequest("moshe"), TransportRequestOptions.builder().withCompress(true).build(),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse newInstance() {
                    return new StringMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {
                    assertThat("hello moshe", equalTo(response.message));
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }
            });

        try {
            StringMessageResponse message = res.get();
            assertThat("hello moshe", equalTo(message.message));
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }
    }

    public void testErrorMessage() {
        serviceA.registerRequestHandler("sayHelloException", StringMessageRequest::new, ThreadPool.Names.GENERIC,
            new TransportRequestHandler<StringMessageRequest>() {
                @Override
                public void messageReceived(StringMessageRequest request, TransportChannel channel) throws Exception {
                    assertThat("moshe", equalTo(request.message));
                    throw new RuntimeException("bad message !!!");
                }
            });

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHelloException",
            new StringMessageRequest("moshe"), new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse newInstance() {
                    return new StringMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {
                    fail("got response instead of exception");
                }

                @Override
                public void handleException(TransportException exp) {
                    assertThat("runtime_exception: bad message !!!", equalTo(exp.getCause().getMessage()));
                }
            });

        try {
            res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), equalTo("runtime_exception: bad message !!!"));
        }
    }

    public void testDisconnectListener() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        TransportConnectionListener disconnectListener = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node) {
                fail("node connected should not be called, all connection have been done previously, node: " + node);
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                latch.countDown();
            }
        };
        serviceA.addConnectionListener(disconnectListener);
        serviceB.close();
        assertThat(latch.await(5, TimeUnit.SECONDS), equalTo(true));
    }

    public void testConcurrentSendRespondAndDisconnect() throws BrokenBarrierException, InterruptedException {
        Set<Exception> sendingErrors = ConcurrentCollections.newConcurrentSet();
        Set<Exception> responseErrors = ConcurrentCollections.newConcurrentSet();
        serviceA.registerRequestHandler("test", TestRequest::new,
            randomBoolean() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC, (request, channel) -> {
                try {
                    channel.sendResponse(new TestResponse());
                } catch (Exception e) {
                    logger.info("caught exception while responding", e);
                    responseErrors.add(e);
                }
            });
        final TransportRequestHandler<TestRequest> ignoringRequestHandler = (request, channel) -> {
            try {
                channel.sendResponse(new TestResponse());
            } catch (Exception e) {
                // we don't really care what's going on B, we're testing through A
                logger.trace("caught exception while responding from node B", e);
            }
        };
        serviceB.registerRequestHandler("test", TestRequest::new, ThreadPool.Names.SAME, ignoringRequestHandler);

        int halfSenders = scaledRandomIntBetween(3, 10);
        final CyclicBarrier go = new CyclicBarrier(halfSenders * 2 + 1);
        final CountDownLatch done = new CountDownLatch(halfSenders * 2);
        for (int i = 0; i < halfSenders; i++) {
            // B senders just generated activity so serciveA can respond, we don't test what's going on there
            final int sender = i;
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.trace("caught exception while sending from B", e);
                }

                @Override
                protected void doRun() throws Exception {
                    go.await();
                    for (int iter = 0; iter < 10; iter++) {
                        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
                        final String info = sender + "_B_" + iter;
                        serviceB.sendRequest(nodeA, "test", new TestRequest(info),
                            new ActionListenerResponseHandler<>(listener, TestResponse::new));
                        try {
                            listener.actionGet();

                        } catch (Exception e) {
                            logger.trace(
                                (Supplier<?>) () -> new ParameterizedMessage("caught exception while sending to node {}", nodeA), e);
                        }
                    }
                }

                @Override
                public void onAfter() {
                    done.countDown();
                }
            });
        }

        for (int i = 0; i < halfSenders; i++) {
            final int sender = i;
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected error", e);
                    sendingErrors.add(e);
                }

                @Override
                protected void doRun() throws Exception {
                    go.await();
                    for (int iter = 0; iter < 10; iter++) {
                        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
                        final String info = sender + "_" + iter;
                        final DiscoveryNode node = nodeB; // capture now
                        try {
                            serviceA.sendRequest(node, "test", new TestRequest(info),
                                new ActionListenerResponseHandler<>(listener, TestResponse::new));
                            try {
                                listener.actionGet();
                            } catch (ConnectTransportException e) {
                                // ok!
                            } catch (Exception e) {
                                logger.error(
                                    (Supplier<?>) () -> new ParameterizedMessage("caught exception while sending to node {}", node), e);
                                sendingErrors.add(e);
                            }
                        } catch (NodeNotConnectedException ex) {
                            // ok
                        }

                    }
                }

                @Override
                public void onAfter() {
                    done.countDown();
                }
            });
        }
        go.await();
        for (int i = 0; i <= 10; i++) {
            if (i % 3 == 0) {
                // simulate restart of nodeB
                serviceB.close();
                MockTransportService newService = buildService("TS_B_" + i, version1, null);
                newService.registerRequestHandler("test", TestRequest::new, ThreadPool.Names.SAME, ignoringRequestHandler);
                serviceB = newService;
                nodeB = newService.getLocalDiscoNode();
                serviceB.connectToNode(nodeA);
                serviceA.connectToNode(nodeB);
            } else if (serviceA.nodeConnected(nodeB)) {
                serviceA.disconnectFromNode(nodeB);
            } else {
                serviceA.connectToNode(nodeB);
            }
        }

        done.await();

        assertThat("found non connection errors while sending", sendingErrors, empty());
        assertThat("found non connection errors while responding", responseErrors, empty());
    }

    public void testNotifyOnShutdown() throws Exception {
        final CountDownLatch latch2 = new CountDownLatch(1);
        try {
            serviceA.registerRequestHandler("foobar", StringMessageRequest::new, ThreadPool.Names.GENERIC,
                (request, channel) -> {
                    try {
                        latch2.await();
                        logger.info("Stop ServiceB now");
                        serviceB.stop();
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                });
            TransportFuture<TransportResponse.Empty> foobar = serviceB.submitRequest(nodeA, "foobar",
                new StringMessageRequest(""), TransportRequestOptions.EMPTY, EmptyTransportResponseHandler.INSTANCE_SAME);
            latch2.countDown();
            try {
                foobar.txGet();
                fail("TransportException expected");
            } catch (TransportException ex) {

            }
        } finally {
            serviceB.close(); // make sure we are fully closed here otherwise we might run into assertions down the road
            serviceA.disconnectFromNode(nodeB);
        }
    }

    public void testTimeoutSendExceptionWithNeverSendingBackResponse() throws Exception {
        serviceA.registerRequestHandler("sayHelloTimeoutNoResponse", StringMessageRequest::new, ThreadPool.Names.GENERIC,
            new TransportRequestHandler<StringMessageRequest>() {
                @Override
                public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                    assertThat("moshe", equalTo(request.message));
                    // don't send back a response
                }
            });

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHelloTimeoutNoResponse",
            new StringMessageRequest("moshe"), TransportRequestOptions.builder().withTimeout(100).build(),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse newInstance() {
                    return new StringMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {
                    fail("got response instead of exception");
                }

                @Override
                public void handleException(TransportException exp) {
                    assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
                }
            });

        try {
            StringMessageResponse message = res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(ReceiveTimeoutTransportException.class));
        }
    }

    public void testTimeoutSendExceptionWithDelayedResponse() throws Exception {
        CountDownLatch waitForever = new CountDownLatch(1);
        CountDownLatch doneWaitingForever = new CountDownLatch(1);
        Semaphore inFlight = new Semaphore(Integer.MAX_VALUE);
        serviceA.registerRequestHandler("sayHelloTimeoutDelayedResponse", StringMessageRequest::new, ThreadPool.Names.GENERIC,
            new TransportRequestHandler<StringMessageRequest>() {
                @Override
                public void messageReceived(StringMessageRequest request, TransportChannel channel) throws InterruptedException {
                    String message = request.message;
                    inFlight.acquireUninterruptibly();
                    try {
                        if ("forever".equals(message)) {
                            waitForever.await();
                        } else {
                            TimeValue sleep = TimeValue.parseTimeValue(message, null, "sleep");
                            Thread.sleep(sleep.millis());
                        }
                        try {
                            channel.sendResponse(new StringMessageResponse("hello " + request.message));
                        } catch (IOException e) {
                            logger.error("Unexpected failure", e);
                            fail(e.getMessage());
                        }
                    } finally {
                        inFlight.release();
                        if ("forever".equals(message)) {
                            doneWaitingForever.countDown();
                        }
                    }
                }
            });
        final CountDownLatch latch = new CountDownLatch(1);
        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHelloTimeoutDelayedResponse",
            new StringMessageRequest("forever"), TransportRequestOptions.builder().withTimeout(100).build(),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse newInstance() {
                    return new StringMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {
                    latch.countDown();
                    fail("got response instead of exception");
                }

                @Override
                public void handleException(TransportException exp) {
                    latch.countDown();
                    assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
                }
            });

        try {
            res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(ReceiveTimeoutTransportException.class));
        }
        latch.await();

        List<Runnable> assertions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int counter = i;
            // now, try and send another request, this times, with a short timeout
            TransportFuture<StringMessageResponse> result = serviceB.submitRequest(nodeA, "sayHelloTimeoutDelayedResponse",
                new StringMessageRequest(counter + "ms"), TransportRequestOptions.builder().withTimeout(3000).build(),
                new TransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        assertThat("hello " + counter + "ms", equalTo(response.message));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("Unexpected failure", exp);
                        fail("got exception instead of a response for " + counter + ": " + exp.getDetailedMessage());
                    }
                });

            assertions.add(() -> {
                StringMessageResponse message = result.txGet();
                assertThat(message.message, equalTo("hello " + counter + "ms"));
            });
        }
        for (Runnable runnable : assertions) {
            runnable.run();
        }
        waitForever.countDown();
        doneWaitingForever.await();
        assertTrue(inFlight.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));
    }

    public void testTracerLog() throws InterruptedException {
        TransportRequestHandler handler = (request, channel) -> channel.sendResponse(new StringMessageResponse(""));
        TransportRequestHandler handlerWithError = new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) throws Exception {
                if (request.timeout() > 0) {
                    Thread.sleep(request.timeout);
                }
                channel.sendResponse(new RuntimeException(""));

            }
        };

        final Semaphore requestCompleted = new Semaphore(0);
        TransportResponseHandler noopResponseHandler = new TransportResponseHandler<StringMessageResponse>() {

            @Override
            public StringMessageResponse newInstance() {
                return new StringMessageResponse();
            }

            @Override
            public void handleResponse(StringMessageResponse response) {
                requestCompleted.release();
            }

            @Override
            public void handleException(TransportException exp) {
                requestCompleted.release();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };

        serviceA.registerRequestHandler("test", StringMessageRequest::new, ThreadPool.Names.SAME, handler);
        serviceA.registerRequestHandler("testError", StringMessageRequest::new, ThreadPool.Names.SAME, handlerWithError);
        serviceB.registerRequestHandler("test", StringMessageRequest::new, ThreadPool.Names.SAME, handler);
        serviceB.registerRequestHandler("testError", StringMessageRequest::new, ThreadPool.Names.SAME, handlerWithError);

        final Tracer tracer = new Tracer(new HashSet<>(Arrays.asList("test", "testError")));
        // the tracer is invoked concurrently after the actual action is executed. that means a Tracer#requestSent can still be in-flight
        // from a handshake executed on connect in the setup method. this might confuse this test since it expects exact number of
        // invocations. To prevent any unrelated events messing with this test we filter on the actions we execute in this test.
        serviceA.addTracer(tracer);
        serviceB.addTracer(tracer);

        tracer.reset(4);
        boolean timeout = randomBoolean();
        TransportRequestOptions options = timeout ? TransportRequestOptions.builder().withTimeout(1).build() :
            TransportRequestOptions.EMPTY;
        serviceA.sendRequest(nodeB, "test", new StringMessageRequest("", 10), options, noopResponseHandler);
        requestCompleted.acquire();
        tracer.expectedEvents.get().await();
        assertThat("didn't see request sent", tracer.sawRequestSent, equalTo(true));
        assertThat("didn't see request received", tracer.sawRequestReceived, equalTo(true));
        assertThat("didn't see response sent", tracer.sawResponseSent, equalTo(true));
        assertThat("didn't see response received", tracer.sawResponseReceived, equalTo(true));
        assertThat("saw error sent", tracer.sawErrorSent, equalTo(false));

        tracer.reset(4);
        serviceA.sendRequest(nodeB, "testError", new StringMessageRequest(""), noopResponseHandler);
        requestCompleted.acquire();
        tracer.expectedEvents.get().await();
        assertThat("didn't see request sent", tracer.sawRequestSent, equalTo(true));
        assertThat("didn't see request received", tracer.sawRequestReceived, equalTo(true));
        assertThat("saw response sent", tracer.sawResponseSent, equalTo(false));
        assertThat("didn't see response received", tracer.sawResponseReceived, equalTo(true));
        assertThat("didn't see error sent", tracer.sawErrorSent, equalTo(true));

        String includeSettings;
        String excludeSettings;
        if (randomBoolean()) {
            // sometimes leave include empty (default)
            includeSettings = randomBoolean() ? "*" : "";
            excludeSettings = "*Error";
        } else {
            includeSettings = "test";
            excludeSettings = "DOESN'T_MATCH";
        }
        clusterSettings.applySettings(Settings.builder()
            .put(TransportService.TRACE_LOG_INCLUDE_SETTING.getKey(), includeSettings)
            .put(TransportService.TRACE_LOG_EXCLUDE_SETTING.getKey(), excludeSettings)
            .build());

        tracer.reset(4);
        serviceA.sendRequest(nodeB, "test", new StringMessageRequest(""), noopResponseHandler);
        requestCompleted.acquire();
        tracer.expectedEvents.get().await();
        assertThat("didn't see request sent", tracer.sawRequestSent, equalTo(true));
        assertThat("didn't see request received", tracer.sawRequestReceived, equalTo(true));
        assertThat("didn't see response sent", tracer.sawResponseSent, equalTo(true));
        assertThat("didn't see response received", tracer.sawResponseReceived, equalTo(true));
        assertThat("saw error sent", tracer.sawErrorSent, equalTo(false));

        tracer.reset(2);
        serviceA.sendRequest(nodeB, "testError", new StringMessageRequest(""), noopResponseHandler);
        requestCompleted.acquire();
        tracer.expectedEvents.get().await();
        assertThat("saw request sent", tracer.sawRequestSent, equalTo(false));
        assertThat("didn't see request received", tracer.sawRequestReceived, equalTo(true));
        assertThat("saw response sent", tracer.sawResponseSent, equalTo(false));
        assertThat("saw response received", tracer.sawResponseReceived, equalTo(false));
        assertThat("didn't see error sent", tracer.sawErrorSent, equalTo(true));
    }

    private static class Tracer extends MockTransportService.Tracer {
        private final Set<String> actions;
        public volatile boolean sawRequestSent;
        public volatile boolean sawRequestReceived;
        public volatile boolean sawResponseSent;
        public volatile boolean sawErrorSent;
        public volatile boolean sawResponseReceived;

        public AtomicReference<CountDownLatch> expectedEvents = new AtomicReference<>();

        Tracer(Set<String> actions) {
            this.actions = actions;
        }

        @Override
        public void receivedRequest(long requestId, String action) {
            super.receivedRequest(requestId, action);
            if (actions.contains(action)) {
                sawRequestReceived = true;
                expectedEvents.get().countDown();
            }
        }

        @Override
        public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            super.requestSent(node, requestId, action, options);
            if (actions.contains(action)) {
                sawRequestSent = true;
                expectedEvents.get().countDown();
            }
        }

        @Override
        public void responseSent(long requestId, String action) {
            super.responseSent(requestId, action);
            if (actions.contains(action)) {
                sawResponseSent = true;
                expectedEvents.get().countDown();
            }
        }

        @Override
        public void responseSent(long requestId, String action, Throwable t) {
            super.responseSent(requestId, action, t);
            if (actions.contains(action)) {
                sawErrorSent = true;
                expectedEvents.get().countDown();
            }
        }

        @Override
        public void receivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
            super.receivedResponse(requestId, sourceNode, action);
            if (actions.contains(action)) {
                sawResponseReceived = true;
                expectedEvents.get().countDown();
            }
        }

        public void reset(int expectedCount) {
            sawRequestSent = false;
            sawRequestReceived = false;
            sawResponseSent = false;
            sawErrorSent = false;
            sawResponseReceived = false;
            expectedEvents.set(new CountDownLatch(expectedCount));
        }
    }


    public static class StringMessageRequest extends TransportRequest {

        private String message;
        private long timeout;

        StringMessageRequest(String message, long timeout) {
            this.message = message;
            this.timeout = timeout;
        }

        public StringMessageRequest() {
        }

        public StringMessageRequest(String message) {
            this(message, -1);
        }

        public long timeout() {
            return timeout;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            message = in.readString();
            timeout = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(message);
            out.writeLong(timeout);
        }
    }

    static class StringMessageResponse extends TransportResponse {

        private String message;

        StringMessageResponse(String message) {
            this.message = message;
        }

        StringMessageResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            message = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(message);
        }
    }


    public static class Version0Request extends TransportRequest {

        int value1;


        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            value1 = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(value1);
        }
    }

    public static class Version1Request extends Version0Request {

        int value2;

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.getVersion().onOrAfter(version1)) {
                value2 = in.readInt();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(version1)) {
                out.writeInt(value2);
            }
        }
    }

    static class Version0Response extends TransportResponse {

        int value1;

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            value1 = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(value1);
        }
    }

    static class Version1Response extends Version0Response {

        int value2;

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.getVersion().onOrAfter(version1)) {
                value2 = in.readInt();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(version1)) {
                out.writeInt(value2);
            }
        }
    }

    public void testVersionFrom0to1() throws Exception {
        serviceB.registerRequestHandler("/version", Version1Request::new, ThreadPool.Names.SAME,
            new TransportRequestHandler<Version1Request>() {
                @Override
                public void messageReceived(Version1Request request, TransportChannel channel) throws Exception {
                    assertThat(request.value1, equalTo(1));
                    assertThat(request.value2, equalTo(0)); // not set, coming from service A
                    Version1Response response = new Version1Response();
                    response.value1 = 1;
                    response.value2 = 2;
                    channel.sendResponse(response);
                    assertEquals(version0, channel.getVersion());
                }
            });

        Version0Request version0Request = new Version0Request();
        version0Request.value1 = 1;
        Version0Response version0Response = serviceA.submitRequest(nodeB, "/version", version0Request,
            new TransportResponseHandler<Version0Response>() {
                @Override
                public Version0Response newInstance() {
                    return new Version0Response();
                }

                @Override
                public void handleResponse(Version0Response response) {
                    assertThat(response.value1, equalTo(1));
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }).txGet();

        assertThat(version0Response.value1, equalTo(1));
    }

    public void testVersionFrom1to0() throws Exception {
        serviceA.registerRequestHandler("/version", Version0Request::new, ThreadPool.Names.SAME,
            new TransportRequestHandler<Version0Request>() {
                @Override
                public void messageReceived(Version0Request request, TransportChannel channel) throws Exception {
                    assertThat(request.value1, equalTo(1));
                    Version0Response response = new Version0Response();
                    response.value1 = 1;
                    channel.sendResponse(response);
                    assertEquals(version0, channel.getVersion());
                }
            });

        Version1Request version1Request = new Version1Request();
        version1Request.value1 = 1;
        version1Request.value2 = 2;
        Version1Response version1Response = serviceB.submitRequest(nodeA, "/version", version1Request,
            new TransportResponseHandler<Version1Response>() {
                @Override
                public Version1Response newInstance() {
                    return new Version1Response();
                }

                @Override
                public void handleResponse(Version1Response response) {
                    assertThat(response.value1, equalTo(1));
                    assertThat(response.value2, equalTo(0)); // initial values, cause its serialized from version 0
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }).txGet();

        assertThat(version1Response.value1, equalTo(1));
        assertThat(version1Response.value2, equalTo(0));
    }

    public void testVersionFrom1to1() throws Exception {
        serviceB.registerRequestHandler("/version", Version1Request::new, ThreadPool.Names.SAME,
            (request, channel) -> {
                assertThat(request.value1, equalTo(1));
                assertThat(request.value2, equalTo(2));
                Version1Response response = new Version1Response();
                response.value1 = 1;
                response.value2 = 2;
                channel.sendResponse(response);
                assertEquals(version1, channel.getVersion());
            });

        Version1Request version1Request = new Version1Request();
        version1Request.value1 = 1;
        version1Request.value2 = 2;
        Version1Response version1Response = serviceB.submitRequest(nodeB, "/version", version1Request,
            new TransportResponseHandler<Version1Response>() {
                @Override
                public Version1Response newInstance() {
                    return new Version1Response();
                }

                @Override
                public void handleResponse(Version1Response response) {
                    assertThat(response.value1, equalTo(1));
                    assertThat(response.value2, equalTo(2));
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }).txGet();

        assertThat(version1Response.value1, equalTo(1));
        assertThat(version1Response.value2, equalTo(2));
    }

    public void testVersionFrom0to0() throws Exception {
        serviceA.registerRequestHandler("/version", Version0Request::new, ThreadPool.Names.SAME,
            (request, channel) -> {
                assertThat(request.value1, equalTo(1));
                Version0Response response = new Version0Response();
                response.value1 = 1;
                channel.sendResponse(response);
                assertEquals(version0, channel.getVersion());
            });

        Version0Request version0Request = new Version0Request();
        version0Request.value1 = 1;
        Version0Response version0Response = serviceA.submitRequest(nodeA, "/version", version0Request,
            new TransportResponseHandler<Version0Response>() {
                @Override
                public Version0Response newInstance() {
                    return new Version0Response();
                }

                @Override
                public void handleResponse(Version0Response response) {
                    assertThat(response.value1, equalTo(1));
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }).txGet();

        assertThat(version0Response.value1, equalTo(1));
    }

    public void testMockFailToSendNoConnectRule() throws Exception {
        serviceA.registerRequestHandler("sayHello", StringMessageRequest::new, ThreadPool.Names.GENERIC,
            (request, channel) -> {
                assertThat("moshe", equalTo(request.message));
                throw new RuntimeException("bad message !!!");
            });

        serviceB.addFailToSendNoConnectRule(serviceA);

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHello",
            new StringMessageRequest("moshe"), new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse newInstance() {
                    return new StringMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {
                    fail("got response instead of exception");
                }

                @Override
                public void handleException(TransportException exp) {
                    Throwable cause = ExceptionsHelper.unwrapCause(exp);
                    assertThat(cause, instanceOf(ConnectTransportException.class));
                    assertThat(((ConnectTransportException) cause).node(), equalTo(nodeA));
                }
            });

        try {
            res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            assertThat(cause, instanceOf(ConnectTransportException.class));
            assertThat(((ConnectTransportException) cause).node(), equalTo(nodeA));
        }

        // wait for the transport to process the sending failure and disconnect from node
        assertBusy(() -> assertFalse(serviceB.nodeConnected(nodeA)));

        // now try to connect again and see that it fails
        try {
            serviceB.connectToNode(nodeA);
            fail("exception should be thrown");
        } catch (ConnectTransportException e) {
            // all is well
        }

        expectThrows(ConnectTransportException.class, () -> serviceB.openConnection(nodeA, MockTcpTransport.LIGHT_PROFILE));
    }

    public void testMockUnresponsiveRule() throws IOException {
        serviceA.registerRequestHandler("sayHello", StringMessageRequest::new, ThreadPool.Names.GENERIC,
            (request, channel) -> {
                assertThat("moshe", equalTo(request.message));
                throw new RuntimeException("bad message !!!");
            });

        serviceB.addUnresponsiveRule(serviceA);

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHello",
            new StringMessageRequest("moshe"), TransportRequestOptions.builder().withTimeout(100).build(),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse newInstance() {
                    return new StringMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {
                    fail("got response instead of exception");
                }

                @Override
                public void handleException(TransportException exp) {
                    assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
                }
            });

        try {
            res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(ReceiveTimeoutTransportException.class));
        }

        try {
            serviceB.disconnectFromNode(nodeA);
            serviceB.connectToNode(nodeA);
            fail("exception should be thrown");
        } catch (ConnectTransportException e) {
            // all is well
        }

        expectThrows(ConnectTransportException.class, () -> serviceB.openConnection(nodeA, MockTcpTransport.LIGHT_PROFILE));
    }


    public void testHostOnMessages() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicReference<TransportAddress> addressA = new AtomicReference<>();
        final AtomicReference<TransportAddress> addressB = new AtomicReference<>();
        serviceB.registerRequestHandler("action1", TestRequest::new, ThreadPool.Names.SAME, new TransportRequestHandler<TestRequest>() {
            @Override
            public void messageReceived(TestRequest request, TransportChannel channel) throws Exception {
                addressA.set(request.remoteAddress());
                channel.sendResponse(new TestResponse());
                latch.countDown();
            }
        });
        serviceA.sendRequest(nodeB, "action1", new TestRequest(), new TransportResponseHandler<TestResponse>() {
            @Override
            public TestResponse newInstance() {
                return new TestResponse();
            }

            @Override
            public void handleResponse(TestResponse response) {
                addressB.set(response.remoteAddress());
                latch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                latch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });

        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("message round trip did not complete within a sensible time frame");
        }

        assertTrue(nodeA.getAddress().getAddress().equals(addressA.get().getAddress()));
        assertTrue(nodeB.getAddress().getAddress().equals(addressB.get().getAddress()));
    }

    public void testBlockingIncomingRequests() throws Exception {
        try (TransportService service = buildService("TS_TEST", version0, null,
            Settings.EMPTY, false, false)) {
            AtomicBoolean requestProcessed = new AtomicBoolean(false);
            service.registerRequestHandler("action", TestRequest::new, ThreadPool.Names.SAME,
                (request, channel) -> {
                    requestProcessed.set(true);
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                });

            DiscoveryNode node = service.getLocalNode();
            serviceA.close();
            serviceA = buildService("TS_A", version0, null,
                Settings.EMPTY, true, false);
            try (Transport.Connection connection = serviceA.openConnection(node, null)) {
                CountDownLatch latch = new CountDownLatch(1);
                serviceA.sendRequest(connection, "action", new TestRequest(), TransportRequestOptions.EMPTY,
                    new TransportResponseHandler<TestResponse>() {
                        @Override
                        public TestResponse newInstance() {
                            return new TestResponse();
                        }

                        @Override
                        public void handleResponse(TestResponse response) {
                            latch.countDown();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            latch.countDown();
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });

                assertFalse(requestProcessed.get());

                service.acceptIncomingRequests();
                assertBusy(() -> assertTrue(requestProcessed.get()));

                latch.await();
            }
        }
    }

    public static class TestRequest extends TransportRequest {

        String info;
        int resendCount;

        public TestRequest() {
        }

        public TestRequest(String info) {
            this.info = info;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            info = in.readOptionalString();
            resendCount = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(info);
            out.writeInt(resendCount);
        }

        @Override
        public String toString() {
            return "TestRequest{" +
                "info='" + info + '\'' +
                '}';
        }
    }

    private static class TestResponse extends TransportResponse {

        String info;

        TestResponse() {
        }

        TestResponse(String info) {
            this.info = info;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            info = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(info);
        }

        @Override
        public String toString() {
            return "TestResponse{" +
                "info='" + info + '\'' +
                '}';
        }
    }

    public void testSendRandomRequests() throws InterruptedException {
        TransportService serviceC = build(
            Settings.builder()
                .put("name", "TS_TEST")
                .put(TransportService.TRACE_LOG_INCLUDE_SETTING.getKey(), "")
                .put(TransportService.TRACE_LOG_EXCLUDE_SETTING.getKey(), "NOTHING")
                .build(),
            version0,
            null, true);
        DiscoveryNode nodeC = serviceC.getLocalNode();
        serviceC.acceptIncomingRequests();

        final CountDownLatch latch = new CountDownLatch(4);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                fail("disconnect should not be called " + node);
            }
        };
        serviceA.addConnectionListener(waitForConnection);
        serviceB.addConnectionListener(waitForConnection);
        serviceC.addConnectionListener(waitForConnection);

        serviceC.connectToNode(nodeA);
        serviceC.connectToNode(nodeB);
        serviceA.connectToNode(nodeC);
        serviceB.connectToNode(nodeC);

        latch.await();
        serviceA.removeConnectionListener(waitForConnection);
        serviceB.removeConnectionListener(waitForConnection);
        serviceC.removeConnectionListener(waitForConnection);


        Map<TransportService, DiscoveryNode> toNodeMap = new HashMap<>();
        toNodeMap.put(serviceA, nodeA);
        toNodeMap.put(serviceB, nodeB);
        toNodeMap.put(serviceC, nodeC);
        AtomicBoolean fail = new AtomicBoolean(false);
        class TestRequestHandler implements TransportRequestHandler<TestRequest> {

            private final TransportService service;

            TestRequestHandler(TransportService service) {
                this.service = service;
            }

            @Override
            public void messageReceived(TestRequest request, TransportChannel channel) throws Exception {
                if (randomBoolean()) {
                    Thread.sleep(randomIntBetween(10, 50));
                }
                if (fail.get()) {
                    throw new IOException("forced failure");
                }

                if (randomBoolean() && request.resendCount++ < 20) {
                    DiscoveryNode node = randomFrom(nodeA, nodeB, nodeC);
                    logger.debug("send secondary request from {} to {} - {}", toNodeMap.get(service), node, request.info);
                    service.sendRequest(node, "action1", new TestRequest("secondary " + request.info),
                        TransportRequestOptions.builder().withCompress(randomBoolean()).build(),
                        new TransportResponseHandler<TestResponse>() {
                            @Override
                            public TestResponse newInstance() {
                                return new TestResponse();
                            }

                            @Override
                            public void handleResponse(TestResponse response) {
                                try {
                                    if (randomBoolean()) {
                                        Thread.sleep(randomIntBetween(10, 50));
                                    }
                                    logger.debug("send secondary response {}", response.info);

                                    channel.sendResponse(response);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                try {
                                    logger.debug("send secondary exception response for request {}", request.info);
                                    channel.sendResponse(exp);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public String executor() {
                                return randomBoolean() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;
                            }
                        });
                } else {
                    logger.debug("send response for {}", request.info);
                    channel.sendResponse(new TestResponse("Response for: " + request.info));
                }

            }
        }
        serviceB.registerRequestHandler("action1", TestRequest::new, randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            new TestRequestHandler(serviceB));
        serviceC.registerRequestHandler("action1", TestRequest::new, randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            new TestRequestHandler(serviceC));
        serviceA.registerRequestHandler("action1", TestRequest::new, randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            new TestRequestHandler(serviceA));
        int iters = randomIntBetween(30, 60);
        CountDownLatch allRequestsDone = new CountDownLatch(iters);
        class TestResponseHandler implements TransportResponseHandler<TestResponse> {

            private final int id;

            TestResponseHandler(int id) {
                this.id = id;
            }

            @Override
            public TestResponse newInstance() {
                return new TestResponse();
            }

            @Override
            public void handleResponse(TestResponse response) {
                logger.debug("---> received response: {}", response.info);
                allRequestsDone.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("---> received exception for id {}", id), exp);
                allRequestsDone.countDown();
                Throwable unwrap = ExceptionsHelper.unwrap(exp, IOException.class);
                assertNotNull(unwrap);
                assertEquals(IOException.class, unwrap.getClass());
                assertEquals("forced failure", unwrap.getMessage());
            }

            @Override
            public String executor() {
                return randomBoolean() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;
            }
        }

        for (int i = 0; i < iters; i++) {
            TransportService service = randomFrom(serviceC, serviceB, serviceA);
            DiscoveryNode node = randomFrom(nodeC, nodeB, nodeA);
            logger.debug("send from {} to {}", toNodeMap.get(service), node);
            service.sendRequest(node, "action1", new TestRequest("REQ[" + i + "]"),
                TransportRequestOptions.builder().withCompress(randomBoolean()).build(), new TestResponseHandler(i));
        }
        logger.debug("waiting for response");
        fail.set(randomBoolean());
        boolean await = allRequestsDone.await(5, TimeUnit.SECONDS);
        if (await == false) {
            logger.debug("now failing forcefully");
            fail.set(true);
            assertTrue(allRequestsDone.await(5, TimeUnit.SECONDS));
        }
        logger.debug("DONE");
        serviceC.close();
        // when we close C here we have to disconnect the service otherwise assertions mit trip with pending connections in tearDown
        // since the disconnect will then happen concurrently and that might confuse the assertions since we disconnect due to a
        // connection reset by peer or other exceptions depending on the implementation
        serviceB.disconnectFromNode(nodeC);
        serviceA.disconnectFromNode(nodeC);
    }

    public void testRegisterHandlerTwice() {
        serviceB.registerRequestHandler("action1", TestRequest::new, randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            (request, message) -> {
                throw new AssertionError("boom");
            });
        expectThrows(IllegalArgumentException.class, () ->
            serviceB.registerRequestHandler("action1", TestRequest::new, randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
                (request, message) -> {
                    throw new AssertionError("boom");
                })
        );

        serviceA.registerRequestHandler("action1", TestRequest::new, randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            (request, message) -> {
                throw new AssertionError("boom");
            });
    }

    public void testTimeoutPerConnection() throws IOException {
        assumeTrue("Works only on BSD network stacks and apparently windows",
            Constants.MAC_OS_X || Constants.FREE_BSD || Constants.WINDOWS);
        try (ServerSocket socket = new MockServerSocket()) {
            // note - this test uses backlog=1 which is implementation specific ie. it might not work on some TCP/IP stacks
            // on linux (at least newer ones) the listen(addr, backlog=1) should just ignore new connections if the queue is full which
            // means that once we received an ACK from the client we just drop the packet on the floor (which is what we want) and we run
            // into a connection timeout quickly. Yet other implementations can for instance can terminate the connection within the 3 way
            // handshake which I haven't tested yet.
            socket.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0), 1);
            socket.setReuseAddress(true);
            DiscoveryNode first = new DiscoveryNode("TEST", new TransportAddress(socket.getInetAddress(),
                socket.getLocalPort()), emptyMap(),
                emptySet(), version0);
            DiscoveryNode second = new DiscoveryNode("TEST", new TransportAddress(socket.getInetAddress(),
                socket.getLocalPort()), emptyMap(),
                emptySet(), version0);
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE);
            // connection with one connection and a large timeout -- should consume the one spot in the backlog queue
            try (TransportService service = buildService("TS_TPC", Version.CURRENT, null,
                Settings.EMPTY, true, false)) {
                IOUtils.close(service.openConnection(first, builder.build()));
                builder.setConnectTimeout(TimeValue.timeValueMillis(1));
                final ConnectionProfile profile = builder.build();
                // now with the 1ms timeout we got and test that is it's applied
                long startTime = System.nanoTime();
                ConnectTransportException ex = expectThrows(ConnectTransportException.class, () -> service.openConnection(second, profile));
                final long now = System.nanoTime();
                final long timeTaken = TimeValue.nsecToMSec(now - startTime);
                assertTrue("test didn't timeout quick enough, time taken: [" + timeTaken + "]",
                    timeTaken < TimeValue.timeValueSeconds(5).millis());
                assertEquals(ex.getMessage(), "[][" + second.getAddress() + "] connect_timeout[1ms]");
            }
        }
    }

    public void testHandshakeWithIncompatVersion() {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        Version version = Version.fromString("2.0.0");
        try (MockTcpTransport transport = new MockTcpTransport(Settings.EMPTY, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(Collections.emptyList()), version);
             MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, transport, version, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            DiscoveryNode node =
                new DiscoveryNode("TS_TPC", "TS_TPC", transport.boundAddress().publishAddress(), emptyMap(), emptySet(), version0);
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE);
            expectThrows(ConnectTransportException.class, () -> serviceA.openConnection(node, builder.build()));
        }
    }

    public void testHandshakeUpdatesVersion() throws IOException {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        Version version = VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT);
        try (MockTcpTransport transport = new MockTcpTransport(Settings.EMPTY, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(Collections.emptyList()), version);
             MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, transport, version, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            DiscoveryNode node =
                new DiscoveryNode("TS_TPC", "TS_TPC", transport.boundAddress().publishAddress(), emptyMap(), emptySet(),
                    Version.fromString("2.0.0"));
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE);
            try (Transport.Connection connection = serviceA.openConnection(node, builder.build())) {
                assertEquals(connection.getVersion(), version);
            }
        }
    }

    public void testTcpHandshake() throws IOException, InterruptedException {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        TcpTransport originalTransport = (TcpTransport) serviceA.getOriginalTransport();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());

        MockTcpTransport transport = new MockTcpTransport(Settings.EMPTY, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(Collections.emptyList())) {
            @Override
            protected String handleRequest(TcpChannel mockChannel, String profileName, StreamInput stream, long requestId,
                                           int messageLengthBytes, Version version, InetSocketAddress remoteAddress, byte status)
                throws IOException {
                return super.handleRequest(mockChannel, profileName, stream, requestId, messageLengthBytes, version, remoteAddress,
                    (byte) (status & ~(1 << 3))); // we flip the isHandshake bit back and act like the handler is not found
            }
        };

        try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, transport, Version.CURRENT, threadPool,
            null)) {
            service.start();
            service.acceptIncomingRequests();
            // this acts like a node that doesn't have support for handshakes
            DiscoveryNode node =
                new DiscoveryNode("TS_TPC", "TS_TPC", transport.boundAddress().publishAddress(), emptyMap(), emptySet(), version0);
            ConnectTransportException exception = expectThrows(ConnectTransportException.class, () -> serviceA.connectToNode(node));
            assertTrue(exception.getCause() instanceof IllegalStateException);
            assertEquals("handshake failed", exception.getCause().getMessage());
        }

        try (TransportService service = buildService("TS_TPC", Version.CURRENT, null);
             TcpTransport.NodeChannels connection = originalTransport.openConnection(
                 new DiscoveryNode("TS_TPC", "TS_TPC", service.boundAddress().publishAddress(), emptyMap(), emptySet(), version0),
                 null
             )) {
            Version version = originalTransport.executeHandshake(connection.getNode(),
                connection.channel(TransportRequestOptions.Type.PING), TimeValue.timeValueSeconds(10));
            assertEquals(version, Version.CURRENT);
        }
    }

    public void testTcpHandshakeTimeout() throws IOException {
        try (ServerSocket socket = new MockServerSocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0), 1);
            socket.setReuseAddress(true);
            DiscoveryNode dummy = new DiscoveryNode("TEST", new TransportAddress(socket.getInetAddress(),
                socket.getLocalPort()), emptyMap(),
                emptySet(), version0);
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE);
            builder.setHandshakeTimeout(TimeValue.timeValueMillis(1));
            ConnectTransportException ex = expectThrows(ConnectTransportException.class,
                () -> serviceA.connectToNode(dummy, builder.build()));
            assertEquals("[][" + dummy.getAddress() + "] handshake_timeout[1ms]", ex.getMessage());
        }
    }

    public void testTcpHandshakeConnectionReset() throws IOException, InterruptedException {
        try (ServerSocket socket = new MockServerSocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0), 1);
            socket.setReuseAddress(true);
            DiscoveryNode dummy = new DiscoveryNode("TEST", new TransportAddress(socket.getInetAddress(),
                socket.getLocalPort()), emptyMap(),
                emptySet(), version0);
            Thread t = new Thread() {
                @Override
                public void run() {
                    try (Socket accept = socket.accept()) {
                        if (randomBoolean()) { // sometimes wait until the other side sends the message
                            accept.getInputStream().read();
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
            t.start();
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE);
            builder.setHandshakeTimeout(TimeValue.timeValueHours(1));
            ConnectTransportException ex = expectThrows(ConnectTransportException.class,
                () -> serviceA.connectToNode(dummy, builder.build()));
            assertEquals(ex.getMessage(), "[][" + dummy.getAddress() + "] general node connection failure");
            assertThat(ex.getCause().getMessage(), startsWith("handshake failed"));
            t.join();
        }
    }

    public void testResponseHeadersArePreserved() throws InterruptedException {
        List<String> executors = new ArrayList<>(ThreadPool.THREAD_POOL_TYPES.keySet());
        CollectionUtil.timSort(executors); // makes sure it's reproducible
        serviceA.registerRequestHandler("action", TestRequest::new, ThreadPool.Names.SAME,
            (request, channel) -> {

                threadPool.getThreadContext().putTransient("boom", new Object());
                threadPool.getThreadContext().addResponseHeader("foo.bar", "baz");
                if ("fail".equals(request.info)) {
                    throw new RuntimeException("boom");
                } else {
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                }
            });

        CountDownLatch latch = new CountDownLatch(2);

        TransportResponseHandler<TransportResponse> transportResponseHandler = new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse newInstance() {
                return TransportResponse.Empty.INSTANCE;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                try {
                    assertSame(response, TransportResponse.Empty.INSTANCE);
                    assertTrue(threadPool.getThreadContext().getResponseHeaders().containsKey("foo.bar"));
                    assertEquals(1, threadPool.getThreadContext().getResponseHeaders().get("foo.bar").size());
                    assertEquals("baz", threadPool.getThreadContext().getResponseHeaders().get("foo.bar").get(0));
                    assertNull(threadPool.getThreadContext().getTransient("boom"));
                } finally {
                    latch.countDown();
                }

            }

            @Override
            public void handleException(TransportException exp) {
                try {
                    assertTrue(threadPool.getThreadContext().getResponseHeaders().containsKey("foo.bar"));
                    assertEquals(1, threadPool.getThreadContext().getResponseHeaders().get("foo.bar").size());
                    assertEquals("baz", threadPool.getThreadContext().getResponseHeaders().get("foo.bar").get(0));
                    assertNull(threadPool.getThreadContext().getTransient("boom"));
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public String executor() {
                return randomFrom(executors);
            }
        };

        serviceB.sendRequest(nodeA, "action", new TestRequest(randomFrom("fail", "pass")), transportResponseHandler);
        serviceA.sendRequest(nodeA, "action", new TestRequest(randomFrom("fail", "pass")), transportResponseHandler);
        latch.await();
    }

    public void testHandlerIsInvokedOnConnectionClose() throws IOException, InterruptedException {
        List<String> executors = new ArrayList<>(ThreadPool.THREAD_POOL_TYPES.keySet());
        CollectionUtil.timSort(executors); // makes sure it's reproducible
        TransportService serviceC = build(Settings.builder().put("name", "TS_TEST").build(), version0, null, true);
        serviceC.registerRequestHandler("action", TestRequest::new, ThreadPool.Names.SAME,
            (request, channel) -> {
                // do nothing
            });
        serviceC.start();
        serviceC.acceptIncomingRequests();
        CountDownLatch latch = new CountDownLatch(1);
        TransportResponseHandler<TransportResponse> transportResponseHandler = new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse newInstance() {
                return TransportResponse.Empty.INSTANCE;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                try {
                    fail("no response expected");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                try {
                    if (exp instanceof SendRequestTransportException) {
                        assertTrue(exp.getCause().getClass().toString(), exp.getCause() instanceof NodeNotConnectedException);
                    } else {
                        // here the concurrent disconnect was faster and invoked the listener first
                        assertTrue(exp.getClass().toString(), exp instanceof NodeDisconnectedException);
                    }
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public String executor() {
                return randomFrom(executors);
            }
        };
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE);
        try (Transport.Connection connection = serviceB.openConnection(serviceC.getLocalNode(), builder.build())) {
            serviceC.close();
            serviceB.sendRequest(connection, "action", new TestRequest("boom"), TransportRequestOptions.EMPTY,
                transportResponseHandler);
        }
        latch.await();
    }

    public void testConcurrentDisconnectOnNonPublishedConnection() throws IOException, InterruptedException {
        MockTransportService serviceC = build(Settings.builder().put("name", "TS_TEST").build(), version0, null, true);
        CountDownLatch receivedLatch = new CountDownLatch(1);
        CountDownLatch sendResponseLatch = new CountDownLatch(1);
        serviceC.registerRequestHandler("action", TestRequest::new, ThreadPool.Names.SAME,
            (request, channel) -> {
                // don't block on a network thread here
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (IOException e1) {
                            throw new UncheckedIOException(e1);
                        }
                    }

                    @Override
                    protected void doRun() throws Exception {
                        receivedLatch.countDown();
                        sendResponseLatch.await();
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    }
                });
            });
        serviceC.start();
        serviceC.acceptIncomingRequests();
        CountDownLatch responseLatch = new CountDownLatch(1);
        TransportResponseHandler<TransportResponse> transportResponseHandler = new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse newInstance() {
                return TransportResponse.Empty.INSTANCE;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                responseLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };

        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE);

        try (Transport.Connection connection = serviceB.openConnection(serviceC.getLocalNode(), builder.build())) {
            serviceB.sendRequest(connection, "action", new TestRequest("hello world"), TransportRequestOptions.EMPTY,
                transportResponseHandler);
            receivedLatch.await();
            serviceC.close();
            sendResponseLatch.countDown();
            responseLatch.await();
        }
    }

    public void testTransportStats() throws Exception {
        MockTransportService serviceC = build(Settings.builder().put("name", "TS_TEST").build(), version0, null, true);
        CountDownLatch receivedLatch = new CountDownLatch(1);
        CountDownLatch sendResponseLatch = new CountDownLatch(1);
        serviceB.registerRequestHandler("action", TestRequest::new, ThreadPool.Names.SAME,
            (request, channel) -> {
                // don't block on a network thread here
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (IOException e1) {
                            throw new UncheckedIOException(e1);
                        }
                    }

                    @Override
                    protected void doRun() throws Exception {
                        receivedLatch.countDown();
                        sendResponseLatch.await();
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    }
                });
            });
        serviceC.start();
        serviceC.acceptIncomingRequests();
        CountDownLatch responseLatch = new CountDownLatch(1);
        TransportResponseHandler<TransportResponse> transportResponseHandler = new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse newInstance() {
                return TransportResponse.Empty.INSTANCE;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                responseLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };

        TransportStats stats = serviceC.transport.getStats(); // nothing transmitted / read yet
        assertEquals(0, stats.getRxCount());
        assertEquals(0, stats.getTxCount());
        assertEquals(0, stats.getRxSize().getBytes());
        assertEquals(0, stats.getTxSize().getBytes());

        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE);
        try (Transport.Connection connection = serviceC.openConnection(serviceB.getLocalNode(), builder.build())) {
            assertBusy(() -> { // netty for instance invokes this concurrently so we better use assert busy here
                TransportStats transportStats = serviceC.transport.getStats(); // we did a single round-trip to do the initial handshake
                assertEquals(1, transportStats.getRxCount());
                assertEquals(1, transportStats.getTxCount());
                assertEquals(25, transportStats.getRxSize().getBytes());
                assertEquals(45, transportStats.getTxSize().getBytes());
            });
            serviceC.sendRequest(connection, "action", new TestRequest("hello world"), TransportRequestOptions.EMPTY,
                transportResponseHandler);
            receivedLatch.await();
            assertBusy(() -> { // netty for instance invokes this concurrently so we better use assert busy here
                TransportStats transportStats = serviceC.transport.getStats(); // request has ben send
                assertEquals(1, transportStats.getRxCount());
                assertEquals(2, transportStats.getTxCount());
                assertEquals(25, transportStats.getRxSize().getBytes());
                assertEquals(91, transportStats.getTxSize().getBytes());
            });
            sendResponseLatch.countDown();
            responseLatch.await();
            stats = serviceC.transport.getStats(); // response has been received
            assertEquals(2, stats.getRxCount());
            assertEquals(2, stats.getTxCount());
            assertEquals(46, stats.getRxSize().getBytes());
            assertEquals(91, stats.getTxSize().getBytes());
        } finally {
            serviceC.close();
        }
    }

    public void testTransportStatsWithException() throws Exception {
        MockTransportService serviceC = build(Settings.builder().put("name", "TS_TEST").build(), version0, null, true);
        CountDownLatch receivedLatch = new CountDownLatch(1);
        CountDownLatch sendResponseLatch = new CountDownLatch(1);
        Exception ex = new RuntimeException("boom");
        ex.setStackTrace(new StackTraceElement[0]);
        serviceB.registerRequestHandler("action", TestRequest::new, ThreadPool.Names.SAME,
            (request, channel) -> {
                // don't block on a network thread here
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (IOException e1) {
                            throw new UncheckedIOException(e1);
                        }
                    }

                    @Override
                    protected void doRun() throws Exception {
                        receivedLatch.countDown();
                        sendResponseLatch.await();
                        onFailure(ex);
                    }
                });
            });
        serviceC.start();
        serviceC.acceptIncomingRequests();
        CountDownLatch responseLatch = new CountDownLatch(1);
        TransportResponseHandler<TransportResponse> transportResponseHandler = new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse newInstance() {
                return TransportResponse.Empty.INSTANCE;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                responseLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };

        TransportStats stats = serviceC.transport.getStats(); // nothing transmitted / read yet
        assertEquals(0, stats.getRxCount());
        assertEquals(0, stats.getTxCount());
        assertEquals(0, stats.getRxSize().getBytes());
        assertEquals(0, stats.getTxSize().getBytes());

        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE);
        try (Transport.Connection connection = serviceC.openConnection(serviceB.getLocalNode(), builder.build())) {
            assertBusy(() -> { // netty for instance invokes this concurrently so we better use assert busy here
                TransportStats transportStats = serviceC.transport.getStats(); // request has ben send
                assertEquals(1, transportStats.getRxCount());
                assertEquals(1, transportStats.getTxCount());
                assertEquals(25, transportStats.getRxSize().getBytes());
                assertEquals(45, transportStats.getTxSize().getBytes());
            });
            serviceC.sendRequest(connection, "action", new TestRequest("hello world"), TransportRequestOptions.EMPTY,
                transportResponseHandler);
            receivedLatch.await();
            assertBusy(() -> { // netty for instance invokes this concurrently so we better use assert busy here
                TransportStats transportStats = serviceC.transport.getStats(); // request has ben send
                assertEquals(1, transportStats.getRxCount());
                assertEquals(2, transportStats.getTxCount());
                assertEquals(25, transportStats.getRxSize().getBytes());
                assertEquals(91, transportStats.getTxSize().getBytes());
            });
            sendResponseLatch.countDown();
            responseLatch.await();
            stats = serviceC.transport.getStats(); // exception response has been received
            assertEquals(2, stats.getRxCount());
            assertEquals(2, stats.getTxCount());
            int addressLen = serviceB.boundAddress().publishAddress().address().getAddress().getAddress().length;
            // if we are bound to a IPv6 address the response address is serialized with the exception so it will be different depending
            // on the stack. The emphemeral port will always be in the same range
            assertEquals(185 + addressLen, stats.getRxSize().getBytes());
            assertEquals(91, stats.getTxSize().getBytes());
        } finally {
            serviceC.close();
        }
    }

    public void testTransportProfilesWithPortAndHost() {
        boolean doIPV6 = NetworkUtils.SUPPORTS_V6;
        List<String> hosts;
        if (doIPV6) {
            hosts = Arrays.asList("_local:ipv6_", "_local:ipv4_");
        } else {
            hosts = Arrays.asList("_local:ipv4_");
        }
        try (MockTransportService serviceC = build(Settings.builder()
            .put("name", "TS_TEST")
            .put("transport.profiles.default.bind_host", "_local:ipv4_")
            .put("transport.profiles.some_profile.port", "8900-9000")
            .put("transport.profiles.some_profile.bind_host", "_local:ipv4_")
            .put("transport.profiles.some_other_profile.port", "8700-8800")
            .putList("transport.profiles.some_other_profile.bind_host", hosts)
            .putList("transport.profiles.some_other_profile.publish_host", "_local:ipv4_")
            .build(), version0, null, true)) {

            serviceC.start();
            serviceC.acceptIncomingRequests();
            Map<String, BoundTransportAddress> profileBoundAddresses = serviceC.transport.profileBoundAddresses();
            assertTrue(profileBoundAddresses.containsKey("some_profile"));
            assertTrue(profileBoundAddresses.containsKey("some_other_profile"));
            assertTrue(profileBoundAddresses.get("some_profile").publishAddress().getPort() >= 8900);
            assertTrue(profileBoundAddresses.get("some_profile").publishAddress().getPort() < 9000);
            assertTrue(profileBoundAddresses.get("some_other_profile").publishAddress().getPort() >= 8700);
            assertTrue(profileBoundAddresses.get("some_other_profile").publishAddress().getPort() < 8800);
            assertEquals(profileBoundAddresses.get("some_profile").boundAddresses().length, 1);
            if (doIPV6) {
                assertTrue(profileBoundAddresses.get("some_other_profile").boundAddresses().length >= 2);
                int ipv4 = 0;
                int ipv6 = 0;
                for (TransportAddress addr : profileBoundAddresses.get("some_other_profile").boundAddresses()) {
                    if (addr.address().getAddress() instanceof Inet4Address) {
                        ipv4++;
                    } else if (addr.address().getAddress() instanceof Inet6Address) {
                        ipv6++;
                    } else {
                        fail("what kind of address is this: " + addr.address().getAddress());
                    }
                }
                assertTrue("num ipv4 is wrong: " + ipv4, ipv4 >= 1);
                assertTrue("num ipv6 is wrong: " + ipv6, ipv6 >= 1);
            } else {
                assertEquals(profileBoundAddresses.get("some_other_profile").boundAddresses().length, 1);
            }
            assertTrue(profileBoundAddresses.get("some_other_profile").publishAddress().address().getAddress() instanceof Inet4Address);
        }
    }

    public void testProfileSettings() {
        boolean enable = randomBoolean();
        Settings globalSettings = Settings.builder()
            .put("network.tcp.no_delay", enable)
            .put("network.tcp.keep_alive", enable)
            .put("network.tcp.reuse_address", enable)
            .put("network.tcp.send_buffer_size", "43000b")
            .put("network.tcp.receive_buffer_size", "42000b")
            .put("network.publish_host", "the_publish_host")
            .put("network.bind_host", "the_bind_host")
            .build();

        Settings globalSettings2 = Settings.builder()
            .put("network.tcp.no_delay", !enable)
            .put("network.tcp.keep_alive", !enable)
            .put("network.tcp.reuse_address", !enable)
            .put("network.tcp.send_buffer_size", "4b")
            .put("network.tcp.receive_buffer_size", "3b")
            .put("network.publish_host", "another_publish_host")
            .put("network.bind_host", "another_bind_host")
            .build();

        Settings transportSettings = Settings.builder()
            .put("transport.tcp_no_delay", enable)
            .put("transport.tcp.keep_alive", enable)
            .put("transport.tcp.reuse_address", enable)
            .put("transport.tcp.send_buffer_size", "43000b")
            .put("transport.tcp.receive_buffer_size", "42000b")
            .put("transport.publish_host", "the_publish_host")
            .put("transport.tcp.port", "9700-9800")
            .put("transport.bind_host", "the_bind_host")
            .put(globalSettings2)
            .build();

        Settings transportSettings2 = Settings.builder()
            .put("transport.tcp_no_delay", !enable)
            .put("transport.tcp.keep_alive", !enable)
            .put("transport.tcp.reuse_address", !enable)
            .put("transport.tcp.send_buffer_size", "5b")
            .put("transport.tcp.receive_buffer_size", "6b")
            .put("transport.publish_host", "another_publish_host")
            .put("transport.tcp.port", "9702-9802")
            .put("transport.bind_host", "another_bind_host")
            .put(globalSettings2)
            .build();
        Settings defaultProfileSettings = Settings.builder()
            .put("transport.profiles.default.tcp_no_delay", enable)
            .put("transport.profiles.default.tcp_keep_alive", enable)
            .put("transport.profiles.default.reuse_address", enable)
            .put("transport.profiles.default.send_buffer_size", "43000b")
            .put("transport.profiles.default.receive_buffer_size", "42000b")
            .put("transport.profiles.default.port", "9700-9800")
            .put("transport.profiles.default.publish_host", "the_publish_host")
            .put("transport.profiles.default.bind_host", "the_bind_host")
            .put("transport.profiles.default.publish_port", 42)
            .put(randomBoolean() ? transportSettings2 : globalSettings2) // ensure that we have profile precedence
            .build();

        Settings profileSettings = Settings.builder()
            .put("transport.profiles.some_profile.tcp_no_delay", enable)
            .put("transport.profiles.some_profile.tcp_keep_alive", enable)
            .put("transport.profiles.some_profile.reuse_address", enable)
            .put("transport.profiles.some_profile.send_buffer_size", "43000b")
            .put("transport.profiles.some_profile.receive_buffer_size", "42000b")
            .put("transport.profiles.some_profile.port", "9700-9800")
            .put("transport.profiles.some_profile.publish_host", "the_publish_host")
            .put("transport.profiles.some_profile.bind_host", "the_bind_host")
            .put("transport.profiles.some_profile.publish_port", 42)
            .put(randomBoolean() ? transportSettings2 : globalSettings2) // ensure that we have profile precedence
            .put(randomBoolean() ? defaultProfileSettings : Settings.EMPTY)
            .build();

        Settings randomSettings = randomFrom(random(), globalSettings, transportSettings, profileSettings);
        ClusterSettings clusterSettings = new ClusterSettings(randomSettings, ClusterSettings
            .BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.validate(randomSettings, false);
        TcpTransport.ProfileSettings settings = new TcpTransport.ProfileSettings(
            Settings.builder().put(randomSettings).put("transport.profiles.some_profile.port", "9700-9800").build(), // port is required
            "some_profile");

        assertEquals(enable, settings.tcpNoDelay);
        assertEquals(enable, settings.tcpKeepAlive);
        assertEquals(enable, settings.reuseAddress);
        assertEquals(43000, settings.sendBufferSize.getBytes());
        assertEquals(42000, settings.receiveBufferSize.getBytes());
        if (randomSettings == profileSettings) {
            assertEquals(42, settings.publishPort);
        } else {
            assertEquals(-1, settings.publishPort);
        }

        if (randomSettings == globalSettings) { // publish host has no global fallback for the profile since we later resolve it based on
            // the bound address
            assertEquals(Collections.emptyList(), settings.publishHosts);
        } else {
            assertEquals(Collections.singletonList("the_publish_host"), settings.publishHosts);
        }
        assertEquals("9700-9800", settings.portOrRange);
        assertEquals(Collections.singletonList("the_bind_host"), settings.bindHosts);
    }

    public void testProfilesIncludesDefault() {
        Set<TcpTransport.ProfileSettings> profileSettings = TcpTransport.getProfileSettings(Settings.EMPTY);
        assertEquals(1, profileSettings.size());
        assertEquals(TcpTransport.DEFAULT_PROFILE, profileSettings.stream().findAny().get().profileName);

        profileSettings = TcpTransport.getProfileSettings(Settings.builder()
            .put("transport.profiles.test.port", "0")
            .build());
        assertEquals(2, profileSettings.size());
        assertEquals(new HashSet<>(Arrays.asList("default", "test")), profileSettings.stream().map(s -> s.profileName).collect(Collectors
            .toSet()));

        profileSettings = TcpTransport.getProfileSettings(Settings.builder()
            .put("transport.profiles.test.port", "0")
            .put("transport.profiles.default.port", "0")
            .build());
        assertEquals(2, profileSettings.size());
        assertEquals(new HashSet<>(Arrays.asList("default", "test")), profileSettings.stream().map(s -> s.profileName).collect(Collectors
            .toSet()));
    }

    public void testChannelCloseWhileConnecting() throws IOException {
        try (MockTransportService service = build(Settings.builder().put("name", "close").build(), version0, null, true)) {
            service.setExecutorName(ThreadPool.Names.SAME); // make sure stuff is executed in a blocking fashion
            service.addConnectionListener(new TransportConnectionListener() {
                @Override
                public void onConnectionOpened(final Transport.Connection connection) {
                    try {
                        closeConnectionChannel(service.getOriginalTransport(), connection);
                    } catch (final IOException e) {
                        throw new AssertionError(e);
                    }
                }
            });
            final ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(1,
                    TransportRequestOptions.Type.BULK,
                    TransportRequestOptions.Type.PING,
                    TransportRequestOptions.Type.RECOVERY,
                    TransportRequestOptions.Type.REG,
                    TransportRequestOptions.Type.STATE);
            final ConnectTransportException e =
                    expectThrows(ConnectTransportException.class, () -> service.openConnection(nodeA, builder.build()));
            assertThat(e, hasToString(containsString(("a channel closed while connecting"))));
        }
    }

    protected abstract void closeConnectionChannel(Transport transport, Transport.Connection connection) throws IOException;

}
