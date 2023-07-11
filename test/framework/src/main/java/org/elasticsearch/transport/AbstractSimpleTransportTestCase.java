/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
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
import java.net.UnknownHostException;
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
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public abstract class AbstractSimpleTransportTestCase extends ESTestCase {

    private static final TimeValue HUNDRED_MS = TimeValue.timeValueMillis(100L);

    // public copy of package-private setting so that tests in other packages can use it
    public static final Setting<Boolean> IGNORE_DESERIALIZATION_ERRORS_SETTING = TcpTransport.IGNORE_DESERIALIZATION_ERRORS_SETTING;

    protected ThreadPool threadPool;
    // we use always a non-alpha or beta version here otherwise minimumCompatibilityVersion will be different for the two used versions
    protected static final VersionInformation version0 = new VersionInformation(
        Version.fromString(String.valueOf(Version.CURRENT.major) + ".0.0"),
        IndexVersion.MINIMUM_COMPATIBLE,
        IndexVersion.current()
    );
    protected static final TransportVersion transportVersion0 = TransportVersion.current();

    protected volatile DiscoveryNode nodeA;
    protected volatile MockTransportService serviceA;
    protected ClusterSettings clusterSettingsA;

    protected static final VersionInformation version1 = new VersionInformation(
        Version.fromId(version0.nodeVersion().id + 1),
        IndexVersion.MINIMUM_COMPATIBLE,
        IndexVersion.current()
    );
    protected static final TransportVersion transportVersion1 = TransportVersion.fromId(transportVersion0.id() + 1);
    protected volatile DiscoveryNode nodeB;
    protected volatile MockTransportService serviceB;

    protected abstract Transport build(Settings settings, TransportVersion version, ClusterSettings clusterSettings, boolean doHandshake);

    protected int channelsPerNodeConnection() {
        // This is a customized profile for this test case.
        return 6;
    }

    protected Set<Setting<?>> getSupportedSettings() {
        return ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        clusterSettingsA = new ClusterSettings(Settings.EMPTY, getSupportedSettings());
        final Settings.Builder connectionSettingsBuilder = Settings.builder()
            .put(TransportSettings.CONNECTIONS_PER_NODE_RECOVERY.getKey(), 1)
            .put(TransportSettings.CONNECTIONS_PER_NODE_BULK.getKey(), 1)
            .put(TransportSettings.CONNECTIONS_PER_NODE_REG.getKey(), 2)
            .put(TransportSettings.CONNECTIONS_PER_NODE_STATE.getKey(), 1)
            .put(TransportSettings.CONNECTIONS_PER_NODE_PING.getKey(), 1);

        connectionSettingsBuilder.put(TransportSettings.TCP_KEEP_ALIVE.getKey(), randomBoolean());
        if (randomBoolean()) {
            connectionSettingsBuilder.put(TransportSettings.TCP_KEEP_IDLE.getKey(), randomIntBetween(1, 300));
        }
        if (randomBoolean()) {
            connectionSettingsBuilder.put(TransportSettings.TCP_KEEP_INTERVAL.getKey(), randomIntBetween(1, 300));
        }
        if (randomBoolean()) {
            connectionSettingsBuilder.put(TransportSettings.TCP_KEEP_COUNT.getKey(), randomIntBetween(1, 10));
        }

        final Settings connectionSettings = connectionSettingsBuilder.build();

        // this one supports dynamic tracer updates
        serviceA = buildService("TS_A", version0, transportVersion0, clusterSettingsA, connectionSettings);
        nodeA = serviceA.getLocalNode();
        // this one doesn't support dynamic tracer updates
        serviceB = buildService("TS_B", version1, transportVersion1, null, connectionSettings);
        nodeB = serviceB.getLocalNode();
        // wait till all nodes are properly connected and the event has been sent, so tests in this class
        // will not get this callback called on the connections done in this setup
        final CountDownLatch latch = new CountDownLatch(2);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                fail("disconnect should not be called " + node);
            }
        };
        serviceA.addConnectionListener(waitForConnection);
        serviceB.addConnectionListener(waitForConnection);
        int numHandshakes = 1;
        connectToNode(serviceA, nodeB);
        connectToNode(serviceB, nodeA);
        assertNumHandshakes(numHandshakes, serviceA.getOriginalTransport());
        assertNumHandshakes(numHandshakes, serviceB.getOriginalTransport());

        assertThat("failed to wait for all nodes to connect", latch.await(5, TimeUnit.SECONDS), equalTo(true));
        serviceA.removeConnectionListener(waitForConnection);
        serviceB.removeConnectionListener(waitForConnection);
    }

    private MockTransportService buildService(
        String name,
        VersionInformation version,
        TransportVersion transportVersion,
        @Nullable ClusterSettings clusterSettings,
        Settings settings,
        boolean acceptRequests,
        boolean doHandshake,
        TransportInterceptor interceptor
    ) {
        Settings updatedSettings = Settings.builder()
            .put(TransportSettings.PORT.getKey(), getPortRange())
            .put(settings)
            .put(Node.NODE_NAME_SETTING.getKey(), name)
            .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), true) // suppress assertions to test production error-handling
            .build();
        if (clusterSettings == null) {
            clusterSettings = new ClusterSettings(updatedSettings, getSupportedSettings());
        }
        Transport transport = build(updatedSettings, transportVersion, clusterSettings, doHandshake);
        MockTransportService service = MockTransportService.createNewService(
            updatedSettings,
            transport,
            version,
            threadPool,
            clusterSettings,
            Collections.emptySet(),
            interceptor
        );
        service.start();
        if (acceptRequests) {
            service.acceptIncomingRequests();
        }
        return service;
    }

    protected MockTransportService buildService(
        String name,
        VersionInformation version,
        TransportVersion transportVersion,
        @Nullable ClusterSettings clusterSettings,
        Settings settings,
        boolean acceptRequests,
        boolean doHandshake
    ) {
        return buildService(
            name,
            version,
            transportVersion,
            clusterSettings,
            settings,
            acceptRequests,
            doHandshake,
            NOOP_TRANSPORT_INTERCEPTOR
        );
    }

    protected MockTransportService buildService(
        String name,
        VersionInformation version,
        TransportVersion transportVersion,
        Settings settings
    ) {
        return buildService(name, version, transportVersion, null, settings);
    }

    protected MockTransportService buildService(
        String name,
        VersionInformation version,
        TransportVersion transportVersion,
        ClusterSettings clusterSettings,
        Settings settings
    ) {
        return buildService(name, version, transportVersion, clusterSettings, settings, true, true);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            assertNoPendingHandshakes(serviceA.getOriginalTransport());
            assertNoPendingHandshakes(serviceB.getOriginalTransport());
        } finally {
            IOUtils.close(serviceA, serviceB, () -> terminate(threadPool));
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
        serviceA.registerRequestHandler(
            "internal:sayHello",
            ThreadPool.Names.GENERIC,
            StringMessageRequest::new,
            (request, channel, task) -> {
                assertThat("moshe", equalTo(request.message));
                try {
                    channel.sendResponse(new StringMessageResponse("hello " + request.message));
                } catch (IOException e) {
                    logger.error("Unexpected failure", e);
                    fail(e.getMessage());
                }
            }
        );

        Future<StringMessageResponse> res = submitRequest(
            serviceB,
            nodeA,
            "internal:sayHello",
            new StringMessageRequest("moshe"),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse read(StreamInput in) throws IOException {
                    return new StringMessageResponse(in);
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
            }
        );

        try {
            StringMessageResponse message = res.get();
            assertThat("hello moshe", equalTo(message.message));
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }

        res = submitRequest(serviceB, nodeA, "internal:sayHello", new StringMessageRequest("moshe"), new TransportResponseHandler<>() {
            @Override
            public StringMessageResponse read(StreamInput in) throws IOException {
                return new StringMessageResponse(in);
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

        serviceA.registerRequestHandler(
            "internal:ping_pong",
            ThreadPool.Names.GENERIC,
            StringMessageRequest::new,
            (request, channel, task) -> {
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
            }
        );
        final Object context = new Object();
        final String executor = randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet().toArray(new String[0]));
        TransportResponseHandler<StringMessageResponse> responseHandler = new TransportResponseHandler<StringMessageResponse>() {
            @Override
            public StringMessageResponse read(StreamInput in) throws IOException {
                return new StringMessageResponse(in);
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

        Future<StringMessageResponse> res = submitRequest(serviceB, nodeA, "internal:ping_pong", ping, responseHandler);

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
        serviceA.registerRequestHandler(
            "internal:localNode",
            ThreadPool.Names.GENERIC,
            StringMessageRequest::new,
            (request, channel, task) -> {
                try {
                    channel.sendResponse(new StringMessageResponse(request.message));
                } catch (IOException e) {
                    exception.set(e);
                }
            }
        );
        final AtomicReference<String> responseString = new AtomicReference<>();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        serviceA.sendRequest(
            nodeA,
            "internal:localNode",
            new StringMessageRequest("test"),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse read(StreamInput in) throws IOException {
                    return new StringMessageResponse(in);
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
            }
        );
        responseLatch.await();
        assertNull(exception.get());
        assertThat(responseString.get(), equalTo("test"));
    }

    public void testMessageListeners() throws Exception {
        final TransportRequestHandler<TransportRequest.Empty> requestHandler = (request, channel, task) -> {
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
        final String ACTION = "internal:action";
        serviceA.registerRequestHandler(ACTION, ThreadPool.Names.GENERIC, TransportRequest.Empty::new, requestHandler);
        serviceB.registerRequestHandler(ACTION, ThreadPool.Names.GENERIC, TransportRequest.Empty::new, requestHandler);

        class CountingListener implements TransportMessageListener {
            AtomicInteger requestsReceived = new AtomicInteger();
            AtomicInteger requestsSent = new AtomicInteger();
            AtomicInteger responseReceived = new AtomicInteger();
            AtomicInteger responseSent = new AtomicInteger();

            @Override
            public void onRequestReceived(long requestId, String action) {
                if (action.equals(ACTION)) {
                    requestsReceived.incrementAndGet();
                }
            }

            @Override
            public void onResponseSent(long requestId, String action, TransportResponse response) {
                if (action.equals(ACTION)) {
                    responseSent.incrementAndGet();
                }
            }

            @Override
            public void onResponseSent(long requestId, String action, Exception error) {
                if (action.equals(ACTION)) {
                    responseSent.incrementAndGet();
                }
            }

            @Override
            @SuppressWarnings("rawtypes")
            public void onResponseReceived(long requestId, Transport.ResponseContext context) {
                if (context.action().equals(ACTION)) {
                    responseReceived.incrementAndGet();
                }
            }

            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) {
                if (action.equals(ACTION)) {
                    requestsSent.incrementAndGet();
                }
            }
        }

        final CountingListener tracerA = new CountingListener();
        final CountingListener tracerB = new CountingListener();
        serviceA.addMessageListener(tracerA);
        serviceB.addMessageListener(tracerB);

        try {
            submitRequest(serviceA, nodeB, ACTION, TransportRequest.Empty.INSTANCE, EmptyTransportResponseHandler.INSTANCE_SAME).get();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
            assertThat(ExceptionsHelper.unwrapCause(e.getCause()).getMessage(), equalTo("simulated"));
        }

        // use assert busy as callbacks are called on a different thread
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
            submitRequest(serviceB, nodeA, ACTION, TransportRequest.Empty.INSTANCE, EmptyTransportResponseHandler.INSTANCE_SAME).get();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
            assertThat(ExceptionsHelper.unwrapCause(e.getCause()).getMessage(), equalTo("simulated"));
        }

        // use assert busy as callbacks are called on a different thread
        assertBusy(() -> {
            assertThat(tracerA.requestsReceived.get(), equalTo(1));
            assertThat(tracerA.requestsSent.get(), equalTo(1));
            assertThat(tracerA.responseReceived.get(), equalTo(1));
            assertThat(tracerA.responseSent.get(), equalTo(1));
            assertThat(tracerB.requestsReceived.get(), equalTo(1));
            assertThat(tracerB.requestsSent.get(), equalTo(1));
            assertThat(tracerB.responseReceived.get(), equalTo(1));
            assertThat(tracerB.responseSent.get(), equalTo(1));
        });

        // use assert busy as callbacks are called on a different thread
        try {
            submitRequest(serviceA, nodeA, ACTION, TransportRequest.Empty.INSTANCE, EmptyTransportResponseHandler.INSTANCE_SAME).get();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
            assertThat(ExceptionsHelper.unwrapCause(e.getCause()).getMessage(), equalTo("simulated"));
        }

        // use assert busy as callbacks are called on a different thread
        assertBusy(() -> {
            assertThat(tracerA.requestsReceived.get(), equalTo(2));
            assertThat(tracerA.requestsSent.get(), equalTo(2));
            assertThat(tracerA.responseReceived.get(), equalTo(2));
            assertThat(tracerA.responseSent.get(), equalTo(2));
            assertThat(tracerB.requestsReceived.get(), equalTo(1));
            assertThat(tracerB.requestsSent.get(), equalTo(1));
            assertThat(tracerB.responseReceived.get(), equalTo(1));
            assertThat(tracerB.responseSent.get(), equalTo(1));
        });
    }

    public void testVoidMessageCompressed() throws Exception {
        try (MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, Settings.EMPTY)) {
            serviceA.registerRequestHandler(
                "internal:sayHello",
                ThreadPool.Names.GENERIC,
                TransportRequest.Empty::new,
                (request, channel, task) -> {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (IOException e) {
                        logger.error("Unexpected failure", e);
                        fail(e.getMessage());
                    }
                }
            );

            Settings settingsWithCompress = Settings.builder()
                .put(TransportSettings.TRANSPORT_COMPRESS.getKey(), Compression.Enabled.TRUE)
                .put(
                    TransportSettings.TRANSPORT_COMPRESSION_SCHEME.getKey(),
                    randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4)
                )
                .build();
            ConnectionProfile connectionProfile = ConnectionProfile.buildDefaultConnectionProfile(settingsWithCompress);
            connectToNode(serviceC, serviceA.getLocalDiscoNode(), connectionProfile);

            Future<TransportResponse.Empty> res = submitRequest(
                serviceC,
                nodeA,
                "internal:sayHello",
                TransportRequest.Empty.INSTANCE,
                new TransportResponseHandler<>() {
                    @Override
                    public TransportResponse.Empty read(StreamInput in) {
                        return TransportResponse.Empty.INSTANCE;
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(TransportResponse.Empty response) {}

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("Unexpected failure", exp);
                        fail("got exception instead of a response: " + exp.getMessage());
                    }
                }
            );
            assertThat(res.get(), notNullValue());
        }
    }

    public void testHelloWorldCompressed() throws Exception {
        try (MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, Settings.EMPTY)) {
            serviceA.registerRequestHandler(
                "internal:sayHello",
                ThreadPool.Names.GENERIC,
                StringMessageRequest::new,
                (request, channel, task) -> {
                    assertThat("moshe", equalTo(request.message));
                    try {
                        channel.sendResponse(new StringMessageResponse("hello " + request.message));
                    } catch (IOException e) {
                        logger.error("Unexpected failure", e);
                        fail(e.getMessage());
                    }
                }
            );

            Settings settingsWithCompress = Settings.builder()
                .put(TransportSettings.TRANSPORT_COMPRESS.getKey(), Compression.Enabled.TRUE)
                .put(
                    TransportSettings.TRANSPORT_COMPRESSION_SCHEME.getKey(),
                    randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4)
                )
                .build();
            ConnectionProfile connectionProfile = ConnectionProfile.buildDefaultConnectionProfile(settingsWithCompress);
            connectToNode(serviceC, serviceA.getLocalDiscoNode(), connectionProfile);

            Future<StringMessageResponse> res = submitRequest(
                serviceC,
                nodeA,
                "internal:sayHello",
                new StringMessageRequest("moshe"),
                new TransportResponseHandler<>() {
                    @Override
                    public StringMessageResponse read(StreamInput in) throws IOException {
                        return new StringMessageResponse(in);
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
                }
            );

            StringMessageResponse message = res.get();
            assertThat("hello moshe", equalTo(message.message));
        }
    }

    public void testIndexingDataCompression() throws Exception {
        try (MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, Settings.EMPTY)) {
            String component = "cccccccccooooooooooooooommmmmmmmmmmppppppppppprrrrrrrreeeeeeeeeessssssssiiiiiiiiiibbbbbbbbllllllllleeeeee";
            String text = component.repeat(30);
            TransportRequestHandler<StringMessageRequest> handler = (request, channel, task) -> {
                assertThat(text, equalTo(request.message));
                try {
                    channel.sendResponse(new StringMessageResponse(""));
                } catch (IOException e) {
                    logger.error("Unexpected failure", e);
                    fail(e.getMessage());
                }
            };
            serviceA.registerRequestHandler("internal:sayHello", ThreadPool.Names.GENERIC, StringMessageRequest::new, handler);
            serviceC.registerRequestHandler("internal:sayHello", ThreadPool.Names.GENERIC, StringMessageRequest::new, handler);

            Settings settingsWithCompress = Settings.builder()
                .put(TransportSettings.TRANSPORT_COMPRESS.getKey(), Compression.Enabled.INDEXING_DATA)
                .put(
                    TransportSettings.TRANSPORT_COMPRESSION_SCHEME.getKey(),
                    randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4)
                )
                .build();
            ConnectionProfile connectionProfile = ConnectionProfile.buildDefaultConnectionProfile(settingsWithCompress);
            connectToNode(serviceC, serviceA.getLocalDiscoNode(), connectionProfile);
            connectToNode(serviceA, serviceC.getLocalDiscoNode(), connectionProfile);

            TransportResponseHandler<StringMessageResponse> responseHandler = new TransportResponseHandler<>() {
                @Override
                public StringMessageResponse read(StreamInput in) throws IOException {
                    return new StringMessageResponse(in);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public void handleResponse(StringMessageResponse response) {}

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Unexpected failure", exp);
                    fail("got exception instead of a response: " + exp.getMessage());
                }
            };

            Future<StringMessageResponse> compressed = submitRequest(
                serviceC,
                serviceA.getLocalDiscoNode(),
                "internal:sayHello",
                new StringMessageRequest(text, -1, true),
                responseHandler
            );
            Future<StringMessageResponse> uncompressed = submitRequest(
                serviceA,
                serviceC.getLocalDiscoNode(),
                "internal:sayHello",
                new StringMessageRequest(text, -1, false),
                responseHandler
            );

            compressed.get();
            uncompressed.get();
            final long bytesLength;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                new StringMessageRequest(text, -1).writeTo(output);
                bytesLength = output.bytes().length();
            }
            assertThat(serviceA.transport().getStats().getRxSize().getBytes(), lessThan(bytesLength));
            assertThat(serviceC.transport().getStats().getRxSize().getBytes(), greaterThan(bytesLength));
        }
    }

    public void testErrorMessage() throws InterruptedException {
        serviceA.registerRequestHandler(
            "internal:sayHelloException",
            ThreadPool.Names.GENERIC,
            StringMessageRequest::new,
            (request, channel, task) -> {
                assertThat("moshe", equalTo(request.message));
                throw new RuntimeException("bad message !!!");
            }
        );

        Future<StringMessageResponse> res = submitRequest(
            serviceB,
            nodeA,
            "internal:sayHelloException",
            new StringMessageRequest("moshe"),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse read(StreamInput in) throws IOException {
                    return new StringMessageResponse(in);
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
            }
        );

        final ExecutionException e = expectThrows(ExecutionException.class, res::get);
        assertThat(e.getCause().getCause().getMessage(), equalTo("runtime_exception: bad message !!!"));
    }

    public void testDisconnectListener() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        TransportConnectionListener disconnectListener = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                fail("node connected should not be called, all connection have been done previously, node: " + node);
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
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
        serviceA.registerRequestHandler(
            "internal:test",
            randomBoolean() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC,
            TestRequest::new,
            (request, channel, task) -> {
                try {
                    channel.sendResponse(new TestResponse((String) null));
                } catch (Exception e) {
                    logger.info("caught exception while responding", e);
                    responseErrors.add(e);
                }
            }
        );
        final TransportRequestHandler<TestRequest> ignoringRequestHandler = (request, channel, task) -> {
            try {
                channel.sendResponse(new TestResponse((String) null));
            } catch (Exception e) {
                // we don't really care what's going on B, we're testing through A
                logger.trace("caught exception while responding from node B", e);
            }
        };
        serviceB.registerRequestHandler("internal:test", ThreadPool.Names.SAME, TestRequest::new, ignoringRequestHandler);

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
                    safeAwait(go);
                    for (int iter = 0; iter < 10; iter++) {
                        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
                        final String info = sender + "_B_" + iter;
                        serviceB.sendRequest(
                            nodeA,
                            "internal:test",
                            new TestRequest(info),
                            new ActionListenerResponseHandler<>(listener, TestResponse::new)
                        );
                        try {
                            listener.actionGet();
                        } catch (Exception e) {
                            logger.trace(() -> format("caught exception while sending to node %s", nodeA), e);
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
                            serviceA.sendRequest(
                                node,
                                "internal:test",
                                new TestRequest(info),
                                new ActionListenerResponseHandler<>(listener, TestResponse::new)
                            );
                            try {
                                listener.actionGet();
                            } catch (ConnectTransportException e) {
                                // ok!
                            } catch (Exception e) {
                                logger.error(() -> format("caught exception while sending to node %s", node), e);
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
                MockTransportService newService = buildService("TS_B_" + i, version1, transportVersion1, Settings.EMPTY);
                newService.registerRequestHandler("internal:test", ThreadPool.Names.SAME, TestRequest::new, ignoringRequestHandler);
                serviceB = newService;
                nodeB = newService.getLocalDiscoNode();
                connectToNode(serviceB, nodeA);
                connectToNode(serviceA, nodeB);
            } else if (serviceA.nodeConnected(nodeB)) {
                serviceA.disconnectFromNode(nodeB);
            } else {
                connectToNode(serviceA, nodeB);
            }
        }

        done.await();

        assertThat("found non connection errors while sending", sendingErrors, empty());
        assertThat("found non connection errors while responding", responseErrors, empty());
    }

    public void testNotifyOnShutdown() throws Exception {
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latch3 = new CountDownLatch(1);
        try {
            serviceA.registerRequestHandler(
                "internal:foobar",
                ThreadPool.Names.GENERIC,
                StringMessageRequest::new,
                (request, channel, task) -> {
                    try {
                        latch2.await();
                        logger.info("Stop ServiceB now");
                        serviceB.stop();
                    } catch (Exception e) {
                        fail(e.getMessage());
                    } finally {
                        latch3.countDown();
                    }
                }
            );
            Future<TransportResponse.Empty> foobar = submitRequest(
                serviceB,
                nodeA,
                "internal:foobar",
                new StringMessageRequest(""),
                EmptyTransportResponseHandler.INSTANCE_SAME
            );
            latch2.countDown();
            assertThat(expectThrows(ExecutionException.class, foobar::get).getCause(), instanceOf(TransportException.class));
            latch3.await();
        } finally {
            serviceB.close(); // make sure we are fully closed here otherwise we might run into assertions down the road
            serviceA.disconnectFromNode(nodeB);
        }
    }

    public void testTimeoutSendExceptionWithNeverSendingBackResponse() throws Exception {
        serviceA.registerRequestHandler(
            "internal:sayHelloTimeoutNoResponse",
            ThreadPool.Names.GENERIC,
            StringMessageRequest::new,
            (request, channel, task) -> assertThat("moshe", equalTo(request.message))
        ); // don't send back a response

        Future<StringMessageResponse> res = submitRequest(
            serviceB,
            nodeA,
            "internal:sayHelloTimeoutNoResponse",
            new StringMessageRequest("moshe"),
            TransportRequestOptions.timeout(HUNDRED_MS),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse read(StreamInput in) throws IOException {
                    return new StringMessageResponse(in);
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
                    assertThat(exp.getStackTrace().length, equalTo(0));
                }
            }
        );

        final ExecutionException e = expectThrows(ExecutionException.class, res::get);
        assertThat(e.getCause(), instanceOf(ReceiveTimeoutTransportException.class));
    }

    public void testTimeoutSendExceptionWithDelayedResponse() throws Exception {
        CountDownLatch waitForever = new CountDownLatch(1);
        CountDownLatch doneWaitingForever = new CountDownLatch(1);
        Semaphore inFlight = new Semaphore(Integer.MAX_VALUE);
        serviceA.registerRequestHandler(
            "internal:sayHelloTimeoutDelayedResponse",
            ThreadPool.Names.GENERIC,
            StringMessageRequest::new,
            (request, channel, task) -> {
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
        );
        final CountDownLatch latch = new CountDownLatch(1);
        Future<StringMessageResponse> res = submitRequest(
            serviceB,
            nodeA,
            "internal:sayHelloTimeoutDelayedResponse",
            new StringMessageRequest("forever"),
            TransportRequestOptions.timeout(HUNDRED_MS),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse read(StreamInput in) throws IOException {
                    return new StringMessageResponse(in);
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
                    assertThat(exp.getStackTrace().length, equalTo(0));
                }
            }
        );

        assertThat(expectThrows(ExecutionException.class, res::get).getCause(), instanceOf(ReceiveTimeoutTransportException.class));
        latch.await();

        List<Runnable> assertions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int counter = i;
            // now, try and send another request, this times, with a short timeout
            Future<StringMessageResponse> result = submitRequest(
                serviceB,
                nodeA,
                "internal:sayHelloTimeoutDelayedResponse",
                new StringMessageRequest(counter + "ms"),
                TransportRequestOptions.timeout(TimeValue.timeValueSeconds(3)),
                new TransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse read(StreamInput in) throws IOException {
                        return new StringMessageResponse(in);
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
                }
            );

            assertions.add(() -> {
                try {
                    assertThat(result.get().message, equalTo("hello " + counter + "ms"));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
        for (Runnable runnable : assertions) {
            runnable.run();
        }
        waitForever.countDown();
        doneWaitingForever.await();
        assertTrue(inFlight.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));
    }

    @TestLogging(
        value = "org.elasticsearch.transport.TransportService.tracer:trace",
        reason = "to ensure we log network events on TRACE level"
    )
    public void testTracerLog() throws Exception {
        TransportRequestHandler<TransportRequest> handler = (request, channel, task) -> channel.sendResponse(new StringMessageResponse(""));
        TransportRequestHandler<StringMessageRequest> handlerWithError = (request, channel, task) -> {
            if (request.timeout() > 0) {
                Thread.sleep(request.timeout);
            }
            channel.sendResponse(new RuntimeException(""));

        };

        TransportResponseHandler<StringMessageResponse> noopResponseHandler = new TransportResponseHandler<StringMessageResponse>() {

            @Override
            public StringMessageResponse read(StreamInput in) throws IOException {
                return new StringMessageResponse(in);
            }

            @Override
            public void handleResponse(StringMessageResponse response) {}

            @Override
            public void handleException(TransportException exp) {}
        };

        serviceA.registerRequestHandler("internal:test", ThreadPool.Names.SAME, StringMessageRequest::new, handler);
        serviceA.registerRequestHandler("internal:testNotSeen", ThreadPool.Names.SAME, StringMessageRequest::new, handler);
        serviceA.registerRequestHandler("internal:testError", ThreadPool.Names.SAME, StringMessageRequest::new, handlerWithError);
        serviceB.registerRequestHandler("internal:test", ThreadPool.Names.SAME, StringMessageRequest::new, handler);
        serviceB.registerRequestHandler("internal:testNotSeen", ThreadPool.Names.SAME, StringMessageRequest::new, handler);
        serviceB.registerRequestHandler("internal:testError", ThreadPool.Names.SAME, StringMessageRequest::new, handlerWithError);

        String includeSettings;
        String excludeSettings;
        if (randomBoolean()) {
            // sometimes leave include empty (default)
            includeSettings = randomBoolean() ? "*" : "";
            excludeSettings = "internal:testNotSeen";
        } else {
            includeSettings = "internal:test,internal:testError";
            excludeSettings = "DOESN'T_MATCH";
        }
        clusterSettingsA.applySettings(
            Settings.builder()
                .put(TransportSettings.TRACE_LOG_INCLUDE_SETTING.getKey(), includeSettings)
                .put(TransportSettings.TRACE_LOG_EXCLUDE_SETTING.getKey(), excludeSettings)
                .build()
        );

        MockLogAppender appender = new MockLogAppender();
        try {
            appender.start();
            Loggers.addAppender(LogManager.getLogger("org.elasticsearch.transport.TransportService.tracer"), appender);

            ////////////////////////////////////////////////////////////////////////
            // tests for included action type "internal:test"
            //

            // serviceA logs the request was sent
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "sent request",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:test].*sent to.*\\{TS_B}.*"
                )
            );
            // serviceB logs the request was received
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "received request",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:test].*received request.*"
                )
            );
            // serviceB logs the response was sent
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "sent response",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:test].*sent response.*"
                )
            );
            // serviceA logs the response was received
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "received response",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:test].*received response from.*\\{TS_B}.*"
                )
            );

            serviceA.sendRequest(nodeB, "internal:test", new StringMessageRequest("", 10), noopResponseHandler);

            assertBusy(appender::assertAllExpectationsMatched);

            ////////////////////////////////////////////////////////////////////////
            // tests for included action type "internal:testError" which returns an error
            //
            // NB we check again for the logging that request was sent and received because we have to wait for them before shutting the
            // appender down. The logging happens after messages are sent so might happen out of order.

            // serviceA logs the request was sent
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "sent request",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:testError].*sent to.*\\{TS_B}.*"
                )
            );
            // serviceB logs the request was received
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "received request",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:testError].*received request.*"
                )
            );
            // serviceB logs the error response was sent
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "sent error response",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:testError].*sent error response.*"
                )
            );
            // serviceA logs the error response was sent
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "received error response",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:testError].*received response from.*\\{TS_B}.*"
                )
            );

            serviceA.sendRequest(nodeB, "internal:testError", new StringMessageRequest(""), noopResponseHandler);

            assertBusy(appender::assertAllExpectationsMatched);

            ////////////////////////////////////////////////////////////////////////
            // tests for excluded action type "internal:testNotSeen"
            //
            // NB We have to assert the messages logged by serviceB because we have to wait for them before shutting the appender down.
            // The logging happens after messages are sent so might happen after the response future is completed.

            // serviceA does not log that it sent the message
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "not seen request sent",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    "*[internal:testNotSeen]*sent to*"
                )
            );
            // serviceB does log that it received the request
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "not seen request received",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:testNotSeen].*received request.*"
                )
            );
            // serviceB does log that it sent the response
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "not seen request received",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    ".*\\[internal:testNotSeen].*sent response.*"
                )
            );
            // serviceA does not log that it received the response
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "not seen request sent",
                    "org.elasticsearch.transport.TransportService.tracer",
                    Level.TRACE,
                    "*[internal:testNotSeen]*received response from*"
                )
            );

            submitRequest(serviceA, nodeB, "internal:testNotSeen", new StringMessageRequest(""), noopResponseHandler).get();

            assertBusy(appender::assertAllExpectationsMatched);
        } finally {
            Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.transport.TransportService.tracer"), appender);
            appender.stop();
        }
    }

    public static class StringMessageRequest extends TransportRequest implements RawIndexingDataTransportRequest {

        private String message;
        private long timeout;
        private boolean isRawIndexingData = false;

        StringMessageRequest(String message, long timeout) {
            this(message, timeout, false);
        }

        StringMessageRequest(String message, long timeout, boolean isRawIndexingData) {
            this.message = message;
            this.timeout = timeout;
            this.isRawIndexingData = isRawIndexingData;
        }

        public StringMessageRequest(StreamInput in) throws IOException {
            super(in);
            message = in.readString();
            timeout = in.readLong();
        }

        public StringMessageRequest(String message) {
            this(message, -1);
        }

        public long timeout() {
            return timeout;
        }

        @Override
        public boolean isRawIndexingData() {
            return isRawIndexingData;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(message);
            out.writeLong(timeout);
        }
    }

    static class StringMessageResponse extends TransportResponse {

        private final String message;

        StringMessageResponse(String message) {
            this.message = message;
        }

        StringMessageResponse(StreamInput in) throws IOException {
            this.message = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(message);
        }
    }

    public static class Version0Request extends TransportRequest {

        int value1;

        Version0Request() {}

        Version0Request(StreamInput in) throws IOException {
            super(in);
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

        Version1Request() {}

        Version1Request(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(transportVersion1)) {
                value2 = in.readInt();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(transportVersion1)) {
                out.writeInt(value2);
            }
        }
    }

    static class Version0Response extends TransportResponse {

        final int value1;

        Version0Response(int value1) {
            this.value1 = value1;
        }

        Version0Response(StreamInput in) throws IOException {
            this.value1 = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(value1);
        }
    }

    static class Version1Response extends Version0Response {

        final int value2;

        Version1Response(int value1, int value2) {
            super(value1);
            this.value2 = value2;
        }

        Version1Response(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(transportVersion1)) {
                value2 = in.readInt();
            } else {
                value2 = 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(transportVersion1)) {
                out.writeInt(value2);
            }
        }
    }

    public void testVersionFrom0to1() throws Exception {
        serviceB.registerRequestHandler("internal:version", ThreadPool.Names.SAME, Version1Request::new, (request, channel, task) -> {
            assertThat(request.value1, equalTo(1));
            assertThat(request.value2, equalTo(0)); // not set, coming from service A
            Version1Response response = new Version1Response(1, 2);
            channel.sendResponse(response);
            assertEquals(transportVersion0, channel.getVersion());
        });

        Version0Request version0Request = new Version0Request();
        version0Request.value1 = 1;
        Version0Response version0Response = submitRequest(
            serviceA,
            nodeB,
            "internal:version",
            version0Request,
            new TransportResponseHandler<Version0Response>() {
                @Override
                public Version0Response read(StreamInput in) throws IOException {
                    return new Version0Response(in);
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
            }
        ).get();

        assertThat(version0Response.value1, equalTo(1));
    }

    public void testVersionFrom1to0() throws Exception {
        serviceA.registerRequestHandler("internal:version", ThreadPool.Names.SAME, Version0Request::new, (request, channel, task) -> {
            assertThat(request.value1, equalTo(1));
            Version0Response response = new Version0Response(1);
            channel.sendResponse(response);
            assertEquals(transportVersion0, channel.getVersion());
        });

        Version1Request version1Request = new Version1Request();
        version1Request.value1 = 1;
        version1Request.value2 = 2;
        Version1Response version1Response = submitRequest(
            serviceB,
            nodeA,
            "internal:version",
            version1Request,
            new TransportResponseHandler<Version1Response>() {
                @Override
                public Version1Response read(StreamInput in) throws IOException {
                    return new Version1Response(in);
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
            }
        ).get();

        assertThat(version1Response.value1, equalTo(1));
        assertThat(version1Response.value2, equalTo(0));
    }

    public void testVersionFrom1to1() throws Exception {
        serviceB.registerRequestHandler("internal:version", ThreadPool.Names.SAME, Version1Request::new, (request, channel, task) -> {
            assertThat(request.value1, equalTo(1));
            assertThat(request.value2, equalTo(2));
            Version1Response response = new Version1Response(1, 2);
            channel.sendResponse(response);
            // channel versions don't make sense on DirectResponseChannel
            assertThat(channel, instanceOf(TaskTransportChannel.class));
            assertThat(((TaskTransportChannel) channel).getChannel(), instanceOf(TransportService.DirectResponseChannel.class));
        });

        Version1Request version1Request = new Version1Request();
        version1Request.value1 = 1;
        version1Request.value2 = 2;
        Version1Response version1Response = submitRequest(
            serviceB,
            nodeB,
            "internal:version",
            version1Request,
            new TransportResponseHandler<Version1Response>() {
                @Override
                public Version1Response read(StreamInput in) throws IOException {
                    return new Version1Response(in);
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
            }
        ).get();

        assertThat(version1Response.value1, equalTo(1));
        assertThat(version1Response.value2, equalTo(2));
    }

    public void testVersionFrom0to0() throws Exception {
        serviceA.registerRequestHandler("internal:version", ThreadPool.Names.SAME, Version0Request::new, (request, channel, task) -> {
            assertThat(request.value1, equalTo(1));
            Version0Response response = new Version0Response(1);
            channel.sendResponse(response);
            // channel versions don't make sense on DirectResponseChannel
            assertThat(channel, instanceOf(TaskTransportChannel.class));
            assertThat(((TaskTransportChannel) channel).getChannel(), instanceOf(TransportService.DirectResponseChannel.class));
        });

        Version0Request version0Request = new Version0Request();
        version0Request.value1 = 1;
        Version0Response version0Response = submitRequest(
            serviceA,
            nodeA,
            "internal:version",
            version0Request,
            new TransportResponseHandler<Version0Response>() {
                @Override
                public Version0Response read(StreamInput in) throws IOException {
                    return new Version0Response(in);
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
            }
        ).get();

        assertThat(version0Response.value1, equalTo(1));
    }

    public void testMockFailToSendNoConnectRule() throws Exception {
        serviceA.registerRequestHandler(
            "internal:sayHello",
            ThreadPool.Names.GENERIC,
            StringMessageRequest::new,
            (request, channel, task) -> {
                assertThat("moshe", equalTo(request.message));
                throw new RuntimeException("bad message !!!");
            }
        );

        serviceB.addFailToSendNoConnectRule(serviceA);

        Future<StringMessageResponse> res = submitRequest(
            serviceB,
            nodeA,
            "internal:sayHello",
            new StringMessageRequest("moshe"),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse read(StreamInput in) throws IOException {
                    return new StringMessageResponse(in);
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
                    assertThat(cause.getMessage(), allOf(containsString(nodeA.getName()), containsString(nodeA.getAddress().toString())));
                }
            }
        );

        final ExecutionException e = expectThrows(ExecutionException.class, res::get);
        Throwable cause = ExceptionsHelper.unwrapCause(e.getCause());
        assertThat(cause, instanceOf(ConnectTransportException.class));
        assertThat(cause.getMessage(), allOf(containsString(nodeA.getName()), containsString(nodeA.getAddress().toString())));

        // wait for the transport to process the sending failure and disconnect from node
        assertBusy(() -> assertFalse(serviceB.nodeConnected(nodeA)));

        // now try to connect again and see that it fails
        expectThrows(ConnectTransportException.class, () -> connectToNode(serviceB, nodeA));
        expectThrows(ConnectTransportException.class, () -> openConnection(serviceB, nodeA, TestProfiles.LIGHT_PROFILE));
    }

    public void testMockUnresponsiveRule() throws InterruptedException {
        serviceA.registerRequestHandler(
            "internal:sayHello",
            ThreadPool.Names.GENERIC,
            StringMessageRequest::new,
            (request, channel, task) -> {
                assertThat("moshe", equalTo(request.message));
                throw new RuntimeException("bad message !!!");
            }
        );

        serviceB.addUnresponsiveRule(serviceA);

        Future<StringMessageResponse> res = submitRequest(
            serviceB,
            nodeA,
            "internal:sayHello",
            new StringMessageRequest("moshe"),
            TransportRequestOptions.timeout(HUNDRED_MS),
            new TransportResponseHandler<StringMessageResponse>() {
                @Override
                public StringMessageResponse read(StreamInput in) throws IOException {
                    return new StringMessageResponse(in);
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
                    assertThat(exp.getStackTrace().length, equalTo(0));
                }
            }
        );

        assertThat(expectThrows(ExecutionException.class, res::get).getCause(), instanceOf(ReceiveTimeoutTransportException.class));
        expectThrows(ConnectTransportException.class, () -> {
            serviceB.disconnectFromNode(nodeA);
            connectToNode(serviceB, nodeA);
        });
        expectThrows(ConnectTransportException.class, () -> openConnection(serviceB, nodeA, TestProfiles.LIGHT_PROFILE));
    }

    public void testHostOnMessages() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicReference<InetSocketAddress> addressA = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> addressB = new AtomicReference<>();
        serviceB.registerRequestHandler("internal:action1", ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
            addressA.set(request.remoteAddress());
            channel.sendResponse(new TestResponse((String) null));
            latch.countDown();
        });
        serviceA.sendRequest(nodeB, "internal:action1", new TestRequest(), new TransportResponseHandler<TestResponse>() {
            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
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
        });

        if (latch.await(10, TimeUnit.SECONDS) == false) {
            fail("message round trip did not complete within a sensible time frame");
        }

        // nodeA opened the connection so the request originates from an ephemeral port, but from the right interface at least
        assertEquals(nodeA.getAddress().address().getAddress(), addressA.get().getAddress());

        // the response originates from the expected transport port
        assertEquals(nodeB.getAddress().address(), addressB.get());
    }

    public void testRejectEarlyIncomingRequests() throws Exception {
        try (TransportService service = buildService("TS_TEST", version0, transportVersion0, null, Settings.EMPTY, false, false)) {
            AtomicBoolean requestProcessed = new AtomicBoolean(false);
            service.registerRequestHandler("internal:action", ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
                requestProcessed.set(true);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });

            DiscoveryNode node = service.getLocalNode();
            serviceA.close();
            serviceA = buildService("TS_A", version0, transportVersion0, null, Settings.EMPTY, true, false);
            try (Transport.Connection connection = openConnection(serviceA, node, null)) {
                CountDownLatch latch = new CountDownLatch(1);
                serviceA.sendRequest(
                    connection,
                    "internal:action",
                    new TestRequest(),
                    TransportRequestOptions.EMPTY,
                    new TransportResponseHandler<TestResponse>() {
                        @Override
                        public TestResponse read(StreamInput in) throws IOException {
                            return new TestResponse(in);
                        }

                        @Override
                        public void handleResponse(TestResponse response) {
                            latch.countDown();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            latch.countDown();
                        }
                    }
                );

                latch.await();
                assertFalse(requestProcessed.get());
            }

            service.acceptIncomingRequests();
            try (Transport.Connection connection = openConnection(serviceA, node, null)) {
                CountDownLatch latch2 = new CountDownLatch(1);
                serviceA.sendRequest(
                    connection,
                    "internal:action",
                    new TestRequest(),
                    TransportRequestOptions.EMPTY,
                    new TransportResponseHandler<TestResponse>() {
                        @Override
                        public TestResponse read(StreamInput in) throws IOException {
                            return new TestResponse(in);
                        }

                        @Override
                        public void handleResponse(TestResponse response) {
                            latch2.countDown();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            latch2.countDown();
                        }
                    }
                );

                latch2.await();
                assertBusy(() -> assertTrue(requestProcessed.get()));
            }
        }
    }

    public static class TestRequest extends TransportRequest {

        String info;
        int resendCount;

        public TestRequest() {}

        public TestRequest(StreamInput in) throws IOException {
            super(in);
            info = in.readOptionalString();
            resendCount = in.readInt();
        }

        public TestRequest(String info) {
            this.info = info;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(info);
            out.writeInt(resendCount);
        }

        @Override
        public String toString() {
            return "TestRequest{" + "info='" + info + '\'' + '}';
        }
    }

    private static class TestResponse extends TransportResponse {

        final String info;

        TestResponse(StreamInput in) throws IOException {
            super(in);
            this.info = in.readOptionalString();
        }

        TestResponse(String info) {
            this.info = info;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(info);
        }

        @Override
        public String toString() {
            return "TestResponse{" + "info='" + info + '\'' + '}';
        }
    }

    public void testSendRandomRequests() throws InterruptedException {
        TransportService serviceC = buildService("TS_C", version0, transportVersion0, Settings.EMPTY);
        DiscoveryNode nodeC = serviceC.getLocalNode();

        final CountDownLatch latch = new CountDownLatch(4);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                fail("disconnect should not be called " + node);
            }
        };
        serviceA.addConnectionListener(waitForConnection);
        serviceB.addConnectionListener(waitForConnection);
        serviceC.addConnectionListener(waitForConnection);

        connectToNode(serviceC, nodeA);
        connectToNode(serviceC, nodeB);
        connectToNode(serviceA, nodeC);
        connectToNode(serviceB, nodeC);

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
            public void messageReceived(TestRequest request, TransportChannel channel, Task task) throws Exception {
                if (randomBoolean()) {
                    Thread.sleep(randomIntBetween(10, 50));
                }
                if (fail.get()) {
                    throw new IOException("forced failure");
                }

                if (randomBoolean() && request.resendCount++ < 20) {
                    DiscoveryNode node = randomFrom(nodeA, nodeB, nodeC);
                    logger.debug("send secondary request from {} to {} - {}", toNodeMap.get(service), node, request.info);
                    service.sendRequest(
                        node,
                        "internal:action1",
                        new TestRequest("secondary " + request.info),
                        TransportRequestOptions.EMPTY,
                        new TransportResponseHandler<TestResponse>() {

                            private final String executor = randomBoolean() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;

                            @Override
                            public TestResponse read(StreamInput in) throws IOException {
                                return new TestResponse(in);
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
                                return executor;
                            }
                        }
                    );
                } else {
                    logger.debug("send response for {}", request.info);
                    channel.sendResponse(new TestResponse("Response for: " + request.info));
                }

            }
        }
        serviceB.registerRequestHandler(
            "internal:action1",
            randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            TestRequest::new,
            new TestRequestHandler(serviceB)
        );
        serviceC.registerRequestHandler(
            "internal:action1",
            randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            TestRequest::new,
            new TestRequestHandler(serviceC)
        );
        serviceA.registerRequestHandler(
            "internal:action1",
            randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            TestRequest::new,
            new TestRequestHandler(serviceA)
        );
        int iters = randomIntBetween(30, 60);
        CountDownLatch allRequestsDone = new CountDownLatch(iters);
        class TestResponseHandler implements TransportResponseHandler<TestResponse> {

            private final int id;
            private final String executor = randomBoolean() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;

            TestResponseHandler(int id) {
                this.id = id;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }

            @Override
            public void handleResponse(TestResponse response) {
                logger.debug("---> received response: {}", response.info);
                allRequestsDone.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug((Supplier<?>) () -> "---> received exception for id " + id, exp);
                allRequestsDone.countDown();
                Throwable unwrap = ExceptionsHelper.unwrap(exp, IOException.class);
                assertNotNull(unwrap);
                assertEquals(IOException.class, unwrap.getClass());
                assertEquals("forced failure", unwrap.getMessage());
            }

            @Override
            public String executor() {
                return executor;
            }
        }

        for (int i = 0; i < iters; i++) {
            TransportService service = randomFrom(serviceC, serviceB, serviceA);
            DiscoveryNode node = randomFrom(nodeC, nodeB, nodeA);
            logger.debug("send from {} to {}", toNodeMap.get(service), node);
            service.sendRequest(
                node,
                "internal:action1",
                new TestRequest("REQ[" + i + "]"),
                TransportRequestOptions.EMPTY,
                new TestResponseHandler(i)
            );
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
        serviceB.registerRequestHandler(
            "internal:action1",
            randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            TestRequest::new,
            (request, message, task) -> {
                throw new AssertionError("boom");
            }
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> serviceB.registerRequestHandler(
                "internal:action1",
                randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
                TestRequest::new,
                (request, message, task) -> {
                    throw new AssertionError("boom");
                }
            )
        );

        serviceA.registerRequestHandler(
            "internal:action1",
            randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC),
            TestRequest::new,
            (request, message, task) -> {
                throw new AssertionError("boom");
            }
        );
    }

    public void testHandshakeWithIncompatVersion() {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        TransportVersion transportVersion = TransportVersion.fromId(TransportVersion.MINIMUM_COMPATIBLE.id() - 1);
        try (
            MockTransportService service = buildService(
                "TS_C",
                new VersionInformation(
                    Version.CURRENT.minimumCompatibilityVersion(),
                    IndexVersion.MINIMUM_COMPATIBLE,
                    IndexVersion.current()
                ),
                transportVersion,
                Settings.EMPTY
            )
        ) {
            TransportAddress address = service.boundAddress().publishAddress();
            DiscoveryNode node = new DiscoveryNode("TS_TPC", "TS_TPC", address, emptyMap(), emptySet(), version0);
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            expectThrows(ConnectTransportException.class, () -> openConnection(serviceA, node, builder.build()));
        }
    }

    public void testHandshakeUpdatesVersion() throws IOException {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.MINIMUM_COMPATIBLE,
            TransportVersion.current()
        );
        try (
            MockTransportService service = buildService(
                "TS_C",
                new VersionInformation(
                    Version.CURRENT.minimumCompatibilityVersion(),
                    IndexVersion.MINIMUM_COMPATIBLE,
                    IndexVersion.current()
                ),
                transportVersion,
                Settings.EMPTY
            )
        ) {
            TransportAddress address = service.boundAddress().publishAddress();
            DiscoveryNode node = new DiscoveryNode(
                "TS_TPC",
                "TS_TPC",
                address,
                emptyMap(),
                emptySet(),
                VersionInformation.inferVersions(Version.fromString("2.0.0"))
            );
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            try (Transport.Connection connection = openConnection(serviceA, node, builder.build())) {
                assertEquals(transportVersion, connection.getTransportVersion());
            }
        }
    }

    public void testKeepAlivePings() throws Exception {
        assumeTrue("only tcp transport has keep alive pings", serviceA.getOriginalTransport() instanceof TcpTransport);
        TcpTransport originalTransport = (TcpTransport) serviceA.getOriginalTransport();

        ConnectionProfile defaultProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        ConnectionProfile connectionProfile = new ConnectionProfile.Builder(defaultProfile).setPingInterval(TimeValue.timeValueMillis(50))
            .build();
        try (TransportService service = buildService("TS_TPC", VersionInformation.CURRENT, TransportVersion.current(), Settings.EMPTY)) {
            PlainActionFuture<Transport.Connection> future = PlainActionFuture.newFuture();
            DiscoveryNode node = new DiscoveryNode(
                "TS_TPC",
                "TS_TPC",
                service.boundAddress().publishAddress(),
                emptyMap(),
                emptySet(),
                version0
            );
            originalTransport.openConnection(node, connectionProfile, future);
            try (Transport.Connection connection = future.actionGet()) {
                assertBusy(() -> { assertTrue(originalTransport.getKeepAlive().successfulPingCount() > 30); });
                assertEquals(0, originalTransport.getKeepAlive().failedPingCount());
            }
        }
    }

    public void testTcpHandshake() {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        ConnectionProfile connectionProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        try (TransportService service = buildService("TS_TPC", VersionInformation.CURRENT, TransportVersion.current(), Settings.EMPTY)) {
            DiscoveryNode node = new DiscoveryNode(
                "TS_TPC",
                "TS_TPC",
                service.boundAddress().publishAddress(),
                emptyMap(),
                emptySet(),
                version0
            );
            PlainActionFuture<Transport.Connection> future = PlainActionFuture.newFuture();
            serviceA.getOriginalTransport().openConnection(node, connectionProfile, future);
            try (Transport.Connection connection = future.actionGet()) {
                assertEquals(TransportVersion.current(), connection.getTransportVersion());
            }
        }
    }

    public void testTcpHandshakeTimeout() throws IOException {
        try (ServerSocket socket = new MockServerSocket()) {
            socket.bind(getLocalEphemeral(), 1);
            socket.setReuseAddress(true);
            DiscoveryNode dummy = DiscoveryNodeUtils.builder("TEST")
                .address(new TransportAddress(socket.getInetAddress(), socket.getLocalPort()))
                .roles(emptySet())
                .version(version0)
                .build();
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            builder.setHandshakeTimeout(TimeValue.timeValueMillis(1));
            ConnectTransportException ex = expectThrows(
                ConnectTransportException.class,
                () -> connectToNode(serviceA, dummy, builder.build())
            );
            assertEquals("[][" + dummy.getAddress() + "] handshake_timeout[1ms]", ex.getMessage());
        }
    }

    public void testTcpHandshakeConnectionReset() throws IOException, InterruptedException {
        try (ServerSocket socket = new MockServerSocket()) {
            socket.bind(getLocalEphemeral(), 1);
            socket.setReuseAddress(true);
            DiscoveryNode dummy = DiscoveryNodeUtils.builder("TEST")
                .address(new TransportAddress(socket.getInetAddress(), socket.getLocalPort()))
                .roles(emptySet())
                .version(version0)
                .build();
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
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            builder.setHandshakeTimeout(TimeValue.timeValueHours(1));
            ConnectTransportException ex = expectThrows(
                ConnectTransportException.class,
                () -> connectToNode(serviceA, dummy, builder.build())
            );
            assertEquals("[][" + dummy.getAddress() + "] general node connection failure", ex.getMessage());
            assertThat(ex.getCause().getMessage(), startsWith("handshake failed"));
            t.join();
        }
    }

    public void testResponseHeadersArePreserved() throws InterruptedException {
        List<String> executors = new ArrayList<>(ThreadPool.THREAD_POOL_TYPES.keySet());
        CollectionUtil.timSort(executors); // makes sure it's reproducible
        serviceA.registerRequestHandler("internal:action", ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {

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

            private final String executor = randomFrom(executors);

            @Override
            public TransportResponse read(StreamInput in) {
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
                return executor;
            }
        };

        serviceB.sendRequest(nodeA, "internal:action", new TestRequest(randomFrom("fail", "pass")), transportResponseHandler);
        serviceA.sendRequest(nodeA, "internal:action", new TestRequest(randomFrom("fail", "pass")), transportResponseHandler);
        latch.await();
    }

    public void testHandlerIsInvokedOnConnectionClose() throws IOException, InterruptedException {
        List<String> executors = new ArrayList<>(ThreadPool.THREAD_POOL_TYPES.keySet());
        CollectionUtil.timSort(executors); // makes sure it's reproducible
        TransportService serviceC = buildService("TS_C", version0, transportVersion0, Settings.EMPTY);
        serviceC.registerRequestHandler("internal:action", ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
            // do nothing
        });
        CountDownLatch latch = new CountDownLatch(1);
        TransportResponseHandler<TransportResponse> transportResponseHandler = new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse read(StreamInput in) {
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
        builder.addConnections(
            1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE
        );
        try (Transport.Connection connection = openConnection(serviceB, serviceC.getLocalNode(), builder.build())) {
            serviceC.close();
            serviceB.sendRequest(
                connection,
                "internal:action",
                new TestRequest("boom"),
                TransportRequestOptions.EMPTY,
                transportResponseHandler
            );
        }
        latch.await();
    }

    public void testConcurrentDisconnectOnNonPublishedConnection() throws IOException, InterruptedException {
        MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, Settings.EMPTY);
        CountDownLatch receivedLatch = new CountDownLatch(1);
        CountDownLatch sendResponseLatch = new CountDownLatch(1);
        serviceC.registerRequestHandler("internal:action", ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
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
        CountDownLatch responseLatch = new CountDownLatch(1);
        TransportResponseHandler<TransportResponse.Empty> transportResponseHandler = new TransportResponseHandler.Empty() {
            @Override
            public void handleResponse(TransportResponse.Empty response) {
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                responseLatch.countDown();
            }
        };

        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(
            1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE
        );

        try (Transport.Connection connection = openConnection(serviceB, serviceC.getLocalNode(), builder.build())) {
            serviceB.sendRequest(
                connection,
                "internal:action",
                new TestRequest("hello world"),
                TransportRequestOptions.EMPTY,
                transportResponseHandler
            );
            receivedLatch.await();
            serviceC.close();
            sendResponseLatch.countDown();
            responseLatch.await();
        }
    }

    public void testTransportStats() throws Exception {
        MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, Settings.EMPTY);
        CountDownLatch receivedLatch = new CountDownLatch(1);
        CountDownLatch sendResponseLatch = new CountDownLatch(1);
        serviceB.registerRequestHandler("internal:action", ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
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
        CountDownLatch responseLatch = new CountDownLatch(1);
        TransportResponseHandler<TransportResponse.Empty> transportResponseHandler = new TransportResponseHandler.Empty() {
            @Override
            public void handleResponse(TransportResponse.Empty response) {
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                responseLatch.countDown();
            }
        };

        TransportStats stats = serviceC.transport.getStats(); // nothing transmitted / read yet
        assertEquals(0, stats.getRxCount());
        assertEquals(0, stats.getTxCount());
        assertEquals(0, stats.getRxSize().getBytes());
        assertEquals(0, stats.getTxSize().getBytes());

        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(
            1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE
        );
        try (Transport.Connection connection = openConnection(serviceC, serviceB.getLocalNode(), builder.build())) {
            assertBusy(() -> { // netty for instance invokes this concurrently so we better use assert busy here
                TransportStats transportStats = serviceC.transport.getStats(); // we did a single round-trip to do the initial handshake
                assertEquals(1, transportStats.getRxCount());
                assertEquals(1, transportStats.getTxCount());
                assertEquals(29, transportStats.getRxSize().getBytes());
                assertEquals(55, transportStats.getTxSize().getBytes());
            });
            serviceC.sendRequest(
                connection,
                "internal:action",
                new TestRequest("hello world"),
                TransportRequestOptions.EMPTY,
                transportResponseHandler
            );
            receivedLatch.await();
            assertBusy(() -> { // netty for instance invokes this concurrently so we better use assert busy here
                TransportStats transportStats = serviceC.transport.getStats(); // request has been send
                assertEquals(1, transportStats.getRxCount());
                assertEquals(2, transportStats.getTxCount());
                assertEquals(29, transportStats.getRxSize().getBytes());
                assertEquals(114, transportStats.getTxSize().getBytes());
            });
            sendResponseLatch.countDown();
            responseLatch.await();
            stats = serviceC.transport.getStats(); // response has been received
            assertEquals(2, stats.getRxCount());
            assertEquals(2, stats.getTxCount());
            assertEquals(54, stats.getRxSize().getBytes());
            assertEquals(114, stats.getTxSize().getBytes());
        } finally {
            serviceC.close();
        }
    }

    public void testAcceptedChannelCount() throws Exception {
        assertBusy(() -> {
            TransportStats transportStats = serviceA.transport.getStats();
            assertEquals(channelsPerNodeConnection(), transportStats.getServerOpen());
        });
        assertBusy(() -> {
            TransportStats transportStats = serviceB.transport.getStats();
            assertEquals(channelsPerNodeConnection(), transportStats.getServerOpen());
        });

        serviceA.close();

        assertBusy(() -> {
            TransportStats transportStats = serviceB.transport.getStats();
            assertEquals(0, transportStats.getServerOpen());
        });
    }

    public void testTransportStatsWithException() throws Exception {
        MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, Settings.EMPTY);
        CountDownLatch receivedLatch = new CountDownLatch(1);
        CountDownLatch sendResponseLatch = new CountDownLatch(1);
        Exception ex = new RuntimeException("boom");
        ex.setStackTrace(new StackTraceElement[0]);
        serviceB.registerRequestHandler("internal:action", ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
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
        CountDownLatch responseLatch = new CountDownLatch(1);
        AtomicReference<TransportException> receivedException = new AtomicReference<>(null);
        TransportResponseHandler<TransportResponse.Empty> transportResponseHandler = new TransportResponseHandler.Empty() {
            @Override
            public void handleResponse(TransportResponse.Empty response) {
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                receivedException.set(exp);
                responseLatch.countDown();
            }
        };

        TransportStats stats = serviceC.transport.getStats(); // nothing transmitted / read yet
        assertEquals(0, stats.getRxCount());
        assertEquals(0, stats.getTxCount());
        assertEquals(0, stats.getRxSize().getBytes());
        assertEquals(0, stats.getTxSize().getBytes());

        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(
            1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE
        );
        try (Transport.Connection connection = openConnection(serviceC, serviceB.getLocalNode(), builder.build())) {
            assertBusy(() -> { // netty for instance invokes this concurrently so we better use assert busy here
                TransportStats transportStats = serviceC.transport.getStats(); // request has been sent
                assertEquals(1, transportStats.getRxCount());
                assertEquals(1, transportStats.getTxCount());
                assertEquals(29, transportStats.getRxSize().getBytes());
                assertEquals(55, transportStats.getTxSize().getBytes());
            });
            serviceC.sendRequest(
                connection,
                "internal:action",
                new TestRequest("hello world"),
                TransportRequestOptions.EMPTY,
                transportResponseHandler
            );
            receivedLatch.await();
            assertBusy(() -> { // netty for instance invokes this concurrently so we better use assert busy here
                TransportStats transportStats = serviceC.transport.getStats(); // request has been sent
                assertEquals(1, transportStats.getRxCount());
                assertEquals(2, transportStats.getTxCount());
                assertEquals(29, transportStats.getRxSize().getBytes());
                assertEquals(114, transportStats.getTxSize().getBytes());
            });
            sendResponseLatch.countDown();
            responseLatch.await();
            stats = serviceC.transport.getStats(); // exception response has been received
            assertEquals(2, stats.getRxCount());
            assertEquals(2, stats.getTxCount());
            TransportException exception = receivedException.get();
            assertNotNull(exception);
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setTransportVersion(transportVersion0);
            exception.writeTo(streamOutput);
            String failedMessage = "Unexpected read bytes size. The transport exception that was received=" + exception;
            // 57 bytes are the non-exception message bytes that have been received. It should include the initial
            // handshake message and the header, version, etc bytes in the exception message.
            assertEquals(failedMessage, 57 + streamOutput.bytes().length(), stats.getRxSize().getBytes());
            assertEquals(114, stats.getTxSize().getBytes());
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
        try (
            MockTransportService serviceC = buildService(
                "TS_C",
                version0,
                transportVersion0,
                Settings.builder()
                    .put("transport.profiles.default.bind_host", "_local:ipv4_")
                    .put("transport.profiles.some_profile.port", "8900-9000")
                    .put("transport.profiles.some_profile.bind_host", "_local:ipv4_")
                    .put("transport.profiles.some_other_profile.port", "8700-8800")
                    .putList("transport.profiles.some_other_profile.bind_host", hosts)
                    .putList("transport.profiles.some_other_profile.publish_host", "_local:ipv4_")
                    .build()
            )
        ) {

            Map<String, BoundTransportAddress> profileBoundAddresses = serviceC.transport.profileBoundAddresses();
            assertTrue(profileBoundAddresses.containsKey("some_profile"));
            assertTrue(profileBoundAddresses.containsKey("some_other_profile"));
            assertTrue(profileBoundAddresses.get("some_profile").publishAddress().getPort() >= 8900);
            assertTrue(profileBoundAddresses.get("some_profile").publishAddress().getPort() < 9000);
            assertTrue(profileBoundAddresses.get("some_other_profile").publishAddress().getPort() >= 8700);
            assertTrue(profileBoundAddresses.get("some_other_profile").publishAddress().getPort() < 8800);
            assertTrue(profileBoundAddresses.get("some_profile").boundAddresses().length >= 1);
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
                assertTrue(profileBoundAddresses.get("some_other_profile").boundAddresses().length >= 1);
            }
            assertTrue(profileBoundAddresses.get("some_other_profile").publishAddress().address().getAddress() instanceof Inet4Address);
        }
    }

    public void testProfileSettings() {
        boolean enable = randomBoolean();
        Settings globalSettings = Settings.builder()
            .put("network.tcp.no_delay", enable)
            .put("network.tcp.keep_alive", enable)
            .put("network.tcp.keep_idle", "42")
            .put("network.tcp.keep_interval", "7")
            .put("network.tcp.keep_count", "13")
            .put("network.tcp.reuse_address", enable)
            .put("network.tcp.send_buffer_size", "43000b")
            .put("network.tcp.receive_buffer_size", "42000b")
            .put("network.publish_host", "the_publish_host")
            .put("network.bind_host", "the_bind_host")
            .build();

        Settings globalSettings2 = Settings.builder()
            .put("network.tcp.no_delay", enable == false)
            .put("network.tcp.keep_alive", enable == false)
            .put("network.tcp.keep_idle", "43")
            .put("network.tcp.keep_interval", "8")
            .put("network.tcp.keep_count", "14")
            .put("network.tcp.reuse_address", enable == false)
            .put("network.tcp.send_buffer_size", "4b")
            .put("network.tcp.receive_buffer_size", "3b")
            .put("network.publish_host", "another_publish_host")
            .put("network.bind_host", "another_bind_host")
            .build();

        Settings transportSettings = Settings.builder()
            .put("transport.tcp.no_delay", enable)
            .put("transport.tcp.keep_alive", enable)
            .put("transport.tcp.keep_idle", "42")
            .put("transport.tcp.keep_interval", "7")
            .put("transport.tcp.keep_count", "13")
            .put("transport.tcp.reuse_address", enable)
            .put("transport.tcp.send_buffer_size", "43000b")
            .put("transport.tcp.receive_buffer_size", "42000b")
            .put("transport.publish_host", "the_publish_host")
            .put("transport.port", "9700-9800")
            .put("transport.bind_host", "the_bind_host")
            .put(globalSettings2)
            .build();

        Settings transportSettings2 = Settings.builder()
            .put("transport.tcp.no_delay", enable == false)
            .put("transport.tcp.keep_alive", enable == false)
            .put("transport.tcp.keep_idle", "43")
            .put("transport.tcp.keep_interval", "8")
            .put("transport.tcp.keep_count", "14")
            .put("transport.tcp.reuse_address", enable == false)
            .put("transport.tcp.send_buffer_size", "5b")
            .put("transport.tcp.receive_buffer_size", "6b")
            .put("transport.publish_host", "another_publish_host")
            .put("transport.port", "9702-9802")
            .put("transport.bind_host", "another_bind_host")
            .put(globalSettings2)
            .build();
        Settings defaultProfileSettings = Settings.builder()
            .put("transport.profiles.default.tcp.no_delay", enable)
            .put("transport.profiles.default.tcp.keep_alive", enable)
            .put("transport.profiles.default.tcp.keep_idle", "42")
            .put("transport.profiles.default.tcp.keep_interval", "7")
            .put("transport.profiles.default.tcp.keep_count", "13")
            .put("transport.profiles.default.tcp.reuse_address", enable)
            .put("transport.profiles.default.tcp.send_buffer_size", "43000b")
            .put("transport.profiles.default.tcp.receive_buffer_size", "42000b")
            .put("transport.profiles.default.port", "9700-9800")
            .put("transport.profiles.default.publish_host", "the_publish_host")
            .put("transport.profiles.default.bind_host", "the_bind_host")
            .put("transport.profiles.default.publish_port", 42)
            .put(randomBoolean() ? transportSettings2 : globalSettings2) // ensure that we have profile precedence
            .build();

        Settings profileSettings = Settings.builder()
            .put("transport.profiles.some_profile.tcp.no_delay", enable)
            .put("transport.profiles.some_profile.tcp.keep_alive", enable)
            .put("transport.profiles.some_profile.tcp.keep_idle", "42")
            .put("transport.profiles.some_profile.tcp.keep_interval", "7")
            .put("transport.profiles.some_profile.tcp.keep_count", "13")
            .put("transport.profiles.some_profile.tcp.reuse_address", enable)
            .put("transport.profiles.some_profile.tcp.send_buffer_size", "43000b")
            .put("transport.profiles.some_profile.tcp.receive_buffer_size", "42000b")
            .put("transport.profiles.some_profile.port", "9700-9800")
            .put("transport.profiles.some_profile.publish_host", "the_publish_host")
            .put("transport.profiles.some_profile.bind_host", "the_bind_host")
            .put("transport.profiles.some_profile.publish_port", 42)
            .put(randomBoolean() ? transportSettings2 : globalSettings2) // ensure that we have profile precedence
            .put(randomBoolean() ? defaultProfileSettings : Settings.EMPTY)
            .build();

        Settings randomSettings = randomFrom(random(), globalSettings, transportSettings, profileSettings);
        ClusterSettings clusterSettings = new ClusterSettings(randomSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.validate(randomSettings, false);
        TcpTransport.ProfileSettings settings = new TcpTransport.ProfileSettings(
            Settings.builder().put(randomSettings).put("transport.profiles.some_profile.port", "9700-9800").build(), // port is required
            "some_profile"
        );

        assertEquals(enable, settings.tcpNoDelay);
        assertEquals(enable, settings.tcpKeepAlive);
        assertEquals(42, settings.tcpKeepIdle);
        assertEquals(7, settings.tcpKeepInterval);
        assertEquals(13, settings.tcpKeepCount);
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
        assertEquals(TransportSettings.DEFAULT_PROFILE, profileSettings.stream().findAny().get().profileName);

        profileSettings = TcpTransport.getProfileSettings(Settings.builder().put("transport.profiles.test.port", "0").build());
        assertEquals(2, profileSettings.size());
        assertEquals(
            new HashSet<>(Arrays.asList("default", "test")),
            profileSettings.stream().map(s -> s.profileName).collect(Collectors.toSet())
        );

        profileSettings = TcpTransport.getProfileSettings(
            Settings.builder().put("transport.profiles.test.port", "0").put("transport.profiles.default.port", "0").build()
        );
        assertEquals(2, profileSettings.size());
        assertEquals(
            new HashSet<>(Arrays.asList("default", "test")),
            profileSettings.stream().map(s -> s.profileName).collect(Collectors.toSet())
        );
    }

    public void testBindUnavailableAddress() {
        int port = serviceA.boundAddress().publishAddress().getPort();
        String address = serviceA.boundAddress().publishAddress().getAddress();
        Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "foobar")
            .put(TransportSettings.HOST.getKey(), address)
            .put(TransportSettings.PORT.getKey(), port)
            .build();
        BindTransportException bindTransportException = expectThrows(
            BindTransportException.class,
            () -> buildService("test", VersionInformation.CURRENT, TransportVersion.current(), settings)
        );
        InetSocketAddress inetSocketAddress = serviceA.boundAddress().publishAddress().address();
        assertEquals("Failed to bind to " + NetworkAddress.format(inetSocketAddress), bindTransportException.getMessage());
    }

    public void testChannelCloseWhileConnecting() {
        try (MockTransportService service = buildService("TS_C", version0, transportVersion0, Settings.EMPTY)) {
            AtomicBoolean connectionClosedListenerCalled = new AtomicBoolean(false);
            service.addConnectionListener(new TransportConnectionListener() {
                @Override
                public void onConnectionOpened(final Transport.Connection connection) {
                    closeConnectionChannel(connection);
                    try {
                        assertBusy(() -> assertTrue(connection.isClosed()));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }

                @Override
                public void onConnectionClosed(Transport.Connection connection) {
                    connectionClosedListenerCalled.set(true);
                }
            });
            final ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            final ConnectTransportException e = expectThrows(
                ConnectTransportException.class,
                () -> openConnection(service, nodeA, builder.build())
            );
            assertThat(e, hasToString(containsString(("a channel closed while connecting"))));
            assertTrue(connectionClosedListenerCalled.get());
        }
    }

    public void testFailToSendTransportException() throws InterruptedException {
        TransportException exception = doFailToSend(new TransportException("fail to send"));
        assertThat(exception.getMessage(), equalTo("fail to send"));
        assertThat(exception.getCause(), nullValue());
    }

    public void testFailToSendIllegalStateException() throws InterruptedException {
        TransportException exception = doFailToSend(new IllegalStateException("fail to send"));
        assertThat(exception, instanceOf(SendRequestTransportException.class));
        assertThat(exception.getMessage(), containsString("fail-to-send-action"));
        assertThat(exception.getCause(), instanceOf(IllegalStateException.class));
        assertThat(exception.getCause().getMessage(), equalTo("fail to send"));
    }

    public void testChannelToString() {
        final String ACTION = "internal:action";
        serviceA.registerRequestHandler(ACTION, ThreadPool.Names.SAME, TransportRequest.Empty::new, (request, channel, task) -> {
            assertThat(
                channel.toString(),
                allOf(
                    containsString("DirectResponseChannel"),
                    containsString('{' + ACTION + '}'),
                    containsString("TaskTransportChannel{task=" + task.getId() + '}')
                )
            );
            assertThat(new ChannelActionListener<>(channel).toString(), containsString(channel.toString()));
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        });
        serviceB.registerRequestHandler(ACTION, ThreadPool.Names.SAME, TransportRequest.Empty::new, (request, channel, task) -> {
            assertThat(
                channel.toString(),
                allOf(
                    containsString("TcpTransportChannel"),
                    containsString('{' + ACTION + '}'),
                    containsString("TaskTransportChannel{task=" + task.getId() + '}'),
                    containsString("localAddress="),
                    containsString(serviceB.getLocalNode().getAddress().toString())
                )
            );
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        });

        PlainActionFuture.get(
            f -> submitRequest(
                serviceA,
                serviceA.getLocalNode(),
                ACTION,
                TransportRequest.Empty.INSTANCE,
                new ActionListenerResponseHandler<>(f, ignored -> TransportResponse.Empty.INSTANCE)
            ),
            10,
            TimeUnit.SECONDS
        );

        PlainActionFuture.get(
            f -> submitRequest(
                serviceA,
                serviceB.getLocalNode(),
                ACTION,
                TransportRequest.Empty.INSTANCE,
                new ActionListenerResponseHandler<>(f, ignored -> TransportResponse.Empty.INSTANCE)
            ),
            10,
            TimeUnit.SECONDS
        );
    }

    public void testActionStats() throws Exception {
        final String ACTION = "internal:action";

        class Request extends TransportRequest {
            final int refSize;

            Request(int refSize) {
                this.refSize = refSize;
            }

            Request(StreamInput in) throws IOException {
                super(in);
                refSize = in.readBytesReference().length();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                writeZeroes(refSize, out);
            }

            static void writeZeroes(int refSize, StreamOutput out) throws IOException {
                out.writeBytesReference(new BytesArray(new byte[refSize]));
            }
        }

        class Response extends TransportResponse {
            final int refSize;

            Response(int refSize) {
                this.refSize = refSize;
            }

            Response(StreamInput in) throws IOException {
                refSize = in.readBytesReference().length();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                Request.writeZeroes(refSize, out);
            }
        }

        final var statsBeforeRequest = serviceB.transport().getStats().getTransportActionStats();
        assertEquals(Set.of(HANDSHAKE_ACTION_NAME), statsBeforeRequest.keySet());
        final var handshakeStats = statsBeforeRequest.get(HANDSHAKE_ACTION_NAME);
        assertEquals(1, handshakeStats.requestCount());
        assertEquals(1, handshakeStats.responseCount());
        assertThat(handshakeStats.totalRequestSize(), greaterThanOrEqualTo(16L));
        assertThat(handshakeStats.totalResponseSize(), greaterThanOrEqualTo(16L));

        final var requestSize = between(0, ByteSizeUnit.MB.toIntBytes(1));
        final var responseSize = between(0, ByteSizeUnit.MB.toIntBytes(1));

        serviceB.registerRequestHandler(ACTION, ThreadPool.Names.SAME, Request::new, (request, channel, task) -> {
            assertEquals(requestSize, request.refSize);
            channel.sendResponse(new Response(responseSize));
        });

        var actualRequestSize = -1L;
        var actualResponseSize = -1L;

        for (int iteration = 1; iteration <= 5; iteration++) {
            assertEquals(
                responseSize,
                PlainActionFuture.<Response, Exception>get(
                    f -> submitRequest(
                        serviceA,
                        serviceB.getLocalNode(),
                        ACTION,
                        new Request(requestSize),
                        new ActionListenerResponseHandler<>(f, Response::new)
                    ),
                    10,
                    TimeUnit.SECONDS
                ).refSize
            );

            final var allTransportActionStats = serviceB.transport().getStats().getTransportActionStats();
            // using a sorted map, so the keys are in a deterministic order:
            assertEquals(List.of(ACTION, HANDSHAKE_ACTION_NAME), allTransportActionStats.keySet().stream().toList());

            final var transportActionStats = allTransportActionStats.get(ACTION);
            assertEquals(iteration, transportActionStats.requestCount());
            assertEquals(iteration, transportActionStats.responseCount());
            if (iteration == 1) {
                actualRequestSize = transportActionStats.totalRequestSize();
                actualResponseSize = transportActionStats.totalResponseSize();
                assertThat(actualRequestSize, allOf(greaterThan(requestSize + 16L), lessThan(requestSize + 256L)));
                assertThat(actualResponseSize, allOf(greaterThan(responseSize + 16L), lessThan(responseSize + 256L)));
            }
            assertEquals(iteration * actualRequestSize, transportActionStats.totalRequestSize());
            assertEquals(iteration * actualResponseSize, transportActionStats.totalResponseSize());
            assertArrayEquals(getConstantMessageSizeHistogram(iteration, actualRequestSize), transportActionStats.requestSizeHistogram());
            assertArrayEquals(getConstantMessageSizeHistogram(iteration, actualResponseSize), transportActionStats.responseSizeHistogram());
        }
    }

    private static long[] getConstantMessageSizeHistogram(int count, long size) {
        final var histogram = new long[29];
        int bucket = 0;
        long bucketLowerBound = 8;
        while (bucket < histogram.length) {
            if (size <= bucketLowerBound) {
                histogram[bucket] = count;
                return histogram;
            }
            bucket++;
            bucketLowerBound <<= 1;
        }
        throw new AssertionError("no bucket found");
    }

    // test that the response handler is invoked on a failure to send
    private TransportException doFailToSend(RuntimeException failToSendException) throws InterruptedException {
        final TransportInterceptor interceptor = new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(final AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        final Transport.Connection connection,
                        final String action,
                        final TransportRequest request,
                        final TransportRequestOptions options,
                        final TransportResponseHandler<T> handler
                    ) {
                        if ("fail-to-send-action".equals(action)) {
                            throw failToSendException;
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };
        try (
            MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, null, Settings.EMPTY, true, true, interceptor)
        ) {
            final CountDownLatch latch = new CountDownLatch(1);
            serviceC.connectToNode(
                serviceA.getLocalDiscoNode(),
                ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY),
                new ActionListener<>() {
                    @Override
                    public void onResponse(final Releasable ignored) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        fail(e.getMessage());
                    }
                }
            );
            latch.await();
            final AtomicReference<TransportException> te = new AtomicReference<>();
            final Transport.Connection connection = serviceC.getConnection(nodeA);
            serviceC.sendRequest(
                connection,
                "fail-to-send-action",
                TransportRequest.Empty.INSTANCE,
                TransportRequestOptions.EMPTY,
                new TransportResponseHandler.Empty() {
                    @Override
                    public void handleResponse(final TransportResponse.Empty response) {
                        fail("handle response should not be invoked");
                    }

                    @Override
                    public void handleException(final TransportException exp) {
                        te.set(exp);
                    }
                }
            );
            assertThat(te.get(), not(nullValue()));
            return te.get();
        }
    }

    private void closeConnectionChannel(Transport.Connection connection) {
        StubbableTransport.WrappedConnection wrappedConnection = (StubbableTransport.WrappedConnection) connection;
        TcpTransport.NodeChannels channels = (TcpTransport.NodeChannels) wrappedConnection.getConnection();
        CloseableChannel.closeChannels(channels.getChannels().subList(0, randomIntBetween(1, channels.getChannels().size())), true);
    }

    @SuppressForbidden(reason = "need local ephemeral port")
    protected InetSocketAddress getLocalEphemeral() throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getLocalHost(), 0);
    }

    protected Set<TcpChannel> getAcceptedChannels(TcpTransport transport) {
        return transport.getAcceptedChannels();
    }

    /**
     * Connect to the specified node with the default connection profile
     *
     * @param service service to connect from
     * @param node the node to connect to
     */
    public static void connectToNode(TransportService service, DiscoveryNode node) throws ConnectTransportException {
        connectToNode(service, node, null);
    }

    /**
     * Connect to the specified node with the given connection profile
     *
     * @param service service to connect from
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use when connecting to this node
     */
    public static void connectToNode(TransportService service, DiscoveryNode node, ConnectionProfile connectionProfile) {
        PlainActionFuture.get(fut -> service.connectToNode(node, connectionProfile, fut.map(x -> null)));
    }

    /**
     * Establishes and returns a new connection to the given node from the given {@link TransportService}.
     *
     * @param service service to connect from
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use
     */
    public static Transport.Connection openConnection(TransportService service, DiscoveryNode node, ConnectionProfile connectionProfile) {
        return PlainActionFuture.get(fut -> service.openConnection(node, connectionProfile, fut));
    }

    public static <T extends TransportResponse> Future<T> submitRequest(
        TransportService transportService,
        DiscoveryNode node,
        String action,
        TransportRequest request,
        TransportResponseHandler<T> handler
    ) throws TransportException {
        return submitRequest(transportService, node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public static <T extends TransportResponse> Future<T> submitRequest(
        TransportService transportService,
        DiscoveryNode node,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<T> handler
    ) throws TransportException {
        final ListenableFuture<T> responseListener = new ListenableFuture<>();
        final TransportResponseHandler<T> futureHandler = new ActionListenerResponseHandler<>(
            responseListener,
            handler,
            handler.executor()
        );
        responseListener.addListener(ActionListener.wrap(handler::handleResponse, e -> handler.handleException((TransportException) e)));
        final PlainActionFuture<T> future = PlainActionFuture.newFuture();
        responseListener.addListener(future);
        transportService.sendRequest(node, action, request, options, futureHandler);
        return future;
    }
}
