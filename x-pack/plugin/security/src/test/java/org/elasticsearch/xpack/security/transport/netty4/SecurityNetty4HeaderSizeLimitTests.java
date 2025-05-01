/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.mocksocket.MockSocket;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TestRequest;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.transport.AbstractSimpleTransportTestCase.IGNORE_DESERIALIZATION_ERRORS_SETTING;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 * Tests that request header size limits are enforced (connection dropped) for the RCS transport port (because this port can
 * possibly be exposed without TLS protection)
 */
public final class SecurityNetty4HeaderSizeLimitTests extends ESTestCase {

    private final int maxHeaderSize = randomIntBetween(64, 128);
    private final BigArrays bigarrays = new BigArrays(null, new NoneCircuitBreakerService(), CircuitBreaker.REQUEST);
    private final Settings settings = Settings.builder()
        .put("node.name", "SecurityNetty4HeaderSizeLimitTests")
        .put(RemoteClusterPortSettings.MAX_REQUEST_HEADER_SIZE.getKey(), maxHeaderSize + "b")
        .put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), "false")
        .put(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED.getKey(), "false")
        .put(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_ENABLED.getKey(), "false")
        .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
        .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), "true")
        .build();
    private ThreadPool threadPool;
    private SecurityNetty4ServerTransport securityNettyTransport;
    private int remoteIngressPort;
    private InetAddress remoteIngressHost;
    private int defaultTransportPort;
    private InetAddress defaultTransportHost;
    private AtomicLong requestIdReceived;

    @Before
    public void startThreadPool() {
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        TaskManager taskManager = new TaskManager(settings, threadPool, Collections.emptySet());
        NetworkService networkService = new NetworkService(Collections.emptyList());
        PageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);
        securityNettyTransport = new SecurityNetty4ServerTransport(
            settings,
            TransportVersion.current(),
            threadPool,
            networkService,
            recycler,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            null,
            mock(SSLService.class),
            new SharedGroupFactory(settings),
            mock(CrossClusterAccessAuthenticationService.class)
        );
        requestIdReceived = new AtomicLong(-1L);
        securityNettyTransport.setMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestReceived(long requestId, String action) {
                requestIdReceived.set(requestId);
            }
        });
        securityNettyTransport.registerRequestHandler(
            new RequestHandlerRegistry<>(
                "internal:test",
                TestRequest::new,
                taskManager,
                (request, channel, task) -> channel.sendResponse(ActionResponse.Empty.INSTANCE),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                false,
                true,
                Tracer.NOOP
            )
        );
        securityNettyTransport.start();

        TransportAddress[] boundRemoteIngressAddresses = securityNettyTransport.boundRemoteIngressAddress().boundAddresses();
        TransportAddress remoteIngressTransportAddress = randomFrom(boundRemoteIngressAddresses);
        remoteIngressPort = remoteIngressTransportAddress.address().getPort();
        remoteIngressHost = remoteIngressTransportAddress.address().getAddress();

        TransportAddress[] boundAddresses = securityNettyTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);
        defaultTransportPort = transportAddress.address().getPort();
        defaultTransportHost = transportAddress.address().getAddress();
    }

    @After
    public void terminateThreadPool() {
        securityNettyTransport.stop();
        terminate(threadPool);
        threadPool = null;
    }

    public void testThatAcceptableHeaderSizeGoesThroughTheRemoteClusterPort() throws Exception {
        int messageLength = randomIntBetween(128, 256);
        long requestId = randomLongBetween(1L, 1000L);
        int acceptableHeaderSize = randomIntBetween(0, maxHeaderSize - TcpHeader.HEADER_SIZE);
        try (
            ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(
                messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE,
                bigarrays
            )
        ) {
            assembleDummyRequest(out, messageLength, requestId, acceptableHeaderSize);

            try (Socket socket = new MockSocket(remoteIngressHost, remoteIngressPort)) {
                socket.getOutputStream().write(out.bytes().array());
                socket.getOutputStream().flush();

                // an error response is returned because the request is malformed
                assertThat(socket.getInputStream().read(), greaterThan(0));
                // but the request has been certainly received and processed
                assertThat(requestIdReceived.get(), is(requestId));
            }
        }
    }

    public void testThatLargerHeaderSizeClosesTheRemoteClusterPort() throws Exception {
        int messageLength = randomIntBetween(128, 256);
        long requestId = randomLongBetween(1L, 1000L);
        int largeHeaderSize = randomIntBetween(
            maxHeaderSize - TcpHeader.HEADER_SIZE + 1,
            messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE - TcpHeader.HEADER_SIZE
        );
        try (
            ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(
                messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE,
                bigarrays
            )
        ) {
            assembleDummyRequest(out, messageLength, requestId, largeHeaderSize);

            try (Socket socket = new MockSocket(remoteIngressHost, remoteIngressPort)) {
                socket.getOutputStream().write(out.bytes().array());
                socket.getOutputStream().flush();

                // channel is closed
                assertThat(socket.getInputStream().read(), is(-1));
                // and a request has NOT been received
                assertThat(requestIdReceived.get(), is(-1L));
            }
        }
    }

    public void testThatLargerHeaderSizeIsAcceptableForDefaultTransportPort() throws Exception {
        int messageLength = randomIntBetween(128, 256);
        long requestId = randomLongBetween(1L, 1000L);
        int largeHeaderSize = randomIntBetween(
            maxHeaderSize - TcpHeader.HEADER_SIZE + 1,
            messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE - TcpHeader.HEADER_SIZE
        );
        try (
            ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(
                messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE,
                bigarrays
            )
        ) {
            assembleDummyRequest(out, messageLength, requestId, largeHeaderSize);

            try (Socket socket = new MockSocket(defaultTransportHost, defaultTransportPort)) {
                socket.getOutputStream().write(out.bytes().array());
                socket.getOutputStream().flush();

                // an error response is returned because the request is malformed
                assertThat(socket.getInputStream().read(), greaterThan(0));
                // but the request has been certainly received and processed
                assertThat(requestIdReceived.get(), is(requestId));
            }
        }
    }

    private void assembleDummyRequest(BytesStreamOutput out, int messageLength, long requestId, int variableHeaderSize) throws IOException {
        out.writeByte((byte) 'E');
        out.writeByte((byte) 'S');
        out.writeInt(messageLength); // message length must not be higher than the bytes array length that's sent on the socket
        out.writeLong(requestId); // request id
        out.writeByte((byte) 0); // status byte (request)
        out.writeInt(TransportVersion.current().id());
        out.writeInt(variableHeaderSize); // variable header size
        out.writeMap(Map.of()); // request headers
        out.writeMap(Map.of()); // response headers
        out.writeString("internal:test"); // action name in header
        out.writeVInt(-1); // make sure request is malformed (it's also possibly malformed for other reasons)
    }
}
