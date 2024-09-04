/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.mocksocket.MockSocket;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportSettings;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.hamcrest.Matchers.is;

/**
 * This test checks, if an HTTP look-alike request (starting with an HTTP method and a space)
 * actually returns text response instead of just dropping the connection
 */
public class Netty4SizeHeaderFrameDecoderTests extends ESTestCase {

    private final Settings settings = Settings.builder()
        .put("node.name", "NettySizeHeaderFrameDecoderTests")
        .put(TransportSettings.BIND_HOST.getKey(), "127.0.0.1")
        .put(TransportSettings.PORT.getKey(), "0")
        .build();

    private ThreadPool threadPool;
    private Netty4Transport nettyTransport;
    private int port;
    private InetAddress host;

    @Before
    public void startThreadPool() {
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        NetworkService networkService = new NetworkService(Collections.emptyList());
        PageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);
        nettyTransport = new Netty4Transport(
            settings,
            TransportVersion.current(),
            threadPool,
            networkService,
            recycler,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(settings)
        );
        nettyTransport.start();

        TransportAddress[] boundAddresses = nettyTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);
        port = transportAddress.address().getPort();
        host = transportAddress.address().getAddress();
    }

    @After
    public void terminateThreadPool() throws InterruptedException {
        nettyTransport.stop();
        terminate(threadPool);
        threadPool = null;
    }

    public void testThatTextMessageIsReturnedOnHTTPLikeRequest() throws Exception {
        String randomMethod = randomFrom("GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH");
        String data = randomMethod + " / HTTP/1.1";

        try (Socket socket = new MockSocket(host, port)) {
            socket.getOutputStream().write(data.getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                assertThat(reader.readLine(), is("This is not an HTTP port"));
            }
        }
    }

    public void testThatNothingIsReturnedForOtherInvalidPackets() throws Exception {
        try (Socket socket = new MockSocket(host, port)) {
            socket.getOutputStream().write("FOOBAR".getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();

            // end of stream
            assertThat(socket.getInputStream().read(), is(-1));
        }
    }

}
