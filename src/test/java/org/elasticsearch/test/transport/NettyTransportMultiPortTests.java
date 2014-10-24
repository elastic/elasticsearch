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
package org.elasticsearch.test.transport;

import com.google.common.base.Charsets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cache.recycler.MockBigArrays;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 1)
public class NettyTransportMultiPortTests extends ElasticsearchTestCase {

    private final static short MAX_RUNS_BEFORE_GIVING_UP = 10;

    // cannot be static due to randomization
    private final String EXPECTED_CONNECTION_ACCEPTED_PORT = "a_" + randomAsciiOfLength(10);
    private final String EXPECTED_CONNECTION_REFUSED_PORT = "r_" + randomAsciiOfLength(10);

    private NettyTransport nettyTransport;
    private ThreadPool threadPool;
    private int runs = 0;

    @After
    public void ensureMaxRunsAreNotReached() {
        if (runs >= MAX_RUNS_BEFORE_GIVING_UP) {
            throw new ElasticsearchException("Maximum number of runs[" + MAX_RUNS_BEFORE_GIVING_UP + "] reached without being able to bind to port");
        }
    }

    @Test
    @TestLogging("shield.transport.netty:DEBUG")
    public void testThatNettyCanBindToMultiplePorts() throws Exception {
        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", EXPECTED_CONNECTION_REFUSED_PORT)
                .put("transport.profiles.default.port", EXPECTED_CONNECTION_ACCEPTED_PORT)
                .put("transport.profiles.client1.port", EXPECTED_CONNECTION_ACCEPTED_PORT)
                .build();

        startNettyTransportAndCheckPortBindings(settings);
    }

    @Test
    @TestLogging("transport.netty:DEBUG")
    public void testThatDefaultProfileInheritsFromStandardSettings() throws Exception {
        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", EXPECTED_CONNECTION_ACCEPTED_PORT)
                .put("transport.profiles.client1.port", EXPECTED_CONNECTION_ACCEPTED_PORT)
                .build();

        startNettyTransportAndCheckPortBindings(settings);
    }

    @Test
    @TestLogging("transport.netty:DEBUG")
    public void testThatProfileWithoutPortSettingsFails() throws Exception {
        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", EXPECTED_CONNECTION_ACCEPTED_PORT)
                .put("transport.profiles.client1.whatever", "foo")
                .build();

        startNettyTransportAndCheckPortBindings(settings);
    }

    @Test
    @TestLogging("transport.netty:DEBUG")
    public void testThatDefaultProfilePortOverridesGeneralConfiguration() throws Exception {
        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", EXPECTED_CONNECTION_REFUSED_PORT)
                .put("transport.netty.port", EXPECTED_CONNECTION_REFUSED_PORT)
                .put("transport.profiles.default.port", EXPECTED_CONNECTION_ACCEPTED_PORT)
                .build();

        startNettyTransportAndCheckPortBindings(settings);
    }

    @Test
    @Network
    public void testThatBindingOnDifferentHostsWorks() throws Exception {
        InetAddress firstNonLoopbackAddress = NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.StackType.IPv4);

        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", EXPECTED_CONNECTION_ACCEPTED_PORT)
                .put("transport.profiles.default.bind_host", "127.0.0.1")
                .put("transport.profiles.client1.bind_host", firstNonLoopbackAddress.getHostAddress())
                .put("transport.profiles.client1.port", EXPECTED_CONNECTION_ACCEPTED_PORT)
                .build();

        startNettyTransportAndCheckPortBindings(settings);
    }

    private void startNettyTransportAndCheckPortBindings(Settings originalSettings) throws Exception {
        while (runs++ < MAX_RUNS_BEFORE_GIVING_UP) {
            try {
                ImmutableSettings.Builder settingsBuilder = settingsBuilder().put(originalSettings);

                int numberOfPorts = 0;
                for (String value : originalSettings.getAsMap().values()) {
                    if (EXPECTED_CONNECTION_ACCEPTED_PORT.equals(value) || EXPECTED_CONNECTION_REFUSED_PORT.equals(value)) {
                        numberOfPorts++;
                    }
                }

                int[] ports = SocketUtil.findFreeLocalPorts(numberOfPorts);
                int idx = 0;
                for (Map.Entry<String, String> entry : originalSettings.getAsMap().entrySet()) {
                    if (EXPECTED_CONNECTION_ACCEPTED_PORT.equals(entry.getValue()) || EXPECTED_CONNECTION_REFUSED_PORT.equals(entry.getValue())) {
                        settingsBuilder.put(entry.getKey(), ports[idx++]);
                    }
                }

                Settings settings = settingsBuilder.build();
                threadPool = new ThreadPool("tst");
                BigArrays bigArrays = new MockBigArrays(settings, new PageCacheRecycler(settings, threadPool), new NoneCircuitBreakerService());

                nettyTransport = new NettyTransport(settings, threadPool, new NetworkService(settings), bigArrays, Version.CURRENT);
                nettyTransport.start();
                assertThat(nettyTransport.lifecycleState(), is(Lifecycle.State.STARTED));

                assertExpectedPorts(originalSettings, settings);
                break;
            } catch (BindTransportException e) {
                logger.warn("Could not bind to port, on test run {}: {}", runs, e.getMessage());
            } finally {
                shutdownNettyTransport();
            }
        }
    }

    public void shutdownNettyTransport() {
        if (nettyTransport != null) {
            nettyTransport.stop();
        }
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    private void assertExpectedPorts(Settings originalSettings, Settings settings) throws Exception {
        for (Map.Entry<String, String> entry : originalSettings.getAsMap().entrySet()) {
            String bindHostSettingName = entry.getKey().replaceFirst("\\.port$", ".bind_host");
            String bindHost = settings.get(bindHostSettingName, "localhost");

            if (EXPECTED_CONNECTION_ACCEPTED_PORT.equals(entry.getValue())) {
                int port = settings.getAsInt(entry.getKey(), null);
                assertPortIsBound(bindHost, port);
                // if bindHost is not localhost, make sure connection is refused on localhost
                if (!"localhost".equals(bindHost)) {
                    assertConnectionRefused("localhost", port);
                }
            } else if (EXPECTED_CONNECTION_REFUSED_PORT.equals(entry.getValue())) {
                int port = settings.getAsInt(entry.getKey(), null);
                assertConnectionRefused(bindHost, port);
            }
        }
    }

    private void assertConnectionRefused(String host, int port) throws Exception {
        try {
            trySocketConnection(new InetSocketTransportAddress(host, port).address());
            fail("Expected to get exception when connecting to port " + port);
        } catch (IOException e) {
            // expected
            logger.info("Got expected connection message {}", e.getMessage());
        }
    }

    private void assertPortIsBound(String host, int port) throws Exception {
        logger.info("Trying to connect to [{}]:[{}]", host, port);
        trySocketConnection(new InetSocketTransportAddress(host, port).address());
    }

    private void trySocketConnection(InetSocketAddress address) throws Exception {
        try (Socket socket = new Socket()) {
            logger.info("Connecting to {}", address);
            socket.connect(address, 500);

            assertThat(socket.isConnected(), is(true));
            try (OutputStream os = socket.getOutputStream()) {
                os.write("foo".getBytes(Charsets.UTF_8));
                os.flush();
            }
        }
    }
}
