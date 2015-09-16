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
package org.elasticsearch.transport.netty;

import com.carrotsearch.hppc.IntHashSet;
import java.nio.charset.StandardCharsets;
import org.elasticsearch.Version;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.test.junit.rule.RepeatOnExceptionRule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.is;

public class NettyTransportMultiPortTests extends ESTestCase {

    private static final int MAX_RETRIES = 10;
    private String host;

    @Rule
    public RepeatOnExceptionRule repeatOnBindExceptionRule = new RepeatOnExceptionRule(logger, MAX_RETRIES, BindTransportException.class);

    @Before
    public void setup() {
        if (randomBoolean()) {
            host = "localhost";
        } else {
            if (NetworkUtils.SUPPORTS_V6 && randomBoolean()) {
                host = "::1";
            } else {
                host = "127.0.0.1";
            }
        }
    }

    @Test
    public void testThatNettyCanBindToMultiplePorts() throws Exception {
        int[] ports = getRandomPorts(3);

        Settings settings = settingsBuilder()
                .put("network.host", host)
                .put("transport.tcp.port", ports[0])
                .put("transport.profiles.default.port", ports[1])
                .put("transport.profiles.client1.port", ports[2])
                .build();

        ThreadPool threadPool = new ThreadPool("tst");
        try (NettyTransport ignored = startNettyTransport(settings, threadPool)) {
            assertConnectionRefused(ports[0]);
            assertPortIsBound(ports[1]);
            assertPortIsBound(ports[2]);
        } finally {
            terminate(threadPool);
        }
    }

    @Test
    public void testThatDefaultProfileInheritsFromStandardSettings() throws Exception {
        int[] ports = getRandomPorts(2);

        Settings settings = settingsBuilder()
                .put("network.host", host)
                .put("transport.tcp.port", ports[0])
                .put("transport.profiles.client1.port", ports[1])
                .build();

        ThreadPool threadPool = new ThreadPool("tst");
        try (NettyTransport ignored = startNettyTransport(settings, threadPool)) {
            assertPortIsBound(ports[0]);
            assertPortIsBound(ports[1]);
        } finally {
            terminate(threadPool);
        }
    }

    @Test
    public void testThatProfileWithoutPortSettingsFails() throws Exception {
        int[] ports = getRandomPorts(1);

        Settings settings = settingsBuilder()
                .put("network.host", host)
                .put("transport.tcp.port", ports[0])
                .put("transport.profiles.client1.whatever", "foo")
                .build();

        ThreadPool threadPool = new ThreadPool("tst");
        try (NettyTransport ignored = startNettyTransport(settings, threadPool)) {
            assertPortIsBound(ports[0]);
        } finally {
            terminate(threadPool);
        }
    }

    @Test
    public void testThatDefaultProfilePortOverridesGeneralConfiguration() throws Exception {
        int[] ports = getRandomPorts(3);

        Settings settings = settingsBuilder()
                .put("network.host", host)
                .put("transport.tcp.port", ports[0])
                .put("transport.netty.port", ports[1])
                .put("transport.profiles.default.port", ports[2])
                .build();

        ThreadPool threadPool = new ThreadPool("tst");
        try (NettyTransport ignored = startNettyTransport(settings, threadPool)) {
            assertConnectionRefused(ports[0]);
            assertConnectionRefused(ports[1]);
            assertPortIsBound(ports[2]);
        } finally {
            terminate(threadPool);
        }
    }

    @Test
    public void testThatProfileWithoutValidNameIsIgnored() throws Exception {
        int[] ports = getRandomPorts(3);

        Settings settings = settingsBuilder()
                .put("network.host", host)
                .put("transport.tcp.port", ports[0])
                // mimics someone trying to define a profile for .local which is the profile for a node request to itself
                .put("transport.profiles." + TransportService.DIRECT_RESPONSE_PROFILE + ".port", ports[1])
                .put("transport.profiles..port", ports[2])
                .build();

        ThreadPool threadPool = new ThreadPool("tst");
        try (NettyTransport ignored = startNettyTransport(settings, threadPool)) {
            assertPortIsBound(ports[0]);
            assertConnectionRefused(ports[1]);
            assertConnectionRefused(ports[2]);
        } finally {
            terminate(threadPool);
        }
    }

    private int[] getRandomPorts(int numberOfPorts) {
        IntHashSet ports = new IntHashSet();

        int nextPort = randomIntBetween(49152, 65535);
        for (int i = 0; i < numberOfPorts; i++) {
            boolean foundPortInRange = false;
            while (!foundPortInRange) {
                if (!ports.contains(nextPort)) {
                    logger.debug("looking to see if port [{}]is available", nextPort);
                    try (ServerSocket serverSocket = new ServerSocket()) {
                        // Set SO_REUSEADDR as we may bind here and not be able
                        // to reuse the address immediately without it.
                        serverSocket.setReuseAddress(NetworkUtils.defaultReuseAddress());
                        serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), nextPort));

                        // bind was a success
                        logger.debug("port [{}] available.", nextPort);
                        foundPortInRange = true;
                        ports.add(nextPort);
                    } catch (IOException e) {
                        // Do nothing
                        logger.debug("port [{}] not available.", e, nextPort);
                    }
                }
                nextPort = randomIntBetween(49152, 65535);
            }
        }
        return ports.toArray();
    }

    private NettyTransport startNettyTransport(Settings settings, ThreadPool threadPool) {
        BigArrays bigArrays = new MockBigArrays(new PageCacheRecycler(settings, threadPool), new NoneCircuitBreakerService());

        NettyTransport nettyTransport = new NettyTransport(settings, threadPool, new NetworkService(settings), bigArrays, Version.CURRENT, new NamedWriteableRegistry());
        nettyTransport.start();

        assertThat(nettyTransport.lifecycleState(), is(Lifecycle.State.STARTED));
        return nettyTransport;
    }

    private void assertConnectionRefused(int port) throws Exception {
        try {
            trySocketConnection(new InetSocketTransportAddress(InetAddress.getByName(host), port).address());
            fail("Expected to get exception when connecting to port " + port);
        } catch (IOException e) {
            // expected
            logger.info("Got expected connection message {}", e.getMessage());
        }
    }

    private void assertPortIsBound(int port) throws Exception {
        assertPortIsBound(host, port);
    }

    private void assertPortIsBound(String host, int port) throws Exception {
        logger.info("Trying to connect to [{}]:[{}]", host, port);
        trySocketConnection(new InetSocketTransportAddress(InetAddress.getByName(host), port).address());
    }

    private void trySocketConnection(InetSocketAddress address) throws Exception {
        try (Socket socket = new Socket()) {
            logger.info("Connecting to {}", address);
            socket.connect(address, 500);

            assertThat(socket.isConnected(), is(true));
            try (OutputStream os = socket.getOutputStream()) {
                os.write("foo".getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
        }
    }
}
