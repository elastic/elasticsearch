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

import com.carrotsearch.hppc.IntOpenHashSet;
import com.google.common.base.Charsets;
import org.elasticsearch.Version;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cache.recycler.MockBigArrays;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 1)
public class NettyTransportMultiPortTests extends ElasticsearchTestCase {

    private NettyTransport nettyTransport;
    private ThreadPool threadPool;

    @After
    public void shutdownNettyTransport() {
        if (nettyTransport != null) {
            nettyTransport.stop();
        }
        if (threadPool != null) {
            threadPool.shutdownNow();
        }

    }

    @Test
    public void testThatNettyCanBindToMultiplePorts() throws Exception {
        int[] ports = getRandomPorts(3);

        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", ports[0])
                .put("transport.profiles.default.port", ports[1])
                .put("transport.profiles.client1.port", ports[2])
                .build();

        startNettyTransport(settings);

        assertConnectionRefused(ports[0]);
        assertPortIsBound(ports[1]);
        assertPortIsBound(ports[2]);
    }

    @Test
    public void testThatDefaultProfileInheritsFromStandardSettings() throws Exception {
        int[] ports = getRandomPorts(2);

        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", ports[0])
                .put("transport.profiles.client1.port", ports[1])
                .build();

        startNettyTransport(settings);

        assertPortIsBound(ports[0]);
        assertPortIsBound(ports[1]);
    }

    @Test
    public void testThatProfileWithoutPortSettingsFails() throws Exception {
        int[] ports = getRandomPorts(1);

        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", ports[0])
                .put("transport.profiles.client1.whatever", "foo")
                .build();

        startNettyTransport(settings);

        assertPortIsBound(ports[0]);
    }

    @Test
    public void testThatDefaultProfilePortOverridesGeneralConfiguration() throws Exception {
        int[] ports = getRandomPorts(3);

        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", ports[0])
                .put("transport.netty.port", ports[1])
                .put("transport.profiles.default.port", ports[2])
                .build();

        startNettyTransport(settings);

        assertConnectionRefused(ports[0]);
        assertConnectionRefused(ports[1]);
        assertPortIsBound(ports[2]);
    }

    @Test
    @Network
    public void testThatBindingOnDifferentHostsWorks() throws Exception {
        int[] ports = getRandomPorts(2);
        InetAddress firstNonLoopbackAddress = NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.StackType.IPv4);

        Settings settings = settingsBuilder()
                .put("network.host", "127.0.0.1")
                .put("transport.tcp.port", ports[0])
                .put("transport.profiles.default.bind_host", "127.0.0.1")
                .put("transport.profiles.client1.bind_host", firstNonLoopbackAddress.getHostAddress())
                .put("transport.profiles.client1.port", ports[1])
                .build();

        startNettyTransport(settings);

        assertPortIsBound("127.0.0.1", ports[0]);
        assertPortIsBound(firstNonLoopbackAddress.getHostAddress(), ports[1]);
        assertConnectionRefused(ports[1]);
    }

    // TODO, make sure one can check more settings and that they are applied correctly

    private int[] getRandomPorts(int numberOfPorts) {
        IntOpenHashSet ports = new IntOpenHashSet();

        for (int i = 0; i < numberOfPorts; i++) {
            int port = randomIntBetween(1025, 65000);
            while (ports.contains(port)) {
                port = randomIntBetween(1025, 65000);
            }
            ports.add(port);
        }

        return ports.toArray();
    }

    private void startNettyTransport(Settings settings) {
        threadPool = new ThreadPool("tst");
        BigArrays bigArrays = new MockBigArrays(settings, new PageCacheRecycler(settings, threadPool), new NoneCircuitBreakerService());

        nettyTransport = new NettyTransport(settings, threadPool, new NetworkService(settings), bigArrays, Version.CURRENT);
        nettyTransport.start();

        assertThat(nettyTransport.lifecycleState(), is(Lifecycle.State.STARTED));
    }

    private void assertConnectionRefused(int port) throws Exception {
        try {
            trySocketConnection(new InetSocketTransportAddress("localhost", port).address());
            fail("Expected to get exception when connecting to port " + port);
        } catch (IOException e) {
            // expected
            logger.info("Got expected connection message {}", e.getMessage());
        }
    }

    private void assertPortIsBound(int port) throws Exception {
        assertPortIsBound("localhost", port);
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
