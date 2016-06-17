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

package org.elasticsearch.discovery.zen.ping.unicast;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.ping.PingContextProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty.NettyTransport;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class UnicastZenPingIT extends ESTestCase {
    public void testSimplePings() throws InterruptedException {
        int startPort = 11000 + randomIntBetween(0, 1000);
        int endPort = startPort + 10;
        Settings settings = Settings.builder()
            .put("cluster.name", "test")
            .put(TransportSettings.PORT.getKey(), startPort + "-" + endPort).build();

        Settings settingsMismatch = Settings.builder().put(settings)
            .put("cluster.name", "mismatch")
            .put(TransportSettings.PORT.getKey(), startPort + "-" + endPort).build();

        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        NetworkService networkService = new NetworkService(settings);
        ElectMasterService electMasterService = new ElectMasterService(settings, Version.CURRENT);

        NetworkHandle handleA = startServices(settings, threadPool, networkService, "UZP_A", Version.CURRENT);
        NetworkHandle handleB = startServices(settings, threadPool, networkService, "UZP_B", Version.CURRENT);
        NetworkHandle handleC = startServices(settingsMismatch, threadPool, networkService, "UZP_C", Version.CURRENT);
        // just fake that no versions are compatible with this node
        Version previousVersion = VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion());
        Version versionD = VersionUtils.randomVersionBetween(random(), previousVersion.minimumCompatibilityVersion(), previousVersion);
        NetworkHandle handleD = startServices(settingsMismatch, threadPool, networkService, "UZP_D", versionD);

        Settings hostsSettings = Settings.builder()
                .putArray("discovery.zen.ping.unicast.hosts",
                NetworkAddress.format(new InetSocketAddress(handleA.address.address().getAddress(), handleA.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleB.address.address().getAddress(), handleB.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleC.address.address().getAddress(), handleC.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleD.address.address().getAddress(), handleD.address.address().getPort())))
                .put("cluster.name", "test")
                .build();

        Settings hostsSettingsMismatch = Settings.builder().put(hostsSettings).put(settingsMismatch).build();
        UnicastZenPing zenPingA = new UnicastZenPing(hostsSettings, threadPool, handleA.transportService, Version.CURRENT, electMasterService, null);
        zenPingA.setPingContextProvider(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(handleA.node).localNodeId("UZP_A").build();
            }

            @Override
            public boolean nodeHasJoinedClusterOnce() {
                return false;
            }
        });
        zenPingA.start();

        UnicastZenPing zenPingB = new UnicastZenPing(hostsSettings, threadPool, handleB.transportService, Version.CURRENT, electMasterService, null);
        zenPingB.setPingContextProvider(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(handleB.node).localNodeId("UZP_B").build();
            }

            @Override
            public boolean nodeHasJoinedClusterOnce() {
                return true;
            }
        });
        zenPingB.start();

        UnicastZenPing zenPingC = new UnicastZenPing(hostsSettingsMismatch, threadPool, handleC.transportService, versionD, electMasterService, null);
        zenPingC.setPingContextProvider(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(handleC.node).localNodeId("UZP_C").build();
            }

            @Override
            public boolean nodeHasJoinedClusterOnce() {
                return false;
            }
        });
        zenPingC.start();

        UnicastZenPing zenPingD = new UnicastZenPing(hostsSettingsMismatch, threadPool, handleD.transportService, Version.CURRENT, electMasterService, null);
        zenPingD.setPingContextProvider(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(handleD.node).localNodeId("UZP_D").build();
            }

            @Override
            public boolean nodeHasJoinedClusterOnce() {
                return false;
            }
        });
        zenPingD.start();

        try {
            logger.info("ping from UZP_A");
            ZenPing.PingResponse[] pingResponses = zenPingA.pingAndWait(TimeValue.timeValueSeconds(1));
            assertThat(pingResponses.length, equalTo(1));
            assertThat(pingResponses[0].node().getId(), equalTo("UZP_B"));
            assertTrue(pingResponses[0].hasJoinedOnce());
            assertCounters(handleA, handleA, handleB, handleC, handleD);

            // ping again, this time from B,
            logger.info("ping from UZP_B");
            pingResponses = zenPingB.pingAndWait(TimeValue.timeValueSeconds(1));
            assertThat(pingResponses.length, equalTo(1));
            assertThat(pingResponses[0].node().getId(), equalTo("UZP_A"));
            assertFalse(pingResponses[0].hasJoinedOnce());
            assertCounters(handleB, handleA, handleB, handleC, handleD);

            logger.info("ping from UZP_C");
            pingResponses = zenPingC.pingAndWait(TimeValue.timeValueSeconds(1));
            assertThat(pingResponses.length, equalTo(0));
            assertCounters(handleC, handleA, handleB, handleC, handleD);

            logger.info("ping from UZP_D");
            pingResponses = zenPingD.pingAndWait(TimeValue.timeValueSeconds(1));
            assertThat(pingResponses.length, equalTo(0));
            assertCounters(handleD, handleA, handleB, handleC, handleD);
        } finally {
            zenPingA.close();
            zenPingB.close();
            zenPingC.close();
            zenPingD.close();
            handleA.transportService.close();
            handleB.transportService.close();
            handleC.transportService.close();
            handleD.transportService.close();
            terminate(threadPool);
        }
    }

    // assert that we tried to ping each of the configured nodes at least once
    private void assertCounters(NetworkHandle that, NetworkHandle...handles) {
        for (NetworkHandle handle : handles) {
            if (handle != that) {
                assertThat(that.counters.get(handle.address).get(), greaterThan(0));
            }
        }
    }

    private NetworkHandle startServices(Settings settings, ThreadPool threadPool, NetworkService networkService, String nodeId, Version version) {
        NettyTransport transport = new NettyTransport(settings, threadPool, networkService, BigArrays.NON_RECYCLING_INSTANCE, version, new NamedWriteableRegistry(), new NoneCircuitBreakerService());
        final TransportService transportService = new TransportService(settings, transport, threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        ConcurrentMap<TransportAddress, AtomicInteger> counters = new ConcurrentHashMap<>();
        transportService.addConnectionListener(new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node) {
                counters.computeIfAbsent(node.getAddress(), k -> new AtomicInteger());
                counters.get(node.getAddress()).incrementAndGet();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
            }
        });
        final DiscoveryNode node = new DiscoveryNode(nodeId, transportService.boundAddress().publishAddress(), emptyMap(), emptySet(), version);
        transportService.setLocalNode(node);
        return new NetworkHandle((InetSocketTransportAddress)transport.boundAddress().publishAddress(), transportService, node, counters);
    }

    private static class NetworkHandle {
        public final InetSocketTransportAddress address;
        public final TransportService transportService;
        public final DiscoveryNode node;
        public final ConcurrentMap<TransportAddress, AtomicInteger> counters;

        public NetworkHandle(InetSocketTransportAddress address, TransportService transportService, DiscoveryNode discoveryNode, ConcurrentMap<TransportAddress, AtomicInteger> counters) {
            this.address = address;
            this.transportService = transportService;
            this.node = discoveryNode;
            this.counters = counters;
        }
    }
}
