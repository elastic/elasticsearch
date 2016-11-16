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

package org.elasticsearch.discovery.zen;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.junit.After;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class UnicastZenPingTests extends ESTestCase {

    private ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private Stack<Closeable> closeables = new Stack<>();

    @After
    public void tearDown() throws Exception {
        try {
            // we need to close these in reverse order they were opened but Java stack is broken, it does not iterate in the expected order
            // (as if you were popping)
            final List<Closeable> reverse = new ArrayList<>();
            while (!closeables.isEmpty()) {
                reverse.add(closeables.pop());
            }
            IOUtils.close(reverse);
        } finally {
            terminate(threadPool);
            super.tearDown();
        }
    }

    private static final UnicastHostsProvider EMPTY_HOSTS_PROVIDER = Collections::emptyList;

    public void testSimplePings() throws IOException, InterruptedException {
        // use ephemeral ports
        final Settings settings = Settings.builder().put("cluster.name", "test").put(TransportSettings.PORT.getKey(), 0).build();
        final Settings settingsMismatch =
            Settings.builder().put(settings).put("cluster.name", "mismatch").put(TransportSettings.PORT.getKey(), 0).build();

        NetworkService networkService = new NetworkService(settings, Collections.emptyList());

        final BiFunction<Settings, Version, Transport> supplier = (s, v) -> new MockTcpTransport(
            s,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            v);

        NetworkHandle handleA = startServices(settings, threadPool, "UZP_A", Version.CURRENT, supplier);
        closeables.push(handleA.transportService);
        NetworkHandle handleB = startServices(settings, threadPool, "UZP_B", Version.CURRENT, supplier);
        closeables.push(handleB.transportService);
        NetworkHandle handleC = startServices(settingsMismatch, threadPool, "UZP_C", Version.CURRENT, supplier);
        closeables.push(handleC.transportService);
        // just fake that no versions are compatible with this node
        Version previousVersion = VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion());
        Version versionD = VersionUtils.randomVersionBetween(random(), previousVersion.minimumCompatibilityVersion(), previousVersion);
        NetworkHandle handleD = startServices(settingsMismatch, threadPool, "UZP_D", versionD, supplier);
        closeables.push(handleD.transportService);

        final ClusterState state = ClusterState.builder(new ClusterName("test")).version(randomPositiveLong()).build();

        Settings hostsSettings = Settings.builder()
                .putArray("discovery.zen.ping.unicast.hosts",
                NetworkAddress.format(new InetSocketAddress(handleA.address.address().getAddress(), handleA.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleB.address.address().getAddress(), handleB.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleC.address.address().getAddress(), handleC.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleD.address.address().getAddress(), handleD.address.address().getPort())))
                .put("cluster.name", "test")
                .build();

        Settings hostsSettingsMismatch = Settings.builder().put(hostsSettings).put(settingsMismatch).build();
        UnicastZenPing zenPingA = new UnicastZenPing(hostsSettings, threadPool, handleA.transportService, EMPTY_HOSTS_PROVIDER);
        zenPingA.start(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().add(handleA.node).localNodeId("UZP_A").build();
            }

            @Override
            public ClusterState clusterState() {
                return ClusterState.builder(state).blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)).build();
            }
        });
        closeables.push(zenPingA);

        UnicastZenPing zenPingB = new UnicastZenPing(hostsSettings, threadPool, handleB.transportService, EMPTY_HOSTS_PROVIDER);
        zenPingB.start(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().add(handleB.node).localNodeId("UZP_B").build();
            }

            @Override
            public ClusterState clusterState() {
                return state;
            }
        });
        closeables.push(zenPingB);

        UnicastZenPing zenPingC = new UnicastZenPing(hostsSettingsMismatch, threadPool, handleC.transportService, EMPTY_HOSTS_PROVIDER) {
            @Override
            protected Version getVersion() {
                return versionD;
            }
        };
        zenPingC.start(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().add(handleC.node).localNodeId("UZP_C").build();
            }

            @Override
            public ClusterState clusterState() {
                return state;
            }
        });
        closeables.push(zenPingC);

        UnicastZenPing zenPingD = new UnicastZenPing(hostsSettingsMismatch, threadPool, handleD.transportService, EMPTY_HOSTS_PROVIDER);
        zenPingD.start(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().add(handleD.node).localNodeId("UZP_D").build();
            }

            @Override
            public ClusterState clusterState() {
                return state;
            }
        });
        closeables.push(zenPingD);

        logger.info("ping from UZP_A");
        Collection<ZenPing.PingResponse> pingResponses = zenPingA.pingAndWait(TimeValue.timeValueSeconds(1));
        assertThat(pingResponses.size(), equalTo(1));
        ZenPing.PingResponse ping = pingResponses.iterator().next();
        assertThat(ping.node().getId(), equalTo("UZP_B"));
        assertThat(ping.getClusterStateVersion(), equalTo(state.version()));
        assertCounters(handleA, handleA, handleB, handleC, handleD);

        // ping again, this time from B,
        logger.info("ping from UZP_B");
        pingResponses = zenPingB.pingAndWait(TimeValue.timeValueSeconds(1));
        assertThat(pingResponses.size(), equalTo(1));
        ping = pingResponses.iterator().next();
        assertThat(ping.node().getId(), equalTo("UZP_A"));
        assertThat(ping.getClusterStateVersion(), equalTo(ElectMasterService.MasterCandidate.UNRECOVERED_CLUSTER_VERSION));
        assertCounters(handleB, handleA, handleB, handleC, handleD);

        logger.info("ping from UZP_C");
        pingResponses = zenPingC.pingAndWait(TimeValue.timeValueSeconds(1));
        assertThat(pingResponses.size(), equalTo(0));
        assertCounters(handleC, handleA, handleB, handleC, handleD);

        logger.info("ping from UZP_D");
        pingResponses = zenPingD.pingAndWait(TimeValue.timeValueSeconds(1));
        assertThat(pingResponses.size(), equalTo(0));
        assertCounters(handleD, handleA, handleB, handleC, handleD);
    }

    public void testUnknownHost() {
        // use ephemeral ports
        final Settings settings = Settings.builder().put("cluster.name", "test").put(TransportSettings.PORT.getKey(), 0).build();

        final NetworkService networkService = new NetworkService(settings, Collections.emptyList());

        final Map<String, TransportAddress[]> addresses = new HashMap<>();
        final BiFunction<Settings, Version, Transport> supplier = (s, v) -> new MockTcpTransport(
            s,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            v) {
            @Override
            public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
                final TransportAddress[] transportAddresses = addresses.get(address);
                if (transportAddresses == null) {
                    throw new UnknownHostException(address);
                } else {
                    return transportAddresses;
                }
            }
        };

        final NetworkHandle handleA = startServices(settings, threadPool, "UZP_A", Version.CURRENT, supplier);
        closeables.push(handleA.transportService);
        final NetworkHandle handleB = startServices(settings, threadPool, "UZP_B", Version.CURRENT, supplier);
        closeables.push(handleB.transportService);
        final NetworkHandle handleC = startServices(settings, threadPool, "UZP_C", Version.CURRENT, supplier);
        closeables.push(handleC.transportService);

        addresses.put(
            "UZP_A",
            new TransportAddress[]{
                new TransportAddress(
                    new InetSocketAddress(handleA.address.address().getAddress(), handleA.address.address().getPort()))});
        addresses.put(
            "UZP_C",
            new TransportAddress[]{
                new TransportAddress(
                    new InetSocketAddress(handleC.address.address().getAddress(), handleC.address.address().getPort()))});

        final Settings hostsSettings = Settings.builder()
            .putArray("discovery.zen.ping.unicast.hosts", "UZP_A", "UZP_B", "UZP_C")
            .put("cluster.name", "test")
            .build();

        final ClusterState state = ClusterState.builder(new ClusterName("test")).version(randomPositiveLong()).build();

        final UnicastZenPing zenPingA = new UnicastZenPing(hostsSettings, threadPool, handleA.transportService, EMPTY_HOSTS_PROVIDER);
        zenPingA.start(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().add(handleA.node).localNodeId("UZP_A").build();
            }

            @Override
            public ClusterState clusterState() {
                return ClusterState.builder(state).blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)).build();
            }
        });
        closeables.push(zenPingA);

        UnicastZenPing zenPingB = new UnicastZenPing(hostsSettings, threadPool, handleB.transportService, EMPTY_HOSTS_PROVIDER);
        zenPingB.start(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().add(handleB.node).localNodeId("UZP_B").build();
            }

            @Override
            public ClusterState clusterState() {
                return state;
            }
        });
        closeables.push(zenPingB);

        UnicastZenPing zenPingC = new UnicastZenPing(hostsSettings, threadPool, handleC.transportService, EMPTY_HOSTS_PROVIDER);
        zenPingC.start(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().add(handleC.node).localNodeId("UZP_C").build();
            }

            @Override
            public ClusterState clusterState() {
                return state;
            }
        });
        closeables.push(zenPingC);

        // the presence of an unresolvable host should not prevent resolvable hosts from being pinged
        {
            final Collection<ZenPing.PingResponse> pingResponses = zenPingA.pingAndWait(TimeValue.timeValueSeconds(3));
            assertThat(pingResponses.size(), equalTo(1));
            ZenPing.PingResponse ping = pingResponses.iterator().next();
            assertThat(ping.node().getId(), equalTo("UZP_C"));
            assertThat(ping.getClusterStateVersion(), equalTo(state.version()));
            assertCounters(handleA, handleA, handleC);
            assertNull(handleA.counters.get(handleB.address));
        }

        // now allow UZP_B to be resolvable
        addresses.put(
            "UZP_B",
            new TransportAddress[]{
                new TransportAddress(
                    new InetSocketAddress(handleB.address.address().getAddress(), handleB.address.address().getPort()))});

        // now we should see pings to UZP_B; this establishes that host resolutions are not cached
        {
            // ping from C so that we can assert on the counters from a fresh source (as opposed to resetting them)
            final Collection<ZenPing.PingResponse> secondPingResponses = zenPingC.pingAndWait(TimeValue.timeValueSeconds(3));
            assertThat(secondPingResponses.size(), equalTo(2));
            final Set<String> ids = new HashSet<>(secondPingResponses.stream().map(p -> p.node().getId()).collect(Collectors.toList()));
            assertThat(ids, equalTo(new HashSet<>(Arrays.asList("UZP_A", "UZP_B"))));
            assertCounters(handleC, handleA, handleB, handleC);
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

    private NetworkHandle startServices(
        final Settings settings,
        final ThreadPool threadPool,
        final String nodeId,
        final Version version,
        final BiFunction<Settings, Version, Transport> supplier) {
        final Transport transport = supplier.apply(settings, version);
        final TransportService transportService = new TransportService(settings, transport, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
        transportService.start();
        transportService.acceptIncomingRequests();
        final ConcurrentMap<TransportAddress, AtomicInteger> counters = ConcurrentCollections.newConcurrentMap();
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
        final DiscoveryNode node =
            new DiscoveryNode(nodeId, transportService.boundAddress().publishAddress(), emptyMap(), emptySet(), version);
        transportService.setLocalNode(node);
        return new NetworkHandle(transport.boundAddress().publishAddress(), transportService, node, counters);
    }

    private static class NetworkHandle {

        public final TransportAddress address;
        public final TransportService transportService;
        public final DiscoveryNode node;
        public final ConcurrentMap<TransportAddress, AtomicInteger> counters;

        public NetworkHandle(
            final TransportAddress address,
            final TransportService transportService,
            final DiscoveryNode discoveryNode,
            final ConcurrentMap<TransportAddress, AtomicInteger> counters) {
            this.address = address;
            this.transportService = transportService;
            this.node = discoveryNode;
            this.counters = counters;
        }

    }

}
