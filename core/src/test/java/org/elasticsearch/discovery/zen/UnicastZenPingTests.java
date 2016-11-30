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

import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.util.concurrent.EsExecutors;
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
import org.junit.Before;
import org.mockito.Matchers;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class UnicastZenPingTests extends ESTestCase {

    private ThreadPool threadPool;
    private ExecutorService executorService;
    // close in reverse order as opened
    private Stack<Closeable> closeables;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory("[" + getClass().getName() + "]");
        executorService =
            EsExecutors.newScaling(getClass().getName(), 0, 2, 60, TimeUnit.SECONDS, threadFactory, threadPool.getThreadContext());
        closeables = new Stack<>();
    }

    @After
    public void tearDown() throws Exception {
        try {
            // JDK stack is broken, it does not iterate in the expected order (http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4475301)
            final List<Closeable> reverse = new ArrayList<>();
            while (!closeables.isEmpty()) {
                reverse.add(closeables.pop());
            }
            IOUtils.close(reverse);
        } finally {
            terminate(executorService);
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
        Collection<ZenPing.PingResponse> pingResponses = zenPingA.pingAndWait(TimeValue.timeValueMillis(100));
        assertThat(pingResponses.size(), equalTo(1));
        ZenPing.PingResponse ping = pingResponses.iterator().next();
        assertThat(ping.node().getId(), equalTo("UZP_B"));
        assertThat(ping.getClusterStateVersion(), equalTo(state.version()));
        assertCounters(handleA, handleA, handleB, handleC, handleD);

        // ping again, this time from B,
        logger.info("ping from UZP_B");
        pingResponses = zenPingB.pingAndWait(TimeValue.timeValueMillis(100));
        assertThat(pingResponses.size(), equalTo(1));
        ping = pingResponses.iterator().next();
        assertThat(ping.node().getId(), equalTo("UZP_A"));
        assertThat(ping.getClusterStateVersion(), equalTo(ElectMasterService.MasterCandidate.UNRECOVERED_CLUSTER_VERSION));
        assertCounters(handleB, handleA, handleB, handleC, handleD);

        logger.info("ping from UZP_C");
        pingResponses = zenPingC.pingAndWait(TimeValue.timeValueMillis(100));
        assertThat(pingResponses.size(), equalTo(0));
        assertCounters(handleC, handleA, handleB, handleC, handleD);

        logger.info("ping from UZP_D");
        pingResponses = zenPingD.pingAndWait(TimeValue.timeValueMillis(100));
        assertThat(pingResponses.size(), equalTo(0));
        assertCounters(handleD, handleA, handleB, handleC, handleD);
    }

    public void testUnknownHostNotCached() {
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
            final Collection<ZenPing.PingResponse> pingResponses = zenPingA.pingAndWait(TimeValue.timeValueMillis(100));
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
            final Collection<ZenPing.PingResponse> secondPingResponses = zenPingC.pingAndWait(TimeValue.timeValueMillis(100));
            assertThat(secondPingResponses.size(), equalTo(2));
            final Set<String> ids = new HashSet<>(secondPingResponses.stream().map(p -> p.node().getId()).collect(Collectors.toList()));
            assertThat(ids, equalTo(new HashSet<>(Arrays.asList("UZP_A", "UZP_B"))));
            assertCounters(handleC, handleA, handleB, handleC);
        }
    }

    public void testPortLimit() throws InterruptedException {
        final NetworkService networkService = new NetworkService(Settings.EMPTY, Collections.emptyList());
        final Transport transport = new MockTcpTransport(
            Settings.EMPTY,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            Version.CURRENT);
        closeables.push(transport);
        final TransportService transportService =
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
        closeables.push(transportService);
        final AtomicInteger idGenerator = new AtomicInteger();
        final int limitPortCounts = randomIntBetween(1, 10);
        final List<DiscoveryNode> discoveryNodes = UnicastZenPing.resolveDiscoveryNodes(
            executorService,
            logger,
            Collections.singletonList("127.0.0.1"),
            limitPortCounts,
            transportService,
            () -> Integer.toString(idGenerator.incrementAndGet()),
            TimeValue.timeValueMillis(100));
        assertThat(discoveryNodes, hasSize(limitPortCounts));
        final Set<Integer> ports = new HashSet<>();
        for (final DiscoveryNode discoveryNode : discoveryNodes) {
            assertTrue(discoveryNode.getAddress().address().getAddress().isLoopbackAddress());
            ports.add(discoveryNode.getAddress().getPort());
        }
        assertThat(ports, equalTo(IntStream.range(9300, 9300 + limitPortCounts).mapToObj(m -> m).collect(Collectors.toSet())));
    }

    public void testUnknownHost() throws InterruptedException {
        final Logger logger = mock(Logger.class);
        final NetworkService networkService = new NetworkService(Settings.EMPTY, Collections.emptyList());
        final String hostname = randomAsciiOfLength(8);
        final UnknownHostException unknownHostException = new UnknownHostException(hostname);
        final Transport transport = new MockTcpTransport(
            Settings.EMPTY,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            Version.CURRENT) {

            @Override
            public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
                throw unknownHostException;
            }

        };
        closeables.push(transport);

        final TransportService transportService =
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
        closeables.push(transportService);
        final AtomicInteger idGenerator = new AtomicInteger();

        final List<DiscoveryNode> discoveryNodes = UnicastZenPing.resolveDiscoveryNodes(
            executorService,
            logger,
            Arrays.asList(hostname),
            1,
            transportService,
            () -> Integer.toString(idGenerator.incrementAndGet()),
            TimeValue.timeValueMillis(100)
        );

        assertThat(discoveryNodes, empty());
        verify(logger).warn("failed to resolve host [" + hostname + "]", unknownHostException);
    }

    public void testResolveTimeout() throws InterruptedException {
        final Logger logger = mock(Logger.class);
        final NetworkService networkService = new NetworkService(Settings.EMPTY, Collections.emptyList());
        final CountDownLatch latch = new CountDownLatch(1);
        final Transport transport = new MockTcpTransport(
            Settings.EMPTY,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            Version.CURRENT) {

            @Override
            public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
                if ("hostname1".equals(address)) {
                    return new TransportAddress[]{new TransportAddress(TransportAddress.META_ADDRESS, 9300)};
                } else if ("hostname2".equals(address)) {
                    try {
                        latch.await();
                        return new TransportAddress[]{new TransportAddress(TransportAddress.META_ADDRESS, 9300)};
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new UnknownHostException(address);
                }
            }

        };
        closeables.push(transport);

        final TransportService transportService =
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
        closeables.push(transportService);
        final AtomicInteger idGenerator = new AtomicInteger();
        final TimeValue resolveTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 200));
        try {
            final List<DiscoveryNode> discoveryNodes = UnicastZenPing.resolveDiscoveryNodes(
                executorService,
                logger,
                Arrays.asList("hostname1", "hostname2"),
                1,
                transportService,
                () -> Integer.toString(idGenerator.incrementAndGet()),
                resolveTimeout);

            assertThat(discoveryNodes, hasSize(1));
            verify(logger).trace(
                "resolved host [{}] to {}", "hostname1",
                new TransportAddress[]{new TransportAddress(TransportAddress.META_ADDRESS, 9300)});
            verify(logger).warn("timed out after [{}] resolving host [{}]", resolveTimeout, "hostname2");
            verifyNoMoreInteractions(logger);
        } finally {
            latch.countDown();
        }
    }

    public void testInvalidHosts() throws InterruptedException {
        final Logger logger = mock(Logger.class);
        final NetworkService networkService = new NetworkService(Settings.EMPTY, Collections.emptyList());
        final Transport transport = new MockTcpTransport(
            Settings.EMPTY,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            Version.CURRENT);
        closeables.push(transport);

        final TransportService transportService =
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
        closeables.push(transportService);
        final AtomicInteger idGenerator = new AtomicInteger();
        final List<DiscoveryNode> discoveryNodes = UnicastZenPing.resolveDiscoveryNodes(
            executorService,
            logger,
            Arrays.asList("127.0.0.1:9300:9300", "127.0.0.1:9301"),
            1,
            transportService,
            () -> Integer.toString(idGenerator.incrementAndGet()),
            TimeValue.timeValueMillis(100));
        assertThat(discoveryNodes, hasSize(1)); // only one of the two is valid and will be used
        assertThat(discoveryNodes.get(0).getAddress().getAddress(), equalTo("127.0.0.1"));
        assertThat(discoveryNodes.get(0).getAddress().getPort(), equalTo(9301));
        verify(logger).warn(eq("failed to resolve host [127.0.0.1:9300:9300]"), Matchers.any(ExecutionException.class));
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
        final TransportService transportService =
            new TransportService(settings, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
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
