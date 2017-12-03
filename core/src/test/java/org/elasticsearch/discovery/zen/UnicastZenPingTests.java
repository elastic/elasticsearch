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
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.mockito.Matchers;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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
            logger.info("shutting down...");
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

    public void testSimplePings() throws IOException, InterruptedException, ExecutionException {
        // use ephemeral ports
        final Settings settings = Settings.builder().put("cluster.name", "test").put(TcpTransport.PORT.getKey(), 0).build();
        final Settings settingsMismatch =
            Settings.builder().put(settings).put("cluster.name", "mismatch").put(TcpTransport.PORT.getKey(), 0).build();

        NetworkService networkService = new NetworkService(Collections.emptyList());

        final BiFunction<Settings, Version, Transport> supplier = (s, v) -> new MockTcpTransport(
            s,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            v) {
            @Override
            public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                                      CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator)
                throws ConnectTransportException {
                throw new AssertionError("zen pings should never connect to node (got [" + node + "])");
            }
        };

        NetworkHandle handleA = startServices(settings, threadPool, "UZP_A", Version.CURRENT, supplier);
        closeables.push(handleA.transportService);
        NetworkHandle handleB = startServices(settings, threadPool, "UZP_B", Version.CURRENT, supplier);
        closeables.push(handleB.transportService);
        NetworkHandle handleC = startServices(settingsMismatch, threadPool, "UZP_C", Version.CURRENT, supplier);
        closeables.push(handleC.transportService);
        final Version versionD;
        if (randomBoolean()) {
            versionD = VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT);
        } else {
            versionD = Version.CURRENT;
        }
        logger.info("UZP_D version set to [{}]", versionD);
        NetworkHandle handleD = startServices(settingsMismatch, threadPool, "UZP_D", versionD, supplier);
        closeables.push(handleD.transportService);

        final ClusterState state = ClusterState.builder(new ClusterName("test")).version(randomNonNegativeLong()).build();
        final ClusterState stateMismatch = ClusterState.builder(new ClusterName("mismatch")).version(randomNonNegativeLong()).build();

        Settings hostsSettings = Settings.builder()
            .putList("discovery.zen.ping.unicast.hosts",
                NetworkAddress.format(new InetSocketAddress(handleA.address.address().getAddress(), handleA.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleB.address.address().getAddress(), handleB.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleC.address.address().getAddress(), handleC.address.address().getPort())),
                NetworkAddress.format(new InetSocketAddress(handleD.address.address().getAddress(), handleD.address.address().getPort())))
            .put("cluster.name", "test")
            .build();

        Settings hostsSettingsMismatch = Settings.builder().put(hostsSettings).put(settingsMismatch).build();
        ClusterState stateA = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .nodes(DiscoveryNodes.builder().add(handleA.node).localNodeId("UZP_A"))
            .build();
        TestUnicastZenPing zenPingA = new TestUnicastZenPing(hostsSettings, threadPool, handleA, EMPTY_HOSTS_PROVIDER, () -> stateA);
        zenPingA.start();
        closeables.push(zenPingA);

        ClusterState stateB = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder().add(handleB.node).localNodeId("UZP_B"))
            .build();
        TestUnicastZenPing zenPingB = new TestUnicastZenPing(hostsSettings, threadPool, handleB, EMPTY_HOSTS_PROVIDER, () -> stateB);
        zenPingB.start();
        closeables.push(zenPingB);

        ClusterState stateC = ClusterState.builder(stateMismatch)
            .nodes(DiscoveryNodes.builder().add(handleC.node).localNodeId("UZP_C"))
            .build();
        TestUnicastZenPing zenPingC = new TestUnicastZenPing(hostsSettingsMismatch, threadPool, handleC,
            EMPTY_HOSTS_PROVIDER, () -> stateC) {
            @Override
            protected Version getVersion() {
                return versionD;
            }
        };
        zenPingC.start();
        closeables.push(zenPingC);

        ClusterState stateD = ClusterState.builder(stateMismatch)
            .nodes(DiscoveryNodes.builder().add(handleD.node).localNodeId("UZP_D"))
            .build();
        TestUnicastZenPing zenPingD = new TestUnicastZenPing(hostsSettingsMismatch, threadPool, handleD,
            EMPTY_HOSTS_PROVIDER, () -> stateD);
        zenPingD.start();
        closeables.push(zenPingD);

        logger.info("ping from UZP_A");
        Collection<ZenPing.PingResponse> pingResponses = zenPingA.pingAndWait().toList();
        assertThat(pingResponses.size(), equalTo(1));
        ZenPing.PingResponse ping = pingResponses.iterator().next();
        assertThat(ping.node().getId(), equalTo("UZP_B"));
        assertThat(ping.getClusterStateVersion(), equalTo(state.version()));
        assertPingCount(handleA, handleB, 3);
        assertPingCount(handleA, handleC, 0); // mismatch, shouldn't ping
        assertPingCount(handleA, handleD, 0); // mismatch, shouldn't ping

        // ping again, this time from B,
        logger.info("ping from UZP_B");
        pingResponses = zenPingB.pingAndWait().toList();
        assertThat(pingResponses.size(), equalTo(1));
        ping = pingResponses.iterator().next();
        assertThat(ping.node().getId(), equalTo("UZP_A"));
        assertThat(ping.getClusterStateVersion(), equalTo(ElectMasterService.MasterCandidate.UNRECOVERED_CLUSTER_VERSION));
        assertPingCount(handleB, handleA, 3);
        assertPingCount(handleB, handleC, 0); // mismatch, shouldn't ping
        assertPingCount(handleB, handleD, 0); // mismatch, shouldn't ping

        logger.info("ping from UZP_C");
        pingResponses = zenPingC.pingAndWait().toList();
        assertThat(pingResponses.size(), equalTo(1));
        assertPingCount(handleC, handleA, 0);
        assertPingCount(handleC, handleB, 0);
        assertPingCount(handleC, handleD, 3);

        logger.info("ping from UZP_D");
        pingResponses = zenPingD.pingAndWait().toList();
        assertThat(pingResponses.size(), equalTo(1));
        assertPingCount(handleD, handleA, 0);
        assertPingCount(handleD, handleB, 0);
        assertPingCount(handleD, handleC, 3);

        zenPingC.close();
        handleD.counters.clear();
        logger.info("ping from UZP_D after closing UZP_C");
        pingResponses = zenPingD.pingAndWait().toList();
        // check that node does not respond to pings anymore after the ping service has been closed
        assertThat(pingResponses.size(), equalTo(0));
        assertPingCount(handleD, handleA, 0);
        assertPingCount(handleD, handleB, 0);
        assertPingCount(handleD, handleC, 3);
    }

    public void testUnknownHostNotCached() throws ExecutionException, InterruptedException {
        // use ephemeral ports
        final Settings settings = Settings.builder().put("cluster.name", "test").put(TcpTransport.PORT.getKey(), 0).build();

        final NetworkService networkService = new NetworkService(Collections.emptyList());

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
            .putList("discovery.zen.ping.unicast.hosts", "UZP_A", "UZP_B", "UZP_C")
            .put("cluster.name", "test")
            .build();

        final ClusterState state = ClusterState.builder(new ClusterName("test")).version(randomNonNegativeLong()).build();

        ClusterState stateA = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .nodes(DiscoveryNodes.builder().add(handleA.node).localNodeId("UZP_A"))
            .build();
        final TestUnicastZenPing zenPingA = new TestUnicastZenPing(hostsSettings, threadPool, handleA, EMPTY_HOSTS_PROVIDER, () -> stateA);
        zenPingA.start();
        closeables.push(zenPingA);

        ClusterState stateB = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder().add(handleB.node).localNodeId("UZP_B"))
            .build();
        TestUnicastZenPing zenPingB = new TestUnicastZenPing(hostsSettings, threadPool, handleB, EMPTY_HOSTS_PROVIDER, () -> stateB);
        zenPingB.start();
        closeables.push(zenPingB);

        ClusterState stateC = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder().add(handleC.node).localNodeId("UZP_C"))
            .build();
        TestUnicastZenPing zenPingC = new TestUnicastZenPing(hostsSettings, threadPool, handleC, EMPTY_HOSTS_PROVIDER, () -> stateC);
        zenPingC.start();
        closeables.push(zenPingC);

        // the presence of an unresolvable host should not prevent resolvable hosts from being pinged
        {
            final Collection<ZenPing.PingResponse> pingResponses = zenPingA.pingAndWait().toList();
            assertThat(pingResponses.size(), equalTo(1));
            ZenPing.PingResponse ping = pingResponses.iterator().next();
            assertThat(ping.node().getId(), equalTo("UZP_C"));
            assertThat(ping.getClusterStateVersion(), equalTo(state.version()));
            assertPingCount(handleA, handleB, 0);
            assertPingCount(handleA, handleC, 3);
            assertNull(handleA.counters.get(handleB.address));
        }

        final HashMap<TransportAddress, Integer> moreThan = new HashMap<>();
        // we should see at least one ping to UZP_B, and one more ping than we have already seen to UZP_C
        moreThan.put(handleB.address, 0);
        moreThan.put(handleC.address, handleA.counters.get(handleC.address).intValue());

        // now allow UZP_B to be resolvable
        addresses.put(
            "UZP_B",
            new TransportAddress[]{
                new TransportAddress(
                    new InetSocketAddress(handleB.address.address().getAddress(), handleB.address.address().getPort()))});

        // now we should see pings to UZP_B; this establishes that host resolutions are not cached
        {
            handleA.counters.clear();
            final Collection<ZenPing.PingResponse> secondPingResponses = zenPingA.pingAndWait().toList();
            assertThat(secondPingResponses.size(), equalTo(2));
            final Set<String> ids = new HashSet<>(secondPingResponses.stream().map(p -> p.node().getId()).collect(Collectors.toList()));
            assertThat(ids, equalTo(new HashSet<>(Arrays.asList("UZP_B", "UZP_C"))));
            assertPingCount(handleA, handleB, 3);
            assertPingCount(handleA, handleC, 3);
        }
    }

    public void testPortLimit() throws InterruptedException {
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final Transport transport = new MockTcpTransport(
            Settings.EMPTY,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            Version.CURRENT) {

            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[]{new TransportAddress(InetAddress.getLoopbackAddress(), 9500)},
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9500)
                );
            }
        };
        closeables.push(transport);
        final TransportService transportService =
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null);
        closeables.push(transportService);
        final int limitPortCounts = randomIntBetween(1, 10);
        final List<DiscoveryNode> discoveryNodes = TestUnicastZenPing.resolveHostsLists(
            executorService,
            logger,
            Collections.singletonList("127.0.0.1"),
            limitPortCounts,
            transportService,
            "test_",
            TimeValue.timeValueSeconds(1));
        assertThat(discoveryNodes, hasSize(limitPortCounts));
        final Set<Integer> ports = new HashSet<>();
        for (final DiscoveryNode discoveryNode : discoveryNodes) {
            assertTrue(discoveryNode.getAddress().address().getAddress().isLoopbackAddress());
            ports.add(discoveryNode.getAddress().getPort());
        }
        assertThat(ports, equalTo(IntStream.range(9300, 9300 + limitPortCounts).mapToObj(m -> m).collect(Collectors.toSet())));
    }

    public void testRemovingLocalAddresses() throws InterruptedException {
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final InetAddress loopbackAddress = InetAddress.getLoopbackAddress();
        final Transport transport = new MockTcpTransport(
            Settings.EMPTY,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            Version.CURRENT) {

            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[]{
                        new TransportAddress(loopbackAddress, 9300),
                        new TransportAddress(loopbackAddress, 9301)
                    },
                    new TransportAddress(loopbackAddress, 9302)
                );
            }
        };
        closeables.push(transport);
        final TransportService transportService =
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null);
        closeables.push(transportService);
        final List<DiscoveryNode> discoveryNodes = TestUnicastZenPing.resolveHostsLists(
            executorService,
            logger,
            Collections.singletonList(NetworkAddress.format(loopbackAddress)),
            10,
            transportService,
            "test_",
            TimeValue.timeValueSeconds(1));
        assertThat(discoveryNodes, hasSize(7));
        final Set<Integer> ports = new HashSet<>();
        for (final DiscoveryNode discoveryNode : discoveryNodes) {
            assertTrue(discoveryNode.getAddress().address().getAddress().isLoopbackAddress());
            ports.add(discoveryNode.getAddress().getPort());
        }
        assertThat(ports, equalTo(IntStream.range(9303, 9310).mapToObj(m -> m).collect(Collectors.toSet())));
    }

    public void testUnknownHost() throws InterruptedException {
        final Logger logger = mock(Logger.class);
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final String hostname = randomAlphaOfLength(8);
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
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[]{new TransportAddress(InetAddress.getLoopbackAddress(), 9300)},
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300)
                );
            }

            @Override
            public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
                throw unknownHostException;
            }

        };
        closeables.push(transport);

        final TransportService transportService =
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null);
        closeables.push(transportService);

        final List<DiscoveryNode> discoveryNodes = TestUnicastZenPing.resolveHostsLists(
            executorService,
            logger,
            Arrays.asList(hostname),
            1,
            transportService,
            "test_",
            TimeValue.timeValueSeconds(1)
        );

        assertThat(discoveryNodes, empty());
        verify(logger).warn("failed to resolve host [" + hostname + "]", unknownHostException);
    }

    public void testResolveTimeout() throws InterruptedException {
        final Logger logger = mock(Logger.class);
        final NetworkService networkService = new NetworkService(Collections.emptyList());
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
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[]{new TransportAddress(InetAddress.getLoopbackAddress(), 9500)},
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9500)
                );
            }

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
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null);
        closeables.push(transportService);
        final TimeValue resolveTimeout = TimeValue.timeValueSeconds(randomIntBetween(1, 3));
        try {
            final List<DiscoveryNode> discoveryNodes = TestUnicastZenPing.resolveHostsLists(
                executorService,
                logger,
                Arrays.asList("hostname1", "hostname2"),
                1,
                transportService,
                "test+",
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

    public void testResolveReuseExistingNodeConnections() throws ExecutionException, InterruptedException {
        final Settings settings = Settings.builder().put("cluster.name", "test").put(TcpTransport.PORT.getKey(), 0).build();

        NetworkService networkService = new NetworkService(Collections.emptyList());

        final BiFunction<Settings, Version, Transport> supplier = (s, v) -> new MockTcpTransport(
            s,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            v);

        NetworkHandle handleA = startServices(settings, threadPool, "UZP_A", Version.CURRENT, supplier, EnumSet.allOf(Role.class));
        closeables.push(handleA.transportService);
        NetworkHandle handleB = startServices(settings, threadPool, "UZP_B", Version.CURRENT, supplier, EnumSet.allOf(Role.class));
        closeables.push(handleB.transportService);

        final boolean useHosts = randomBoolean();
        final Settings.Builder hostsSettingsBuilder = Settings.builder().put("cluster.name", "test");
        if (useHosts) {
            hostsSettingsBuilder.putList("discovery.zen.ping.unicast.hosts",
                NetworkAddress.format(new InetSocketAddress(handleB.address.address().getAddress(), handleB.address.address().getPort()))
            );
        } else {
            hostsSettingsBuilder.put("discovery.zen.ping.unicast.hosts", (String) null);
        }
        final Settings hostsSettings = hostsSettingsBuilder.build();
        final ClusterState state = ClusterState.builder(new ClusterName("test")).version(randomNonNegativeLong()).build();

        // connection to reuse
        handleA.transportService.connectToNode(handleB.node);

        // install a listener to check that no new connections are made
        handleA.transportService.addConnectionListener(new TransportConnectionListener() {
            @Override
            public void onConnectionOpened(Transport.Connection connection) {
                fail("should not open any connections. got [" + connection.getNode() + "]");
            }
        });

        final ClusterState stateA = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .nodes(DiscoveryNodes.builder().add(handleA.node).add(handleB.node).localNodeId("UZP_A"))
            .build();
        final TestUnicastZenPing zenPingA = new TestUnicastZenPing(hostsSettings, threadPool, handleA, EMPTY_HOSTS_PROVIDER, () -> stateA);
        zenPingA.start();
        closeables.push(zenPingA);

        final ClusterState stateB = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder().add(handleB.node).localNodeId("UZP_B"))
            .build();
        TestUnicastZenPing zenPingB = new TestUnicastZenPing(hostsSettings, threadPool, handleB, EMPTY_HOSTS_PROVIDER, () -> stateB);
        zenPingB.start();
        closeables.push(zenPingB);

        Collection<ZenPing.PingResponse> pingResponses = zenPingA.pingAndWait().toList();
        assertThat(pingResponses.size(), equalTo(1));
        ZenPing.PingResponse ping = pingResponses.iterator().next();
        assertThat(ping.node().getId(), equalTo("UZP_B"));
        assertThat(ping.getClusterStateVersion(), equalTo(state.version()));

    }

    public void testPingingTemporalPings() throws ExecutionException, InterruptedException {
        final Settings settings = Settings.builder().put("cluster.name", "test").put(TcpTransport.PORT.getKey(), 0).build();

        NetworkService networkService = new NetworkService(Collections.emptyList());

        final BiFunction<Settings, Version, Transport> supplier = (s, v) -> new MockTcpTransport(
            s,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            v);

        NetworkHandle handleA = startServices(settings, threadPool, "UZP_A", Version.CURRENT, supplier, EnumSet.allOf(Role.class));
        closeables.push(handleA.transportService);
        NetworkHandle handleB = startServices(settings, threadPool, "UZP_B", Version.CURRENT, supplier, EnumSet.allOf(Role.class));
        closeables.push(handleB.transportService);

        final Settings hostsSettings = Settings.builder()
            .put("cluster.name", "test")
            .put("discovery.zen.ping.unicast.hosts", (String) null) // use nodes for simplicity
            .build();
        final ClusterState state = ClusterState.builder(new ClusterName("test")).version(randomNonNegativeLong()).build();
        final ClusterState stateA = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .nodes(DiscoveryNodes.builder().add(handleA.node).add(handleB.node).localNodeId("UZP_A")).build();

        final TestUnicastZenPing zenPingA = new TestUnicastZenPing(hostsSettings, threadPool, handleA, EMPTY_HOSTS_PROVIDER, () -> stateA);
        zenPingA.start();
        closeables.push(zenPingA);

        // Node B doesn't know about A!
        final ClusterState stateB = ClusterState.builder(state).nodes(
            DiscoveryNodes.builder().add(handleB.node).localNodeId("UZP_B")).build();
        TestUnicastZenPing zenPingB = new TestUnicastZenPing(hostsSettings, threadPool, handleB, EMPTY_HOSTS_PROVIDER, () -> stateB);
        zenPingB.start();
        closeables.push(zenPingB);

        {
            logger.info("pinging from UZP_A so UZP_B will learn about it");
            Collection<ZenPing.PingResponse> pingResponses = zenPingA.pingAndWait().toList();
            assertThat(pingResponses.size(), equalTo(1));
            ZenPing.PingResponse ping = pingResponses.iterator().next();
            assertThat(ping.node().getId(), equalTo("UZP_B"));
            assertThat(ping.getClusterStateVersion(), equalTo(state.version()));
        }
        {
            logger.info("pinging from UZP_B");
            Collection<ZenPing.PingResponse> pingResponses = zenPingB.pingAndWait().toList();
            assertThat(pingResponses.size(), equalTo(1));
            ZenPing.PingResponse ping = pingResponses.iterator().next();
            assertThat(ping.node().getId(), equalTo("UZP_A"));
            assertThat(ping.getClusterStateVersion(), equalTo(-1L)); // A has a block
        }
    }

    public void testInvalidHosts() throws InterruptedException {
        final Logger logger = mock(Logger.class);
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final Transport transport = new MockTcpTransport(
            Settings.EMPTY,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            networkService,
            Version.CURRENT) {
            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[]{new TransportAddress(InetAddress.getLoopbackAddress(), 9300)},
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300)
                );
            }
        };
        closeables.push(transport);

        final TransportService transportService =
            new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null);
        closeables.push(transportService);
        final List<DiscoveryNode> discoveryNodes = TestUnicastZenPing.resolveHostsLists(
            executorService,
            logger,
            Arrays.asList("127.0.0.1:9300:9300", "127.0.0.1:9301"),
            1,
            transportService,
            "test_",
            TimeValue.timeValueSeconds(1));
        assertThat(discoveryNodes, hasSize(1)); // only one of the two is valid and will be used
        assertThat(discoveryNodes.get(0).getAddress().getAddress(), equalTo("127.0.0.1"));
        assertThat(discoveryNodes.get(0).getAddress().getPort(), equalTo(9301));
        verify(logger).warn(eq("failed to resolve host [127.0.0.1:9300:9300]"), Matchers.any(ExecutionException.class));
    }

    private void assertPingCount(final NetworkHandle fromNode, final NetworkHandle toNode, int expectedCount) {
        final AtomicInteger counter = fromNode.counters.getOrDefault(toNode.address, new AtomicInteger());
        final String onNodeName = fromNode.node.getName();
        assertNotNull("handle for [" + onNodeName + "] has no 'expected' counter", counter);
        final String forNodeName = toNode.node.getName();
        assertThat("node [" + onNodeName + "] ping count to [" + forNodeName + "] is unexpected",
            counter.get(), equalTo(expectedCount));
    }

    private NetworkHandle startServices(
        final Settings settings,
        final ThreadPool threadPool,
        final String nodeId,
        final Version version,
        final BiFunction<Settings, Version, Transport> supplier) {
        return startServices(settings, threadPool, nodeId, version, supplier, emptySet());

    }

    private NetworkHandle startServices(
        final Settings settings,
        final ThreadPool threadPool,
        final String nodeId,
        final Version version,
        final BiFunction<Settings, Version, Transport> supplier,
        final Set<Role> nodeRoles) {
        final Settings nodeSettings = Settings.builder().put(settings)
            .put("node.name", nodeId)
            .put(TransportService.TRACE_LOG_INCLUDE_SETTING.getKey(), "internal:discovery/zen/unicast")
            .build();
        final Transport transport = supplier.apply(nodeSettings, version);
        final MockTransportService transportService =
            new MockTransportService(nodeSettings, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR,  boundAddress ->
                new DiscoveryNode(nodeId, nodeId, boundAddress.publishAddress(), emptyMap(), nodeRoles, version), null);
        transportService.start();
        transportService.acceptIncomingRequests();
        final ConcurrentMap<TransportAddress, AtomicInteger> counters = ConcurrentCollections.newConcurrentMap();
        transportService.addTracer(new MockTransportService.Tracer() {
            @Override
            public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
                counters.computeIfAbsent(node.getAddress(), k -> new AtomicInteger());
                counters.get(node.getAddress()).incrementAndGet();
            }
        });
        return new NetworkHandle(transport.boundAddress().publishAddress(), transportService, transportService.getLocalNode(), counters);
    }

    private static class NetworkHandle {

        public final TransportAddress address;
        public final TransportService transportService;
        public final DiscoveryNode node;
        public final ConcurrentMap<TransportAddress, AtomicInteger> counters;

        NetworkHandle(
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

    private static class TestUnicastZenPing extends UnicastZenPing {

        TestUnicastZenPing(Settings settings, ThreadPool threadPool, NetworkHandle networkHandle,
                           UnicastHostsProvider unicastHostsProvider, PingContextProvider contextProvider) {
            super(Settings.builder().put("node.name", networkHandle.node.getName()).put(settings).build(),
                threadPool, networkHandle.transportService, unicastHostsProvider, contextProvider);
        }

        volatile CountDownLatch allTasksCompleted;
        volatile AtomicInteger pendingTasks;
        volatile CountDownLatch pingingRoundClosed;

        PingCollection pingAndWait() throws ExecutionException, InterruptedException {
            allTasksCompleted = new CountDownLatch(1);
            pingingRoundClosed = new CountDownLatch(1);
            pendingTasks = new AtomicInteger();
            // mark the three sending rounds as ongoing
            markTaskAsStarted("send pings");
            markTaskAsStarted("send pings");
            markTaskAsStarted("send pings");
            final AtomicReference<PingCollection> response = new AtomicReference<>();
            ping(response::set, TimeValue.timeValueMillis(1), TimeValue.timeValueSeconds(1));
            pingingRoundClosed.await();
            final PingCollection result = response.get();
            assertNotNull("pinging didn't complete",  result);
            return result;
        }

        @Override
        protected void finishPingingRound(PingingRound pingingRound) {
            // wait for all activity to finish before closing
            try {
                allTasksCompleted.await();
            } catch (InterruptedException e) {
                // ok, finish anyway
            }
            super.finishPingingRound(pingingRound);
            pingingRoundClosed.countDown();
        }

        @Override
        protected void sendPings(TimeValue timeout, PingingRound pingingRound) {
            super.sendPings(timeout, pingingRound);
            markTaskAsCompleted("send pings");
        }

        @Override
        protected void submitToExecutor(AbstractRunnable abstractRunnable) {
            markTaskAsStarted("executor runnable");
            super.submitToExecutor(new AbstractRunnable() {
                @Override
                public void onRejection(Exception e) {
                    try {
                        super.onRejection(e);
                    } finally {
                        markTaskAsCompleted("executor runnable (rejected)");
                    }
                }

                @Override
                public void onAfter() {
                    markTaskAsCompleted("executor runnable");
                }

                @Override
                protected void doRun() throws Exception {
                    abstractRunnable.run();
                }

                @Override
                public void onFailure(Exception e) {
                    // we shouldn't really end up here.
                    throw new AssertionError("unexpected error", e);
                }
            });
        }

        private void markTaskAsStarted(String task) {
            logger.trace("task [{}] started. count [{}]", task, pendingTasks.incrementAndGet());
        }

        private void markTaskAsCompleted(String task) {
            final int left = pendingTasks.decrementAndGet();
            logger.trace("task [{}] completed. count [{}]", task, left);
            if (left == 0) {
                allTasksCompleted.countDown();
            }
        }

        @Override
        protected TransportResponseHandler<UnicastPingResponse> getPingResponseHandler(PingingRound pingingRound, DiscoveryNode node) {
            markTaskAsStarted("ping [" + node + "]");
            TransportResponseHandler<UnicastPingResponse> original = super.getPingResponseHandler(pingingRound, node);
            return new TransportResponseHandler<UnicastPingResponse>() {
                @Override
                public UnicastPingResponse newInstance() {
                    return original.newInstance();
                }

                @Override
                public void handleResponse(UnicastPingResponse response) {
                    original.handleResponse(response);
                    markTaskAsCompleted("ping [" + node + "]");
                }

                @Override
                public void handleException(TransportException exp) {
                    original.handleException(exp);
                    markTaskAsCompleted("ping [" + node + "] (error)");
                }

                @Override
                public String executor() {
                    return original.executor();
                }
            };
        }
    }

}
