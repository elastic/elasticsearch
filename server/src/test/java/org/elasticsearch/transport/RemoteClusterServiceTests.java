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
package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.startsWith;

public class RemoteClusterServiceTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes, Version version) {
        return startTransport(id, knownNodes, version, Settings.EMPTY);
    }

    private MockTransportService startTransport(
            final String id,
            final List<DiscoveryNode> knownNodes,
            final Version version,
            final Settings settings) {
        return RemoteClusterConnectionTests.startTransport(
                id, knownNodes, version, threadPool, settings);
    }

    public void testSettingsAreRegistered() {
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterAware.REMOTE_CLUSTERS_SEEDS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CONNECTIONS_PER_CLUSTER));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_NODE_ATTRIBUTE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTER_COMPRESS));
    }

    public void testRemoteClusterSeedSetting() {
        // simple validation
        Settings settings = Settings.builder()
            .put("cluster.remote.foo.seeds", "192.168.0.1:8080")
            .put("cluster.remote.bar.seed", "[::1]:9090").build();
        RemoteClusterAware.REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));

        Settings brokenSettings = Settings.builder()
            .put("cluster.remote.foo.seeds", "192.168.0.1").build();
        expectThrows(IllegalArgumentException.class, () ->
        RemoteClusterAware.REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(brokenSettings).forEach(setting -> setting.get(brokenSettings)));

        Settings brokenPortSettings = Settings.builder()
            .put("cluster.remote.foo.seeds", "192.168.0.1:123456789123456789").build();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> RemoteClusterAware.REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(brokenSettings)
                .forEach(setting -> setting.get(brokenPortSettings))
        );
        assertEquals("failed to parse port", e.getMessage());
    }

    public void testBuildRemoteClustersDynamicConfig() throws Exception {
        Map<String, Tuple<String, List<Tuple<String, Supplier<DiscoveryNode>>>>> map =
                RemoteClusterService.buildRemoteClustersDynamicConfig(
                        Settings.builder()
                                .put("cluster.remote.foo.seeds", "192.168.0.1:8080")
                                .put("cluster.remote.bar.seeds", "[::1]:9090")
                                .put("cluster.remote.boom.seeds", "boom-node1.internal:1000")
                                .put("cluster.remote.boom.proxy", "foo.bar.com:1234")
                                .put("search.remote.quux.seeds", "quux:9300")
                                .put("search.remote.quux.proxy", "quux-proxy:19300")
                                .build());
        assertThat(map.keySet(), containsInAnyOrder(equalTo("foo"), equalTo("bar"), equalTo("boom"), equalTo("quux")));
        assertThat(map.get("foo").v2(), hasSize(1));
        assertThat(map.get("bar").v2(), hasSize(1));
        assertThat(map.get("boom").v2(), hasSize(1));
        assertThat(map.get("quux").v2(), hasSize(1));

        DiscoveryNode foo = map.get("foo").v2().get(0).v2().get();
        assertEquals("", map.get("foo").v1());
        assertEquals(foo.getAddress(), new TransportAddress(new InetSocketAddress(InetAddress.getByName("192.168.0.1"), 8080)));
        assertEquals(foo.getId(), "foo#192.168.0.1:8080");
        assertEquals(foo.getVersion(), Version.CURRENT.minimumCompatibilityVersion());

        DiscoveryNode bar = map.get("bar").v2().get(0).v2().get();
        assertEquals(bar.getAddress(), new TransportAddress(new InetSocketAddress(InetAddress.getByName("[::1]"), 9090)));
        assertEquals(bar.getId(), "bar#[::1]:9090");
        assertEquals("", map.get("bar").v1());
        assertEquals(bar.getVersion(), Version.CURRENT.minimumCompatibilityVersion());

        DiscoveryNode boom = map.get("boom").v2().get(0).v2().get();
        assertEquals(boom.getAddress(), new TransportAddress(TransportAddress.META_ADDRESS, 0));
        assertEquals("boom-node1.internal", boom.getHostName());
        assertEquals(boom.getId(), "boom#boom-node1.internal:1000");
        assertEquals("foo.bar.com:1234", map.get("boom").v1());
        assertEquals(boom.getVersion(), Version.CURRENT.minimumCompatibilityVersion());

        DiscoveryNode quux = map.get("quux").v2().get(0).v2().get();
        assertEquals(quux.getAddress(), new TransportAddress(TransportAddress.META_ADDRESS, 0));
        assertEquals("quux", quux.getHostName());
        assertEquals(quux.getId(), "quux#quux:9300");
        assertEquals("quux-proxy:19300", map.get("quux").v1());
        assertEquals(quux.getVersion(), Version.CURRENT.minimumCompatibilityVersion());

        assertSettingDeprecationsAndWarnings(new String[]{"search.remote.quux.seeds", "search.remote.quux.proxy"});
    }

    public void testBuildRemoteClustersDynamicConfigWithDuplicate() {
        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterService.buildRemoteClustersDynamicConfig(
                        Settings.builder()
                                .put("cluster.remote.foo.seeds", "192.168.0.1:8080")
                                .put("search.remote.foo.seeds", "192.168.0.1:8080")
                                .build()));
        assertThat(e, hasToString(containsString("found duplicate remote cluster configurations for cluster alias [foo]")));
        assertSettingDeprecationsAndWarnings(new String[]{"search.remote.foo.seeds"});
    }

    public void testBuildRemoteClustersDynamicConfigWithDuplicates() {
        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterService.buildRemoteClustersDynamicConfig(
                        Settings.builder()
                                .put("cluster.remote.foo.seeds", "192.168.0.1:8080")
                                .put("search.remote.foo.seeds", "192.168.0.1:8080")
                                .put("cluster.remote.bar.seeds", "192.168.0.1:8080")
                                .put("search.remote.bar.seeds", "192.168.0.1:8080")
                                .build()));
        assertThat(e, hasToString(containsString("found duplicate remote cluster configurations for cluster aliases [bar,foo]")));
        assertSettingDeprecationsAndWarnings(new String[]{"search.remote.bar.seeds", "search.remote.foo.seeds"});
    }

    public void testGroupClusterIndices() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
             MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalDiscoNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalDiscoNode();
            knownNodes.add(cluster1Transport.getLocalDiscoNode());
            knownNodes.add(cluster2Transport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    assertFalse(service.isRemoteClusterRegistered("foo"));
                    Map<String, List<String>> perClusterIndices = service.groupClusterIndices(service.getRemoteClusterNames(),
                        new String[]{"foo:bar", "cluster_1:bar", "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo",
                            "cluster*:baz", "*:boo", "no*match:boo"},
                        i -> false);
                    List<String> localIndices = perClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertNotNull(localIndices);
                    assertEquals(Arrays.asList("foo:bar", "foo", "no*match:boo"), localIndices);
                    assertEquals(2, perClusterIndices.size());
                    assertEquals(Arrays.asList("bar", "test", "baz", "boo"), perClusterIndices.get("cluster_1"));
                    assertEquals(Arrays.asList("foo:bar", "foo*", "baz", "boo"), perClusterIndices.get("cluster_2"));

                    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
                        service.groupClusterIndices(service.getRemoteClusterNames(), new String[]{"foo:bar", "cluster_1:bar",
                            "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo"}, "cluster_1:bar"::equals));

                    assertEquals("Can not filter indices; index cluster_1:bar exists but there is also a remote cluster named:" +
                            " cluster_1", iae.getMessage());
                }
            }
        }
    }

    public void testGroupIndices() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
             MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalDiscoNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalDiscoNode();
            knownNodes.add(cluster1Transport.getLocalDiscoNode());
            knownNodes.add(cluster2Transport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    assertFalse(service.isRemoteClusterRegistered("foo"));
                    {
                        Map<String, OriginalIndices> perClusterIndices = service.groupIndices(IndicesOptions.LENIENT_EXPAND_OPEN,
                            new String[]{"foo:bar", "cluster_1:bar", "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo",
                                "cluster*:baz", "*:boo", "no*match:boo"},
                            i -> false);
                        assertEquals(3, perClusterIndices.size());
                        assertArrayEquals(new String[]{"foo:bar", "foo", "no*match:boo"},
                            perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).indices());
                        assertArrayEquals(new String[]{"bar", "test", "baz", "boo"}, perClusterIndices.get("cluster_1").indices());
                        assertArrayEquals(new String[]{"foo:bar", "foo*", "baz", "boo"}, perClusterIndices.get("cluster_2").indices());
                    }
                    {
                        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
                            service.groupClusterIndices(service.getRemoteClusterNames(), new String[]{"foo:bar", "cluster_1:bar",
                                "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo"}, "cluster_1:bar"::equals));
                        assertEquals("Can not filter indices; index cluster_1:bar exists but there is also a remote cluster named:" +
                            " cluster_1", iae.getMessage());
                    }
                    {
                        Map<String, OriginalIndices> perClusterIndices = service.groupIndices(IndicesOptions.LENIENT_EXPAND_OPEN,
                            new String[]{"cluster_1:bar", "cluster_2:foo*"},
                            i -> false);
                        assertEquals(2, perClusterIndices.size());
                        assertArrayEquals(new String[]{"bar"}, perClusterIndices.get("cluster_1").indices());
                        assertArrayEquals(new String[]{"foo*"}, perClusterIndices.get("cluster_2").indices());
                    }
                    {
                        Map<String, OriginalIndices> perClusterIndices = service.groupIndices(IndicesOptions.LENIENT_EXPAND_OPEN,
                            Strings.EMPTY_ARRAY,
                            i -> false);
                        assertEquals(1, perClusterIndices.size());
                        assertArrayEquals(Strings.EMPTY_ARRAY, perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).indices());
                    }
                }
            }
        }
    }

    public void testIncrementallyAddClusters() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
             MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalDiscoNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalDiscoNode();
            knownNodes.add(cluster1Transport.getLocalDiscoNode());
            knownNodes.add(cluster2Transport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(Settings.EMPTY, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.updateRemoteCluster("cluster_1", Collections.singletonList(cluster1Seed.getAddress().toString()), null);
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    service.updateRemoteCluster("cluster_2", Collections.singletonList(cluster2Seed.getAddress().toString()), null);
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    service.updateRemoteCluster("cluster_2", Collections.emptyList(), null);
                    assertFalse(service.isRemoteClusterRegistered("cluster_2"));
                    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                        () -> service.updateRemoteCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, Collections.emptyList(), null));
                    assertEquals("remote clusters must not have the empty string as its key", iae.getMessage());
                }
            }
        }
    }

    public void testDefaultPingSchedule() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("cluster_1_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            TimeValue pingSchedule;
            Settings.Builder settingsBuilder = Settings.builder();
            settingsBuilder.putList("cluster.remote.cluster_1.seeds", seedNode.getAddress().toString());
            if (randomBoolean()) {
                pingSchedule = TimeValue.timeValueSeconds(randomIntBetween(1, 10));
                settingsBuilder.put(TransportSettings.PING_SCHEDULE.getKey(), pingSchedule).build();
            } else {
                pingSchedule = TimeValue.MINUS_ONE;
            }
            Settings settings = settingsBuilder.build();
            try (MockTransportService transportService = MockTransportService.createNewService(settings,
                Version.CURRENT, threadPool, null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                try (RemoteClusterService service = new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    service.updateRemoteCluster("cluster_1", Collections.singletonList(seedNode.getAddress().toString()), null);
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    RemoteClusterConnection remoteClusterConnection = service.getRemoteClusterConnection("cluster_1");
                    assertEquals(pingSchedule, remoteClusterConnection.getConnectionManager().getConnectionProfile().getPingInterval());
                }
            }
        }
    }

    public void testCustomPingSchedule() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
             MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalDiscoNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalDiscoNode();
            knownNodes.add(cluster1Transport.getLocalDiscoNode());
            knownNodes.add(cluster2Transport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder settingsBuilder = Settings.builder();
            if (randomBoolean()) {
                settingsBuilder.put(TransportSettings.PING_SCHEDULE.getKey(), TimeValue.timeValueSeconds(randomIntBetween(1, 10)));
            }
            Settings transportSettings = settingsBuilder.build();

            try (MockTransportService transportService = MockTransportService.createNewService(transportSettings, Version.CURRENT,
                threadPool, null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());
                TimeValue pingSchedule1 = // randomBoolean() ? TimeValue.MINUS_ONE :
                    TimeValue.timeValueSeconds(randomIntBetween(1, 10));
                builder.put("cluster.remote.cluster_1.transport.ping_schedule", pingSchedule1);
                TimeValue pingSchedule2 = //randomBoolean() ? TimeValue.MINUS_ONE :
                    TimeValue.timeValueSeconds(randomIntBetween(1, 10));
                builder.put("cluster.remote.cluster_2.transport.ping_schedule", pingSchedule2);
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    service.updateRemoteCluster("cluster_1", Collections.singletonList(cluster1Seed.getAddress().toString()), null);
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    RemoteClusterConnection remoteClusterConnection1 = service.getRemoteClusterConnection("cluster_1");
                    assertEquals(pingSchedule1, remoteClusterConnection1.getConnectionManager().getConnectionProfile().getPingInterval());
                    RemoteClusterConnection remoteClusterConnection2 = service.getRemoteClusterConnection("cluster_2");
                    assertEquals(pingSchedule2, remoteClusterConnection2.getConnectionManager().getConnectionProfile().getPingInterval());
                }
            }
        }
    }

    public void testRemoteNodeAttribute() throws IOException, InterruptedException {
        final Settings settings =
                Settings.builder().put("cluster.remote.node.attr", "gateway").build();
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Settings gateway = Settings.builder().put("node.attr.gateway", true).build();
        try (MockTransportService c1N1 =
                     startTransport("cluster_1_node_1", knownNodes, Version.CURRENT);
             MockTransportService c1N2 =
                     startTransport("cluster_1_node_2", knownNodes, Version.CURRENT, gateway);
             MockTransportService c2N1 =
                     startTransport("cluster_2_node_1", knownNodes, Version.CURRENT);
             MockTransportService c2N2 =
                     startTransport("cluster_2_node_2", knownNodes, Version.CURRENT, gateway)) {
            final DiscoveryNode c1N1Node = c1N1.getLocalDiscoNode();
            final DiscoveryNode c1N2Node = c1N2.getLocalDiscoNode();
            final DiscoveryNode c2N1Node = c2N1.getLocalDiscoNode();
            final DiscoveryNode c2N2Node = c2N2.getLocalDiscoNode();
            knownNodes.add(c1N1Node);
            knownNodes.add(c1N2Node);
            knownNodes.add(c2N1Node);
            knownNodes.add(c2N2Node);
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                final Settings.Builder builder = Settings.builder();
                builder.putList(
                        "cluster.remote.cluster_1.seeds", c1N1Node.getAddress().toString());
                builder.putList(
                        "cluster.remote.cluster_2.seeds", c2N1Node.getAddress().toString());
                try (RemoteClusterService service =
                             new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                            "cluster_1",
                            Arrays.asList(c1N1Node.getAddress().toString(), c1N2Node.getAddress().toString()), null,
                            connectionListener(firstLatch));
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                            "cluster_2",
                            Arrays.asList(c2N1Node.getAddress().toString(), c2N2Node.getAddress().toString()), null,
                            connectionListener(secondLatch));
                    secondLatch.await();

                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertFalse(service.isRemoteNodeConnected("cluster_1", c1N1Node));
                    assertTrue(service.isRemoteNodeConnected("cluster_1", c1N2Node));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    assertFalse(service.isRemoteNodeConnected("cluster_2", c2N1Node));
                    assertTrue(service.isRemoteNodeConnected("cluster_2", c2N2Node));
                    assertEquals(0, transportService.getConnectionManager().size());
                }
            }
        }
    }

    public void testRemoteNodeRoles() throws IOException, InterruptedException {
        final Settings settings = Settings.EMPTY;
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Settings data = Settings.builder().put("node.master", false).build();
        final Settings dedicatedMaster = Settings.builder().put("node.data", false).put("node.ingest", "false").build();
        try (MockTransportService c1N1 =
                     startTransport("cluster_1_node_1", knownNodes, Version.CURRENT, dedicatedMaster);
             MockTransportService c1N2 =
                     startTransport("cluster_1_node_2", knownNodes, Version.CURRENT, data);
             MockTransportService c2N1 =
                     startTransport("cluster_2_node_1", knownNodes, Version.CURRENT, dedicatedMaster);
             MockTransportService c2N2 =
                     startTransport("cluster_2_node_2", knownNodes, Version.CURRENT, data)) {
            final DiscoveryNode c1N1Node = c1N1.getLocalDiscoNode();
            final DiscoveryNode c1N2Node = c1N2.getLocalDiscoNode();
            final DiscoveryNode c2N1Node = c2N1.getLocalDiscoNode();
            final DiscoveryNode c2N2Node = c2N2.getLocalDiscoNode();
            knownNodes.add(c1N1Node);
            knownNodes.add(c1N2Node);
            knownNodes.add(c2N1Node);
            knownNodes.add(c2N2Node);
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                final Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", c1N1Node.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seeds", c2N1Node.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                            "cluster_1",
                            Arrays.asList(c1N1Node.getAddress().toString(), c1N2Node.getAddress().toString()), null,
                            connectionListener(firstLatch));
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                            "cluster_2",
                            Arrays.asList(c2N1Node.getAddress().toString(), c2N2Node.getAddress().toString()), null,
                            connectionListener(secondLatch));
                    secondLatch.await();

                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertFalse(service.isRemoteNodeConnected("cluster_1", c1N1Node));
                    assertTrue(service.isRemoteNodeConnected("cluster_1", c1N2Node));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    assertFalse(service.isRemoteNodeConnected("cluster_2", c2N1Node));
                    assertTrue(service.isRemoteNodeConnected("cluster_2", c2N2Node));
                    assertEquals(0, transportService.getConnectionManager().size());
                }
            }
        }
    }

    private ActionListener<Void> connectionListener(final CountDownLatch latch) {
        return ActionListener.wrap(x -> latch.countDown(), x -> fail());
    }

    public void testCollectNodes() throws InterruptedException, IOException {
        final Settings settings = Settings.EMPTY;
        final List<DiscoveryNode> knownNodes_c1 = new CopyOnWriteArrayList<>();
        final List<DiscoveryNode> knownNodes_c2 = new CopyOnWriteArrayList<>();

        try (MockTransportService c1N1 =
                 startTransport("cluster_1_node_1", knownNodes_c1, Version.CURRENT, settings);
             MockTransportService c1N2 =
                 startTransport("cluster_1_node_2", knownNodes_c1, Version.CURRENT, settings);
             MockTransportService c2N1 =
                 startTransport("cluster_2_node_1", knownNodes_c2, Version.CURRENT, settings);
             MockTransportService c2N2 =
                 startTransport("cluster_2_node_2", knownNodes_c2, Version.CURRENT, settings)) {
            final DiscoveryNode c1N1Node = c1N1.getLocalDiscoNode();
            final DiscoveryNode c1N2Node = c1N2.getLocalDiscoNode();
            final DiscoveryNode c2N1Node = c2N1.getLocalDiscoNode();
            final DiscoveryNode c2N2Node = c2N2.getLocalDiscoNode();
            knownNodes_c1.add(c1N1Node);
            knownNodes_c1.add(c1N2Node);
            knownNodes_c2.add(c2N1Node);
            knownNodes_c2.add(c2N2Node);
            Collections.shuffle(knownNodes_c1, random());
            Collections.shuffle(knownNodes_c2, random());

            try (MockTransportService transportService = MockTransportService.createNewService(
                settings,
                Version.CURRENT,
                threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                final Settings.Builder builder = Settings.builder();
                builder.putList(
                    "cluster.remote.cluster_1.seeds", c1N1Node.getAddress().toString());
                builder.putList(
                    "cluster.remote.cluster_2.seeds", c2N1Node.getAddress().toString());
                try (RemoteClusterService service =
                         new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_1",
                        Arrays.asList(c1N1Node.getAddress().toString(), c1N2Node.getAddress().toString()), null,
                        connectionListener(firstLatch));
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_2",
                        Arrays.asList(c2N1Node.getAddress().toString(), c2N2Node.getAddress().toString()), null,
                        connectionListener(secondLatch));
                    secondLatch.await();
                    CountDownLatch latch = new CountDownLatch(1);
                    service.collectNodes(new HashSet<>(Arrays.asList("cluster_1", "cluster_2")),
                        new ActionListener<BiFunction<String, String, DiscoveryNode>>() {
                        @Override
                        public void onResponse(BiFunction<String, String, DiscoveryNode> func) {
                            try {
                                assertEquals(c1N1Node, func.apply("cluster_1", c1N1Node.getId()));
                                assertEquals(c1N2Node, func.apply("cluster_1", c1N2Node.getId()));
                                assertEquals(c2N1Node, func.apply("cluster_2", c2N1Node.getId()));
                                assertEquals(c2N2Node, func.apply("cluster_2", c2N2Node.getId()));
                            } finally {
                                latch.countDown();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                throw new AssertionError(e);
                            } finally {
                                latch.countDown();
                            }
                        }
                    });
                    latch.await();
                    {
                        CountDownLatch failLatch = new CountDownLatch(1);
                        AtomicReference<Exception> ex = new AtomicReference<>();
                        service.collectNodes(new HashSet<>(Arrays.asList("cluster_1", "cluster_2", "no such cluster")),
                            new ActionListener<BiFunction<String, String, DiscoveryNode>>() {
                                @Override
                                public void onResponse(BiFunction<String, String, DiscoveryNode> stringStringDiscoveryNodeBiFunction) {
                                    try {
                                        fail("should not be called");
                                    } finally {
                                        failLatch.countDown();
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    try {
                                        ex.set(e);
                                    } finally {
                                        failLatch.countDown();
                                    }
                                }
                            });
                        failLatch.await();
                        assertNotNull(ex.get());
                        assertTrue(ex.get() instanceof IllegalArgumentException);
                        assertEquals("no such remote cluster: [no such cluster]", ex.get().getMessage());
                    }
                    {
                        logger.info("closing all source nodes");
                        // close all targets and check for the transport level failure path
                        IOUtils.close(c1N1, c1N2, c2N1, c2N2);
                        logger.info("all source nodes are closed");
                        CountDownLatch failLatch = new CountDownLatch(1);
                        AtomicReference<Exception> ex = new AtomicReference<>();
                        service.collectNodes(new HashSet<>(Arrays.asList("cluster_1", "cluster_2")),
                            new ActionListener<BiFunction<String, String, DiscoveryNode>>() {
                                @Override
                                public void onResponse(BiFunction<String, String, DiscoveryNode> stringStringDiscoveryNodeBiFunction) {
                                    try {
                                        fail("should not be called");
                                    } finally {
                                        failLatch.countDown();
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    try {
                                        ex.set(e);
                                    } finally {
                                        failLatch.countDown();
                                    }
                                }
                            });
                        failLatch.await();
                        assertNotNull(ex.get());
                        if (ex.get() instanceof  IllegalStateException) {
                            assertThat(ex.get().getMessage(), anyOf(equalTo("no seed node left"), startsWith
                                ("No node available for cluster:")));
                        } else {
                            if (ex.get() instanceof TransportException == false) {
                                // we have an issue for this see #25301
                                logger.error("expected TransportException but got a different one see #25301", ex.get());
                            }
                            assertTrue("expected TransportException but got a different one [" + ex.get().getClass().toString() + "]",
                                ex.get() instanceof TransportException);
                        }
                    }
                }
            }
        }
    }

    public void testCollectSearchShards() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        MockTransportService[] mockTransportServices = new MockTransportService[numClusters];
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        for (int i = 0; i < numClusters; i++) {
            List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
            MockTransportService remoteSeedTransport = startTransport("node_remote" + i, knownNodes, Version.CURRENT);
            mockTransportServices[i] = remoteSeedTransport;
            DiscoveryNode remoteSeedNode = remoteSeedTransport.getLocalDiscoNode();
            knownNodes.add(remoteSeedNode);
            nodes[i] = remoteSeedNode;
            builder.put("cluster.remote.remote" + i + ".seeds", remoteSeedNode.getAddress().toString());
            remoteIndicesByCluster.put("remote" + i, new OriginalIndices(new String[]{"index"}, IndicesOptions.lenientExpandOpen()));
        }
        Settings settings = builder.build();

        try {
            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterService remoteClusterService = new RemoteClusterService(settings, service)) {
                    assertFalse(remoteClusterService.isCrossClusterSearchEnabled());
                    remoteClusterService.initializeRemoteClusters();
                    assertTrue(remoteClusterService.isCrossClusterSearchEnabled());
                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        awaitLatch(latch, 5, TimeUnit.SECONDS);
                        assertNull(failure.get());
                        assertNotNull(response.get());
                        Map<String, ClusterSearchShardsResponse> map = response.get();
                        assertEquals(numClusters, map.size());
                        for (int i = 0; i < numClusters; i++) {
                            String clusterAlias = "remote" + i;
                            assertTrue(map.containsKey(clusterAlias));
                            ClusterSearchShardsResponse shardsResponse = map.get(clusterAlias);
                            assertEquals(1, shardsResponse.getNodes().length);
                        }
                    }
                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), "index_not_found",
                                null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        awaitLatch(latch, 5, TimeUnit.SECONDS);
                        assertNull(response.get());
                        assertNotNull(failure.get());
                        assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                        RemoteTransportException remoteTransportException = (RemoteTransportException) failure.get();
                        assertEquals(RestStatus.NOT_FOUND, remoteTransportException.status());
                    }
                    int numDisconnectedClusters = randomIntBetween(1, numClusters);
                    Set<DiscoveryNode> disconnectedNodes = new HashSet<>(numDisconnectedClusters);
                    Set<Integer> disconnectedNodesIndices = new HashSet<>(numDisconnectedClusters);
                    while(disconnectedNodes.size() < numDisconnectedClusters) {
                        int i = randomIntBetween(0, numClusters - 1);
                        if (disconnectedNodes.add(nodes[i])) {
                            assertTrue(disconnectedNodesIndices.add(i));
                        }
                    }

                    CountDownLatch disconnectedLatch = new CountDownLatch(numDisconnectedClusters);
                    for (RemoteClusterConnection connection : remoteClusterService.getConnections()) {
                        connection.getConnectionManager().addListener(new TransportConnectionListener() {
                            @Override
                            public void onNodeDisconnected(DiscoveryNode node) {
                                if (disconnectedNodes.remove(node)) {
                                    disconnectedLatch.countDown();
                                }
                            }
                        });
                    }

                    for (DiscoveryNode disconnectedNode : disconnectedNodes) {
                        service.addFailToSendNoConnectRule(disconnectedNode.getAddress());
                    }

                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        awaitLatch(latch, 5, TimeUnit.SECONDS);
                        assertNull(response.get());
                        assertNotNull(failure.get());
                        assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                        assertThat(failure.get().getMessage(), containsString("error while communicating with remote cluster ["));
                        assertThat(failure.get().getCause(), instanceOf(NodeDisconnectedException.class));
                    }

                    //setting skip_unavailable to true for all the disconnected clusters will make the request succeed again
                    for (int i : disconnectedNodesIndices) {
                        remoteClusterService.updateSkipUnavailable("remote" + i, true);
                    }
                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        awaitLatch(latch, 5, TimeUnit.SECONDS);
                        assertNull(failure.get());
                        assertNotNull(response.get());
                        Map<String, ClusterSearchShardsResponse> map = response.get();
                        assertEquals(numClusters, map.size());
                        for (int i = 0; i < numClusters; i++) {
                            String clusterAlias = "remote" + i;
                            assertTrue(map.containsKey(clusterAlias));
                            ClusterSearchShardsResponse shardsResponse = map.get(clusterAlias);
                            if (disconnectedNodesIndices.contains(i)) {
                                assertTrue(shardsResponse == ClusterSearchShardsResponse.EMPTY);
                            } else {
                                assertTrue(shardsResponse != ClusterSearchShardsResponse.EMPTY);
                            }
                        }
                    }

                    //give transport service enough time to realize that the node is down, and to notify the connection listeners
                    //so that RemoteClusterConnection is left with no connected nodes, hence it will retry connecting next
                    assertTrue(disconnectedLatch.await(5, TimeUnit.SECONDS));

                    service.clearAllRules();
                    if (randomBoolean()) {
                        for (int i : disconnectedNodesIndices) {
                            if (randomBoolean()) {
                                remoteClusterService.updateSkipUnavailable("remote" + i, true);
                            }

                        }
                    }
                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        awaitLatch(latch, 5, TimeUnit.SECONDS);
                        assertNull(failure.get());
                        assertNotNull(response.get());
                        Map<String, ClusterSearchShardsResponse> map = response.get();
                        assertEquals(numClusters, map.size());
                        for (int i = 0; i < numClusters; i++) {
                            String clusterAlias = "remote" + i;
                            assertTrue(map.containsKey(clusterAlias));
                            ClusterSearchShardsResponse shardsResponse = map.get(clusterAlias);
                            assertNotSame(ClusterSearchShardsResponse.EMPTY, shardsResponse);
                        }
                    }
                    assertEquals(0, service.getConnectionManager().size());
                }
            }
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testRemoteClusterSkipIfDisconnectedSetting() {
        {
            Settings settings = Settings.builder()
                    .put("cluster.remote.foo.skip_unavailable", true)
                    .put("cluster.remote.bar.skip_unavailable", false).build();
            RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));
        }
        {
            Settings brokenSettings = Settings.builder()
                    .put("cluster.remote.foo.skip_unavailable", "broken").build();
            expectThrows(IllegalArgumentException.class, () ->
                    RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(brokenSettings)
                            .forEach(setting -> setting.get(brokenSettings)));
        }

        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,
                new HashSet<>(Arrays.asList(RemoteClusterAware.REMOTE_CLUSTERS_SEEDS,
                        RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE)));
        {
            Settings settings = Settings.builder().put("cluster.remote.foo.skip_unavailable", randomBoolean()).build();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> service.validate(settings, true));
            assertEquals("missing required setting [cluster.remote.foo.seeds] for setting [cluster.remote.foo.skip_unavailable]",
                    iae.getMessage());
        }
        {
            try (MockTransportService remoteSeedTransport = startTransport("seed", new CopyOnWriteArrayList<>(), Version.CURRENT)) {
                String seed = remoteSeedTransport.getLocalDiscoNode().getAddress().toString();
                service.validate(Settings.builder().put("cluster.remote.foo.skip_unavailable", randomBoolean())
                        .put("cluster.remote.foo.seeds", seed).build(), true);
                service.validate(Settings.builder().put("cluster.remote.foo.seeds", seed).build(), true);

                AbstractScopedSettings service2 = new ClusterSettings(Settings.builder().put("cluster.remote.foo.seeds", seed).build(),
                        new HashSet<>(Arrays.asList(RemoteClusterAware.REMOTE_CLUSTERS_SEEDS,
                                RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE)));
                service2.validate(Settings.builder().put("cluster.remote.foo.skip_unavailable", randomBoolean()).build(), false);
            }
        }
    }

    public void testGetNodePredicateNodeRoles() {
        TransportAddress address = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        Predicate<DiscoveryNode> nodePredicate = RemoteClusterService.getNodePredicate(Settings.EMPTY);
        {
            DiscoveryNode all = new DiscoveryNode("id", address, Collections.emptyMap(),
                    new HashSet<>(EnumSet.allOf(DiscoveryNode.Role.class)), Version.CURRENT);
            assertTrue(nodePredicate.test(all));
        }
        {
            DiscoveryNode dataMaster = new DiscoveryNode("id", address, Collections.emptyMap(),
                    new HashSet<>(EnumSet.of(DiscoveryNode.Role.DATA, DiscoveryNode.Role.MASTER)), Version.CURRENT);
            assertTrue(nodePredicate.test(dataMaster));
        }
        {
            DiscoveryNode dedicatedMaster = new DiscoveryNode("id", address, Collections.emptyMap(),
                    new HashSet<>(EnumSet.of(DiscoveryNode.Role.MASTER)), Version.CURRENT);
            assertFalse(nodePredicate.test(dedicatedMaster));
        }
        {
            DiscoveryNode dedicatedIngest = new DiscoveryNode("id", address, Collections.emptyMap(),
                    new HashSet<>(EnumSet.of(DiscoveryNode.Role.INGEST)), Version.CURRENT);
            assertTrue(nodePredicate.test(dedicatedIngest));
        }
        {
            DiscoveryNode masterIngest = new DiscoveryNode("id", address, Collections.emptyMap(),
                    new HashSet<>(EnumSet.of(DiscoveryNode.Role.INGEST, DiscoveryNode.Role.MASTER)), Version.CURRENT);
            assertTrue(nodePredicate.test(masterIngest));
        }
        {
            DiscoveryNode dedicatedData = new DiscoveryNode("id", address, Collections.emptyMap(),
                    new HashSet<>(EnumSet.of(DiscoveryNode.Role.DATA)), Version.CURRENT);
            assertTrue(nodePredicate.test(dedicatedData));
        }
        {
            DiscoveryNode ingestData = new DiscoveryNode("id", address, Collections.emptyMap(),
                    new HashSet<>(EnumSet.of(DiscoveryNode.Role.DATA, DiscoveryNode.Role.INGEST)), Version.CURRENT);
            assertTrue(nodePredicate.test(ingestData));
        }
        {
            DiscoveryNode coordOnly = new DiscoveryNode("id", address, Collections.emptyMap(),
                    new HashSet<>(EnumSet.noneOf(DiscoveryNode.Role.class)), Version.CURRENT);
            assertTrue(nodePredicate.test(coordOnly));
        }
    }

    public void testGetNodePredicateNodeVersion() {
        TransportAddress address = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        Set<DiscoveryNode.Role> roles = new HashSet<>(EnumSet.allOf(DiscoveryNode.Role.class));
        Predicate<DiscoveryNode> nodePredicate = RemoteClusterService.getNodePredicate(Settings.EMPTY);
        Version version = VersionUtils.randomVersion(random());
        DiscoveryNode node = new DiscoveryNode("id", address, Collections.emptyMap(), roles, version);
        assertThat(nodePredicate.test(node), equalTo(Version.CURRENT.isCompatible(version)));
    }

    public void testGetNodePredicateNodeAttrs() {
        TransportAddress address = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        Set<DiscoveryNode.Role> roles = new HashSet<>(EnumSet.allOf(DiscoveryNode.Role.class));
        Settings settings = Settings.builder().put("cluster.remote.node.attr", "gateway").build();
        Predicate<DiscoveryNode> nodePredicate = RemoteClusterService.getNodePredicate(settings);
        {
            DiscoveryNode nonGatewayNode = new DiscoveryNode("id", address, Collections.singletonMap("gateway", "false"),
                    roles, Version.CURRENT);
            assertFalse(nodePredicate.test(nonGatewayNode));
            assertTrue(RemoteClusterService.getNodePredicate(Settings.EMPTY).test(nonGatewayNode));
        }
        {
            DiscoveryNode gatewayNode = new DiscoveryNode("id", address, Collections.singletonMap("gateway", "true"),
                    roles, Version.CURRENT);
            assertTrue(nodePredicate.test(gatewayNode));
            assertTrue(RemoteClusterService.getNodePredicate(Settings.EMPTY).test(gatewayNode));
        }
        {
            DiscoveryNode noAttrNode = new DiscoveryNode("id", address, Collections.emptyMap(), roles, Version.CURRENT);
            assertFalse(nodePredicate.test(noAttrNode));
            assertTrue(RemoteClusterService.getNodePredicate(Settings.EMPTY).test(noAttrNode));
        }
    }

    public void testGetNodePredicatesCombination() {
        TransportAddress address = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        Settings settings = Settings.builder().put("cluster.remote.node.attr", "gateway").build();
        Predicate<DiscoveryNode> nodePredicate = RemoteClusterService.getNodePredicate(settings);
        Set<DiscoveryNode.Role> allRoles = new HashSet<>(EnumSet.allOf(DiscoveryNode.Role.class));
        Set<DiscoveryNode.Role> dedicatedMasterRoles = new HashSet<>(EnumSet.of(DiscoveryNode.Role.MASTER));
        {
            DiscoveryNode node = new DiscoveryNode("id", address, Collections.singletonMap("gateway", "true"),
                    dedicatedMasterRoles, Version.CURRENT);
            assertFalse(nodePredicate.test(node));
        }
        {
            DiscoveryNode node = new DiscoveryNode("id", address, Collections.singletonMap("gateway", "false"),
                    dedicatedMasterRoles, Version.CURRENT);
            assertFalse(nodePredicate.test(node));
        }
        {
            DiscoveryNode node = new DiscoveryNode("id", address, Collections.singletonMap("gateway", "false"),
                    dedicatedMasterRoles, Version.CURRENT);
            assertFalse(nodePredicate.test(node));
        }
        {
            DiscoveryNode node = new DiscoveryNode("id", address, Collections.singletonMap("gateway", "true"),
                    allRoles, Version.CURRENT);
            assertTrue(nodePredicate.test(node));
        }
    }

    public void testRemoteClusterWithProxy() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService cluster_1_node0 = startTransport("cluster_1_node0", knownNodes, Version.CURRENT);
             MockTransportService cluster_1_node_1 = startTransport("cluster_1_node1", knownNodes, Version.CURRENT);
             MockTransportService cluster_2_node0 = startTransport("cluster_2_node0", Collections.emptyList(), Version.CURRENT)) {
            knownNodes.add(cluster_1_node0.getLocalDiscoNode());
            knownNodes.add(cluster_1_node_1.getLocalDiscoNode());
            String cluster1Proxy = "1.1.1.1:99";
            String cluster2Proxy = "2.2.2.2:99";
            Map<String, DiscoveryNode> nodesCluster1 = new HashMap<>();
            nodesCluster1.put("cluster_1_node0", cluster_1_node0.getLocalDiscoNode());
            nodesCluster1.put("cluster_1_node1", cluster_1_node_1.getLocalDiscoNode());
            Map<String, Map<String, DiscoveryNode>> mapping = new HashMap<>();
            mapping.put(cluster1Proxy, nodesCluster1);
            mapping.put(cluster2Proxy, Collections.singletonMap("cluster_2_node0", cluster_2_node0.getLocalDiscoNode()));

            Collections.shuffle(knownNodes, random());
            Transport proxyTransport = RemoteClusterConnectionTests.getProxyTransport(threadPool, mapping);
            try (MockTransportService transportService = MockTransportService.createNewService(Settings.EMPTY, proxyTransport,
                Version.CURRENT, threadPool, null, Collections.emptySet());) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", "cluster_1_node0:8080");
                builder.put("cluster.remote.cluster_1.proxy", cluster1Proxy);
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    updateRemoteCluster(service, "cluster_1", Collections.singletonList("cluster_1_node1:8081"), cluster1Proxy);
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertFalse(service.isRemoteClusterRegistered("cluster_2"));
                    updateRemoteCluster(service, "cluster_2", Collections.singletonList("cluster_2_node0:9300"), cluster2Proxy);
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    List<RemoteConnectionInfo> infos = service.getRemoteConnectionInfos().collect(Collectors.toList());
                    for (RemoteConnectionInfo info : infos) {
                        switch (info.clusterAlias) {
                            case "cluster_1":
                                assertEquals(2, info.numNodesConnected);
                                break;
                            case "cluster_2":
                                assertEquals(1, info.numNodesConnected);
                                break;
                            default:
                                fail("unknown cluster: " + info.clusterAlias);
                        }
                    }
                    service.updateRemoteCluster("cluster_2", Collections.emptyList(), randomBoolean() ? cluster2Proxy : null);
                    assertFalse(service.isRemoteClusterRegistered("cluster_2"));
                }
            }
        }
    }

    private void updateRemoteCluster(RemoteClusterService service, String clusterAlias, List<String> addresses, String proxyAddress)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        ActionListener<Void> listener = ActionListener.wrap(x -> latch.countDown(), x -> {
            exceptionAtomicReference.set(x);
            latch.countDown();
        });
        service.updateRemoteCluster(clusterAlias, addresses, proxyAddress, listener);
        latch.await();
        if (exceptionAtomicReference.get() != null) {
            throw exceptionAtomicReference.get();
        }
    }
}
