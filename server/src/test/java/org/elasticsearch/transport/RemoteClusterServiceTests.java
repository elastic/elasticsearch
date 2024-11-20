/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.test.NodeRoles.masterOnlyNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteClusterServiceTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(
        String id,
        List<DiscoveryNode> knownNodes,
        VersionInformation version,
        TransportVersion transportVersion
    ) {
        return startTransport(id, knownNodes, version, transportVersion, Settings.EMPTY);
    }

    private MockTransportService startTransport(
        final String id,
        final List<DiscoveryNode> knownNodes,
        final VersionInformation version,
        final TransportVersion transportVersion,
        final Settings settings
    ) {
        return RemoteClusterConnectionTests.startTransport(id, knownNodes, version, transportVersion, threadPool, settings);
    }

    public void testSettingsAreRegistered() {
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_NODE_ATTRIBUTE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTER_COMPRESS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SniffConnectionStrategy.REMOTE_CONNECTIONS_PER_CLUSTER));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(ProxyConnectionStrategy.PROXY_ADDRESS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS));
    }

    public void testRemoteClusterSeedSetting() {
        // simple validation
        Settings settings = Settings.builder()
            .put("cluster.remote.foo.seeds", "192.168.0.1:8080")
            .put("cluster.remote.bar.seeds", "[::1]:9090")
            .build();
        SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));

        Settings brokenSettings = Settings.builder().put("cluster.remote.foo.seeds", "192.168.0.1").build();
        expectThrows(
            IllegalArgumentException.class,
            () -> SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getAllConcreteSettings(brokenSettings)
                .forEach(setting -> setting.get(brokenSettings))
        );

        Settings brokenPortSettings = Settings.builder().put("cluster.remote.foo.seeds", "192.168.0.1:123456789123456789").build();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getAllConcreteSettings(brokenSettings)
                .forEach(setting -> setting.get(brokenPortSettings))
        );
        assertEquals("failed to parse port", e.getMessage());
    }

    public void testGroupClusterIndices() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport(
                "cluster_1_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService cluster2Transport = startTransport(
                "cluster_2_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            knownNodes.add(cluster2Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
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
                        Map<String, List<String>> perClusterIndices = service.groupClusterIndices(
                            service.getRemoteClusterNames(),
                            new String[] {
                                "cluster_1:bar",
                                "cluster_2:foo:bar",
                                "cluster_1:test",
                                "cluster_2:foo*",
                                "foo",
                                "cluster*:baz",
                                "*:boo" }
                        );
                        List<String> localIndices = perClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertNotNull(localIndices);
                        assertEquals("foo", localIndices.get(0));
                        assertEquals(2, perClusterIndices.size());
                        assertEquals(Arrays.asList("bar", "test", "baz", "boo"), perClusterIndices.get("cluster_1"));
                        assertEquals(Arrays.asList("foo:bar", "foo*", "baz", "boo"), perClusterIndices.get("cluster_2"));
                    }

                    expectThrows(
                        NoSuchRemoteClusterException.class,
                        () -> service.groupClusterIndices(
                            service.getRemoteClusterNames(),
                            new String[] { "foo:bar", "cluster_1:bar", "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo" }
                        )
                    );

                    expectThrows(
                        NoSuchRemoteClusterException.class,
                        () -> service.groupClusterIndices(
                            service.getRemoteClusterNames(),
                            new String[] { "cluster_1:bar", "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "does_not_exist:*" }
                        )
                    );

                    // test cluster exclusions
                    {
                        String[] indices = shuffledList(List.of("cluster*:foo*", "foo", "-cluster_1:*", "*:boo")).toArray(new String[0]);

                        Map<String, List<String>> perClusterIndices = service.groupClusterIndices(service.getRemoteClusterNames(), indices);
                        assertEquals(2, perClusterIndices.size());
                        List<String> localIndexes = perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertNotNull(localIndexes);
                        assertEquals(1, localIndexes.size());
                        assertEquals("foo", localIndexes.get(0));

                        List<String> cluster2 = perClusterIndices.get("cluster_2");
                        assertNotNull(cluster2);
                        assertEquals(2, cluster2.size());
                        assertEquals(List.of("boo", "foo*"), cluster2.stream().sorted().toList());
                    }
                    {
                        String[] indices = shuffledList(List.of("*:*", "-clu*_1:*", "*:boo")).toArray(new String[0]);

                        Map<String, List<String>> perClusterIndices = service.groupClusterIndices(service.getRemoteClusterNames(), indices);
                        assertEquals(1, perClusterIndices.size());

                        List<String> cluster2 = perClusterIndices.get("cluster_2");
                        assertNotNull(cluster2);
                        assertEquals(2, cluster2.size());
                        assertEquals(List.of("*", "boo"), cluster2.stream().sorted().toList());
                    }
                    {
                        String[] indices = shuffledList(List.of("cluster*:foo*", "cluster_2:*", "foo", "-cluster_1:*", "-c*:*")).toArray(
                            new String[0]
                        );

                        Map<String, List<String>> perClusterIndices = service.groupClusterIndices(service.getRemoteClusterNames(), indices);
                        assertEquals(1, perClusterIndices.size());
                        List<String> localIndexes = perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertNotNull(localIndexes);
                        assertEquals(1, localIndexes.size());
                        assertEquals("foo", localIndexes.get(0));
                    }
                    {
                        String[] indices = shuffledList(List.of("cluster*:*", "foo", "-*:*")).toArray(new String[0]);

                        Map<String, List<String>> perClusterIndices = service.groupClusterIndices(service.getRemoteClusterNames(), indices);
                        assertEquals(1, perClusterIndices.size());
                        List<String> localIndexes = perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertNotNull(localIndexes);
                        assertEquals(1, localIndexes.size());
                        assertEquals("foo", localIndexes.get(0));
                    }
                    {
                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> service.groupClusterIndices(
                                service.getRemoteClusterNames(),
                                // -cluster_1:foo* is not allowed, only -cluster_1:*
                                new String[] { "cluster_1:bar", "-cluster_2:foo*", "cluster_1:test", "cluster_2:foo*", "foo" }
                            )
                        );
                        assertThat(
                            e.getMessage(),
                            equalTo("To exclude a cluster you must specify the '*' wildcard for the index expression, but found: [foo*]")
                        );
                    }
                    {
                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> service.groupClusterIndices(
                                service.getRemoteClusterNames(),
                                // -cluster_1:* will fail since cluster_1 was never included in order to qualify to be excluded
                                new String[] { "-cluster_1:*", "cluster_2:foo*", "foo" }
                            )
                        );
                        assertThat(
                            e.getMessage(),
                            equalTo(
                                "Attempt to exclude cluster [cluster_1] failed as it is not included in the list of clusters to "
                                    + "be included: [(local), cluster_2]. Input: [-cluster_1:*,cluster_2:foo*,foo]"
                            )
                        );
                    }
                    {
                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> service.groupClusterIndices(service.getRemoteClusterNames(), new String[] { "-cluster_1:*" })
                        );
                        assertThat(
                            e.getMessage(),
                            equalTo(
                                "Attempt to exclude cluster [cluster_1] failed as it is not included in the list of clusters to "
                                    + "be included: []. Input: [-cluster_1:*]"
                            )
                        );
                    }
                    {
                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> service.groupClusterIndices(service.getRemoteClusterNames(), new String[] { "-*:*" })
                        );
                        assertThat(
                            e.getMessage(),
                            equalTo(
                                "Attempt to exclude clusters [cluster_1, cluster_2] failed as they are not included in the list of "
                                    + "clusters to be included: []. Input: [-*:*]"
                            )
                        );
                    }
                    {
                        String[] indices = shuffledList(List.of("cluster*:*", "*:foo", "-*:*")).toArray(new String[0]);

                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> service.groupClusterIndices(service.getRemoteClusterNames(), indices)
                        );
                        assertThat(
                            e.getMessage(),
                            containsString("The '-' exclusions in the index expression list excludes all indexes. Nothing to search.")
                        );
                    }
                }
            }
        }
    }

    public void testGroupIndices() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport(
                "cluster_1_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService cluster2Transport = startTransport(
                "cluster_2_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            knownNodes.add(cluster2Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
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
                        Map<String, OriginalIndices> perClusterIndices = service.groupIndices(
                            IndicesOptions.LENIENT_EXPAND_OPEN,
                            new String[] {
                                "cluster_1:bar",
                                "cluster_2:foo:bar",
                                "cluster_1:test",
                                "cluster_2:foo*",
                                "foo",
                                "cluster*:baz",
                                "*:boo" }
                        );
                        assertEquals(3, perClusterIndices.size());
                        assertArrayEquals(
                            new String[] { "foo" },
                            perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).indices()
                        );
                        assertArrayEquals(new String[] { "bar", "test", "baz", "boo" }, perClusterIndices.get("cluster_1").indices());
                        assertArrayEquals(new String[] { "foo:bar", "foo*", "baz", "boo" }, perClusterIndices.get("cluster_2").indices());
                    }
                    {
                        expectThrows(
                            NoSuchRemoteClusterException.class,
                            () -> service.groupClusterIndices(
                                service.getRemoteClusterNames(),
                                new String[] { "foo:bar", "cluster_1:bar", "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo" }
                            )
                        );
                    }
                    {
                        Map<String, OriginalIndices> perClusterIndices = service.groupIndices(
                            IndicesOptions.LENIENT_EXPAND_OPEN,
                            new String[] { "cluster_1:bar", "cluster_2:foo*" }
                        );
                        assertEquals(2, perClusterIndices.size());
                        assertArrayEquals(new String[] { "bar" }, perClusterIndices.get("cluster_1").indices());
                        assertArrayEquals(new String[] { "foo*" }, perClusterIndices.get("cluster_2").indices());
                    }
                    {
                        Map<String, OriginalIndices> perClusterIndices = service.groupIndices(
                            IndicesOptions.LENIENT_EXPAND_OPEN,
                            Strings.EMPTY_ARRAY
                        );
                        assertEquals(1, perClusterIndices.size());
                        assertArrayEquals(Strings.EMPTY_ARRAY, perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).indices());
                    }
                }
            }
        }
    }

    public void testGroupIndicesWithoutRemoteClusterClientRole() throws Exception {
        final Settings settings = onlyRoles(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "node-1").build(),
            Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE)
        );
        try (RemoteClusterService service = new RemoteClusterService(settings, null)) {
            assertFalse(service.isEnabled());
            assertFalse(service.isCrossClusterSearchEnabled());
            final IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> service.groupIndices(IndicesOptions.LENIENT_EXPAND_OPEN, new String[] { "cluster_1:bar", "cluster_2:foo*" })
            );
            assertThat(error.getMessage(), equalTo("node [node-1] does not have the remote cluster client role enabled"));
        }
    }

    public void testIncrementallyAddClusters() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport(
                "cluster_1_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService cluster2Transport = startTransport(
                "cluster_2_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            knownNodes.add(cluster2Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(Settings.EMPTY, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());
                    Settings cluster1Settings = createSettings(
                        "cluster_1",
                        Collections.singletonList(cluster1Seed.getAddress().toString())
                    );
                    PlainActionFuture<Void> clusterAdded = new PlainActionFuture<>();
                    // Add the cluster on a different thread to test that we wait for a new cluster to
                    // connect before returning.
                    new Thread(() -> {
                        try {
                            service.validateAndUpdateRemoteCluster("cluster_1", cluster1Settings);
                            clusterAdded.onResponse(null);
                        } catch (Exception e) {
                            clusterAdded.onFailure(e);
                        }
                    }).start();
                    clusterAdded.actionGet();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    Settings cluster2Settings = createSettings(
                        "cluster_2",
                        Collections.singletonList(cluster2Seed.getAddress().toString())
                    );
                    service.validateAndUpdateRemoteCluster("cluster_2", cluster2Settings);
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    Settings cluster2SettingsDisabled = createSettings("cluster_2", Collections.emptyList());
                    service.validateAndUpdateRemoteCluster("cluster_2", cluster2SettingsDisabled);
                    assertFalse(service.isRemoteClusterRegistered("cluster_2"));
                    IllegalArgumentException iae = expectThrows(
                        IllegalArgumentException.class,
                        () -> service.validateAndUpdateRemoteCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, Settings.EMPTY)
                    );
                    assertEquals("remote clusters must not have the empty string as its key", iae.getMessage());
                }
            }
        }
    }

    public void testDefaultPingSchedule() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport(
                "cluster_1_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode seedNode = seedTransport.getLocalNode();
            knownNodes.add(seedTransport.getLocalNode());
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
            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    settings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                try (RemoteClusterService service = new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    service.validateAndUpdateRemoteCluster(
                        "cluster_1",
                        createSettings("cluster_1", Collections.singletonList(seedNode.getAddress().toString()))
                    );
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
        try (
            MockTransportService cluster1Transport = startTransport(
                "cluster_1_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService cluster2Transport = startTransport(
                "cluster_2_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            knownNodes.add(cluster2Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder settingsBuilder = Settings.builder();
            if (randomBoolean()) {
                settingsBuilder.put(TransportSettings.PING_SCHEDULE.getKey(), TimeValue.timeValueSeconds(randomIntBetween(1, 10)));
            }
            Settings transportSettings = settingsBuilder.build();

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    transportSettings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());
                TimeValue pingSchedule1 = // randomBoolean() ? TimeValue.MINUS_ONE :
                    TimeValue.timeValueSeconds(randomIntBetween(1, 10));
                builder.put("cluster.remote.cluster_1.transport.ping_schedule", pingSchedule1);
                TimeValue pingSchedule2 = // randomBoolean() ? TimeValue.MINUS_ONE :
                    TimeValue.timeValueSeconds(randomIntBetween(1, 10));
                builder.put("cluster.remote.cluster_2.transport.ping_schedule", pingSchedule2);
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    service.initializeRemoteClusters();
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    RemoteClusterConnection remoteClusterConnection1 = service.getRemoteClusterConnection("cluster_1");
                    assertEquals(pingSchedule1, remoteClusterConnection1.getConnectionManager().getConnectionProfile().getPingInterval());
                    RemoteClusterConnection remoteClusterConnection2 = service.getRemoteClusterConnection("cluster_2");
                    assertEquals(pingSchedule2, remoteClusterConnection2.getConnectionManager().getConnectionProfile().getPingInterval());
                }
            }
        }
    }

    public void testChangeSettings() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport(
                "cluster_1_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    service.initializeRemoteClusters();
                    RemoteClusterConnection remoteClusterConnection = service.getRemoteClusterConnection("cluster_1");
                    Settings.Builder settingsChange = Settings.builder();
                    TimeValue pingSchedule = TimeValue.timeValueSeconds(randomIntBetween(6, 8));
                    settingsChange.put("cluster.remote.cluster_1.transport.ping_schedule", pingSchedule);
                    boolean compressionScheme = randomBoolean();
                    Compression.Enabled enabledChange = randomFrom(Compression.Enabled.TRUE, Compression.Enabled.FALSE);
                    if (compressionScheme) {
                        settingsChange.put("cluster.remote.cluster_1.transport.compression_scheme", Compression.Scheme.DEFLATE);
                    } else {
                        settingsChange.put("cluster.remote.cluster_1.transport.compress", enabledChange);
                    }
                    settingsChange.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
                    service.validateAndUpdateRemoteCluster("cluster_1", settingsChange.build());
                    assertBusy(remoteClusterConnection::isClosed);

                    remoteClusterConnection = service.getRemoteClusterConnection("cluster_1");
                    ConnectionProfile connectionProfile = remoteClusterConnection.getConnectionManager().getConnectionProfile();
                    assertEquals(pingSchedule, connectionProfile.getPingInterval());
                    if (compressionScheme) {
                        assertEquals(Compression.Enabled.INDEXING_DATA, connectionProfile.getCompressionEnabled());
                        assertEquals(Compression.Scheme.DEFLATE, connectionProfile.getCompressionScheme());
                    } else {
                        assertEquals(enabledChange, connectionProfile.getCompressionEnabled());
                        assertEquals(Compression.Scheme.LZ4, connectionProfile.getCompressionScheme());
                    }
                }
            }
        }
    }

    public void testRemoteNodeAttribute() throws IOException, InterruptedException {
        final Settings settings = Settings.builder().put("cluster.remote.node.attr", "gateway").build();
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Settings gateway = Settings.builder().put("node.attr.gateway", true).build();
        try (
            MockTransportService c1N1 = startTransport(
                "cluster_1_node_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService c1N2 = startTransport(
                "cluster_1_node_2",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                gateway
            );
            MockTransportService c2N1 = startTransport(
                "cluster_2_node_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService c2N2 = startTransport(
                "cluster_2_node_2",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                gateway
            )
        ) {
            final DiscoveryNode c1N1Node = c1N1.getLocalNode();
            final DiscoveryNode c1N2Node = c1N2.getLocalNode();
            final DiscoveryNode c2N1Node = c2N1.getLocalNode();
            final DiscoveryNode c2N2Node = c2N2.getLocalNode();
            knownNodes.add(c1N1Node);
            knownNodes.add(c1N2Node);
            knownNodes.add(c2N1Node);
            knownNodes.add(c2N2Node);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    settings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                final Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seed", c1N1Node.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seed", c2N1Node.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_1",
                        createSettings("cluster_1", Arrays.asList(c1N1Node.getAddress().toString(), c1N2Node.getAddress().toString())),
                        connectionListener(firstLatch)
                    );
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_2",
                        createSettings("cluster_2", Arrays.asList(c2N1Node.getAddress().toString(), c2N2Node.getAddress().toString())),
                        connectionListener(secondLatch)
                    );
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
        final Settings data = nonMasterNode();
        final Settings dedicatedMaster = masterOnlyNode();
        try (
            MockTransportService c1N1 = startTransport(
                "cluster_1_node_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                dedicatedMaster
            );
            MockTransportService c1N2 = startTransport(
                "cluster_1_node_2",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                data
            );
            MockTransportService c2N1 = startTransport(
                "cluster_2_node_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                dedicatedMaster
            );
            MockTransportService c2N2 = startTransport(
                "cluster_2_node_2",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                data
            )
        ) {
            final DiscoveryNode c1N1Node = c1N1.getLocalNode();
            final DiscoveryNode c1N2Node = c1N2.getLocalNode();
            final DiscoveryNode c2N1Node = c2N1.getLocalNode();
            final DiscoveryNode c2N2Node = c2N2.getLocalNode();
            knownNodes.add(c1N1Node);
            knownNodes.add(c1N2Node);
            knownNodes.add(c2N1Node);
            knownNodes.add(c2N2Node);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    settings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                final Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seed", c1N1Node.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seed", c2N1Node.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_1",
                        createSettings("cluster_1", Arrays.asList(c1N1Node.getAddress().toString(), c1N2Node.getAddress().toString())),
                        connectionListener(firstLatch)
                    );
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_2",
                        createSettings("cluster_2", Arrays.asList(c2N1Node.getAddress().toString(), c2N2Node.getAddress().toString())),
                        connectionListener(secondLatch)
                    );
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

    private ActionListener<RemoteClusterService.RemoteClusterConnectionStatus> connectionListener(final CountDownLatch latch) {
        return ActionTestUtils.assertNoFailureListener(x -> latch.countDown());
    }

    public void testCollectNodes() throws InterruptedException, IOException {
        final Settings settings = Settings.EMPTY;
        final List<DiscoveryNode> knownNodes_c1 = new CopyOnWriteArrayList<>();
        final List<DiscoveryNode> knownNodes_c2 = new CopyOnWriteArrayList<>();

        try (
            MockTransportService c1N1 = startTransport(
                "cluster_1_node_1",
                knownNodes_c1,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                settings
            );
            MockTransportService c1N2 = startTransport(
                "cluster_1_node_2",
                knownNodes_c1,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                settings
            );
            MockTransportService c2N1 = startTransport(
                "cluster_2_node_1",
                knownNodes_c2,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                settings
            );
            MockTransportService c2N2 = startTransport(
                "cluster_2_node_2",
                knownNodes_c2,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                settings
            )
        ) {
            final DiscoveryNode c1N1Node = c1N1.getLocalNode();
            final DiscoveryNode c1N2Node = c1N2.getLocalNode();
            final DiscoveryNode c2N1Node = c2N1.getLocalNode();
            final DiscoveryNode c2N2Node = c2N2.getLocalNode();
            knownNodes_c1.add(c1N1Node);
            knownNodes_c1.add(c1N2Node);
            knownNodes_c2.add(c2N1Node);
            knownNodes_c2.add(c2N2Node);
            Collections.shuffle(knownNodes_c1, random());
            Collections.shuffle(knownNodes_c2, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    settings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                final Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_1.seed", c1N1Node.getAddress().toString());
                builder.putList("cluster.remote.cluster_2.seed", c2N1Node.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());

                    final CountDownLatch firstLatch = new CountDownLatch(1);

                    service.updateRemoteCluster(
                        "cluster_1",
                        createSettings("cluster_1", Arrays.asList(c1N1Node.getAddress().toString(), c1N2Node.getAddress().toString())),
                        connectionListener(firstLatch)
                    );
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_2",
                        createSettings("cluster_2", Arrays.asList(c2N1Node.getAddress().toString(), c2N2Node.getAddress().toString())),
                        connectionListener(secondLatch)
                    );
                    secondLatch.await();
                    CountDownLatch latch = new CountDownLatch(1);
                    service.collectNodes(
                        new HashSet<>(Arrays.asList("cluster_1", "cluster_2")),
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
                        }
                    );
                    latch.await();
                    {
                        CountDownLatch failLatch = new CountDownLatch(1);
                        AtomicReference<Exception> ex = new AtomicReference<>();
                        service.collectNodes(
                            new HashSet<>(Arrays.asList("cluster_1", "cluster_2", "no such cluster")),
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
                            }
                        );
                        failLatch.await();
                        assertNotNull(ex.get());
                        assertTrue(ex.get() instanceof NoSuchRemoteClusterException);
                        assertEquals("no such remote cluster: [no such cluster]", ex.get().getMessage());
                    }
                    {
                        logger.info("closing all source nodes");
                        // close all targets and check for the transport level failure path
                        IOUtils.close(c1N1, c1N2, c2N1, c2N2);
                        logger.info("all source nodes are closed");
                        CountDownLatch failLatch = new CountDownLatch(1);
                        AtomicReference<Exception> ex = new AtomicReference<>();
                        service.collectNodes(
                            new HashSet<>(Arrays.asList("cluster_1", "cluster_2")),
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
                            }
                        );
                        failLatch.await();
                        assertNotNull(ex.get());
                        if (ex.get() instanceof IllegalStateException) {
                            assertThat(
                                ex.get().getMessage(),
                                either(equalTo("Unable to open any connections to remote cluster [cluster_1]")).or(
                                    equalTo("Unable to open any connections to remote cluster [cluster_2]")
                                )
                            );
                        } else {
                            assertThat(
                                ex.get(),
                                either(instanceOf(TransportException.class)).or(instanceOf(NoSuchRemoteClusterException.class))
                                    .or(instanceOf(NoSeedNodeLeftException.class))
                            );
                        }
                    }
                }
            }
        }
    }

    public void testRemoteClusterSkipIfDisconnectedSetting() {
        {
            Settings settings = Settings.builder()
                .put("cluster.remote.foo.seeds", "127.0.0.1:9300")
                .put("cluster.remote.foo.skip_unavailable", true)
                .build();
            RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));
        }
        {
            Settings brokenSettingsDependency = Settings.builder().put("cluster.remote.foo.skip_unavailable", true).build();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(brokenSettingsDependency)
                    .forEach(setting -> setting.get(brokenSettingsDependency))
            );
            assertEquals(
                "Cannot configure setting [cluster.remote.foo.skip_unavailable] if remote cluster is not enabled.",
                iae.getMessage()
            );
        }
        {
            Settings brokenSettingsType = Settings.builder().put("cluster.remote.foo.skip_unavailable", "broken").build();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(brokenSettingsType)
                    .forEach(setting -> setting.get(brokenSettingsType))
            );
        }

        {
            Settings settings = Settings.builder()
                .put("cluster.remote.foo.mode", "proxy")
                .put("cluster.remote.foo.proxy_address", "127.0.0.1:9300")
                .put("cluster.remote.foo.transport.ping_schedule", "5s")
                .build();
            RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));
        }
        {
            Settings brokenSettingsDependency = Settings.builder()
                .put("cluster.remote.foo.proxy_address", "127.0.0.1:9300")
                .put("cluster.remote.foo.transport.ping_schedule", "5s")
                .build();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE.getAllConcreteSettings(brokenSettingsDependency)
                    .forEach(setting -> setting.get(brokenSettingsDependency))
            );
            assertEquals(
                "Cannot configure setting [cluster.remote.foo.transport.ping_schedule] if remote cluster is not enabled.",
                iae.getMessage()
            );
        }

        {
            Settings settings = Settings.builder()
                .put("cluster.remote.foo.seeds", "127.0.0.1:9300")
                .put("cluster.remote.foo.transport.compress", false)
                .build();
            RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));
        }
        {
            Settings brokenSettingsDependency = Settings.builder()
                .put("cluster.remote.foo.proxy_address", "127.0.0.1:9300")
                .put("cluster.remote.foo.transport.compress", true)
                .build();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getAllConcreteSettings(brokenSettingsDependency)
                    .forEach(setting -> setting.get(brokenSettingsDependency))
            );
            assertEquals(
                "Cannot configure setting [cluster.remote.foo.transport.compress] if remote cluster is not enabled.",
                iae.getMessage()
            );
        }

        AbstractScopedSettings service = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(Arrays.asList(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS, RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE))
        );
        {
            Settings brokenSettingsDependency = Settings.builder().put("cluster.remote.foo.skip_unavailable", randomBoolean()).build();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> service.validate(brokenSettingsDependency, true)
            );
            assertEquals(
                "Cannot configure setting [cluster.remote.foo.skip_unavailable] if remote cluster is not enabled.",
                iae.getMessage()
            );
        }
    }

    public void testReconnectWhenStrategySettingsUpdated() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster_node_0 = startTransport(
                "cluster_node_0",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService cluster_node_1 = startTransport(
                "cluster_node_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {

            final DiscoveryNode node0 = cluster_node_0.getLocalNode();
            final DiscoveryNode node1 = cluster_node_1.getLocalNode();
            knownNodes.add(node0);
            knownNodes.add(node1);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();

                final Settings.Builder builder = Settings.builder();
                builder.putList("cluster.remote.cluster_test.seeds", Collections.singletonList(node0.getAddress().toString()));
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());

                    final RemoteClusterConnection firstRemoteClusterConnection = service.getRemoteClusterConnection("cluster_test");
                    assertTrue(firstRemoteClusterConnection.isNodeConnected(node0));
                    assertTrue(firstRemoteClusterConnection.isNodeConnected(node1));
                    assertEquals(2, firstRemoteClusterConnection.getNumNodesConnected());
                    assertFalse(firstRemoteClusterConnection.isClosed());

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_test",
                        createSettings("cluster_test", Collections.singletonList(node0.getAddress().toString())),
                        connectionListener(firstLatch)
                    );
                    firstLatch.await();

                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(firstRemoteClusterConnection.isNodeConnected(node0));
                    assertTrue(firstRemoteClusterConnection.isNodeConnected(node1));
                    assertEquals(2, firstRemoteClusterConnection.getNumNodesConnected());
                    assertFalse(firstRemoteClusterConnection.isClosed());
                    assertSame(firstRemoteClusterConnection, service.getRemoteClusterConnection("cluster_test"));

                    final List<String> newSeeds = new ArrayList<>();
                    newSeeds.add(node1.getAddress().toString());
                    if (randomBoolean()) {
                        newSeeds.add(node0.getAddress().toString());
                        Collections.shuffle(newSeeds, random());
                    }

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster("cluster_test", createSettings("cluster_test", newSeeds), connectionListener(secondLatch));
                    secondLatch.await();

                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertBusy(() -> {
                        assertFalse(firstRemoteClusterConnection.isNodeConnected(node0));
                        assertFalse(firstRemoteClusterConnection.isNodeConnected(node1));
                        assertEquals(0, firstRemoteClusterConnection.getNumNodesConnected());
                        assertTrue(firstRemoteClusterConnection.isClosed());
                    });

                    final RemoteClusterConnection secondRemoteClusterConnection = service.getRemoteClusterConnection("cluster_test");
                    assertTrue(secondRemoteClusterConnection.isNodeConnected(node0));
                    assertTrue(secondRemoteClusterConnection.isNodeConnected(node1));
                    assertEquals(2, secondRemoteClusterConnection.getNumNodesConnected());
                    assertFalse(secondRemoteClusterConnection.isClosed());
                }
            }
        }
    }

    public static void updateSkipUnavailable(RemoteClusterService service, String clusterAlias, boolean skipUnavailable) {
        RemoteClusterConnection connection = service.getRemoteClusterConnection(clusterAlias);
        connection.setSkipUnavailable(skipUnavailable);
    }

    public static void addConnectionListener(RemoteClusterService service, TransportConnectionListener listener) {
        for (RemoteClusterConnection connection : service.getConnections()) {
            ConnectionManager connectionManager = connection.getConnectionManager();
            connectionManager.addListener(listener);
        }
    }

    public void testSkipUnavailable() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport(
                "seed_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode seedNode = seedTransport.getLocalNode();
            knownNodes.add(seedNode);
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalNode().getAddress().toString());
            try (
                MockTransportService service = MockTransportService.createNewService(
                    builder.build(),
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();

                assertTrue(service.getRemoteClusterService().isSkipUnavailable("cluster1"));

                if (randomBoolean()) {
                    updateSkipUnavailable(service.getRemoteClusterService(), "cluster1", false);
                    assertFalse(service.getRemoteClusterService().isSkipUnavailable("cluster1"));
                }

                updateSkipUnavailable(service.getRemoteClusterService(), "cluster1", true);
                assertTrue(service.getRemoteClusterService().isSkipUnavailable("cluster1"));
            }
        }
    }

    public void testRemoteClusterServiceNotEnabledGetRemoteClusterConnection() {
        final Settings settings = removeRoles(Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
        try (
            MockTransportService service = MockTransportService.createNewService(
                settings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                null
            )
        ) {
            service.start();
            service.acceptIncomingRequests();
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> service.getRemoteClusterService().getRemoteClusterConnection("test")
            );
            assertThat(e.getMessage(), equalTo("this node does not have the remote_cluster_client role"));
        }
    }

    public void testRemoteClusterServiceNotEnabledGetCollectNodes() {
        final Settings settings = removeRoles(Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
        try (
            MockTransportService service = MockTransportService.createNewService(
                settings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                null
            )
        ) {
            service.start();
            service.acceptIncomingRequests();
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> service.getRemoteClusterService().collectNodes(Set.of(), ActionListener.noop())
            );
            assertThat(e.getMessage(), equalTo("this node does not have the remote_cluster_client role"));
        }
    }

    public void testUseDifferentTransportProfileForCredentialsProtectedRemoteClusters() throws IOException, InterruptedException {
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService c1 = startTransport(
                "cluster_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                Settings.builder()
                    .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
                    .put(RemoteClusterPortSettings.PORT.getKey(), "0")
                    .build()
            );
            MockTransportService c2 = startTransport("cluster_2", knownNodes, VersionInformation.CURRENT, TransportVersion.current());
        ) {
            final DiscoveryNode c1Node = c1.getLocalNode().withTransportAddress(c1.boundRemoteAccessAddress().publishAddress());
            final DiscoveryNode c2Node = c2.getLocalNode();

            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("cluster.remote.cluster_1.credentials", randomAlphaOfLength(10));
            final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    settings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                // remote cluster_1 has a credentials and uses the _remote_cluster transport profile
                transportService.addConnectBehavior(c1Node.getAddress(), (transport, discoveryNode, profile, listener) -> {
                    assertThat(profile.getTransportProfile(), equalTo("_remote_cluster"));
                    transport.openConnection(discoveryNode, profile, listener);
                });
                // remote cluster_2 has no credentials and uses legacy model
                transportService.addConnectBehavior(c2Node.getAddress(), (transport, discoveryNode, profile, listener) -> {
                    assertThat(profile.getTransportProfile(), equalTo("default"));
                    transport.openConnection(discoveryNode, profile, listener);
                });
                transportService.start();
                transportService.acceptIncomingRequests();
                try (RemoteClusterService service = new RemoteClusterService(settings, transportService)) {
                    service.initializeRemoteClusters();

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    final Settings.Builder firstRemoteClusterSettingsBuilder = Settings.builder();
                    final boolean firstRemoteClusterProxyMode = randomBoolean();
                    if (firstRemoteClusterProxyMode) {
                        firstRemoteClusterSettingsBuilder.put("cluster.remote.cluster_1.mode", "proxy")
                            .put("cluster.remote.cluster_1.proxy_address", c1Node.getAddress().toString());
                    } else {
                        firstRemoteClusterSettingsBuilder.put("cluster.remote.cluster_1.seeds", c1Node.getAddress().toString());
                    }
                    service.updateRemoteCluster("cluster_1", firstRemoteClusterSettingsBuilder.build(), connectionListener(firstLatch));
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    final Settings.Builder secondRemoteClusterSettingsBuilder = Settings.builder();
                    final boolean secondRemoteClusterProxyMode = randomBoolean();
                    if (secondRemoteClusterProxyMode) {
                        secondRemoteClusterSettingsBuilder.put("cluster.remote.cluster_2.mode", "proxy")
                            .put("cluster.remote.cluster_2.proxy_address", c2Node.getAddress().toString());
                    } else {
                        secondRemoteClusterSettingsBuilder.put("cluster.remote.cluster_2.seeds", c2Node.getAddress().toString());
                    }
                    service.updateRemoteCluster("cluster_2", secondRemoteClusterSettingsBuilder.build(), connectionListener(secondLatch));
                    secondLatch.await();

                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    if (firstRemoteClusterProxyMode) {
                        assertFalse(service.isRemoteNodeConnected("cluster_1", c1Node));
                    } else {
                        assertTrue(service.isRemoteNodeConnected("cluster_1", c1Node));
                    }
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    if (secondRemoteClusterProxyMode) {
                        assertFalse(service.isRemoteNodeConnected("cluster_2", c2Node));
                    } else {
                        assertTrue(service.isRemoteNodeConnected("cluster_2", c2Node));
                    }
                    // No local node connection
                    assertEquals(0, transportService.getConnectionManager().size());
                }
            }
        }
    }

    public void testUpdateRemoteClusterCredentialsRebuildsConnectionWithCorrectProfile() throws IOException, InterruptedException {
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService c = startTransport(
                "cluster_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                Settings.builder()
                    .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
                    .put(RemoteClusterPortSettings.PORT.getKey(), "0")
                    .build()
            )
        ) {
            final DiscoveryNode discoNode = c.getLocalNode().withTransportAddress(c.boundRemoteAccessAddress().publishAddress());
            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();

                try (RemoteClusterService service = new RemoteClusterService(Settings.EMPTY, transportService)) {
                    service.initializeRemoteClusters();

                    final Settings clusterSettings = buildRemoteClusterSettings("cluster_1", discoNode.getAddress().toString());
                    final CountDownLatch latch = new CountDownLatch(1);
                    service.updateRemoteCluster("cluster_1", clusterSettings, connectionListener(latch));
                    latch.await();

                    assertConnectionHasProfile(service.getRemoteClusterConnection("cluster_1"), "default");

                    {
                        final MockSecureSettings secureSettings = new MockSecureSettings();
                        secureSettings.setString("cluster.remote.cluster_1.credentials", randomAlphaOfLength(10));
                        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
                        final Settings settings = Settings.builder().put(clusterSettings).setSecureSettings(secureSettings).build();
                        service.updateRemoteClusterCredentials(() -> settings, listener);
                        listener.actionGet(10, TimeUnit.SECONDS);
                    }

                    assertConnectionHasProfile(
                        service.getRemoteClusterConnection("cluster_1"),
                        RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE
                    );

                    {
                        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
                        service.updateRemoteClusterCredentials(
                            // Settings without credentials constitute credentials removal
                            () -> clusterSettings,
                            listener
                        );
                        listener.actionGet(10, TimeUnit.SECONDS);
                    }

                    assertConnectionHasProfile(service.getRemoteClusterConnection("cluster_1"), "default");
                }
            }
        }
    }

    public void testUpdateRemoteClusterCredentialsRebuildsMultipleConnectionsDespiteFailures() throws IOException, InterruptedException {
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService c1 = startTransport(
                "cluster_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                Settings.builder()
                    .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
                    .put(RemoteClusterPortSettings.PORT.getKey(), "0")
                    .build()
            );
            MockTransportService c2 = startTransport(
                "cluster_2",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                Settings.builder()
                    .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
                    .put(RemoteClusterPortSettings.PORT.getKey(), "0")
                    .build()
            )
        ) {
            final DiscoveryNode c1DiscoNode = c1.getLocalNode().withTransportAddress(c1.boundRemoteAccessAddress().publishAddress());
            final DiscoveryNode c2DiscoNode = c2.getLocalNode().withTransportAddress(c2.boundRemoteAccessAddress().publishAddress());
            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                // fail on connection attempt
                transportService.addConnectBehavior(c2DiscoNode.getAddress(), (transport, discoveryNode, profile, listener) -> {
                    throw new RuntimeException("bad cluster");
                });

                transportService.start();
                transportService.acceptIncomingRequests();

                final String goodCluster = randomAlphaOfLength(10);
                final String badCluster = randomValueOtherThan(goodCluster, () -> randomAlphaOfLength(10));
                final String missingCluster = randomValueOtherThanMany(
                    alias -> alias.equals(goodCluster) || alias.equals(badCluster),
                    () -> randomAlphaOfLength(10)
                );
                try (RemoteClusterService service = new RemoteClusterService(Settings.EMPTY, transportService)) {
                    service.initializeRemoteClusters();

                    final Settings cluster1Settings = buildRemoteClusterSettings(goodCluster, c1DiscoNode.getAddress().toString());
                    final var latch = new CountDownLatch(1);
                    service.updateRemoteCluster(goodCluster, cluster1Settings, connectionListener(latch));
                    latch.await();

                    final Settings cluster2Settings = buildRemoteClusterSettings(badCluster, c2DiscoNode.getAddress().toString());
                    final PlainActionFuture<RemoteClusterService.RemoteClusterConnectionStatus> future = new PlainActionFuture<>();
                    service.updateRemoteCluster(badCluster, cluster2Settings, future);
                    final var ex = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
                    assertThat(ex.getMessage(), containsString("bad cluster"));

                    assertConnectionHasProfile(service.getRemoteClusterConnection(goodCluster), "default");
                    assertConnectionHasProfile(service.getRemoteClusterConnection(badCluster), "default");
                    expectThrows(NoSuchRemoteClusterException.class, () -> service.getRemoteClusterConnection(missingCluster));

                    {
                        final MockSecureSettings secureSettings = new MockSecureSettings();
                        secureSettings.setString("cluster.remote." + badCluster + ".credentials", randomAlphaOfLength(10));
                        secureSettings.setString("cluster.remote." + goodCluster + ".credentials", randomAlphaOfLength(10));
                        secureSettings.setString("cluster.remote." + missingCluster + ".credentials", randomAlphaOfLength(10));
                        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
                        final Settings settings = Settings.builder()
                            .put(cluster1Settings)
                            .put(cluster2Settings)
                            .setSecureSettings(secureSettings)
                            .build();
                        service.updateRemoteClusterCredentials(() -> settings, listener);
                        listener.actionGet(10, TimeUnit.SECONDS);
                    }

                    assertConnectionHasProfile(
                        service.getRemoteClusterConnection(goodCluster),
                        RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE
                    );
                    assertConnectionHasProfile(
                        service.getRemoteClusterConnection(badCluster),
                        RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE
                    );
                    expectThrows(NoSuchRemoteClusterException.class, () -> service.getRemoteClusterConnection(missingCluster));

                    {
                        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
                        final Settings settings = Settings.builder().put(cluster1Settings).put(cluster2Settings).build();
                        service.updateRemoteClusterCredentials(
                            // Settings without credentials constitute credentials removal
                            () -> settings,
                            listener
                        );
                        listener.actionGet(10, TimeUnit.SECONDS);
                    }

                    assertConnectionHasProfile(service.getRemoteClusterConnection(goodCluster), "default");
                    assertConnectionHasProfile(service.getRemoteClusterConnection(badCluster), "default");
                    expectThrows(NoSuchRemoteClusterException.class, () -> service.getRemoteClusterConnection(missingCluster));
                }
            }
        }
    }

    private static void assertConnectionHasProfile(RemoteClusterConnection remoteClusterConnection, String expectedConnectionProfile) {
        assertThat(
            remoteClusterConnection.getConnectionManager().getConnectionProfile().getTransportProfile(),
            equalTo(expectedConnectionProfile)
        );
    }

    private Settings buildRemoteClusterSettings(String clusterAlias, String address) {
        final Settings.Builder settings = Settings.builder();
        final boolean proxyMode = randomBoolean();
        if (proxyMode) {
            settings.put("cluster.remote." + clusterAlias + ".mode", "proxy")
                .put("cluster.remote." + clusterAlias + ".proxy_address", address);
        } else {
            settings.put("cluster.remote." + clusterAlias + ".seeds", address);
        }
        return settings.build();
    }

    public void testLogsConnectionResult() throws IOException {

        try (
            var remote = startTransport("remote", List.of(), VersionInformation.CURRENT, TransportVersion.current(), Settings.EMPTY);
            var local = startTransport("local", List.of(), VersionInformation.CURRENT, TransportVersion.current(), Settings.EMPTY);
            var remoteClusterService = new RemoteClusterService(Settings.EMPTY, local)
        ) {
            var clusterSettings = ClusterSettings.createBuiltInClusterSettings();
            remoteClusterService.listenForUpdates(clusterSettings);

            assertThatLogger(
                () -> clusterSettings.applySettings(
                    Settings.builder().putList("cluster.remote.remote_1.seeds", remote.getLocalNode().getAddress().toString()).build()
                ),
                RemoteClusterService.class,
                new MockLog.SeenEventExpectation(
                    "Should log when connecting to remote",
                    RemoteClusterService.class.getCanonicalName(),
                    Level.INFO,
                    "remote cluster connection [remote_1] updated: CONNECTED"
                )
            );

            assertThatLogger(
                () -> clusterSettings.applySettings(Settings.EMPTY),
                RemoteClusterService.class,
                new MockLog.SeenEventExpectation(
                    "Should log when disconnecting from remote",
                    RemoteClusterService.class.getCanonicalName(),
                    Level.INFO,
                    "remote cluster connection [remote_1] updated: DISCONNECTED"
                )
            );

            assertThatLogger(
                () -> clusterSettings.applySettings(Settings.builder().put(randomIdentifier(), randomIdentifier()).build()),
                RemoteClusterService.class,
                new MockLog.UnseenEventExpectation(
                    "Should not log when changing unrelated setting",
                    RemoteClusterService.class.getCanonicalName(),
                    Level.INFO,
                    "*"
                )
            );
        }
    }

    private static Settings createSettings(String clusterAlias, List<String> seeds) {
        Settings.Builder builder = Settings.builder();
        builder.put(
            SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).getKey(),
            Strings.collectionToCommaDelimitedString(seeds)
        );
        return builder.build();
    }

}
