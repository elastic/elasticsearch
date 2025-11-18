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
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.FixForMultiProject;
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
import java.util.function.Consumer;

import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.test.NodeRoles.masterOnlyNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.elasticsearch.transport.RemoteClusterSettings.ProxyConnectionStrategySettings;
import static org.elasticsearch.transport.RemoteClusterSettings.SniffConnectionStrategySettings;
import static org.elasticsearch.transport.RemoteClusterSettings.toConfig;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteClusterServiceTests extends ESTestCase {

    private static final Settings REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS = Settings.builder()
        .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
        .put(RemoteClusterPortSettings.PORT.getKey(), "0")
        .build();
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final ProjectResolver projectResolver = DefaultProjectResolver.INSTANCE;

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(final String id, final List<DiscoveryNode> knownNodes, final Settings settings) {
        return RemoteClusterConnectionTests.startTransport(
            id,
            knownNodes,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool,
            settings
        );
    }

    private MockTransportService startTransport() {
        return startTransport(Settings.EMPTY);
    }

    private MockTransportService startTransport(Settings settings) {
        return startTransport(UUIDs.randomBase64UUID(), List.of(), settings);
    }

    private MockTransportService startTransport(final String id) {
        return startTransport(id, List.of(), Settings.EMPTY);
    }

    private MockTransportService startTransport(final String id, Settings settings) {
        return startTransport(id, List.of(), settings);
    }

    private MockTransportService startTransport(final String id, List<DiscoveryNode> knownNodes) {
        return startTransport(id, knownNodes, Settings.EMPTY);
    }

    public void testSettingsAreRegistered() {
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterSettings.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterSettings.REMOTE_NODE_ATTRIBUTE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterSettings.REMOTE_CLUSTER_PING_SCHEDULE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterSettings.REMOTE_CLUSTER_COMPRESS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterSettings.REMOTE_CONNECTION_MODE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SniffConnectionStrategySettings.REMOTE_CONNECTIONS_PER_CLUSTER));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterSettings.REMOTE_CLUSTER_CREDENTIALS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SniffConnectionStrategySettings.REMOTE_NODE_CONNECTIONS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(ProxyConnectionStrategySettings.PROXY_ADDRESS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(ProxyConnectionStrategySettings.REMOTE_SOCKET_CONNECTIONS));
    }

    public void testRemoteClusterSeedSetting() {
        // simple validation
        Settings settings = Settings.builder()
            .put("cluster.remote.foo.seeds", "192.168.0.1:8080")
            .put("cluster.remote.bar.seeds", "[::1]:9090")
            .build();
        SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));

        Settings brokenSettings = Settings.builder().put("cluster.remote.foo.seeds", "192.168.0.1").build();
        expectThrows(
            IllegalArgumentException.class,
            () -> SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS.getAllConcreteSettings(brokenSettings)
                .forEach(setting -> setting.get(brokenSettings))
        );

        Settings brokenPortSettings = Settings.builder().put("cluster.remote.foo.seeds", "192.168.0.1:123456789123456789").build();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS.getAllConcreteSettings(brokenSettings)
                .forEach(setting -> setting.get(brokenPortSettings))
        );
        assertEquals("failed to parse port", e.getMessage());
    }

    public void testGroupClusterIndices() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes);
            MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes)
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            knownNodes.add(cluster2Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
            builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());

            try (MockTransportService transportService = startTransport(builder.build())) {
                final var service = transportService.getRemoteClusterService();
                assertTrue(hasRegisteredClusters(service));
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                assertTrue(isRemoteClusterRegistered(service, "cluster_2"));
                assertFalse(isRemoteClusterRegistered(service, "foo"));
                {
                    Map<String, List<String>> perClusterIndices = service.groupClusterIndices(
                        service.getRegisteredRemoteClusterNames(),
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
                        service.getRegisteredRemoteClusterNames(),
                        new String[] { "foo:bar", "cluster_1:bar", "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo" }
                    )
                );

                expectThrows(
                    NoSuchRemoteClusterException.class,
                    () -> service.groupClusterIndices(
                        service.getRegisteredRemoteClusterNames(),
                        new String[] { "cluster_1:bar", "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "does_not_exist:*" }
                    )
                );

                // test cluster exclusions
                {
                    String[] indices = shuffledList(List.of("cluster*:foo*", "foo", "-cluster_1:*", "*:boo")).toArray(new String[0]);

                    Map<String, List<String>> perClusterIndices = service.groupClusterIndices(
                        service.getRegisteredRemoteClusterNames(),
                        indices
                    );
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

                    Map<String, List<String>> perClusterIndices = service.groupClusterIndices(
                        service.getRegisteredRemoteClusterNames(),
                        indices
                    );
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

                    Map<String, List<String>> perClusterIndices = service.groupClusterIndices(
                        service.getRegisteredRemoteClusterNames(),
                        indices
                    );
                    assertEquals(1, perClusterIndices.size());
                    List<String> localIndexes = perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertNotNull(localIndexes);
                    assertEquals(1, localIndexes.size());
                    assertEquals("foo", localIndexes.get(0));
                }
                {
                    String[] indices = shuffledList(List.of("cluster*:*", "foo", "-*:*")).toArray(new String[0]);

                    Map<String, List<String>> perClusterIndices = service.groupClusterIndices(
                        service.getRegisteredRemoteClusterNames(),
                        indices
                    );
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
                            service.getRegisteredRemoteClusterNames(),
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
                            service.getRegisteredRemoteClusterNames(),
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
                        () -> service.groupClusterIndices(service.getRegisteredRemoteClusterNames(), new String[] { "-cluster_1:*" })
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
                        () -> service.groupClusterIndices(service.getRegisteredRemoteClusterNames(), new String[] { "-*:*" })
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
                        () -> service.groupClusterIndices(service.getRegisteredRemoteClusterNames(), indices)
                    );
                    assertThat(
                        e.getMessage(),
                        containsString("The '-' exclusions in the index expression list excludes all indexes. Nothing to search.")
                    );
                }
            }
        }
    }

    public void testGroupIndices() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes);
            MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes)
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            knownNodes.add(cluster2Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
            builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());

            try (MockTransportService transportService = startTransport(builder.build())) {
                final var service = transportService.getRemoteClusterService();
                assertTrue(hasRegisteredClusters(service));
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                assertTrue(isRemoteClusterRegistered(service, "cluster_2"));
                assertFalse(isRemoteClusterRegistered(service, "foo"));
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
                    assertArrayEquals(new String[] { "foo" }, perClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).indices());
                    assertArrayEquals(new String[] { "bar", "test", "baz", "boo" }, perClusterIndices.get("cluster_1").indices());
                    assertArrayEquals(new String[] { "foo:bar", "foo*", "baz", "boo" }, perClusterIndices.get("cluster_2").indices());
                }
                {
                    expectThrows(
                        NoSuchRemoteClusterException.class,
                        () -> service.groupClusterIndices(
                            service.getRegisteredRemoteClusterNames(),
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

    public void testGroupIndicesWithoutRemoteClusterClientRole() throws Exception {
        final Settings settings = onlyRoles(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "node-1").build(),
            Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE)
        );
        try (var transportService = startTransport("node-1", settings)) {
            final var service = transportService.getRemoteClusterService();
            expectThrows(IllegalArgumentException.class, service::ensureClientIsEnabled);
            assertFalse(hasRegisteredClusters(service));
            final IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> service.groupIndices(IndicesOptions.LENIENT_EXPAND_OPEN, new String[] { "cluster_1:bar", "cluster_2:foo*" })
            );
            assertThat(error.getMessage(), equalTo("node [node-1] does not have the remote cluster client role enabled"));
        }
    }

    public void testIncrementallyAddClusters() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes);
            MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes)
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            knownNodes.add(cluster2Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = startTransport()) {
                final var service = transportService.getRemoteClusterService();
                assertFalse(hasRegisteredClusters(service));
                Settings cluster1Settings = createSettings("cluster_1", Collections.singletonList(cluster1Seed.getAddress().toString()));
                PlainActionFuture<Void> clusterAdded = new PlainActionFuture<>();
                // Add the cluster on a different thread to test that we wait for a new cluster to
                // connect before returning.
                new Thread(() -> {
                    try {
                        service.updateLinkedProject(toConfig("cluster_1", cluster1Settings));
                        clusterAdded.onResponse(null);
                    } catch (Exception e) {
                        clusterAdded.onFailure(e);
                    }
                }).start();
                clusterAdded.actionGet();
                assertTrue(hasRegisteredClusters(service));
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                Settings cluster2Settings = createSettings("cluster_2", Collections.singletonList(cluster2Seed.getAddress().toString()));
                service.updateLinkedProject(toConfig("cluster_2", cluster2Settings));
                assertTrue(hasRegisteredClusters(service));
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                assertTrue(isRemoteClusterRegistered(service, "cluster_2"));
                Settings cluster2SettingsDisabled = createSettings("cluster_2", Collections.emptyList());
                service.updateLinkedProject(toConfig("cluster_2", cluster2SettingsDisabled));
                assertFalse(isRemoteClusterRegistered(service, "cluster_2"));
                IllegalArgumentException iae = expectThrows(
                    IllegalArgumentException.class,
                    () -> service.updateLinkedProject(toConfig(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, Settings.EMPTY))
                );
                assertEquals("remote clusters must not have the empty string as its key", iae.getMessage());
            }
        }
    }

    public void testDefaultPingSchedule() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("cluster_1_node", knownNodes)) {
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
            try (MockTransportService transportService = startTransport(settings)) {
                final var service = transportService.getRemoteClusterService();
                assertTrue(hasRegisteredClusters(service));
                final var newSettings = createSettings("cluster_1", Collections.singletonList(seedNode.getAddress().toString()));
                final var mergedSettings = Settings.builder().put(settings, false).put(newSettings, false).build();
                service.updateLinkedProject(toConfig("cluster_1", mergedSettings));
                assertTrue(hasRegisteredClusters(service));
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                RemoteClusterConnection remoteClusterConnection = service.getRemoteClusterConnection("cluster_1");
                assertEquals(pingSchedule, remoteClusterConnection.getConnectionManager().getConnectionProfile().getPingInterval());
            }
        }
    }

    public void testCustomPingSchedule() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes);
            MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes)
        ) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            DiscoveryNode cluster2Seed = cluster2Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            knownNodes.add(cluster2Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            if (randomBoolean()) {
                builder.put(TransportSettings.PING_SCHEDULE.getKey(), TimeValue.timeValueSeconds(randomIntBetween(1, 10)));
            }
            builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());
            builder.putList("cluster.remote.cluster_2.seeds", cluster2Seed.getAddress().toString());
            TimeValue pingSchedule1 = // randomBoolean() ? TimeValue.MINUS_ONE :
                TimeValue.timeValueSeconds(randomIntBetween(1, 10));
            builder.put("cluster.remote.cluster_1.transport.ping_schedule", pingSchedule1);
            TimeValue pingSchedule2 = // randomBoolean() ? TimeValue.MINUS_ONE :
                TimeValue.timeValueSeconds(randomIntBetween(1, 10));
            builder.put("cluster.remote.cluster_2.transport.ping_schedule", pingSchedule2);
            try (MockTransportService transportService = startTransport(builder.build())) {
                final var service = transportService.getRemoteClusterService();
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                RemoteClusterConnection remoteClusterConnection1 = service.getRemoteClusterConnection("cluster_1");
                assertEquals(pingSchedule1, remoteClusterConnection1.getConnectionManager().getConnectionProfile().getPingInterval());
                RemoteClusterConnection remoteClusterConnection2 = service.getRemoteClusterConnection("cluster_2");
                assertEquals(pingSchedule2, remoteClusterConnection2.getConnectionManager().getConnectionProfile().getPingInterval());
            }
        }
    }

    public void testChangeSettings() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes)) {
            DiscoveryNode cluster1Seed = cluster1Transport.getLocalNode();
            knownNodes.add(cluster1Transport.getLocalNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster_1.seeds", cluster1Seed.getAddress().toString());

            try (MockTransportService transportService = startTransport(builder.build())) {
                final var service = transportService.getRemoteClusterService();
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
                service.updateLinkedProject(toConfig("cluster_1", settingsChange.build()));
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

    public void testRemoteNodeAttribute() throws InterruptedException {
        final Settings settings = Settings.builder().put("cluster.remote.node.attr", "gateway").build();
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Settings gateway = Settings.builder().put("node.attr.gateway", true).build();
        try (
            MockTransportService c1N1 = startTransport("cluster_1_node_1", knownNodes);
            MockTransportService c1N2 = startTransport("cluster_1_node_2", knownNodes, gateway);
            MockTransportService c2N1 = startTransport("cluster_2_node_1", knownNodes);
            MockTransportService c2N2 = startTransport("cluster_2_node_2", knownNodes, gateway)
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

            try (MockTransportService transportService = startTransport(settings)) {
                final var service = transportService.getRemoteClusterService();
                assertFalse(hasRegisteredClusters(service));

                final CountDownLatch firstLatch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_1", settings, List.of(c1N1Node, c1N2Node), connectionListener(firstLatch));
                firstLatch.await();

                final CountDownLatch secondLatch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_2", settings, List.of(c2N1Node, c2N2Node), connectionListener(secondLatch));
                secondLatch.await();

                assertTrue(hasRegisteredClusters(service));
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                assertFalse(isRemoteNodeConnected(service, "cluster_1", c1N1Node));
                assertTrue(isRemoteNodeConnected(service, "cluster_1", c1N2Node));
                assertTrue(isRemoteClusterRegistered(service, "cluster_2"));
                assertFalse(isRemoteNodeConnected(service, "cluster_2", c2N1Node));
                assertTrue(isRemoteNodeConnected(service, "cluster_2", c2N2Node));
                assertEquals(0, transportService.getConnectionManager().size());
            }
        }
    }

    public void testRemoteNodeRoles() throws InterruptedException {
        final Settings settings = Settings.EMPTY;
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Settings data = nonMasterNode();
        final Settings dedicatedMaster = masterOnlyNode();
        try (
            MockTransportService c1N1 = startTransport("cluster_1_node_1", knownNodes, dedicatedMaster);
            MockTransportService c1N2 = startTransport("cluster_1_node_2", knownNodes, data);
            MockTransportService c2N1 = startTransport("cluster_2_node_1", knownNodes, dedicatedMaster);
            MockTransportService c2N2 = startTransport("cluster_2_node_2", knownNodes, data)
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

            try (MockTransportService transportService = startTransport(settings)) {
                final var service = transportService.getRemoteClusterService();
                assertFalse(hasRegisteredClusters(service));

                final CountDownLatch firstLatch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_1", settings, List.of(c1N1Node, c1N2Node), connectionListener(firstLatch));
                firstLatch.await();

                final CountDownLatch secondLatch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_2", settings, List.of(c2N1Node, c2N2Node), connectionListener(secondLatch));
                secondLatch.await();

                assertTrue(hasRegisteredClusters(service));
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                assertFalse(isRemoteNodeConnected(service, "cluster_1", c1N1Node));
                assertTrue(isRemoteNodeConnected(service, "cluster_1", c1N2Node));
                assertTrue(isRemoteClusterRegistered(service, "cluster_2"));
                assertFalse(isRemoteNodeConnected(service, "cluster_2", c2N1Node));
                assertTrue(isRemoteNodeConnected(service, "cluster_2", c2N2Node));
                assertEquals(0, transportService.getConnectionManager().size());
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
            MockTransportService c1N1 = startTransport("cluster_1_node_1", knownNodes_c1, settings);
            MockTransportService c1N2 = startTransport("cluster_1_node_2", knownNodes_c1, settings);
            MockTransportService c2N1 = startTransport("cluster_2_node_1", knownNodes_c2, settings);
            MockTransportService c2N2 = startTransport("cluster_2_node_2", knownNodes_c2, settings)
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

            try (MockTransportService transportService = startTransport(settings)) {
                final var service = transportService.getRemoteClusterService();
                assertFalse(hasRegisteredClusters(service));

                final CountDownLatch firstLatch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_1", settings, List.of(c1N1Node, c1N2Node), connectionListener(firstLatch));
                firstLatch.await();

                final CountDownLatch secondLatch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_2", settings, List.of(c2N1Node, c2N2Node), connectionListener(secondLatch));
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

    public void testCollectNodesConcurrentWithSettingsChanges() {
        final List<DiscoveryNode> knownNodes_c1 = new CopyOnWriteArrayList<>();

        try (var c1N1 = startTransport("cluster_1_node_1", knownNodes_c1)) {
            final var c1N1Node = c1N1.getLocalNode();
            knownNodes_c1.add(c1N1Node);
            final var seedList = List.of(c1N1Node.getAddress().toString());
            final var initialSettings = createSettings("cluster_1", seedList);

            try (var transportService = startTransport(initialSettings)) {
                final var service = transportService.getRemoteClusterService();
                assertTrue(hasRegisteredClusters(service));
                final var numTasks = between(3, 5);
                final var taskLatch = new CountDownLatch(numTasks);

                ESTestCase.startInParallel(numTasks, threadNumber -> {
                    if (threadNumber == 0) {
                        taskLatch.countDown();
                        boolean isLinked = true;
                        while (taskLatch.getCount() != 0) {
                            final var future = new PlainActionFuture<RemoteClusterService.RemoteClusterConnectionStatus>();
                            final var settings = createSettings("cluster_1", isLinked ? Collections.emptyList() : seedList);
                            updateRemoteCluster(service, "cluster_1", initialSettings, settings, future);
                            safeGet(future);
                            isLinked = isLinked == false;
                        }
                        return;
                    }

                    // Verify collectNodes() always invokes the listener, even if the node is concurrently being unlinked.
                    try {
                        for (int i = 0; i < 10; ++i) {
                            final var latch = new CountDownLatch(1);
                            final var exRef = new AtomicReference<Exception>();
                            service.collectNodes(Set.of("cluster_1"), new LatchedActionListener<>(new ActionListener<>() {
                                @Override
                                public void onResponse(BiFunction<String, String, DiscoveryNode> func) {
                                    assertEquals(c1N1Node, func.apply("cluster_1", c1N1Node.getId()));
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    exRef.set(e);
                                }
                            }, latch));
                            safeAwait(latch);
                            if (exRef.get() != null) {
                                assertThat(
                                    exRef.get(),
                                    either(instanceOf(TransportException.class)).or(instanceOf(NoSuchRemoteClusterException.class))
                                        .or(instanceOf(AlreadyClosedException.class))
                                        .or(instanceOf(NoSeedNodeLeftException.class))
                                );
                            }
                        }
                    } finally {
                        taskLatch.countDown();
                    }
                });
            }
        }
    }

    public void testRemoteClusterSkipIfDisconnectedSetting() {
        {
            Settings settings = Settings.builder()
                .put("cluster.remote.foo.seeds", "127.0.0.1:9300")
                .put("cluster.remote.foo.skip_unavailable", true)
                .build();
            RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(settings)
                .forEach(setting -> setting.get(settings));
        }
        {
            Settings brokenSettingsDependency = Settings.builder().put("cluster.remote.foo.skip_unavailable", true).build();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(brokenSettingsDependency)
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
                () -> RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(brokenSettingsType)
                    .forEach(setting -> setting.get(brokenSettingsType))
            );
        }

        {
            Settings settings = Settings.builder()
                .put("cluster.remote.foo.mode", "proxy")
                .put("cluster.remote.foo.proxy_address", "127.0.0.1:9300")
                .put("cluster.remote.foo.transport.ping_schedule", "5s")
                .build();
            RemoteClusterSettings.REMOTE_CLUSTER_PING_SCHEDULE.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));
        }
        {
            Settings brokenSettingsDependency = Settings.builder()
                .put("cluster.remote.foo.proxy_address", "127.0.0.1:9300")
                .put("cluster.remote.foo.transport.ping_schedule", "5s")
                .build();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterSettings.REMOTE_CLUSTER_PING_SCHEDULE.getAllConcreteSettings(brokenSettingsDependency)
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
            RemoteClusterSettings.REMOTE_CLUSTER_COMPRESS.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));
        }
        {
            Settings brokenSettingsDependency = Settings.builder()
                .put("cluster.remote.foo.proxy_address", "127.0.0.1:9300")
                .put("cluster.remote.foo.transport.compress", true)
                .build();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterSettings.REMOTE_CLUSTER_COMPRESS.getAllConcreteSettings(brokenSettingsDependency)
                    .forEach(setting -> setting.get(brokenSettingsDependency))
            );
            assertEquals(
                "Cannot configure setting [cluster.remote.foo.transport.compress] if remote cluster is not enabled.",
                iae.getMessage()
            );
        }

        AbstractScopedSettings service = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(
                Arrays.asList(SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS, RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE)
            )
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
            MockTransportService cluster_node_0 = startTransport("cluster_node_0", knownNodes);
            MockTransportService cluster_node_1 = startTransport("cluster_node_1", knownNodes)
        ) {

            final DiscoveryNode node0 = cluster_node_0.getLocalNode();
            final DiscoveryNode node1 = cluster_node_1.getLocalNode();
            knownNodes.add(node0);
            knownNodes.add(node1);
            Collections.shuffle(knownNodes, random());
            final Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster_test.seeds", Collections.singletonList(node0.getAddress().toString()));
            final var initialSettings = builder.build();

            try (MockTransportService transportService = startTransport(initialSettings)) {
                final var service = transportService.getRemoteClusterService();
                assertTrue(hasRegisteredClusters(service));

                final RemoteClusterConnection firstRemoteClusterConnection = service.getRemoteClusterConnection("cluster_test");
                assertTrue(firstRemoteClusterConnection.isNodeConnected(node0));
                assertTrue(firstRemoteClusterConnection.isNodeConnected(node1));
                assertEquals(2, firstRemoteClusterConnection.getNumNodesConnected());
                assertFalse(firstRemoteClusterConnection.isClosed());

                final CountDownLatch firstLatch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_test", initialSettings, List.of(node0), connectionListener(firstLatch));
                firstLatch.await();

                assertTrue(hasRegisteredClusters(service));
                assertTrue(firstRemoteClusterConnection.isNodeConnected(node0));
                assertTrue(firstRemoteClusterConnection.isNodeConnected(node1));
                assertEquals(2, firstRemoteClusterConnection.getNumNodesConnected());
                assertFalse(firstRemoteClusterConnection.isClosed());
                assertSame(firstRemoteClusterConnection, service.getRemoteClusterConnection("cluster_test"));

                final List<DiscoveryNode> newSeeds = new ArrayList<>();
                newSeeds.add(node1);
                if (randomBoolean()) {
                    newSeeds.add(node0);
                    Collections.shuffle(newSeeds, random());
                }

                final CountDownLatch secondLatch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_test", initialSettings, newSeeds, connectionListener(secondLatch));
                secondLatch.await();

                assertTrue(hasRegisteredClusters(service));
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

    public static void updateSkipUnavailable(RemoteClusterService service, String clusterAlias, boolean skipUnavailable) {
        RemoteClusterConnection connection = service.getRemoteClusterConnection(clusterAlias);
        connection.setSkipUnavailable(skipUnavailable);
    }

    public static void addConnectionListener(
        RemoteClusterService service,
        Set<String> clusterAliases,
        TransportConnectionListener listener
    ) {
        for (final var clusterAlias : clusterAliases) {
            final var connection = service.getRemoteClusterConnection(clusterAlias);
            ConnectionManager connectionManager = connection.getConnectionManager();
            connectionManager.addListener(listener);
        }
    }

    public void testSkipUnavailable() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes)) {
            DiscoveryNode seedNode = seedTransport.getLocalNode();
            knownNodes.add(seedNode);
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalNode().getAddress().toString());
            try (MockTransportService service = startTransport(builder.build())) {
                assertTrue(service.getRemoteClusterService().isSkipUnavailable("cluster1").orElse(true));

                if (randomBoolean()) {
                    updateSkipUnavailable(service.getRemoteClusterService(), "cluster1", false);
                    assertFalse(service.getRemoteClusterService().isSkipUnavailable("cluster1").orElse(true));
                }

                updateSkipUnavailable(service.getRemoteClusterService(), "cluster1", true);
                assertTrue(service.getRemoteClusterService().isSkipUnavailable("cluster1").orElse(true));
            }
        }
    }

    public void testRemoteClusterServiceNotEnabledGetRemoteClusterConnection() {
        final Settings settings = Settings.builder()
            .put(removeRoles(Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)))
            .put(Node.NODE_NAME_SETTING.getKey(), "node-1")
            .build();
        try (MockTransportService service = startTransport("node-1", settings)) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> service.getRemoteClusterService().getRemoteClusterConnection("test")
            );
            assertThat(e.getMessage(), equalTo("node [node-1] does not have the [remote_cluster_client] role"));
        }
    }

    public void testRemoteClusterServiceEnsureClientIsEnabled() throws IOException {
        final var nodeNameSettings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "node-1").build();

        // Shouldn't throw when the remote cluster client role is enabled.
        final var settingsWithRemoteClusterClientRole = Settings.builder()
            .put(nodeNameSettings)
            .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
            .build();
        try (var transportService = startTransport(settingsWithRemoteClusterClientRole)) {
            transportService.getRemoteClusterService().ensureClientIsEnabled();
        }

        // Expect throws when missing the remote cluster client role.
        final var settingsWithoutRemoteClusterClientRole = Settings.builder()
            .put(nodeNameSettings)
            .put(removeRoles(Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)))
            .build();
        try (var transportService = startTransport("node-1", settingsWithoutRemoteClusterClientRole)) {
            final var service = transportService.getRemoteClusterService();
            final var exception = expectThrows(IllegalArgumentException.class, service::ensureClientIsEnabled);
            assertThat(exception.getMessage(), equalTo("node [node-1] does not have the [remote_cluster_client] role"));
        }

        // Expect throws when missing both the remote cluster client role and search node role when stateless is enabled.
        final var statelessEnabledSettingsOnNonSearchNode = Settings.builder()
            .put(nodeNameSettings)
            .put(onlyRole(DiscoveryNodeRole.INDEX_ROLE))
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
        try (var transportService = startTransport("node-1", statelessEnabledSettingsOnNonSearchNode)) {
            final var service = transportService.getRemoteClusterService();
            final var exception = expectThrows(IllegalArgumentException.class, service::ensureClientIsEnabled);
            assertThat(
                exception.getMessage(),
                equalTo(
                    "node [node-1] must have the [remote_cluster_client] role or the [search] role "
                        + "in stateless environments to use linked project client features"
                )
            );
        }

        // Shouldn't throw when stateless is enabled on a search node, or a node with remote cluster client role, or both.
        final var statelessEnabledOnSearchNodeSettings = Settings.builder()
            .put(nodeNameSettings)
            .put(onlyRole(DiscoveryNodeRole.SEARCH_ROLE))
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
        try (var transportService = startTransport(statelessEnabledOnSearchNodeSettings)) {
            transportService.getRemoteClusterService().ensureClientIsEnabled();
        }
        final var statelessEnabledOnRemoteClusterClientSettings = Settings.builder()
            .put(nodeNameSettings)
            .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
        try (var transportService = startTransport(statelessEnabledOnRemoteClusterClientSettings)) {
            transportService.getRemoteClusterService().ensureClientIsEnabled();
        }
        final var statelessEnabledOnSearchNodeAndRemoteClusterClientSettings = Settings.builder()
            .put(nodeNameSettings)
            .put(onlyRoles(Set.of(DiscoveryNodeRole.SEARCH_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)))
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
        try (var transportService = startTransport(statelessEnabledOnSearchNodeAndRemoteClusterClientSettings)) {
            transportService.getRemoteClusterService().ensureClientIsEnabled();
        }
    }

    public void testRemoteClusterServiceNotEnabledGetCollectNodes() {
        final Settings settings = Settings.builder()
            .put(removeRoles(Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)))
            .put(Node.NODE_NAME_SETTING.getKey(), "node-1")
            .build();
        try (MockTransportService service = startTransport("node-1", settings)) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> service.getRemoteClusterService().collectNodes(Set.of(), ActionListener.noop())
            );
            assertThat(e.getMessage(), equalTo("node [node-1] does not have the [remote_cluster_client] role"));
        }
    }

    public void testUseDifferentTransportProfileForCredentialsProtectedRemoteClusters() throws InterruptedException {
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService c1 = startTransport("cluster_1", knownNodes, REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS);
            MockTransportService c2 = startTransport("cluster_2", knownNodes, REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS)
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
                final var service = transportService.getRemoteClusterService();
                final CountDownLatch firstLatch = new CountDownLatch(1);
                final Settings.Builder firstRemoteClusterSettingsBuilder = Settings.builder();
                final boolean firstRemoteClusterProxyMode = randomBoolean();
                if (firstRemoteClusterProxyMode) {
                    firstRemoteClusterSettingsBuilder.put("cluster.remote.cluster_1.mode", "proxy")
                        .put("cluster.remote.cluster_1.proxy_address", c1Node.getAddress().toString());
                } else {
                    firstRemoteClusterSettingsBuilder.put("cluster.remote.cluster_1.seeds", c1Node.getAddress().toString());
                }
                final var updatedSettings1 = firstRemoteClusterSettingsBuilder.build();
                updateRemoteCluster(service, "cluster_1", settings, updatedSettings1, connectionListener(firstLatch));
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
                final var updatedSettings2 = secondRemoteClusterSettingsBuilder.build();
                updateRemoteCluster(service, "cluster_2", settings, updatedSettings2, connectionListener(secondLatch));
                secondLatch.await();

                assertTrue(hasRegisteredClusters(service));
                assertTrue(isRemoteClusterRegistered(service, "cluster_1"));
                if (firstRemoteClusterProxyMode) {
                    assertFalse(isRemoteNodeConnected(service, "cluster_1", c1Node));
                } else {
                    assertTrue(isRemoteNodeConnected(service, "cluster_1", c1Node));
                }
                assertTrue(isRemoteClusterRegistered(service, "cluster_2"));
                if (secondRemoteClusterProxyMode) {
                    assertFalse(isRemoteNodeConnected(service, "cluster_2", c2Node));
                } else {
                    assertTrue(isRemoteNodeConnected(service, "cluster_2", c2Node));
                }
                // No local node connection
                assertEquals(0, transportService.getConnectionManager().size());
            }
        }
    }

    public void testUpdateRemoteClusterCredentialsRebuildsConnectionWithCorrectProfile() throws InterruptedException {
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService c = startTransport("cluster_1", knownNodes, REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS)) {
            final DiscoveryNode discoNode = c.getLocalNode().withTransportAddress(c.boundRemoteAccessAddress().publishAddress());
            try (MockTransportService transportService = startTransport()) {
                final var service = transportService.getRemoteClusterService();
                final Settings clusterSettings = buildRemoteClusterSettings("cluster_1", discoNode.getAddress().toString());
                final CountDownLatch latch = new CountDownLatch(1);
                updateRemoteCluster(service, "cluster_1", Settings.EMPTY, clusterSettings, connectionListener(latch));
                latch.await();

                assertConnectionHasProfile(service.getRemoteClusterConnection("cluster_1"), "default");

                {
                    final MockSecureSettings secureSettings = new MockSecureSettings();
                    secureSettings.setString("cluster.remote.cluster_1.credentials", randomAlphaOfLength(10));
                    final PlainActionFuture<RemoteClusterService.RemoteClusterConnectionStatus> listener = new PlainActionFuture<>();
                    final Settings settings = Settings.builder().put(clusterSettings).setSecureSettings(secureSettings).build();
                    final var result = service.getRemoteClusterCredentialsManager().updateClusterCredentials(settings);
                    assertThat(result.addedClusterAliases(), equalTo(Set.of("cluster_1")));
                    final var config = buildLinkedProjectConfig("cluster_1", Settings.EMPTY, settings);
                    service.updateRemoteCluster(config, true, listener);
                    listener.actionGet(10, TimeUnit.SECONDS);
                }

                assertConnectionHasProfile(
                    service.getRemoteClusterConnection("cluster_1"),
                    RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE
                );

                {
                    final PlainActionFuture<RemoteClusterService.RemoteClusterConnectionStatus> listener = new PlainActionFuture<>();
                    // Settings without credentials constitute credentials removal
                    final var result = service.getRemoteClusterCredentialsManager().updateClusterCredentials(clusterSettings);
                    assertThat(result.addedClusterAliases().size(), equalTo(0));
                    assertThat(result.removedClusterAliases(), equalTo(Set.of("cluster_1")));
                    final var config = buildLinkedProjectConfig("cluster_1", Settings.EMPTY, clusterSettings);
                    service.updateRemoteCluster(config, true, listener);
                    listener.actionGet(10, TimeUnit.SECONDS);
                }

                assertConnectionHasProfile(service.getRemoteClusterConnection("cluster_1"), "default");
            }
        }
    }

    public void testUpdateRemoteClusterCredentialsRebuildsMultipleConnectionsDespiteFailures() throws InterruptedException {
        try (
            MockTransportService c1 = startTransport("cluster_1", REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS);
            MockTransportService c2 = startTransport("cluster_2", REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS)
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
                final var service = transportService.getRemoteClusterService();
                final Settings cluster1Settings = buildRemoteClusterSettings(goodCluster, c1DiscoNode.getAddress().toString());
                final var latch = new CountDownLatch(1);
                updateRemoteCluster(service, goodCluster, Settings.EMPTY, cluster1Settings, connectionListener(latch));
                latch.await();

                final Settings cluster2Settings = buildRemoteClusterSettings(badCluster, c2DiscoNode.getAddress().toString());
                final PlainActionFuture<RemoteClusterService.RemoteClusterConnectionStatus> future = new PlainActionFuture<>();
                updateRemoteCluster(service, badCluster, Settings.EMPTY, cluster2Settings, future);
                final var ex = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
                assertThat(ex.getMessage(), containsString("bad cluster"));

                assertConnectionHasProfile(service.getRemoteClusterConnection(goodCluster), "default");
                assertConnectionHasProfile(service.getRemoteClusterConnection(badCluster), "default");
                expectThrows(NoSuchRemoteClusterException.class, () -> service.getRemoteClusterConnection(missingCluster));
                final Set<String> aliases = Set.of(badCluster, goodCluster, missingCluster);
                final ActionListener<RemoteClusterService.RemoteClusterConnectionStatus> noop = ActionListener.noop();

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
                    final var result = service.getRemoteClusterCredentialsManager().updateClusterCredentials(settings);
                    assertThat(result.addedClusterAliases(), equalTo(aliases));
                    try (var connectionRefs = new RefCountingRunnable(() -> listener.onResponse(null))) {
                        for (String alias : aliases) {
                            final var config = buildLinkedProjectConfig(alias, Settings.EMPTY, settings);
                            service.updateRemoteCluster(config, true, ActionListener.releaseAfter(noop, connectionRefs.acquire()));
                        }
                    }
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
                    // Settings without credentials constitute credentials removal
                    final var result = service.getRemoteClusterCredentialsManager().updateClusterCredentials(settings);
                    assertThat(result.addedClusterAliases().size(), equalTo(0));
                    assertThat(result.removedClusterAliases(), equalTo(aliases));
                    try (var connectionRefs = new RefCountingRunnable(() -> listener.onResponse(null))) {
                        for (String alias : aliases) {
                            final var config = buildLinkedProjectConfig(alias, Settings.EMPTY, settings);
                            service.updateRemoteCluster(config, true, ActionListener.releaseAfter(noop, connectionRefs.acquire()));
                        }
                    }
                    listener.actionGet(10, TimeUnit.SECONDS);
                }

                assertConnectionHasProfile(service.getRemoteClusterConnection(goodCluster), "default");
                assertConnectionHasProfile(service.getRemoteClusterConnection(badCluster), "default");
                expectThrows(NoSuchRemoteClusterException.class, () -> service.getRemoteClusterConnection(missingCluster));
            }
        }
    }

    public void testCorrectTransportProfileUsedWhenCPSEnabled() {
        final var knownNodes = new CopyOnWriteArrayList<DiscoveryNode>();
        try (var seedTransport = startTransport("seed_node", knownNodes, REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS)) {
            knownNodes.add(seedTransport.getLocalNode());
            final var settings = Settings.builder()
                .putList("cluster.remote.cluster1.seeds", seedTransport.getLocalNode().getAddress().toString())
                .put("serverless.cross_project.enabled", true)
                .build();
            try (var transportService = startTransport(settings)) {
                final var service = transportService.getRemoteClusterService();
                assertTrue(hasRegisteredClusters(service));
                assertConnectionHasProfile(
                    service.getRemoteClusterConnection("cluster1"),
                    RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE
                );
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

    public void testLogsConnectionResult() {
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        try (
            var remote = startTransport("remote");
            var local = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                clusterSettings
            )
        ) {
            local.start();
            local.acceptIncomingRequests();

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

    public void testSetSkipUnavailable() {
        final var skipUnavailableProperty = RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("remote")
            .getKey();
        final var seedNodeProperty = SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("remote").getKey();
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings();

        try (
            var remote1Transport = startTransport("remote1");
            var remote2Transport = startTransport("remote2");
            var local = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                clusterSettings
            )
        ) {
            final var remoteClusterService = local.getRemoteClusterService();
            record SkipUnavailableTestConfig(
                boolean skipUnavailable,
                MockTransportService seedNodeTransportService,
                boolean isNewConnection,
                boolean rebuildExpected
            ) {}

            final Consumer<SkipUnavailableTestConfig> applySettingsAndVerify = (cfg) -> {
                final var currentConnection = cfg.isNewConnection ? null : remoteClusterService.getRemoteClusterConnection("remote");
                clusterSettings.applySettings(
                    Settings.builder()
                        .put(skipUnavailableProperty, cfg.skipUnavailable)
                        .put(seedNodeProperty, cfg.seedNodeTransportService.getLocalNode().getAddress().toString())
                        .build()
                );
                assertTrue(isRemoteClusterRegistered(remoteClusterService, "remote"));
                assertEquals(cfg.skipUnavailable, remoteClusterService.isSkipUnavailable("remote").orElseThrow());
                if (cfg.rebuildExpected) {
                    assertNotSame(currentConnection, remoteClusterService.getRemoteClusterConnection("remote"));
                } else if (cfg.isNewConnection == false) {
                    assertSame(currentConnection, remoteClusterService.getRemoteClusterConnection("remote"));
                }
            };

            // Apply the initial settings and verify the new connection is built.
            var skipUnavailable = randomBoolean();
            applySettingsAndVerify.accept(new SkipUnavailableTestConfig(skipUnavailable, remote1Transport, true, true));

            // Change skip_unavailable value, but not seed node, connection should not be rebuilt, but skip_unavailable should be modified.
            skipUnavailable = skipUnavailable == false;
            applySettingsAndVerify.accept(new SkipUnavailableTestConfig(skipUnavailable, remote1Transport, false, false));

            // Change the seed node but not skip_unavailable, connection should be rebuilt and skip_unavailable should stay the same.
            applySettingsAndVerify.accept(new SkipUnavailableTestConfig(skipUnavailable, remote2Transport, false, true));

            // Change skip_unavailable value and the seed node, connection should be rebuilt and skip_unavailable should also be modified.
            skipUnavailable = skipUnavailable == false;
            applySettingsAndVerify.accept(new SkipUnavailableTestConfig(skipUnavailable, remote1Transport, false, true));
        }
    }

    @FixForMultiProject(description = "Refactor to add the linked project ID associated with the alias.")
    private LinkedProjectConfig buildLinkedProjectConfig(String alias, Settings staticSettings, Settings newSettings) {
        final var mergedSettings = Settings.builder().put(staticSettings, false).put(newSettings, false).build();
        return RemoteClusterSettings.toConfig(projectResolver.getProjectId(), ProjectId.DEFAULT, alias, mergedSettings);
    }

    private void updateRemoteCluster(
        RemoteClusterService service,
        String alias,
        Settings settings,
        List<DiscoveryNode> seedNodes,
        ActionListener<RemoteClusterService.RemoteClusterConnectionStatus> listener
    ) {
        final var newSettings = createSettings(alias, seedNodes.stream().map(n -> n.getAddress().toString()).toList());
        updateRemoteCluster(service, alias, settings, newSettings, listener);
    }

    private void updateRemoteCluster(
        RemoteClusterService service,
        String alias,
        Settings settings,
        Settings newSettings,
        ActionListener<RemoteClusterService.RemoteClusterConnectionStatus> listener
    ) {
        service.updateRemoteCluster(buildLinkedProjectConfig(alias, settings, newSettings), false, listener);
    }

    private static Settings createSettings(String clusterAlias, List<String> seeds) {
        Settings.Builder builder = Settings.builder();
        builder.put(
            SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).getKey(),
            Strings.collectionToCommaDelimitedString(seeds)
        );
        return builder.build();
    }

    private static boolean hasRegisteredClusters(RemoteClusterService service) {
        return service.getRegisteredRemoteClusterNames().isEmpty() == false;
    }

    private static boolean isRemoteClusterRegistered(RemoteClusterService service, String clusterName) {
        return service.getRegisteredRemoteClusterNames().contains(clusterName);
    }

    public static boolean isRemoteNodeConnected(RemoteClusterService service, String clusterName, DiscoveryNode node) {
        return service.getRemoteClusterConnection(clusterName).isNodeConnected(node);
    }
}
