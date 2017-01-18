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
package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class RemoteClusterServiceTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes, Version version) {
        return RemoteClusterConnectionTests.startTransport(id, knownNodes, version, threadPool);
    }

    public void testSettingsAreRegistered() {
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTERS_SEEDS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CONNECTIONS_PER_CLUSTER));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_NODE_ATTRIBUTE));
    }

    public void testRemoteClusterSeedSetting() {
        // simple validation
        Settings settings = Settings.builder()
            .put("search.remote.foo.seeds", "192.168.0.1:8080")
            .put("search.remote.bar.seed", "[::1]:9090").build();
        RemoteClusterService.REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));

        Settings brokenSettings = Settings.builder()
            .put("search.remote.foo.seeds", "192.168.0.1").build();
        expectThrows(IllegalArgumentException.class, () ->
        RemoteClusterService.REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(brokenSettings).forEach(setting -> setting.get(brokenSettings)));
    }

    public void testBuiltRemoteClustersSeeds() throws Exception {
        Map<String, List<DiscoveryNode>> map = RemoteClusterService.buildRemoteClustersSeeds(
            Settings.builder().put("search.remote.foo.seeds", "192.168.0.1:8080").put("search.remote.bar.seeds", "[::1]:9090").build());
        assertEquals(2, map.size());
        assertTrue(map.containsKey("foo"));
        assertTrue(map.containsKey("bar"));
        assertEquals(1, map.get("foo").size());
        assertEquals(1, map.get("bar").size());

        DiscoveryNode foo = map.get("foo").get(0);

        assertEquals(foo.getAddress(), new TransportAddress(new InetSocketAddress(InetAddress.getByName("192.168.0.1"), 8080)));
        assertEquals(foo.getId(), "foo#192.168.0.1:8080");
        assertEquals(foo.getVersion(), Version.CURRENT.minimumCompatibilityVersion());

        DiscoveryNode bar = map.get("bar").get(0);
        assertEquals(bar.getAddress(), new TransportAddress(new InetSocketAddress(InetAddress.getByName("[::1]"), 9090)));
        assertEquals(bar.getId(), "bar#[::1]:9090");
        assertEquals(bar.getVersion(), Version.CURRENT.minimumCompatibilityVersion());
    }


    public void testGroupClusterIndices() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
             MockTransportService otherSeedTransport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode otherSeedNode = otherSeedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(otherSeedTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putArray("search.remote.cluster_1.seeds", seedNode.getAddress().toString());
                builder.putArray("search.remote.cluster_2.seeds", otherSeedNode.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    assertFalse(service.isRemoteClusterRegistered("foo"));
                    Map<String, List<String>> perClusterIndices = service.groupClusterIndices(new String[]{"foo:bar", "cluster_1:bar",
                        "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo"}, i -> false);
                    String[] localIndices = perClusterIndices.computeIfAbsent(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY,
                        k -> Collections.emptyList()).toArray(new String[0]);
                    assertNotNull(perClusterIndices.remove(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY));
                    assertArrayEquals(new String[]{"foo:bar", "foo"}, localIndices);
                    assertEquals(2, perClusterIndices.size());
                    assertEquals(Arrays.asList("bar", "test"), perClusterIndices.get("cluster_1"));
                    assertEquals(Arrays.asList("foo:bar", "foo*"), perClusterIndices.get("cluster_2"));

                    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
                        service.groupClusterIndices(new String[]{"foo:bar", "cluster_1:bar",
                            "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo"}, i -> "cluster_1:bar".equals(i)));

                    assertEquals("Can not filter indices; index cluster_1:bar exists but there is also a remote cluster named:" +
                            " cluster_1", iae.getMessage());
                }
            }
        }
    }

    public void testIncrementallyAddClusters() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
             MockTransportService otherSeedTransport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode otherSeedNode = otherSeedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(otherSeedTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putArray("search.remote.cluster_1.seeds", seedNode.getAddress().toString());
                builder.putArray("search.remote.cluster_2.seeds", otherSeedNode.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(Settings.EMPTY, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.updateRemoteCluster("cluster_1", Collections.singletonList(seedNode.getAddress().address()));
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    service.updateRemoteCluster("cluster_2", Collections.singletonList(otherSeedNode.getAddress().address()));
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    service.updateRemoteCluster("cluster_2", Collections.emptyList());
                    assertFalse(service.isRemoteClusterRegistered("cluster_2"));
                    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                        () -> service.updateRemoteCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, Collections.emptyList()));
                    assertEquals("remote clusters must not have the empty string as its key", iae.getMessage());
                }
            }
        }
    }

    public void testProcessRemoteShards() throws IOException {
        try (RemoteClusterService service = new RemoteClusterService(Settings.EMPTY, null)) {
            assertFalse(service.isCrossClusterSearchEnabled());
            List<ShardIterator> iteratorList = new ArrayList<>();
            Map<String, ClusterSearchShardsResponse> searchShardsResponseMap = new HashMap<>();
            DiscoveryNode[] nodes = new DiscoveryNode[] {
                new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT),
                new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT)
            };
            Map<String, AliasFilter> indicesAndAliases = new HashMap<>();
            indicesAndAliases.put("foo", new AliasFilter(new TermsQueryBuilder("foo", "bar"), Strings.EMPTY_ARRAY));
            indicesAndAliases.put("bar", new AliasFilter(new MatchAllQueryBuilder(), Strings.EMPTY_ARRAY));
            ClusterSearchShardsGroup[] groups = new ClusterSearchShardsGroup[] {
                new ClusterSearchShardsGroup(new ShardId("foo", "foo_id", 0),
                    new ShardRouting[] {TestShardRouting.newShardRouting("foo", 0, "node1", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("foo", 0, "node2", false, ShardRoutingState.STARTED)}),
                new ClusterSearchShardsGroup(new ShardId("foo", "foo_id", 1),
                    new ShardRouting[] {TestShardRouting.newShardRouting("foo", 0, "node1", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("foo", 1, "node2", false, ShardRoutingState.STARTED)}),
                new ClusterSearchShardsGroup(new ShardId("bar", "bar_id", 0),
                    new ShardRouting[] {TestShardRouting.newShardRouting("bar", 0, "node2", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("bar", 0, "node1", false, ShardRoutingState.STARTED)})
            };
            searchShardsResponseMap.put("test_cluster_1", new ClusterSearchShardsResponse(groups, nodes, indicesAndAliases));
            Map<String, AliasFilter> remoteAliases = new HashMap<>();
            service.processRemoteShards(searchShardsResponseMap, iteratorList, remoteAliases);
            assertEquals(3, iteratorList.size());
            for (ShardIterator iterator : iteratorList) {
                if (iterator.shardId().getIndexName().endsWith("foo")) {
                    assertTrue(iterator.shardId().getId() == 0 || iterator.shardId().getId() == 1);
                    assertEquals("test_cluster_1:foo", iterator.shardId().getIndexName());
                    ShardRouting shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "foo");
                    shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "foo");
                    assertNull(iterator.nextOrNull());
                } else {
                    assertEquals(0, iterator.shardId().getId());
                    assertEquals("test_cluster_1:bar", iterator.shardId().getIndexName());
                    ShardRouting shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "bar");
                    shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "bar");
                    assertNull(iterator.nextOrNull());
                }
            }
            assertEquals(2, remoteAliases.size());
            assertTrue(remoteAliases.toString(), remoteAliases.containsKey("foo_id"));
            assertTrue(remoteAliases.toString(), remoteAliases.containsKey("bar_id"));
            assertEquals(new TermsQueryBuilder("foo", "bar"), remoteAliases.get("foo_id").getQueryBuilder());
            assertEquals(new MatchAllQueryBuilder(), remoteAliases.get("bar_id").getQueryBuilder());
        }
    }
}
