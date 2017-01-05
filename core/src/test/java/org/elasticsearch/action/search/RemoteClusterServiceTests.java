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
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.net.InetAddress;
import java.net.InetSocketAddress;
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

    public void testRemoteClusterSeedSetting() {
        // simple validation
        RemoteClusterService.REMOTE_CLUSTERS_SEEDS.get(Settings.builder()
            .put("search.remote.seeds.foo", "192.168.0.1:8080")
            .put("search.remote.seeds.bar", "[::1]:9090").build());

        expectThrows(IllegalArgumentException.class, () ->
        RemoteClusterService.REMOTE_CLUSTERS_SEEDS.get(Settings.builder()
            .put("search.remote.seeds.foo", "192.168.0.1").build()));
    }

    public void testBuiltRemoteClustersSeeds() throws Exception {
        Map<String, List<DiscoveryNode>> map = RemoteClusterService.buildRemoteClustersSeeds(
            RemoteClusterService.REMOTE_CLUSTERS_SEEDS.get(Settings.builder()
            .put("search.remote.seeds.foo", "192.168.0.1:8080")
            .put("search.remote.seeds.bar", "[::1]:9090").build()));
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


    public void testFilterIndices() {
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
                builder.putArray("search.remote.seeds.cluster_1", seedNode.getAddress().toString());
                builder.putArray("search.remote.seeds.cluster_2", otherSeedNode.getAddress().toString());
                RemoteClusterService service = new RemoteClusterService(builder.build(), transportService);
                service.initializeRemoteClusters();
                Map<String, List<String>> perClusterIndices = new HashMap<>();
                String[] localIndices = service.filterIndices(perClusterIndices, new String[]{"foo:bar", "cluster_1:bar",
                    "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo"});
                assertArrayEquals(new String[]{"foo:bar", "foo"}, localIndices);
                assertEquals(2, perClusterIndices.size());
                assertEquals(Arrays.asList("bar", "test"), perClusterIndices.get("cluster_1"));
                assertEquals(Arrays.asList("foo:bar", "foo*"), perClusterIndices.get("cluster_2"));
            }
        }
    }
}
