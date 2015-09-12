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

package org.elasticsearch.indices.store;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.Version.CURRENT;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.is;

/**
 */
public class IndicesStoreTests extends ESTestCase {

    private final static ShardRoutingState[] NOT_STARTED_STATES;

    static {
        Set<ShardRoutingState> set = new HashSet<>();
        set.addAll(Arrays.asList(ShardRoutingState.values()));
        set.remove(ShardRoutingState.STARTED);
        NOT_STARTED_STATES = set.toArray(new ShardRoutingState[set.size()]);
    }

    private IndicesStore indicesStore;
    private DiscoveryNode localNode;

    @Before
    public void before() {
        localNode = new DiscoveryNode("abc", new LocalTransportAddress("abc"), Version.CURRENT);
        indicesStore = new IndicesStore();
    }

    @Test
    public void testShardCanBeDeleted_noShardRouting() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        ClusterState.Builder clusterState = ClusterState.builder(new ClusterName("test"));
        clusterState.metaData(MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(numShards).numberOfReplicas(numReplicas)));
        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", 1));

        assertFalse(indicesStore.shardCanBeDeleted(clusterState.build(), routingTable.build()));
    }

    @Test
    public void testShardCanBeDeleted_noShardStarted() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        ClusterState.Builder clusterState = ClusterState.builder(new ClusterName("test"));
        clusterState.metaData(MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(numShards).numberOfReplicas(numReplicas)));
        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", 1));

        for (int i = 0; i < numShards; i++) {
            int unStartedShard = randomInt(numReplicas);
            for (int j=0; j <= numReplicas; j++) {
                ShardRoutingState state;
                if (j == unStartedShard) {
                    state = randomFrom(NOT_STARTED_STATES);
                } else {
                    state = randomFrom(ShardRoutingState.values());
                }
                UnassignedInfo unassignedInfo = null;
                if (state == ShardRoutingState.UNASSIGNED) {
                    unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
                }
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, "xyz", null, null, j == 0, state, 0, unassignedInfo));
            }
        }
        assertFalse(indicesStore.shardCanBeDeleted(clusterState.build(), routingTable.build()));
    }

    @Test
    public void testShardCanBeDeleted_shardExistsLocally() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        ClusterState.Builder clusterState = ClusterState.builder(new ClusterName("test"));
        clusterState.metaData(MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(numShards).numberOfReplicas(numReplicas)));
        clusterState.nodes(DiscoveryNodes.builder().localNodeId(localNode.id()).put(localNode).put(new DiscoveryNode("xyz", new LocalTransportAddress("xyz"), Version.CURRENT)));
        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", 1));
        int localShardId = randomInt(numShards - 1);
        for (int i = 0; i < numShards; i++) {
            String nodeId = i == localShardId ? localNode.getId() : randomBoolean() ? "abc" : "xyz";
            String relocationNodeId = randomBoolean() ? null : randomBoolean() ? localNode.getId() : "xyz";
            routingTable.addShard(TestShardRouting.newShardRouting("test", i, nodeId, relocationNodeId, true, ShardRoutingState.STARTED, 0));
            for (int j = 0; j < numReplicas; j++) {
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, nodeId, relocationNodeId, false, ShardRoutingState.STARTED, 0));
            }
        }

        // Shard exists locally, can't delete shard
        assertFalse(indicesStore.shardCanBeDeleted(clusterState.build(), routingTable.build()));
    }

    @Test
    public void testShardCanBeDeleted_nodeNotInList() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        ClusterState.Builder clusterState = ClusterState.builder(new ClusterName("test"));
        clusterState.metaData(MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(numShards).numberOfReplicas(numReplicas)));
        clusterState.nodes(DiscoveryNodes.builder().localNodeId(localNode.id()).put(localNode));
        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", 1));
        for (int i = 0; i < numShards; i++) {
            String relocatingNodeId = randomBoolean() ? null : "def";
            routingTable.addShard(TestShardRouting.newShardRouting("test", i, "xyz", relocatingNodeId, true, ShardRoutingState.STARTED, 0));
            for (int j = 0; j < numReplicas; j++) {
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, "xyz", relocatingNodeId, false, ShardRoutingState.STARTED, 0));
            }
        }

        // null node -> false
        assertFalse(indicesStore.shardCanBeDeleted(clusterState.build(), routingTable.build()));
    }

    @Test
    public void testShardCanBeDeleted_nodeVersion() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        // Most of the times don't test bwc and use current version
        final Version nodeVersion = randomBoolean() ? CURRENT : randomVersion(random());
        ClusterState.Builder clusterState = ClusterState.builder(new ClusterName("test"));
        clusterState.metaData(MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(numShards).numberOfReplicas(numReplicas)));
        clusterState.nodes(DiscoveryNodes.builder().localNodeId(localNode.id()).put(localNode).put(new DiscoveryNode("xyz", new LocalTransportAddress("xyz"), nodeVersion)));
        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", 1));
        for (int i = 0; i < numShards; i++) {
            routingTable.addShard(TestShardRouting.newShardRouting("test", i, "xyz", null, true, ShardRoutingState.STARTED, 0));
            for (int j = 0; j < numReplicas; j++) {
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, "xyz", null, false, ShardRoutingState.STARTED, 0));
            }
        }

        // shard exist on other node (abc)
        assertTrue(indicesStore.shardCanBeDeleted(clusterState.build(), routingTable.build()));
    }

    @Test
    public void testShardCanBeDeleted_relocatingNode() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        ClusterState.Builder clusterState = ClusterState.builder(new ClusterName("test"));
        clusterState.metaData(MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(numShards).numberOfReplicas(numReplicas)));
        final Version nodeVersion = randomBoolean() ? CURRENT : randomVersion(random());

        clusterState.nodes(DiscoveryNodes.builder().localNodeId(localNode.id())
                .put(localNode)
                .put(new DiscoveryNode("xyz", new LocalTransportAddress("xyz"), Version.CURRENT))
                .put(new DiscoveryNode("def", new LocalTransportAddress("def"), nodeVersion) // <-- only set relocating, since we're testing that in this test
                ));
        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", 1));
        for (int i = 0; i < numShards; i++) {
            routingTable.addShard(TestShardRouting.newShardRouting("test", i, "xyz", "def", true, ShardRoutingState.STARTED, 0));
            for (int j = 0; j < numReplicas; j++) {
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, "xyz", "def", false, ShardRoutingState.STARTED, 0));
            }
        }

        // shard exist on other node (abc and def)
        assertTrue(indicesStore.shardCanBeDeleted(clusterState.build(), routingTable.build()));
    }

}
