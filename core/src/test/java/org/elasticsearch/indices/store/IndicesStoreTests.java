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
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.Version.CURRENT;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.VersionUtils.randomVersion;

public class IndicesStoreTests extends ESTestCase {
    private static final ShardRoutingState[] NOT_STARTED_STATES;

    static {
        Set<ShardRoutingState> set = new HashSet<>();
        set.addAll(Arrays.asList(ShardRoutingState.values()));
        set.remove(ShardRoutingState.STARTED);
        NOT_STARTED_STATES = set.toArray(new ShardRoutingState[set.size()]);
    }

    private DiscoveryNode localNode;

    @Before
    public void createLocalNode() {
        localNode = new DiscoveryNode("abc", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    }

    public void testShardCanBeDeletedNoShardRouting() throws Exception {
        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", "_na_", 1));
        assertFalse(IndicesStore.shardCanBeDeleted(localNode.getId(), routingTable.build()));
    }

    public void testShardCanBeDeletedNoShardStarted() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", "_na_", 1));

        for (int i = 0; i < numShards; i++) {
            int unStartedShard = randomInt(numReplicas);
            for (int j = 0; j <= numReplicas; j++) {
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
                String relocatingNodeId = state == ShardRoutingState.RELOCATING ? randomAsciiOfLength(10) : null;
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, randomAsciiOfLength(10), relocatingNodeId, j == 0, state, unassignedInfo));
            }
        }

        assertFalse(IndicesStore.shardCanBeDeleted(localNode.getId(), routingTable.build()));
    }

    public void testShardCanBeDeletedShardExistsLocally() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", "_na_", 1));
        int localShardId = randomInt(numShards - 1);
        for (int i = 0; i < numShards; i++) {
            int localNodeIndex = randomInt(numReplicas);
            boolean primaryOnLocalNode = i == localShardId && localNodeIndex == numReplicas;
            routingTable.addShard(TestShardRouting.newShardRouting("test", i, primaryOnLocalNode ? localNode.getId() : randomAsciiOfLength(10), true, ShardRoutingState.STARTED));
            for (int j = 0; j < numReplicas; j++) {
                boolean replicaOnLocalNode = i == localShardId && localNodeIndex == j;
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, replicaOnLocalNode ? localNode.getId() : randomAsciiOfLength(10), false, ShardRoutingState.STARTED));
            }
        }

        // Shard exists locally, can't delete shard
        assertFalse(IndicesStore.shardCanBeDeleted(localNode.getId(), routingTable.build()));
    }
}
