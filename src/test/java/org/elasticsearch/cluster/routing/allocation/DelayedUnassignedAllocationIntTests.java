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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.DelayUnassignedAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class DelayedUnassignedAllocationIntTests extends ElasticsearchIntegrationTest {

    @Test
    public void testDelayedAllocation() throws Exception {
        // start a single master node
        final String masterNode = internalCluster().startNodesAsync(1, Settings.builder().put("node.data", false)
                .put(DelayUnassignedAllocation.DELAY_ALLOCATION_DURATION, "1h")
                        // the test infra can maintain same node name when restarting a node
                .put(DelayUnassignedAllocation.DELAY_ALLOCATION_NODE_KEY, DelayUnassignedAllocation.NodeKey.NAME)
                .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 0) // no rebalancing
                .build()).get().get(0);
        // and 4 data nodes
        Settings settings = Settings.builder().put("node.master", false).build();
        internalCluster().startNodesAsync(4, settings).get();

        logger.info("create an index, and make sure the shard and replicas are sync'ed for fast recovery");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen("test");
        final int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "doc", "" + i).setSource("foo", "bar").get();
        }
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, internalCluster().numDataNodes() - 2)));
        ensureGreen("test");

        final ClusterState fullClusterState = client().admin().cluster().prepareState().all().get().getState();
        final Set<ShardId> shardsOnStartedNode = new HashSet<>();
        final Set<ShardId> shardsOnClosedNode = new HashSet<>();
        final AtomicReference<String> nodeNameRef = new AtomicReference<>();
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                nodeNameRef.set(nodeName);
                List<MutableShardRouting> shardsOnRestartedNodeWhenStarted = fullClusterState.routingNodes().node(fullClusterState.nodes().resolveNode(nodeName).getId()).shardsWithState(ShardRoutingState.STARTED);
                shardsOnStartedNode.addAll(toShardIds(shardsOnRestartedNodeWhenStarted));
                List<MutableShardRouting> unassignedAfterClose = client(masterNode).admin().cluster().prepareState().all().get().getState().routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED);
                shardsOnClosedNode.addAll(toShardIds(unassignedAfterClose));
                assertThat(client(masterNode).admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(shardsOnStartedNode.size()));
                return null;
            }
        });

        logger.info("making sure that during the restart operation, when the node restarted was closed, the node shards were unassigned");
        assertThat(shardsOnStartedNode, equalTo(shardsOnClosedNode));
        ensureGreen("test");
        logger.info("verify that all the shards that were on the node are assigned to it again after it was restarted");
        final ClusterState postRestartClusterState = client().admin().cluster().prepareState().all().get().getState();
        List<MutableShardRouting> shardsOnRestartedNodeWhenStarted = postRestartClusterState.routingNodes().node(fullClusterState.nodes().resolveNode(nodeNameRef.get()).getId()).shardsWithState(ShardRoutingState.STARTED);
        assertThat(toShardIds(shardsOnRestartedNodeWhenStarted), equalTo(shardsOnClosedNode));
        assertThat(client(masterNode).admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(0));
    }

    @Test
    public void testDurationExpiry() throws Exception {
        // start a single master node
        final String masterNode = internalCluster().startNodesAsync(1, Settings.builder().put("node.data", false)
                .put(DelayUnassignedAllocation.DELAY_ALLOCATION_DURATION, "1h")
                        // the test infra can maintain same node name when restarting a node
                .put(DelayUnassignedAllocation.DELAY_ALLOCATION_NODE_KEY, DelayUnassignedAllocation.NodeKey.NAME)
                .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 0) // no rebalancing
                .build()).get().get(0);
        // and 4 data nodes
        Settings settings = Settings.builder().put("node.master", false).build();
        internalCluster().startNodesAsync(4, settings).get();

        logger.info("create an index, and make sure the shard and replicas are sync'ed for fast recovery");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen("test");
        final int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "doc", "" + i).setSource("foo", "bar").get();
        }
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, internalCluster().numDataNodes() - 2)));
        ensureGreen("test");

        internalCluster().stopRandomDataNode();
        ensureYellow("test");
        assertThat(client(masterNode).admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), greaterThan(0));

        if (randomBoolean()) {
            logger.info("updating delay duration to 1ms, it should expire and allow to get to green");
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(DelayUnassignedAllocation.DELAY_ALLOCATION_DURATION, "1ms")));
        } else {
            logger.info("executed reroute request with delayed duration of 1ms, should allow the cluster to get to green");
            assertAcked(client().admin().cluster().prepareReroute().setDelayedDuration(TimeValue.timeValueMillis(1)));
        }
        ensureGreen("test");
        assertThat(client(masterNode).admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(0));
    }

    private Set<ShardId> toShardIds(Iterable<MutableShardRouting> shards) {
        Set<ShardId> ret = new HashSet<>();
        for (MutableShardRouting shard : shards) {
            ret.add(shard.shardId());
        }
        return ret;
    }
}
