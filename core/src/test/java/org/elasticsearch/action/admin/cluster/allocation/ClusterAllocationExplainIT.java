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

package org.elasticsearch.action.admin.cluster.allocation;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for the cluster allocation explanation
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public final class ClusterAllocationExplainIT extends ESIntegTestCase {

    @TestLogging("_root:DEBUG")
    public void testDelayShards() throws Exception {
        logger.info("--> starting 3 nodes");
        internalCluster().startNodes(3);

        // Wait for all 3 nodes to be up
        logger.info("--> waiting for 3 nodes to be up");
        ensureStableCluster(3);

        logger.info("--> creating 'test' index");
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "1m")
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 5)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1))
                .setWaitForActiveShards(ActiveShardCount.ALL).get());

        logger.info("--> stopping a random node");
        assertTrue(internalCluster().stopRandomDataNode());

        logger.info("--> waiting for the master to remove the stopped node from the cluster state");
        ensureStableCluster(2);

        ClusterAllocationExplainResponse resp = client().admin().cluster().prepareAllocationExplain().useAnyUnassignedShard().get();
        ClusterAllocationExplanation cae = resp.getExplanation();
        assertThat(cae.getShard().getIndexName(), equalTo("test"));
        assertFalse(cae.isPrimary());
        assertFalse(cae.isAssigned());
        AllocateUnassignedDecision decision = cae.getShardAllocationDecision().getAllocateDecision();
        assertThat("expecting a remaining delay, got: " + decision.getRemainingDelayInMillis(),
            decision.getRemainingDelayInMillis(), greaterThan(0L));
    }

    public void testUnassignedShards() throws Exception {
        logger.info("--> starting 3 nodes");
        String noAttrNode = internalCluster().startNode();
        String barAttrNode = internalCluster().startNode(Settings.builder().put("node.attr.bar", "baz"));
        String fooBarAttrNode = internalCluster().startNode(Settings.builder()
                .put("node.attr.foo", "bar")
                .put("node.attr.bar", "baz"));

        // Wait for all 3 nodes to be up
        logger.info("--> waiting for 3 nodes to be up");
        client().admin().cluster().health(Requests.clusterHealthRequest().waitForNodes("3")).actionGet();

        client().admin().indices().prepareCreate("anywhere")
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 5)
                        .put("index.number_of_replicas", 1))
                .setWaitForActiveShards(ActiveShardCount.ALL)  // wait on all shards
                .get();

        client().admin().indices().prepareCreate("only-baz")
                .setSettings(Settings.builder()
                        .put("index.routing.allocation.include.bar", "baz")
                        .put("index.number_of_shards", 5)
                        .put("index.number_of_replicas", 1))
                .setWaitForActiveShards(ActiveShardCount.ALL)
                .get();

        client().admin().indices().prepareCreate("only-foo")
                .setSettings(Settings.builder()
                        .put("index.routing.allocation.include.foo", "bar")
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1))
                .get();

        ClusterAllocationExplainResponse resp = client().admin().cluster().prepareAllocationExplain()
                .setIndex("only-foo")
                .setShard(0)
                .setPrimary(false)
                .get();
        ClusterAllocationExplanation cae = resp.getExplanation();
        assertThat(cae.getShard().getIndexName(), equalTo("only-foo"));
        assertFalse(cae.isPrimary());
        assertFalse(cae.isAssigned());
        AllocateUnassignedDecision allocateUnassignedDecision = cae.getShardAllocationDecision().getAllocateDecision();
        assertFalse(allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.FETCH_PENDING);
        assertThat(UnassignedInfo.Reason.INDEX_CREATED, equalTo(cae.getUnassignedInfo().getReason()));
        assertThat("expecting no remaining delay: " + allocateUnassignedDecision.getRemainingDelayInMillis(),
            allocateUnassignedDecision.getRemainingDelayInMillis(), equalTo(0L));

        List<NodeAllocationResult> explanations = allocateUnassignedDecision.getNodeDecisions();

        int barAttrWeight = -1;
        int fooBarAttrWeight = -1;
        for (NodeAllocationResult explanation : explanations) {
            DiscoveryNode node = explanation.getNode();
            String nodeName = node.getName();
            AllocationDecision nodeDecision = explanation.getNodeDecision();
            Decision d = explanation.getCanAllocateDecision();
            int weight = explanation.getWeightRanking();

            assertEquals(d.type(), Decision.Type.NO);
            assertEquals(AllocationDecision.NO, nodeDecision);
            if (noAttrNode.equals(nodeName)) {
                assertThat(d.toString(), containsString("node does not match index setting [index.routing.allocation.include] " +
                                                            "filters [foo:\"bar\"]"));
            } else if (barAttrNode.equals(nodeName)) {
                assertThat(d.toString(), containsString("node does not match index setting [index.routing.allocation.include] " +
                                                            "filters [foo:\"bar\"]"));
                barAttrWeight = weight;
            } else if (fooBarAttrNode.equals(nodeName)) {
                assertThat(d.toString(), containsString("the shard cannot be allocated to the same node"));
                fooBarAttrWeight = weight;
            } else {
                fail("unexpected node with name: " + nodeName +
                     ", I have: " + noAttrNode + ", " + barAttrNode + ", " + fooBarAttrNode);
            }
        }
        assertFalse(barAttrWeight == fooBarAttrWeight);
    }

    public void testUnassignedPrimaryWithExistingIndex() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        logger.info("--> creating an index with 1 primary, 0 replicas");
        createIndexAndIndexData(1, 0);

        logger.info("--> stopping the node with the primary");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName()));
        ensureStableCluster(1);

        ClusterAllocationExplanation explanation = runExplain(true);
    }

    //TODO: intermittently fails
    public void testUnassignedPrimaryDueToFailedInitialization() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        logger.info("--> creating an index with 1 primary, 0 replicas");
        createIndexAndIndexData(1, 0);
        Index index = resolveIndex("idx");
        String primaryNode = primaryNodeName();
        Path shardPath = internalCluster().getInstance(NodeEnvironment.class, primaryNode).availableShardPaths(new ShardId(index, 0))[0];

        logger.info("--> stopping the node with the primary [{}]", primaryNode);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        ensureStableCluster(1);

        logger.info("--> deleting a cfs file to make the shard copy corrupt");
        IOUtils.rm(shardPath.resolve("index/_0.cfs"));

        logger.info("--> restarting the node with the primary [{}]", primaryNode);
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", primaryNode).build());
        ensureStableCluster(2);
        // wait till we passed the fetching shard data phase
        assertBusy(() -> {
            UnassignedInfo unassignedInfo = client().admin().cluster().prepareAllocationExplain().setIndex("idx").setShard(0)
                .setPrimary(true).get().getExplanation().getUnassignedInfo();
            assertNotNull("unassigned info is null", unassignedInfo);
            assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_NO, unassignedInfo.getLastAllocationStatus());
        });

        ClusterAllocationExplanation explanation = runExplain(true);
    }

    public void testUnassignedReplicaDelayedAllocation() throws Exception {
        logger.info("--> starting 3 nodes");
        internalCluster().startNodes(3);
        ensureStableCluster(3);

        logger.info("--> creating an index with 1 primary, 1 replica");
        createIndexAndIndexData(1, 1);
        logger.info("--> stopping the node with the replica");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName()));
        ensureStableCluster(2);

        logger.info("--> observing delayed allocation...");
        ClusterAllocationExplanation explanation = runExplain(false);
    }

    public void testUnassignedReplicaWithPriorCopy() throws Exception {
        logger.info("--> starting 3 nodes");
        List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);

        logger.info("--> creating an index with 1 primary and 1 replica");
        createIndexAndIndexData(1, 1);
        String primaryNodeName = primaryNodeName();
        nodes.remove(primaryNodeName);

        logger.info("--> shutting down all nodes except the one that holds the primary");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes.get(1)));
        ensureStableCluster(1);

        logger.info("--> setting allocation filtering to only allow allocation on the currently running node");
        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.allocation.include._name", primaryNodeName)).get();

        logger.info("--> restarting the stopped nodes");
        internalCluster().startNode(Settings.builder().put("node.name", nodes.get(0)).build());
        internalCluster().startNode(Settings.builder().put("node.name", nodes.get(1)).build());
        runExplain(false); // run once to get out of pending fetch state

        ClusterAllocationExplanation explanation = runExplain(false);
    }

    public void testAllocationFilteringOnIndexCreation() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        logger.info("--> creating an index with 1 primary, 0 replicas, with allocation filtering so the primary can't be assigned");
        createIndexAndIndexData(1, 0, Settings.builder().put("index.routing.allocation.include._name", "non_existent_node").build(), false);

        ClusterAllocationExplanation explanation = runExplain(true);
    }

    public void testAllocationFilteringPreventsShardMove() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        logger.info("--> creating an index with 1 primary and 0 replicas");
        createIndexAndIndexData(1, 0);

        logger.info("--> setting up allocation filtering to prevent allocation to both nodes");
        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.allocation.include._name", "non_existent_node")).get();

        ClusterAllocationExplanation explanation = runExplain(true);
    }

    public void testRebalancingNotAllowed() throws Exception {
        logger.info("--> starting a single node");
        internalCluster().startNode();

        logger.info("--> creating an index with 5 shards, all allocated to the single node");
        createIndexAndIndexData(5, 0);

        logger.info("--> disabling rebalancing on the index");
        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.rebalance.enable", "none")).get();

        logger.info("--> starting another node, with rebalancing disabled, it should get no shards");
        internalCluster().startNode();

        ClusterAllocationExplanation explanation = runExplain(true);
    }

    public void testWorseBalance() throws Exception {
        logger.info("--> starting a single node");
        internalCluster().startNode();

        logger.info("--> creating an index with 5 shards, all allocated to the single node");
        createIndexAndIndexData(5, 0);

        logger.info("--> setting balancing threshold really high, so it won't be met");
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(
            Settings.builder().put("cluster.routing.allocation.balance.threshold", 1000.0f)).get();

        logger.info("--> starting another node, with the rebalance threshold so high, it should not get any shards");
        internalCluster().startNode();

        ClusterAllocationExplanation explanation = runExplain(true);
    }

    public void testBetterBalanceButCannotAllocate() throws Exception {
        logger.info("--> starting a single node");
        String firstNode = internalCluster().startNode();

        logger.info("--> creating an index with 5 shards, all allocated to the single node");
        createIndexAndIndexData(5, 0);

        logger.info("--> setting up allocation filtering to only allow allocation to the current node");
        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.allocation.include._name", firstNode)).get();

        logger.info("--> starting another node, with filtering not allowing allocation to the new node, it should not get any shards");
        internalCluster().startNode();

        ClusterAllocationExplanation explanation = runExplain(true);
    }

    private ClusterAllocationExplanation runExplain(boolean primary) throws Exception {
        ClusterAllocationExplanation explanation = client().admin().cluster().prepareAllocationExplain()
            .setIndex("idx").setShard(0).setPrimary(primary).get().getExplanation();
        if (logger.isInfoEnabled()) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            logger.info("--> explain json output: \n{}", explanation.toXContent(builder, ToXContent.EMPTY_PARAMS).string());
        }
        return explanation;
    }

    private void createIndexAndIndexData(int numPrimaries, int numReplicas) {
        createIndexAndIndexData(numPrimaries, numReplicas, Settings.EMPTY, true);
    }

    private void createIndexAndIndexData(int numPrimaries, int numReplicas, Settings settings, boolean waitForShards) {
        client().admin().indices().prepareCreate("idx")
            .setSettings(Settings.builder()
                             .put("index.number_of_shards", numPrimaries)
                             .put("index.number_of_replicas", numReplicas)
                             .put(settings))
            .setWaitForActiveShards(waitForShards ? ActiveShardCount.ALL : ActiveShardCount.NONE)
            .get();
        if (waitForShards) {
            for (int i = 0; i < 10; i++) {
                index("idx", "t", Integer.toString(i), Collections.singletonMap("f1", Integer.toString(i)));
            }
            refresh("idx");
        }
    }

    private String primaryNodeName() {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index("idx").shard(0).primaryShard().currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node().getName();
    }

    private String replicaNodeName() {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index("idx").shard(0).replicaShards().get(0).currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node().getName();
    }
}
