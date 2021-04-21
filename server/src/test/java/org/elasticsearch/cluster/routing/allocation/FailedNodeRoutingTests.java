/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.cluster.ClusterStateChanges;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;

public class FailedNodeRoutingTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(FailedNodeRoutingTests.class);

    public void testSimpleFailedNodeTest() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()).build());

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test1"))
                .addAsNew(metadata.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start 4 nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node4").numberOfShardsWithState(STARTED), equalTo(1));


        logger.info("remove 2 nodes where primaries are allocated, reroute");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .remove(clusterState.routingTable().index("test1").shard(0).primaryShard().currentNodeId())
                .remove(clusterState.routingTable().index("test2").shard(0).primaryShard().currentNodeId())
        )
                .build();
        clusterState = strategy.disassociateDeadNodes(clusterState, true, "reroute");
        routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.numberOfShardsWithState(STARTED), equalTo(1));
            assertThat(routingNode.numberOfShardsWithState(INITIALIZING), equalTo(1));
        }
    }

    public void testRandomClusterPromotesNewestReplica() throws InterruptedException {

        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
        ClusterState state = randomInitialClusterState();

        // randomly add nodes of mixed versions
        logger.info("--> adding random nodes");
        for (int i = 0; i < randomIntBetween(4, 8); i++) {
            DiscoveryNodes newNodes = DiscoveryNodes.builder(state.nodes())
                .add(createNode()).build();
            state = ClusterState.builder(state).nodes(newNodes).build();
            state = cluster.reroute(state, new ClusterRerouteRequest()); // always reroute after adding node
        }

        // Log the node versions (for debugging if necessary)
        for (ObjectCursor<DiscoveryNode> cursor : state.nodes().getDataNodes().values()) {
            Version nodeVer = cursor.value.getVersion();
            logger.info("--> node [{}] has version [{}]", cursor.value.getId(), nodeVer);
        }

        // randomly create some indices
        logger.info("--> creating some indices");
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            String name = "index_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            Settings.Builder settingsBuilder = Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 4))
                .put(SETTING_NUMBER_OF_REPLICAS, randomIntBetween(2, 4));
            CreateIndexRequest request = new CreateIndexRequest(name, settingsBuilder.build()).waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex(name));
        }

        logger.info("--> starting shards");
        state = cluster.applyStartedShards(state, state.getRoutingNodes().shardsWithState(INITIALIZING));
        logger.info("--> starting replicas a random number of times");
        for (int i = 0; i < randomIntBetween(1,10); i++) {
            state = cluster.applyStartedShards(state, state.getRoutingNodes().shardsWithState(INITIALIZING));
        }

        boolean keepGoing = true;
        while (keepGoing) {
            List<ShardRouting> primaries = state.getRoutingNodes().shardsWithState(STARTED)
                .stream().filter(ShardRouting::primary).collect(Collectors.toList());

            // Pick a random subset of primaries to fail
            List<FailedShard> shardsToFail = new ArrayList<>();
            List<ShardRouting> failedPrimaries = randomSubsetOf(primaries);
            failedPrimaries.stream().forEach(sr -> {
                shardsToFail.add(new  FailedShard(randomFrom(sr), "failed primary", new Exception(), randomBoolean()));
            });

            logger.info("--> state before failing shards: {}", state);
            state = cluster.applyFailedShards(state, shardsToFail);

            final ClusterState compareState = state;
            failedPrimaries.forEach(shardRouting -> {
                logger.info("--> verifying version for {}", shardRouting);

                ShardRouting newPrimary = compareState.routingTable().index(shardRouting.index())
                    .shard(shardRouting.id()).primaryShard();
                Version newPrimaryVersion = getNodeVersion(newPrimary, compareState);

                logger.info("--> new primary is on version {}: {}", newPrimaryVersion, newPrimary);
                compareState.routingTable().shardRoutingTable(newPrimary.shardId()).shardsWithState(STARTED)
                    .stream()
                    .forEach(sr -> {
                        Version candidateVer = getNodeVersion(sr, compareState);
                        if (candidateVer != null) {
                            logger.info("--> candidate on {} node; shard routing: {}", candidateVer, sr);
                            assertTrue("candidate was not on the newest version, new primary is on " +
                                    newPrimaryVersion + " and there is a candidate on " + candidateVer,
                                candidateVer.onOrBefore(newPrimaryVersion));
                        }
                    });
            });

            keepGoing = randomBoolean();
        }
        terminate(threadPool);
    }

    private static Version getNodeVersion(ShardRouting shardRouting, ClusterState state) {
        return Optional.ofNullable(state.getNodes().get(shardRouting.currentNodeId())).map(DiscoveryNode::getVersion).orElse(null);
    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    public ClusterState randomInitialClusterState() {
        List<DiscoveryNode> allNodes = new ArrayList<>();
        DiscoveryNode localNode = createNode(DiscoveryNodeRole.MASTER_ROLE); // local node is the master
        allNodes.add(localNode);
        // at least two nodes that have the data role so that we can allocate shards
        allNodes.add(createNode(DiscoveryNodeRole.DATA_ROLE));
        allNodes.add(createNode(DiscoveryNodeRole.DATA_ROLE));
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            allNodes.add(createNode());
        }
        ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[allNodes.size()]));
        return state;
    }


    protected DiscoveryNode createNode(DiscoveryNodeRole... mustHaveRoles) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles()));
        Collections.addAll(roles, mustHaveRoles);
        final String id = String.format(Locale.ROOT, "node_%03d", nodeIdGenerator.incrementAndGet());
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), roles,
            VersionUtils.randomIndexCompatibleVersion(random()));
    }

}
