/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.indices.cluster.ClusterStateChanges;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class AutoExpandReplicasTests extends ESTestCase {

    public void testParseSettings() {
        AutoExpandReplicas autoExpandReplicas = AutoExpandReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_replicas", "0-5").build()
        );
        assertEquals(0, autoExpandReplicas.minReplicas());
        assertEquals(5, autoExpandReplicas.maxReplicas());
        assertFalse(autoExpandReplicas.expandToAllNodes());

        autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "0-all").build());
        assertEquals(0, autoExpandReplicas.minReplicas());
        assertEquals(Integer.MAX_VALUE, autoExpandReplicas.maxReplicas());
        assertTrue(autoExpandReplicas.expandToAllNodes());

        autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "1-all").build());
        assertEquals(1, autoExpandReplicas.minReplicas());
        assertEquals(Integer.MAX_VALUE, autoExpandReplicas.maxReplicas());
        assertTrue(autoExpandReplicas.expandToAllNodes());
    }

    public void testInvalidValues() {
        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "boom").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [boom] at index -1", ex.getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "1-boom").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [1-boom] at index 1", ex.getMessage());
            assertEquals("For input string: \"boom\"", ex.getCause().getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "boom-1").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [boom-1] at index 4", ex.getMessage());
            assertEquals("For input string: \"boom\"", ex.getCause().getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "2-1").build());
        } catch (IllegalArgumentException ex) {
            assertEquals("[index.auto_expand_replicas] minReplicas must be =< maxReplicas but wasn't 2 > 1", ex.getMessage());
        }

    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    protected DiscoveryNode createNode(Version version, DiscoveryNodeRole... mustHaveRoles) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles()));
        Collections.addAll(roles, mustHaveRoles);
        final String id = Strings.format("node_%03d", nodeIdGenerator.incrementAndGet());
        return DiscoveryNodeUtils.builder(id).name(id).roles(roles).version(version).build();
    }

    protected DiscoveryNode createNode(DiscoveryNodeRole... mustHaveRoles) {
        return createNode(Version.CURRENT, mustHaveRoles);
    }

    /**
     * Checks that when nodes leave the cluster that the auto-expand-replica functionality only triggers after failing the shards on
     * the removed nodes. This ensures that active shards on other live nodes are not failed if the primary resided on a now dead node.
     * Instead, one of the replicas on the live nodes first gets promoted to primary, and the auto-expansion (removing replicas) only
     * triggers in a follow-up step.
     */
    public void testAutoExpandWhenNodeLeavesAndPossiblyRejoins() throws InterruptedException {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        try {
            List<DiscoveryNode> allNodes = new ArrayList<>();
            DiscoveryNode localNode = createNode(DiscoveryNodeRole.MASTER_ROLE); // local node is the master
            allNodes.add(localNode);
            int numDataNodes = randomIntBetween(3, 5);
            List<DiscoveryNode> dataNodes = new ArrayList<>(numDataNodes);
            for (int i = 0; i < numDataNodes; i++) {
                dataNodes.add(createNode(DiscoveryNodeRole.DATA_ROLE));
            }
            allNodes.addAll(dataNodes);
            ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));

            CreateIndexRequest request = new CreateIndexRequest(
                "index",
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_AUTO_EXPAND_REPLICAS, "0-all").build()
            ).waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex("index"));
            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(
                    state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING)
                );
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            IndexShardRoutingTable preTable = state.routingTable().index("index").shard(0);
            final Set<String> unchangedNodeIds;
            final IndexShardRoutingTable postTable;

            if (randomBoolean()) {
                // simulate node removal
                List<DiscoveryNode> nodesToRemove = randomSubsetOf(2, dataNodes);
                unchangedNodeIds = dataNodes.stream()
                    .filter(n -> nodesToRemove.contains(n) == false)
                    .map(DiscoveryNode::getId)
                    .collect(Collectors.toSet());

                state = cluster.removeNodes(state, nodesToRemove);
                postTable = state.routingTable().index("index").shard(0);

                assertTrue("not all shards started in " + state.toString(), postTable.allShardsStarted());
                assertThat(
                    postTable.toString(),
                    postTable.getPromotableAllocationIds(),
                    everyItem(is(in(preTable.getPromotableAllocationIds())))
                );
            } else {
                // fake an election where conflicting nodes are removed and readded
                state = ClusterState.builder(state).nodes(state.nodes().withMasterNodeId(null)).build();

                List<DiscoveryNode> conflictingNodes = randomSubsetOf(2, dataNodes);
                unchangedNodeIds = dataNodes.stream()
                    .filter(n -> conflictingNodes.contains(n) == false)
                    .map(DiscoveryNode::getId)
                    .collect(Collectors.toSet());

                List<DiscoveryNode> nodesToAdd = conflictingNodes.stream()
                    .map(
                        n -> new DiscoveryNode(
                            n.getName(),
                            n.getId(),
                            buildNewFakeTransportAddress(),
                            n.getAttributes(),
                            n.getRoles(),
                            n.getVersion()
                        )
                    )
                    .collect(Collectors.toCollection(ArrayList::new));

                if (randomBoolean()) {
                    nodesToAdd.add(createNode(DiscoveryNodeRole.DATA_ROLE));
                }

                state = cluster.joinNodesAndBecomeMaster(state, nodesToAdd, TransportVersion.current());
                postTable = state.routingTable().index("index").shard(0);
            }

            Set<String> unchangedAllocationIds = RoutingNodesHelper.asStream(preTable)
                .filter(shr -> unchangedNodeIds.contains(shr.currentNodeId()))
                .map(shr -> shr.allocationId().getId())
                .collect(Collectors.toSet());

            assertThat(postTable.toString(), unchangedAllocationIds, everyItem(is(in(postTable.getPromotableAllocationIds()))));

            RoutingNodesHelper.asStream(postTable).forEach(shardRouting -> {
                if (shardRouting.assignedToNode() && unchangedAllocationIds.contains(shardRouting.allocationId().getId())) {
                    assertTrue("Shard should be active: " + shardRouting, shardRouting.active());
                }
            });
        } finally {
            terminate(threadPool);
        }
    }

    public void testOnlyAutoExpandAllocationFilteringAfterAllNodesUpgraded() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        try {
            List<DiscoveryNode> allNodes = new ArrayList<>();
            DiscoveryNode localNode = createNode(
                VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_1),
                DiscoveryNodeRole.MASTER_ROLE,
                DiscoveryNodeRole.DATA_ROLE
            ); // local node is the master
            DiscoveryNode oldNode = createNode(
                VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_1),
                DiscoveryNodeRole.DATA_ROLE
            ); // local node is the master
            allNodes.add(localNode);
            allNodes.add(oldNode);
            ClusterState state = ClusterStateCreationUtils.state(
                localNode,
                localNode,
                allNodes.toArray(new DiscoveryNode[0]),
                TransportVersion.V_7_0_0
            );

            CreateIndexRequest request = new CreateIndexRequest(
                "index",
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_AUTO_EXPAND_REPLICAS, "0-all").build()
            ).waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex("index"));
            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(
                    state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING)
                );
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            DiscoveryNode newNode = createNode(Version.V_7_6_0, DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE); // local node
                                                                                                                             // is the
                                                                                                                             // master

            state = cluster.addNode(state, newNode, TransportVersion.V_7_6_0);

            // use allocation filtering
            state = cluster.updateSettings(
                state,
                new UpdateSettingsRequest("index").settings(
                    Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", oldNode.getName()).build()
                )
            );

            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(
                    state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING)
                );
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            // check that presence of old node means that auto-expansion does not take allocation filtering into account
            assertThat(state.routingTable().index("index").shard(0).size(), equalTo(3));

            // remove old node and check that auto-expansion takes allocation filtering into account
            state = cluster.removeNodes(state, Collections.singletonList(oldNode));
            assertThat(state.routingTable().index("index").shard(0).size(), equalTo(2));
        } finally {
            terminate(threadPool);
        }
    }

    public void testCalculateDesiredNumberOfReplicas() {
        int lowerBound = between(0, 9);
        int upperBound = between(lowerBound + 1, 10);
        String settingValue = lowerBound + "-" + randomFrom(upperBound, "all");
        AutoExpandReplicas autoExpandReplicas = AutoExpandReplicas.SETTING.get(
            Settings.builder().put(SETTING_AUTO_EXPAND_REPLICAS, settingValue).build()
        );
        int max = autoExpandReplicas.maxReplicas();
        int matchingNodes = between(0, max);
        assertThat(autoExpandReplicas.calculateDesiredNumberOfReplicas(matchingNodes), equalTo(Math.max(lowerBound, matchingNodes - 1)));
        assertThat(autoExpandReplicas.calculateDesiredNumberOfReplicas(max + 1), equalTo(max));
    }
}
