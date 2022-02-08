/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.indices.cluster.ClusterStateChanges;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

public class DesiredBalanceShardsAllocatorTests extends ESTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void createThreadPool() {
        assertNull(threadPool);
        threadPool = new TestThreadPool("test");
    }

    @AfterClass
    public static void terminateThreadPool() {
        try {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        } finally {
            threadPool = null;
        }
    }

    private static DiscoveryNodes randomDiscoveryNodes() {
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();

        final DiscoveryNode localNode = new DiscoveryNode("master-node", buildNewFakeTransportAddress(), Collections.emptyMap(), Set.of(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);

        builder.add(localNode);
        builder.localNodeId(localNode.getId());
        builder.masterNodeId(localNode.getId());

        for (int i = between(1, 5); i > 0; i--) {
            builder.add(randomDataNode("data-node-" + i));
        }
        for (int i = between(1, 5); i > 0; i--) {
            builder.add(randomNonDataNode("non-data-node-" + i));
        }

        return builder.build();
    }

    private static DiscoveryNode randomDataNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(
                randomValueOtherThanMany(
                    roles -> roles.stream().noneMatch(DiscoveryNodeRole::canContainData),
                    () -> randomSubsetOf(DiscoveryNodeRole.roles())
                )
            ),
            Version.CURRENT
        );
    }

    private static DiscoveryNode randomNonDataNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(
                randomSubsetOf(DiscoveryNodeRole.roles().stream().filter(r -> r.canContainData() == false).collect(Collectors.toSet()))
            ),
            Version.CURRENT
        );
    }

    public void testNeedsRefreshIfDataNodesChanged() {

        final ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT).nodes(randomDiscoveryNodes()).build();

        assertFalse(DesiredBalanceShardsAllocator.needsRefresh(initialState, initialState));

        assertFalse(
            DesiredBalanceShardsAllocator.needsRefresh(
                initialState,
                ClusterState.builder(initialState)
                    .nodes(DiscoveryNodes.builder(initialState.nodes()).add(randomNonDataNode("new-node")))
                    .build()
            )
        );

        assertFalse(
            DesiredBalanceShardsAllocator.needsRefresh(
                initialState,
                ClusterState.builder(initialState).nodes(DiscoveryNodes.builder(initialState.nodes()).remove("non-data-node-1")).build()
            )
        );

        assertTrue(
            DesiredBalanceShardsAllocator.needsRefresh(
                initialState,
                ClusterState.builder(initialState)
                    .nodes(DiscoveryNodes.builder(initialState.nodes()).add(randomDataNode("new-node")))
                    .build()
            )
        );

        assertTrue(
            DesiredBalanceShardsAllocator.needsRefresh(
                initialState,
                ClusterState.builder(initialState).nodes(DiscoveryNodes.builder(initialState.nodes()).remove("data-node-1")).build()
            )
        );
    }

    public void testNeedsRefreshIfIndicesChanged() {

        final ClusterState.Builder initialStateBuilder = ClusterState.builder(ClusterName.DEFAULT).nodes(randomDiscoveryNodes());

        for (int i = between(1, 5); i > 0; i--) {
            final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("index-" + i);

        }

        final ClusterStateChanges clusterStateChanges = new ClusterStateChanges(xContentRegistry(), threadPool);

        ClusterState clusterState = ClusterState.EMPTY_STATE;
        for (int i = between(1, 5); i > 0; i--) {
            final String name = "index-" + i;
            final Settings.Builder settingsBuilder = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3));
            if (randomBoolean()) {
                int min = between(0, 2);
                settingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, min + "-" + (randomBoolean() ? min + between(0, 2) : "all"));
            } else {
                settingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, between(0, 2));
            }
            clusterState = clusterStateChanges.createIndex(clusterState,
                new CreateIndexRequest(name, settingsBuilder.build()).waitForActiveShards(ActiveShardCount.NONE));
            assertTrue(clusterState.metadata().hasIndex(name));
        }





    }

}
