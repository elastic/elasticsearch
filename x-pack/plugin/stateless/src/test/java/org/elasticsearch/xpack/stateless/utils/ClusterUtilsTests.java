/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.utils;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.stateless.TestUtils.clusterStateWithShardOnLocalNode;
import static org.elasticsearch.xpack.stateless.TestUtils.indexMetadata;
import static org.hamcrest.Matchers.is;

public class ClusterUtilsTests extends ESTestCase {

    public void testIsShardLocallyAllocatedReturnsFalseWhenLocalNodeIdIsNull() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings())) {
            final ShardId shardId = new ShardId("index", randomUUID(), 0);
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(ClusterName.DEFAULT)
                    .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("node", "node")))
                    .build()
            );
            assertThat(ClusterUtils.isShardLocallyAllocated(clusterService, shardId), is(false));
        }
    }

    public void testIsShardLocallyAllocatedReturnsFalseWhenShardNotOnLocalNode() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings())) {
            final ShardId localShard = new ShardId("local", randomUUID(), 0);
            final ShardId remoteShard = new ShardId("remote", randomUUID(), 0);
            ClusterServiceUtils.setState(
                clusterService,
                clusterStateWithShardOnLocalNode(localShard, indexMetadata(localShard.getIndexName(), localShard.getIndex().getUUID()))
            );
            assertThat(ClusterUtils.isShardLocallyAllocated(clusterService, localShard), is(true));
            assertThat(ClusterUtils.isShardLocallyAllocated(clusterService, remoteShard), is(false));
        }
    }

    public void testIsShardLocallyAllocatedReturnsTrueForStartedShardOnLocalNode() {
        assertShardLocallyAllocatedForRoutingState(ShardRoutingState.STARTED, true);
    }

    public void testIsShardLocallyAllocatedReturnsTrueForRelocationTargetOnLocalNode() {
        assertShardLocallyAllocatedForRoutingState(ShardRoutingState.INITIALIZING, true);
    }

    public void testIsShardLocallyAllocatedReturnsTrueForRelocatingShardOnLocalNode() {
        assertShardLocallyAllocatedForRoutingState(ShardRoutingState.RELOCATING, true);
    }

    private void assertShardLocallyAllocatedForRoutingState(ShardRoutingState routingState, boolean expected) {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings())) {
            final ShardId shardId = new ShardId("index", randomUUID(), 0);
            ClusterServiceUtils.setState(
                clusterService,
                clusterStateWithShardOnLocalNode(shardId, indexMetadata(shardId.getIndexName(), shardId.getIndex().getUUID()), routingState)
            );
            assertThat(ClusterUtils.isShardLocallyAllocated(clusterService, shardId), is(expected));
        }
    }

    private static ClusterSettings clusterSettings() {
        return new ClusterSettings(Settings.EMPTY, Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }
}
