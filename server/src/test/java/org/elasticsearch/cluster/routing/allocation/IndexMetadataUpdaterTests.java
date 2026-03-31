/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IndexMetadataUpdaterTests extends ESTestCase {

    public void testApplyChangesAcrossMultipleProjects() {
        final IndexMetadataUpdater updater = new IndexMetadataUpdater();

        final ProjectId project1 = randomUniqueProjectId();
        final ProjectId project2 = randomUniqueProjectId();
        final ProjectId project3 = randomUniqueProjectId();

        final DiscoveryNode node1 = DiscoveryNodeUtils.create("n1");
        final DiscoveryNode node2 = DiscoveryNodeUtils.create("n2");
        final DiscoveryNode node3 = DiscoveryNodeUtils.create("n3");

        final DiscoveryNodes allNodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(allNodes)
            .putProjectMetadata(
                ProjectMetadata.builder(project1)
                    .put(
                        IndexMetadata.builder("index1")
                            .settings(indexSettings(IndexVersion.current(), 1, 2).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                            .build(),
                        false
                    )
                    .build()
            )
            .putProjectMetadata(
                ProjectMetadata.builder(project2)
                    .put(
                        IndexMetadata.builder("index1")
                            .settings(indexSettings(IndexVersion.current(), 3, 0).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                            .build(),
                        false
                    )
                    .put(
                        IndexMetadata.builder("index2")
                            .settings(indexSettings(IndexVersion.current(), 3, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                            .build(),
                        false
                    )
                    .build()
            )
            .putProjectMetadata(
                ProjectMetadata.builder(project3)
                    .put(
                        IndexMetadata.builder("index3")
                            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                            .build(),
                        false
                    )
                    .build()
            )
            .build();

        clusterState = ClusterState.builder(clusterState)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(clusterState.metadata(), RoutingTable.Builder::addAsNew))
            .build();

        // No allocation ids at start
        for (ProjectMetadata project : clusterState.metadata().projects().values()) {
            for (IndexMetadata index : project.indices().values()) {
                assertThat(index.getInSyncAllocationIds(), notNullValue());
                assertThat(index.getInSyncAllocationIds(), hasKey(0));
                assertThat(index.getInSyncAllocationIds().get(0), hasSize(0));
            }
        }

        final String p1i1s0p = startShard(updater, node1, clusterState.metadata().getProject(project1).index("index1"), 0, true);
        final String p1i1s0r = startShard(updater, node2, clusterState.metadata().getProject(project1).index("index1"), 0, false);
        final String p2i1s1p = startShard(updater, node3, clusterState.metadata().getProject(project2).index("index1"), 1, true);
        final String p2i2s2r = startShard(updater, node1, clusterState.metadata().getProject(project2).index("index2"), 2, false);
        final String p2i2s0p = startShard(updater, node2, clusterState.metadata().getProject(project2).index("index2"), 0, true);
        final String p3i3s0p = startShard(updater, node3, clusterState.metadata().getProject(project3).index("index3"), 0, true);

        final Metadata updatedMetadata = updater.applyChanges(clusterState.metadata(), clusterState.globalRoutingTable());
        assertThat(updatedMetadata, not(sameInstance(clusterState.metadata())));

        assertThat(
            updatedMetadata.getProject(project1).index("index1").getInSyncAllocationIds().get(0),
            containsInAnyOrder(p1i1s0p, p1i1s0r)
        );
        assertThat(updatedMetadata.getProject(project2).index("index1").getInSyncAllocationIds().get(1), containsInAnyOrder(p2i1s1p));
        assertThat(updatedMetadata.getProject(project2).index("index2").getInSyncAllocationIds().get(2), containsInAnyOrder(p2i2s2r));
        assertThat(updatedMetadata.getProject(project2).index("index2").getInSyncAllocationIds().get(0), containsInAnyOrder(p2i2s0p));
        assertThat(updatedMetadata.getProject(project3).index("index3").getInSyncAllocationIds().get(0), containsInAnyOrder(p3i3s0p));
    }

    private static String startShard(
        IndexMetadataUpdater updater,
        DiscoveryNode node,
        IndexMetadata index,
        int shardNumber,
        boolean primary
    ) {
        final AllocationId allocationId = AllocationId.newInitializing();
        final ShardId shardId = new ShardId(index.getIndex(), shardNumber);
        final ShardRouting init = new TestShardRouting.Builder(shardId, node.getId(), primary, ShardRoutingState.INITIALIZING)
            .withAllocationId(allocationId)
            .build();
        final ShardRouting started = new TestShardRouting.Builder(shardId, node.getId(), primary, ShardRoutingState.STARTED)
            .withAllocationId(allocationId)
            .build();
        updater.shardStarted(init, started);
        return allocationId.getId();
    }

}
