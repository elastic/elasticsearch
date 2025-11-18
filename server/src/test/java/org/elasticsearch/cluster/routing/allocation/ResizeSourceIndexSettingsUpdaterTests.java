/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ResizeSourceIndexSettingsUpdaterTests extends ESAllocationTestCase {

    public void testResizeIndexSettingsRemovedAfterStart() {
        final DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(newNode("node1", "id1", MASTER_DATA_ROLES))
            .add(newNode("node2", "id2", MASTER_DATA_ROLES))
            .build();

        final DiscoveryNode resizeNode = randomFrom(discoveryNodes.getDataNodes().values());

        final int additionalProjects = 0; // TODO: Include additional projects once reroute works

        final String sourceIndex = "source";
        final String targetIndex = "target";

        final ProjectMetadata sourceProject = ProjectMetadata.builder(
            additionalProjects == 0 ? Metadata.DEFAULT_PROJECT_ID : randomUniqueProjectId()
        )
            .put(
                IndexMetadata.builder(sourceIndex)
                    .settings(
                        settings(IndexVersion.current()).put(
                            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name",
                            resizeNode.getName()
                        ).put("index.blocks.write", true).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
                    )
                    .numberOfShards(2)
                    .numberOfReplicas(0)
                    .setRoutingNumShards(16)
            )
            .build();
        final Metadata.Builder mBuilder = Metadata.builder().put(sourceProject);

        for (int i = 0; i < additionalProjects; i++) {
            final ProjectMetadata.Builder project = ProjectMetadata.builder(randomUniqueProjectId());
            final int indexCount = randomIntBetween(0, 5);
            for (int j = 0; j < indexCount; j++) {
                project.put(
                    IndexMetadata.builder(randomFrom(sourceIndex, targetIndex, "index_" + j))
                        .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                        .numberOfShards(randomIntBetween(1, 3))
                        .numberOfReplicas(randomIntBetween(1, 3))
                );
            }
            mBuilder.put(project);
        }

        final Metadata sourceMetadata = mBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(sourceMetadata, RoutingTable.Builder::addAsNew))
            .metadata(sourceMetadata)
            .nodes(discoveryNodes)
            .build();

        {
            IndexRoutingTable sourceRoutingTable = clusterState.routingTable(sourceProject.id()).index(sourceIndex);
            assertThat(sourceRoutingTable.size(), equalTo(2));
            assertThat(sourceRoutingTable.shard(0).primaryShard().state(), equalTo(UNASSIGNED));
            assertThat(sourceRoutingTable.shard(1).primaryShard().state(), equalTo(UNASSIGNED));
        }

        final AllocationService allocationService = createAllocationService(
            Settings.builder()
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 16)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 16)
                .build()
        );
        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());

        {
            IndexRoutingTable sourceRoutingTable = clusterState.routingTable(sourceProject.id()).index(sourceIndex);
            assertThat(sourceRoutingTable.size(), equalTo(2));
            assertThat(sourceRoutingTable.shard(0).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(sourceRoutingTable.shard(1).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(sourceRoutingTable.shard(0).primaryShard().currentNodeId(), equalTo(resizeNode.getId()));
            assertThat(sourceRoutingTable.shard(1).primaryShard().currentNodeId(), equalTo(resizeNode.getId()));
        }

        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);

        {
            IndexRoutingTable sourceRoutingTable = clusterState.routingTable(sourceProject.id()).index(sourceIndex);
            assertThat(sourceRoutingTable.size(), equalTo(2));
            assertThat(sourceRoutingTable.shard(0).primaryShard().state(), equalTo(STARTED));
            assertThat(sourceRoutingTable.shard(1).primaryShard().state(), equalTo(STARTED));
            assertThat(sourceRoutingTable.shard(0).primaryShard().currentNodeId(), equalTo(resizeNode.getId()));
            assertThat(sourceRoutingTable.shard(1).primaryShard().currentNodeId(), equalTo(resizeNode.getId()));
        }

        final int targetNumShards = randomFrom(1, 2, 4, 8, 16);
        final int targetNumReplicas = randomInt(2);
        final Settings.Builder targetSettings = indexSettings(IndexVersion.current(), targetNumShards, targetNumReplicas);
        targetSettings.put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), sourceIndex);
        targetSettings.put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), sourceProject.index(sourceIndex).getIndexUUID());
        final boolean isShrink = randomBoolean();
        if (isShrink) {
            targetSettings.put(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY, resizeNode.getId());
        }
        final boolean hasLifecyclePolicy = randomBoolean();
        if (hasLifecyclePolicy) {
            targetSettings.put(IndexMetadata.LIFECYCLE_NAME, "policy");
        }

        clusterState = ClusterState.builder(clusterState)
            .putProjectMetadata(
                ProjectMetadata.builder(clusterState.metadata().getProject(sourceProject.id()))
                    .put(IndexMetadata.builder(targetIndex).settings(targetSettings).setRoutingNumShards(16))
            )
            .build();
        clusterState = ClusterState.builder(clusterState)
            .routingTable(
                // Add any new/unrouted indices (i.e. "target") to the routing table, but leave existing entries alone
                GlobalRoutingTableTestHelper.updateRoutingTable(clusterState, RoutingTable.Builder::addAsNew, (buider, index) -> {})
            )
            .build();

        {
            IndexRoutingTable targetRoutingTable = clusterState.routingTable(sourceProject.id()).index(targetIndex);
            assertThat(targetRoutingTable.size(), equalTo(targetNumShards));
            for (int i = 0; i < targetNumShards; i++) {
                ShardRouting shardRouting = targetRoutingTable.shard(i).primaryShard();
                assertThat(shardRouting.toString(), shardRouting.state(), equalTo(UNASSIGNED));
            }
        }

        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());

        {
            IndexMetadata targetIndexMetadata = clusterState.metadata().getProject(sourceProject.id()).index(targetIndex);
            assertThat(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.exists(targetIndexMetadata.getSettings()), is(true));
            assertThat(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.exists(targetIndexMetadata.getSettings()), is(true));
            assertThat(targetIndexMetadata.getSettings().hasValue(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY), is(isShrink));
            IndexRoutingTable targetRoutingTable = clusterState.routingTable(sourceProject.id()).index(targetIndex);
            assertThat(targetRoutingTable.size(), equalTo(targetNumShards));
            for (int i = 0; i < targetNumShards; i++) {
                ShardRouting shardRouting = targetRoutingTable.shard(i).primaryShard();
                assertThat(shardRouting.toString(), shardRouting.state(), equalTo(INITIALIZING));
            }
        }

        while (true) {
            IndexRoutingTable targetIndexRoutingTable = clusterState.routingTable(sourceProject.id()).index(targetIndex);
            List<ShardRouting> initializing = targetIndexRoutingTable.shardsWithState(INITIALIZING);
            if (initializing.isEmpty()) {
                break;
            }

            IndexMetadata targetIndexMetadata = clusterState.metadata().getProject(sourceProject.id()).index(targetIndex);
            assertThat(
                IndexMetadata.INDEX_RESIZE_SOURCE_NAME.exists(targetIndexMetadata.getSettings()),
                is(hasLifecyclePolicy || (targetIndexRoutingTable.allPrimaryShardsActive() == false))
            );
            assertThat(
                IndexMetadata.INDEX_RESIZE_SOURCE_UUID.exists(targetIndexMetadata.getSettings()),
                is(targetIndexRoutingTable.allPrimaryShardsActive() == false)
            );
            assertThat(
                targetIndexMetadata.getSettings().hasValue(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY),
                is(targetIndexRoutingTable.allPrimaryShardsActive() ? false : isShrink)
            );

            clusterState = startShardsAndReroute(allocationService, clusterState, randomNonEmptySubsetOf(initializing));
        }

        {
            IndexMetadata targetIndexMetadata = clusterState.metadata().getProject(sourceProject.id()).index(targetIndex);
            assertThat(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.exists(targetIndexMetadata.getSettings()), is(hasLifecyclePolicy));
            assertThat(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.exists(targetIndexMetadata.getSettings()), is(false));
            assertThat(targetIndexMetadata.getSettings().hasValue(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY), is(false));
            IndexRoutingTable targetRoutingTable = clusterState.routingTable(sourceProject.id()).index(targetIndex);
            assertThat(targetRoutingTable.size(), equalTo(targetNumShards));
            for (int i = 0; i < targetNumShards; i++) {
                ShardRouting shardRouting = targetRoutingTable.shard(i).primaryShard();
                assertThat(shardRouting.toString(), shardRouting.state(), equalTo(STARTED));
            }
        }
    }

    public void testHandleChangesAcrossMultipleProjects() {
        final ResizeSourceIndexSettingsUpdater updater = new ResizeSourceIndexSettingsUpdater();
        final Metadata metadata = Metadata.builder()
            .put(ProjectMetadata.builder(ProjectId.fromId("p1")))
            .put(
                ProjectMetadata.builder(ProjectId.fromId("p2"))
                    .put(
                        IndexMetadata.builder("index-a")
                            .settings(
                                indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, "index-w")
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, randomUUID())
                            )
                    )
                    .put(
                        IndexMetadata.builder("index-b")
                            .settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                    )
            )
            .put(
                ProjectMetadata.builder(ProjectId.fromId("p3"))
                    .put(
                        IndexMetadata.builder("index-a")
                            .settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                    )
                    .put(
                        IndexMetadata.builder("index-b")
                            .settings(
                                indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, "index-x")
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, randomUUID())
                                    .put(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY, "node1,node2")
                            )
                    )
                    .put(
                        IndexMetadata.builder("index-c")
                            .settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                    )
            )
            .put(
                ProjectMetadata.builder(ProjectId.fromId("p4"))
                    .put(
                        IndexMetadata.builder("index-a")
                            .settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                    )
                    .put(
                        IndexMetadata.builder("index-c")
                            .settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                    )
                    .put(
                        IndexMetadata.builder("index-d")
                            .settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                    )
            )
            .put(
                ProjectMetadata.builder(ProjectId.fromId("p5"))
                    .put(
                        IndexMetadata.builder("index-a")
                            .settings(
                                indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, "index-y")
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, randomUUID())
                                    .put(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY, "node2,node3")
                            )
                    )
                    .put(
                        IndexMetadata.builder("index-c")
                            .settings(
                                indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, "index-z")
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, randomUUID())
                            )
                    )
            )
            .build();

        assertThat(updater.numberOfChanges(), is(0));

        GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew);
        Metadata updated = updater.applyChanges(metadata, routingTable);
        assertThat(updated, sameInstance(metadata));

        for (ProjectMetadata project : metadata.projects().values()) {
            for (IndexMetadata index : project.indices().values()) {
                final ShardId shard = new ShardId(index.getIndex(), 0);
                updater.shardStarted(
                    new TestShardRouting.Builder(shard, "node0", true, INITIALIZING).withRecoverySource(
                        index.getResizeSourceIndex() == null
                            ? RecoverySource.PeerRecoverySource.INSTANCE
                            : RecoverySource.LocalShardsRecoverySource.INSTANCE
                    ).build(),
                    newShardRouting(shard, "node0", true, STARTED)
                );
            }
        }

        // only indices with a local peer recovery are tracked as "changes", and those must also have a resize-source-uuid setting
        assertThat(updater.numberOfChanges(), is(4));

        routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(metadata, (builder, indexMetadata) -> {
            final Index index = indexMetadata.getIndex();
            builder.add(
                IndexRoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, index)
                    .addShard(newShardRouting(new ShardId(index, 0), "node0", true, STARTED))
                    .build()
            );
        });
        updated = updater.applyChanges(metadata, routingTable);
        assertThat(updated, not(sameInstance(metadata)));

        for (ProjectMetadata project : updated.projects().values()) {
            for (IndexMetadata index : project.indices().values()) {
                assertThat(index.getSettings().get(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY), nullValue());
                assertThat(index.getSettings().get(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY), nullValue());
            }
        }
    }
}
