/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotStressTestsHelper.TrackedCluster;
import org.elasticsearch.snapshots.SnapshotStressTestsHelper.TransferableReleasables;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.snapshots.SnapshotStressTestsHelper.mustSucceed;
import static org.elasticsearch.snapshots.SnapshotStressTestsHelper.nodeNames;
import static org.elasticsearch.snapshots.SnapshotStressTestsHelper.tryAcquirePermit;

public class StatelessSnapshotStressTestsIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // Rebalancing is causing some checks after restore to randomly fail
            // due to https://github.com/elastic/elasticsearch/issues/9421
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            // Speed up master failover
            .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), "1s")
            // max 1 miss with 1s frequency may be unstable on slow CI machines, so keep it at 2
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 2);
    }

    public void testRandomActivitiesStatelessSnapshotDisabled() throws InterruptedException {
        doTestRandomActivities(Settings.EMPTY);
    }

    public void testRandomActivitiesStatelessSnapshotReadFromObjectStore() throws InterruptedException {
        doTestRandomActivities(
            Settings.builder()
                .put(
                    StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                    StatelessSnapshotSettings.StatelessSnapshotEnabledStatus.READ_FROM_OBJECT_STORE
                )
                .build()
        );
    }

    public void testRandomActivitiesStatelessSnapshotEnabled() throws InterruptedException {
        doTestRandomActivities(
            Settings.builder()
                .put(
                    StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                    StatelessSnapshotSettings.StatelessSnapshotEnabledStatus.ENABLED
                )
                .build()
        );
    }

    public void testRandomActivitiesStatelessSnapshotAndRelocationEnabled() throws InterruptedException {
        doTestRandomActivities(
            Settings.builder()
                .put(
                    StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                    StatelessSnapshotSettings.StatelessSnapshotEnabledStatus.ENABLED
                )
                .put(StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
                .build()
        );
    }

    private void doTestRandomActivities(Settings extraSettings) throws InterruptedException {
        final int numIndexNodes = between(1, 3);
        logger.info("--> starting [{}] indexing nodes", numIndexNodes);
        final var indexNodeNames = new ArrayList<String>();
        for (int i = 0; i < numIndexNodes; i++) {
            indexNodeNames.add(startMasterAndIndexNode(extraSettings));
        }
        final int numSearchNodes = between(0, 3);
        logger.info("--> starting [{}] search nodes", numSearchNodes);
        if (numSearchNodes > 0) {
            startSearchNodes(numSearchNodes, extraSettings);
        }
        ensureStableCluster(numIndexNodes + numSearchNodes);

        final DiscoveryNodes discoveryNodes = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get()
            .getState()
            .nodes();
        final var trackedCluster = buildTrackedCluster(
            discoveryNodes,
            StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.get(extraSettings)
        );
        trackedCluster.run();

        indexNodeNames.forEach(nodeName -> {
            logger.info("--> asserting no commit is tracked for snapshots on [{}]", nodeName);
            internalCluster().getInstance(SnapshotsCommitService.class, nodeName).assertEmptyTracking();
        });
    }

    private static TrackedCluster buildTrackedCluster(DiscoveryNodes discoveryNodes, boolean relocationDuringSnapshotEnabled) {
        final var mapOfIndexNodeNameToId = discoveryNodes.stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .collect(Collectors.toUnmodifiableMap(DiscoveryNode::getName, DiscoveryNode::getId));

        final var numSearchNodes = (int) discoveryNodes.stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .count();

        return new TrackedCluster(internalCluster(), nodeNames(discoveryNodes.getMasterNodes()), nodeNames(discoveryNodes.getDataNodes())) {
            @Override
            protected int numberOfReplicasUpperBound() {
                return numSearchNodes;
            }

            @Override
            protected void startAdditionalActivities() {
                if (relocationDuringSnapshotEnabled && mapOfIndexNodeNameToId.size() > 1) {
                    logger.info("--> starting shard movements with allocation filtering");
                    startAllocationFiltering();
                }
            }

            private void startAllocationFiltering() {
                enqueueAction(() -> {
                    boolean rerun = true;
                    try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                        if (usually()) {
                            return;
                        }

                        final List<TrackedIndex> trackedIndices = indices.values().stream().toList();
                        if (trackedIndices.isEmpty()) {
                            return;
                        }
                        final var trackedIndex = randomFrom(trackedIndices);
                        if (localReleasables.add(tryAcquirePermit(trackedIndex.permits())) == null) {
                            return;
                        }

                        if (localReleasables.add(blockNodeRestarts()) == null) {
                            return;
                        }
                        final var excludedNode = randomFrom(
                            shuffledNodes.stream().filter(node -> mapOfIndexNodeNameToId.containsKey(node.nodeName())).toList()
                        );
                        final String excludedNodeId = mapOfIndexNodeNameToId.get(excludedNode.nodeName());
                        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);

                        final Releasable releaseAll = localReleasables.transfer();

                        logger.info(" --> moving index [{}] away from node [{}]", trackedIndex.indexName(), excludedNode.nodeName());

                        SubscribableListener.<AcknowledgedResponse>newForked(
                            l -> indicesAdmin().prepareUpdateSettings(trackedIndex.indexName())
                                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", excludedNode.nodeName()))
                                .execute(l)
                        ).addListener(mustSucceed(acknowledgedResponse -> {
                            assertTrue(acknowledgedResponse.isAcknowledged());
                            logger.info(
                                "--> updated index [{}] settings to exclude node [{}]",
                                trackedIndex.indexName(),
                                excludedNode.nodeName()
                            );
                            // Add a listener to trigger allocation filtering again once relocation is completed for the current one
                            ClusterServiceUtils.addTemporaryStateListener(
                                clusterService,
                                state -> state.routingTable(ProjectId.DEFAULT)
                                    .index(trackedIndex.indexName())
                                    .allShards()
                                    .map(IndexShardRoutingTable::primaryShard)
                                    .allMatch(
                                        shardRouting -> shardRouting.active()
                                            && excludedNodeId.equals(shardRouting.currentNodeId()) == false
                                    ),
                                TimeValue.ONE_MINUTE
                            ).addListener(mustSucceed(ignored -> {
                                logger.info("--> moved index [{}] away from node [{}]", trackedIndex.indexName(), excludedNode.nodeName());
                                Releasables.close(releaseAll);
                                startAllocationFiltering();
                            }));
                        }));
                        rerun = false;

                    } finally {
                        if (rerun) {
                            startAllocationFiltering();
                        }
                    }
                });
            }
        };
    }
}
