/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.elasticsearch.snapshots.SnapshotsInProgressSerializationTests.randomSnapshot;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class PerNodeShardSnapshotCounterTests extends ESTestCase {

    public void testDisabledWhenLimitIsZero() {
        final var perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(
            0,
            SnapshotsInProgress.EMPTY,
            DiscoveryNodes.EMPTY_NODES,
            randomBoolean()
        );
        assertTrue(perNodeShardSnapshotCounter.hasCapacityOnAnyNode());
        assertTrue(perNodeShardSnapshotCounter.tryStartShardSnapshotOnNode(randomIdentifier()));
        assertTrue(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(randomIdentifier()));
    }

    public void testNodesCapacities() {
        final var snapshotNodeIds = randomList(1, 5, () -> randomAlphaOfLength(16));
        final var nonSnapshotNodeIds = randomList(0, 5, () -> randomAlphaOfLength(18));

        final boolean isStateless = randomBoolean();
        final Set<DiscoveryNodeRole> snapshotNodeRole;
        final Set<DiscoveryNodeRole> nonSnapshotNodeRole;
        if (isStateless) {
            snapshotNodeRole = Set.of(DiscoveryNodeRole.INDEX_ROLE);
            nonSnapshotNodeRole = Set.of(randomFrom(DiscoveryNodeRole.SEARCH_ROLE, DiscoveryNodeRole.ML_ROLE));
        } else {
            snapshotNodeRole = Set.copyOf(
                randomNonEmptySubsetOf(
                    Set.of(
                        DiscoveryNodeRole.DATA_ROLE,
                        DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
                        DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
                        DiscoveryNodeRole.DATA_COLD_NODE_ROLE,
                        DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE,
                        DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE
                    )
                )
            );
            nonSnapshotNodeRole = Set.copyOf(
                randomNonEmptySubsetOf(
                    Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.ML_ROLE, DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)
                )
            );
        }

        final var nodesBuilder = DiscoveryNodes.builder();
        for (var snapshotNodeId : snapshotNodeIds) {
            nodesBuilder.add(DiscoveryNodeUtils.builder(snapshotNodeId).name(snapshotNodeId).roles(snapshotNodeRole).build());
        }
        for (var nonSnapshotNodeId : nonSnapshotNodeIds) {
            nodesBuilder.add(DiscoveryNodeUtils.builder(nonSnapshotNodeId).name(nonSnapshotNodeId).roles(nonSnapshotNodeRole).build());
        }
        final DiscoveryNodes discoveryNodes = nodesBuilder.build();

        final int perNodeLimit = between(1, 10);

        // Basic test when there is no ongoing snapshots
        {
            final var perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(
                perNodeLimit,
                SnapshotsInProgress.EMPTY,
                discoveryNodes,
                isStateless
            );
            assertTrue(perNodeShardSnapshotCounter.hasCapacityOnAnyNode());

            // Always false for non-snapshotting nodes
            for (var nonSnapshotNodeId : nonSnapshotNodeIds) {
                assertFalse(perNodeShardSnapshotCounter.tryStartShardSnapshotOnNode(nonSnapshotNodeId));
                assertFalse(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(nonSnapshotNodeId));
            }

            // Cannot complete where there is nothing running
            for (var snapshotNodeId : snapshotNodeIds) {
                assertFalse(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(snapshotNodeId));
            }

            // Can start up to the limit
            for (int i = 0; i < perNodeLimit; i++) {
                for (var snapshotNodeId : snapshotNodeIds) {
                    assertTrue(perNodeShardSnapshotCounter.tryStartShardSnapshotOnNode(snapshotNodeId));
                }
            }
            // Cannot start beyond the limit
            for (var snapshotNodeId : snapshotNodeIds) {
                assertFalse(perNodeShardSnapshotCounter.tryStartShardSnapshotOnNode(snapshotNodeId));
            }
            // Can complete all started snapshots
            for (int i = 0; i < perNodeLimit; i++) {
                for (var snapshotNodeId : snapshotNodeIds) {
                    assertTrue(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(snapshotNodeId));
                }
            }
            // Cannot complete when nothing is running
            for (var snapshotNodeId : snapshotNodeIds) {
                assertFalse(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(snapshotNodeId));
            }
        }

        // random snapshots
        {
            final int numRepos = between(1, 3);
            SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY;
            for (int i = 0; i < numRepos; i++) {
                final var numSnapshots = between(1, 5);
                for (int j = 0; j < numSnapshots; j++) {
                    final var entry = randomSnapshot(ProjectId.DEFAULT, "repo-" + i, () -> randomFrom(snapshotNodeIds));
                    if (entry.hasAssignedQueuedShards()) {
                        assertTrue(entry.shards().values().stream().anyMatch(ShardSnapshotStatus::isAssignedQueued));
                    } else {
                        assertFalse(entry.shards().values().stream().anyMatch(ShardSnapshotStatus::isAssignedQueued));
                    }
                    snapshotsInProgress = snapshotsInProgress.withAddedEntry(entry);
                }
            }

            final var perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(
                perNodeLimit,
                snapshotsInProgress,
                discoveryNodes,
                isStateless
            );

            // Always false for non-snapshotting nodes
            for (var nonSnapshotNodeId : nonSnapshotNodeIds) {
                assertFalse(perNodeShardSnapshotCounter.tryStartShardSnapshotOnNode(nonSnapshotNodeId));
                assertFalse(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(nonSnapshotNodeId));
            }

            for (var snapshotNodeId : snapshotNodeIds) {
                final var numRunning = runningShardSnapshotsForNode(snapshotsInProgress, snapshotNodeId);
                final var started = perNodeShardSnapshotCounter.tryStartShardSnapshotOnNode(snapshotNodeId);
                if (started) {
                    assertThat(numRunning, lessThan(perNodeLimit));
                    if (numRunning > 0) {
                        assertTrue(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(snapshotNodeId));
                    } else {
                        assertFalse(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(snapshotNodeId));
                    }
                } else {
                    assertThat(numRunning, greaterThanOrEqualTo(perNodeLimit));
                    assertTrue(perNodeShardSnapshotCounter.completeShardSnapshotOnNode(snapshotNodeId));
                }
            }
        }
    }

    private int runningShardSnapshotsForNode(SnapshotsInProgress snapshotsInProgress, String nodeId) {
        int result = 0;
        final var allEntries = snapshotsInProgress.asStream().toList();
        for (var entry : allEntries) {
            if (entry.isClone() == false && entry.state().completed() == false) {
                for (ShardSnapshotStatus shardSnapshot : entry.shards().values()) {
                    if (nodeId.equals(shardSnapshot.nodeId())) {
                        if (shardSnapshot.state() == SnapshotsInProgress.ShardState.INIT) {
                            result++;
                        } else if (shardSnapshot.state() == SnapshotsInProgress.ShardState.ABORTED
                            && (shardSnapshot.reason() == null || shardSnapshot.reason().startsWith("assigned-queued aborted") == false)) {
                                result++;
                            }
                    }
                }
            }
        }
        return result;
    }
}
