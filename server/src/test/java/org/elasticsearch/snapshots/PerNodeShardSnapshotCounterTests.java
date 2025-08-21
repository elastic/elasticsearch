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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    public void testNodesCapacitiesWithKnownStates() {
        final var snapshotNodeId = randomAlphaOfLength(16);
        final boolean isStateless = randomBoolean();
        final DiscoveryNodes discoveryNodes = randomDiscoveryNodes(List.of(snapshotNodeId), List.of(randomAlphaOfLength(18)), isStateless);
        final int perNodeLimit = 1; // Capacity for only a single shard snapshot

        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY;
        {
            final var perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(
                perNodeLimit,
                snapshotsInProgress,
                discoveryNodes,
                isStateless
            );
            // Available capacity for only a single shard snapshot
            assertTrue(perNodeShardSnapshotCounter.hasCapacityOnAnyNode());
            assertTrue(perNodeShardSnapshotCounter.tryStartShardSnapshotOnNode(snapshotNodeId));
            assertFalse(perNodeShardSnapshotCounter.tryStartShardSnapshotOnNode(snapshotNodeId));
        }

        // Clone does not count towards the limit
        {
            final IndexId indexId = new IndexId(randomIdentifier(), randomUUID());
            final SnapshotsInProgress.Entry cloneEntry = SnapshotsInProgress.startClone(
                new Snapshot(ProjectId.DEFAULT, randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID())),
                new SnapshotId(randomIdentifier(), randomUUID()),
                Map.of(indexId.getName(), indexId),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                IndexVersion.current()
            );
            snapshotsInProgress.withAddedEntry(
                cloneEntry.withClones(
                    Map.of(new RepositoryShardId(indexId, 0), new ShardSnapshotStatus(snapshotNodeId, new ShardGeneration(1L)))
                )
            );
            final var perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(
                perNodeLimit,
                snapshotsInProgress,
                discoveryNodes,
                isStateless
            );
            assertTrue(perNodeShardSnapshotCounter.hasCapacityOnAnyNode());
        }

        // Enumerate states that do not count towards the limit
        {
            final Map<ShardId, ShardSnapshotStatus> shards = new HashMap<>();
            // Add shards that do not count towards limit
            shards.put(randomShardId(), ShardSnapshotStatus.UNASSIGNED_QUEUED);
            shards.put(randomShardId(), ShardSnapshotStatus.MISSING);
            shards.put(
                randomShardId(),
                ShardSnapshotStatus.success(snapshotNodeId, new ShardSnapshotResult(new ShardGeneration(1L), ByteSizeValue.ofBytes(1L), 1))
            );
            shards.put(
                randomShardId(),
                new ShardSnapshotStatus(snapshotNodeId, SnapshotsInProgress.ShardState.WAITING, new ShardGeneration(1L))
            );
            shards.put(
                randomShardId(),
                new ShardSnapshotStatus(snapshotNodeId, SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL, new ShardGeneration(1L))
            );
            shards.put(
                randomShardId(),
                new ShardSnapshotStatus(
                    snapshotNodeId,
                    SnapshotsInProgress.ShardState.FAILED,
                    new ShardGeneration(1L),
                    randomAlphaOfLengthBetween(10, 20)
                )
            );
            // aborted assigned-queued shard
            shards.put(
                randomShardId(),
                new ShardSnapshotStatus(null, SnapshotsInProgress.ShardState.ABORTED, new ShardGeneration(1L), "assigned-queued aborted")
            );
            // assigned-queued shard
            shards.put(randomShardId(), ShardSnapshotStatus.assignedQueued(snapshotNodeId, randomFrom(new ShardGeneration(1L), null)));

            final Map<String, IndexId> indexIds = shards.keySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(ShardId::getIndexName, shardId -> new IndexId(shardId.getIndexName(), randomUUID())));

            snapshotsInProgress = snapshotsInProgress.withAddedEntry(
                SnapshotsInProgress.Entry.snapshot(
                    new Snapshot(ProjectId.DEFAULT, randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID())),
                    randomBoolean(),
                    randomBoolean(),
                    SnapshotsInProgress.State.STARTED,
                    indexIds,
                    List.of(),
                    List.of(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    shards,
                    null,
                    Map.of(),
                    IndexVersion.current()
                )
            );

            final var perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(
                perNodeLimit,
                snapshotsInProgress,
                discoveryNodes,
                isStateless
            );
            assertTrue(perNodeShardSnapshotCounter.hasCapacityOnAnyNode());
        }

        // Shard snapshots with states counting towards
        {
            // INIT shard
            final Map<ShardId, ShardSnapshotStatus> shards = new HashMap<>();
            final ShardId shardId = randomShardId();
            shards.put(shardId, new ShardSnapshotStatus(snapshotNodeId, new ShardGeneration(1L)));
            var entry = SnapshotsInProgress.Entry.snapshot(
                new Snapshot(ProjectId.DEFAULT, randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID())),
                randomBoolean(),
                randomBoolean(),
                SnapshotsInProgress.State.STARTED,
                Map.of(shardId.getIndexName(), new IndexId(shardId.getIndexName(), randomUUID())),
                List.of(),
                List.of(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                shards,
                null,
                Map.of(),
                IndexVersion.current()
            );
            snapshotsInProgress = snapshotsInProgress.withAddedEntry(entry);
            var perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(
                perNodeLimit,
                snapshotsInProgress,
                discoveryNodes,
                isStateless
            );
            assertFalse(perNodeShardSnapshotCounter.hasCapacityOnAnyNode()); // no capacity left due to the INIT shard

            // Aborted shard snapshot that was previously in INIT state still count towards the limit
            entry = entry.abort(snapshotNodeId, (ignore0, ignore1) -> {});
            assertNotNull(entry);
            snapshotsInProgress = snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(
                ProjectId.DEFAULT,
                entry.repository(),
                List.of(entry)
            );
            perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(perNodeLimit, snapshotsInProgress, discoveryNodes, isStateless);
            assertFalse(perNodeShardSnapshotCounter.hasCapacityOnAnyNode());

            // Complete the entry by fail the shard snapshot and the node capacity is released
            entry = entry.withShardStates(
                Map.of(
                    shardId,
                    new ShardSnapshotStatus(snapshotNodeId, SnapshotsInProgress.ShardState.FAILED, new ShardGeneration(1L), "failed")
                )
            );
            snapshotsInProgress = snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(
                ProjectId.DEFAULT,
                entry.repository(),
                List.of(entry)
            );
            perNodeShardSnapshotCounter = new PerNodeShardSnapshotCounter(perNodeLimit, snapshotsInProgress, discoveryNodes, isStateless);
            assertTrue(perNodeShardSnapshotCounter.hasCapacityOnAnyNode());
        }
    }

    public void testNodesCapacitiesWithRandomSnapshots() {
        final var snapshotNodeIds = randomList(1, 5, () -> randomAlphaOfLength(16));
        final var nonSnapshotNodeIds = randomList(0, 5, () -> randomAlphaOfLength(18));
        final boolean isStateless = randomBoolean();
        final DiscoveryNodes discoveryNodes = randomDiscoveryNodes(snapshotNodeIds, nonSnapshotNodeIds, isStateless);
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

    private static DiscoveryNodes randomDiscoveryNodes(List<String> snapshotNodeIds, List<String> nonSnapshotNodeIds, boolean isStateless) {
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
        return discoveryNodes;
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

    private ShardId randomShardId() {
        return new ShardId(new Index(randomAlphaOfLength(12), randomUUID()), between(0, 5));
    }
}
