/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterSnapshotStatsTests extends AbstractWireSerializingTestCase<ClusterSnapshotStats> {

    @Override
    protected Writeable.Reader<ClusterSnapshotStats> instanceReader() {
        return ClusterSnapshotStats::readFrom;
    }

    // BWC warning: these go over the wire as strings, be careful when changing them!
    private static final String[] SHARD_STATE_NAMES = new String[] {
        "INIT",
        "SUCCESS",
        "FAILED",
        "ABORTED",
        "MISSING",
        "WAITING",
        "QUEUED" };

    @Override
    protected ClusterSnapshotStats createTestInstance() {
        return new ClusterSnapshotStats(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomList(0, 10, ClusterSnapshotStatsTests::createRepositoryStats)
        );
    }

    @Override
    protected ClusterSnapshotStats mutateInstance(ClusterSnapshotStats instance) throws IOException {
        return switch (between(1, 5)) {
            case 1 -> new ClusterSnapshotStats(
                randomValueOtherThan(instance.snapshotsInProgressCount(), ESTestCase::randomNonNegativeInt),
                instance.incompleteShardSnapshotCount(),
                instance.deletionsInProgressCount(),
                instance.cleanupsInProgressCount(),
                instance.statsByRepository()
            );
            case 2 -> new ClusterSnapshotStats(
                instance.snapshotsInProgressCount(),
                randomValueOtherThan(instance.incompleteShardSnapshotCount(), ESTestCase::randomNonNegativeInt),
                instance.deletionsInProgressCount(),
                instance.cleanupsInProgressCount(),
                instance.statsByRepository()
            );
            case 3 -> new ClusterSnapshotStats(
                instance.snapshotsInProgressCount(),
                instance.incompleteShardSnapshotCount(),
                randomValueOtherThan(instance.deletionsInProgressCount(), ESTestCase::randomNonNegativeInt),
                instance.cleanupsInProgressCount(),
                instance.statsByRepository()
            );
            case 4 -> new ClusterSnapshotStats(
                instance.snapshotsInProgressCount(),
                instance.incompleteShardSnapshotCount(),
                instance.deletionsInProgressCount(),
                randomValueOtherThan(instance.cleanupsInProgressCount(), ESTestCase::randomNonNegativeInt),
                instance.statsByRepository()
            );
            case 5 -> new ClusterSnapshotStats(
                instance.snapshotsInProgressCount(),
                instance.incompleteShardSnapshotCount(),
                instance.deletionsInProgressCount(),
                instance.cleanupsInProgressCount(),
                mutatePerRepositoryStats(instance.statsByRepository())
            );
            default -> throw new AssertionError("impossible");
        };
    }

    private static List<ClusterSnapshotStats.PerRepositoryStats> mutatePerRepositoryStats(
        List<ClusterSnapshotStats.PerRepositoryStats> statsByRepository
    ) {
        if (statsByRepository.isEmpty() || (randomBoolean() && statsByRepository.size() < 10)) {
            return CollectionUtils.appendToCopy(statsByRepository, createRepositoryStats());
        }

        var toModify = between(0, statsByRepository.size() - 1);
        var newList = new ArrayList<>(statsByRepository);

        if (randomBoolean()) {
            newList.remove(toModify);
        } else {
            newList.set(toModify, mutateRepositoryStats(newList.get(toModify)));
        }

        return Collections.unmodifiableList(newList);
    }

    private static ClusterSnapshotStats.PerRepositoryStats createRepositoryStats() {
        return new ClusterSnapshotStats.PerRepositoryStats(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            new EnumMap<SnapshotsInProgress.ShardState, Integer>(
                Arrays.stream(SHARD_STATE_NAMES)
                    .collect(Collectors.toMap(SnapshotsInProgress.ShardState::valueOf, ignored -> randomNonNegativeInt()))
            )
        );
    }

    private static ClusterSnapshotStats.PerRepositoryStats mutateRepositoryStats(ClusterSnapshotStats.PerRepositoryStats instance) {
        return switch (between(1, 12)) {
            case 1 -> new ClusterSnapshotStats.PerRepositoryStats(
                randomValueOtherThan(instance.repositoryName(), () -> randomAlphaOfLength(10)),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 2 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                randomValueOtherThan(instance.repositoryType(), () -> randomAlphaOfLength(10)),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 3 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                randomValueOtherThan(instance.snapshotCount(), ESTestCase::randomNonNegativeInt),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 4 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                randomValueOtherThan(instance.cloneCount(), ESTestCase::randomNonNegativeInt),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 5 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                randomValueOtherThan(instance.finalizationsCount(), ESTestCase::randomNonNegativeInt),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 6 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                randomValueOtherThan(instance.totalShards(), ESTestCase::randomNonNegativeInt),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 7 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                randomValueOtherThan(instance.completeShards(), ESTestCase::randomNonNegativeInt),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 8 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                randomValueOtherThan(instance.deletionsCount(), ESTestCase::randomNonNegativeInt),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 9 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                randomValueOtherThan(instance.snapshotDeletionsCount(), ESTestCase::randomNonNegativeInt),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 10 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                randomValueOtherThan(instance.activeDeletionsCount(), ESTestCase::randomNonNegativeInt),
                instance.firstStartTimeMillis(),
                instance.shardStates()
            );
            case 11 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                randomValueOtherThan(instance.firstStartTimeMillis(), ESTestCase::randomNonNegativeLong),
                instance.shardStates()
            );
            case 12 -> new ClusterSnapshotStats.PerRepositoryStats(
                instance.repositoryName(),
                instance.repositoryType(),
                instance.snapshotCount(),
                instance.cloneCount(),
                instance.finalizationsCount(),
                instance.totalShards(),
                instance.completeShards(),
                instance.deletionsCount(),
                instance.snapshotDeletionsCount(),
                instance.activeDeletionsCount(),
                instance.firstStartTimeMillis(),
                new EnumMap<>(mutateShardStates(instance.shardStates()))
            );
            default -> throw new AssertionError("impossible");
        };
    }

    private static Map<SnapshotsInProgress.ShardState, Integer> mutateShardStates(Map<SnapshotsInProgress.ShardState, Integer> instance) {
        final var keyToModify = randomFrom(SnapshotsInProgress.ShardState.values());
        return instance.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getKey() == keyToModify ? randomValueOtherThan(e.getValue(), ESTestCase::randomNonNegativeInt) : e.getValue()
                )
            );
    }

    public void testEmpty() {
        assertEquals(ClusterSnapshotStats.EMPTY, ClusterSnapshotStats.of(ClusterState.EMPTY_STATE, randomNonNegativeLong()));
    }

    public void testComputation() {
        final var startTimes = new long[] { randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong() };
        final var currentTimeMillis = randomLongBetween(Arrays.stream(startTimes).max().getAsLong(), Long.MAX_VALUE);
        assertEquals(
            new ClusterSnapshotStats(
                2,
                2,
                1,
                1,
                List.of(
                    new ClusterSnapshotStats.PerRepositoryStats(
                        "test-repo",
                        "test-repo-type",
                        1,
                        1,
                        0,
                        3,
                        1,
                        1,
                        1,
                        0,
                        Arrays.stream(startTimes).min().getAsLong(),
                        new EnumMap<SnapshotsInProgress.ShardState, Integer>(
                            Map.of(
                                SnapshotsInProgress.ShardState.INIT,
                                1,
                                SnapshotsInProgress.ShardState.SUCCESS,
                                1,
                                SnapshotsInProgress.ShardState.FAILED,
                                0,
                                SnapshotsInProgress.ShardState.ABORTED,
                                0,
                                SnapshotsInProgress.ShardState.MISSING,
                                0,
                                SnapshotsInProgress.ShardState.WAITING,
                                0,
                                SnapshotsInProgress.ShardState.QUEUED,
                                1
                            )
                        )
                    )
                )
            ),
            ClusterSnapshotStats.of(
                ClusterState.builder(ClusterState.EMPTY_STATE)
                    .metadata(
                        Metadata.builder()
                            .putCustom(
                                RepositoriesMetadata.TYPE,
                                new RepositoriesMetadata(List.of(new RepositoryMetadata("test-repo", "test-repo-type", Settings.EMPTY)))
                            )
                    )
                    .putCustom(
                        SnapshotsInProgress.TYPE,
                        SnapshotsInProgress.EMPTY.withAddedEntry(
                            SnapshotsInProgress.Entry.snapshot(
                                new Snapshot("test-repo", new SnapshotId("snapshot", "uuid")),
                                randomBoolean(),
                                randomBoolean(),
                                SnapshotsInProgress.State.INIT,
                                Map.of("index", new IndexId("index", "uuid")),
                                List.of(),
                                List.of(),
                                startTimes[0],
                                randomNonNegativeLong(),
                                Map.of(
                                    new ShardId("index", "uuid", 0),
                                    new SnapshotsInProgress.ShardSnapshotStatus("node", new ShardGeneration("gen")),
                                    new ShardId("index", "uuid", 1),
                                    new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.ShardState.QUEUED, null),
                                    new ShardId("index", "uuid", 2),
                                    SnapshotsInProgress.ShardSnapshotStatus.success(
                                        "node",
                                        new ShardSnapshotResult(new ShardGeneration("gen"), ByteSizeValue.ZERO, 0)
                                    )
                                ),
                                null,
                                Map.of(),
                                IndexVersion.current()
                            )
                        )
                            .withAddedEntry(
                                SnapshotsInProgress.startClone(
                                    new Snapshot("test-repo", new SnapshotId("clone", "uuid")),
                                    new SnapshotId("clone-source", "uuid"),
                                    Map.of("index", new IndexId("index", "index-id")),
                                    startTimes[1],
                                    randomNonNegativeLong(),
                                    IndexVersion.current()
                                )
                            )
                    )
                    .putCustom(
                        SnapshotDeletionsInProgress.TYPE,
                        SnapshotDeletionsInProgress.of(
                            List.of(
                                new SnapshotDeletionsInProgress.Entry(
                                    List.of(new SnapshotId("deleting", "uuid")),
                                    "test-repo",
                                    startTimes[2],
                                    randomNonNegativeLong(),
                                    SnapshotDeletionsInProgress.State.WAITING
                                )
                            )
                        )
                    )
                    .putCustom(
                        RepositoryCleanupInProgress.TYPE,
                        new RepositoryCleanupInProgress(
                            List.of(new RepositoryCleanupInProgress.Entry("test-repo", randomNonNegativeLong()))
                        )
                    )
                    .build(),
                currentTimeMillis
            )
        );

    }

}
