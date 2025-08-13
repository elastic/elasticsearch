/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public record ClusterSnapshotStats(
    int snapshotsInProgressCount,
    int incompleteShardSnapshotCount,
    int deletionsInProgressCount,
    int cleanupsInProgressCount,
    List<PerRepositoryStats> statsByRepository
) implements ToXContentObject, Writeable {

    public static final ClusterSnapshotStats EMPTY = new ClusterSnapshotStats(0, 0, 0, 0, List.of());

    public static ClusterSnapshotStats of(ClusterState clusterState, long currentTimeMillis) {
        return of(
            RepositoriesMetadata.get(clusterState),
            SnapshotsInProgress.get(clusterState),
            SnapshotDeletionsInProgress.get(clusterState),
            RepositoryCleanupInProgress.get(clusterState),
            currentTimeMillis
        );
    }

    private static ClusterSnapshotStats of(
        RepositoriesMetadata repositoriesMetadata,
        SnapshotsInProgress snapshotsInProgress,
        SnapshotDeletionsInProgress snapshotDeletionsInProgress,
        RepositoryCleanupInProgress repositoryCleanupInProgress,
        long currentTimeMillis
    ) {

        final var snapshotsInProgressCount = snapshotsInProgress.count();
        var incompleteShardSnapshotCount = 0;
        final var deletionsInProgressCount = snapshotDeletionsInProgress.getEntries().size();
        final var cleanupsInProgressCount = repositoryCleanupInProgress.entries().size();
        final var perRepositoryStats = new ArrayList<PerRepositoryStats>(repositoriesMetadata.repositories().size());

        for (RepositoryMetadata repository : repositoriesMetadata.repositories()) {

            final var repositoryName = repository.name();
            final var repositoryType = repository.type();

            var snapshotCount = 0;
            var cloneCount = 0;
            var finalizationsCount = 0;
            var totalShards = 0;
            var completeShards = 0;
            final var shardStatesAccumulator = Arrays.stream(SnapshotsInProgress.ShardState.values())
                .collect(Collectors.toMap(Function.identity(), ignored -> new AtomicInteger(), (s1, s2) -> {
                    assert false;
                    return s1;
                }, () -> new EnumMap<>(SnapshotsInProgress.ShardState.class)));
            var deletionsCount = 0;
            var snapshotDeletionsCount = 0;
            var activeDeletionsCount = 0;
            var firstStartTimeMillis = currentTimeMillis;

            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(repositoryName)) {
                firstStartTimeMillis = Math.min(firstStartTimeMillis, entry.startTime());

                if (entry.state().completed()) {
                    finalizationsCount += 1;
                }

                if (entry.isClone()) {
                    cloneCount += 1;
                } else {
                    snapshotCount += 1;
                    totalShards += entry.shards().size();

                    for (SnapshotsInProgress.ShardSnapshotStatus value : entry.shards().values()) {
                        if (value.state().completed()) {
                            completeShards += 1;
                        }

                        shardStatesAccumulator.get(value.state()).incrementAndGet();
                    }
                }
            }

            for (SnapshotDeletionsInProgress.Entry entry : snapshotDeletionsInProgress.getEntries()) {
                if (entry.repository().equals(repositoryName)) {
                    firstStartTimeMillis = Math.min(firstStartTimeMillis, entry.startTime());
                    deletionsCount += 1;
                    snapshotDeletionsCount += entry.snapshots().size();
                    if (entry.state() == SnapshotDeletionsInProgress.State.STARTED) {
                        activeDeletionsCount += 1;
                    }
                }
            }

            perRepositoryStats.add(
                new PerRepositoryStats(
                    repositoryName,
                    repositoryType,
                    snapshotCount,
                    cloneCount,
                    finalizationsCount,
                    totalShards,
                    completeShards,
                    deletionsCount,
                    snapshotDeletionsCount,
                    activeDeletionsCount,
                    firstStartTimeMillis,
                    new EnumMap<SnapshotsInProgress.ShardState, Integer>(
                        shardStatesAccumulator.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()))
                    )
                )
            );

            incompleteShardSnapshotCount += totalShards - completeShards;
        }

        return new ClusterSnapshotStats(
            snapshotsInProgressCount,
            incompleteShardSnapshotCount,
            deletionsInProgressCount,
            cleanupsInProgressCount,
            List.copyOf(perRepositoryStats)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();

        builder.startObject("current_counts");
        builder.field("snapshots", snapshotsInProgressCount);
        builder.field("shard_snapshots", incompleteShardSnapshotCount);
        builder.field("snapshot_deletions", deletionsInProgressCount);
        builder.field("concurrent_operations", snapshotsInProgressCount + deletionsInProgressCount);
        // NB cleanups are not "concurrent operations", not counted here ^
        builder.field("cleanups", cleanupsInProgressCount);
        builder.endObject();

        builder.startObject("repositories");
        for (PerRepositoryStats perRepositoryStats : statsByRepository) {
            perRepositoryStats.toXContent(builder, params);
        }
        builder.endObject();

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(snapshotsInProgressCount);
        out.writeVInt(incompleteShardSnapshotCount);
        out.writeVInt(deletionsInProgressCount);
        out.writeVInt(cleanupsInProgressCount);
        out.writeCollection(statsByRepository);
    }

    public static ClusterSnapshotStats readFrom(StreamInput in) throws IOException {
        return new ClusterSnapshotStats(
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readCollectionAsList(PerRepositoryStats::readFrom)
        );
    }

    record PerRepositoryStats(
        String repositoryName,
        String repositoryType,
        int snapshotCount,
        int cloneCount,
        int finalizationsCount,
        int totalShards,
        int completeShards,
        int deletionsCount,
        int snapshotDeletionsCount,
        int activeDeletionsCount,
        long firstStartTimeMillis,
        EnumMap<SnapshotsInProgress.ShardState, Integer> shardStates
    ) implements ToXContentFragment, Writeable {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(repositoryName);
            builder.field("type", repositoryType);

            builder.startObject("current_counts");
            builder.field("snapshots", snapshotCount);
            builder.field("clones", cloneCount);
            builder.field("finalizations", finalizationsCount);
            builder.field("deletions", deletionsCount);
            builder.field("snapshot_deletions", snapshotDeletionsCount);
            builder.field("active_deletions", activeDeletionsCount);

            builder.startObject("shards");
            builder.field("total", totalShards);
            builder.field("complete", completeShards);
            builder.field("incomplete", totalShards - completeShards);
            builder.startObject("states");
            for (Map.Entry<SnapshotsInProgress.ShardState, Integer> entry : shardStates.entrySet()) {
                builder.field(entry.getKey().toString(), entry.getValue());
            }
            builder.endObject();
            builder.endObject();
            builder.endObject();

            builder.timestampFieldsFromUnixEpochMillis("oldest_start_time_millis", "oldest_start_time", firstStartTimeMillis);

            return builder.endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repositoryName);
            out.writeString(repositoryType);
            out.writeVInt(snapshotCount);
            out.writeVInt(cloneCount);
            out.writeVInt(finalizationsCount);
            out.writeVInt(totalShards);
            out.writeVInt(completeShards);
            out.writeVInt(deletionsCount);
            out.writeVInt(snapshotDeletionsCount);
            out.writeVInt(activeDeletionsCount);
            out.writeVLong(firstStartTimeMillis);
            out.writeMap(shardStates, (o, state) -> o.writeString(state.toString()), StreamOutput::writeVInt);
        }

        static PerRepositoryStats readFrom(StreamInput in) throws IOException {
            return new PerRepositoryStats(
                in.readString(),
                in.readString(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVLong(),
                new EnumMap<SnapshotsInProgress.ShardState, Integer>(
                    in.readMap(i -> SnapshotsInProgress.ShardState.valueOf(in.readString()), StreamInput::readVInt)
                )
            );
        }
    }
}
