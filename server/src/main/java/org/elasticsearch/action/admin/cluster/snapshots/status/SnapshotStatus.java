/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * Status of a snapshot
 */
public class SnapshotStatus implements ChunkedToXContentObject, Writeable {

    private final Snapshot snapshot;

    private final State state;

    private final List<SnapshotIndexShardStatus> shards;

    private Map<String, SnapshotIndexStatus> indicesStatus;

    private SnapshotShardsStats shardsStats;

    private SnapshotStats stats;

    @Nullable
    private final Boolean includeGlobalState;

    SnapshotStatus(StreamInput in) throws IOException {
        snapshot = new Snapshot(in);
        state = State.fromValue(in.readByte());
        shards = in.readCollectionAsImmutableList(SnapshotIndexShardStatus::new);
        includeGlobalState = in.readOptionalBoolean();
        final long startTime = in.readLong();
        final long time = in.readLong();
        updateShardStats(startTime, time);
    }

    SnapshotStatus(
        Snapshot snapshot,
        State state,
        List<SnapshotIndexShardStatus> shards,
        Boolean includeGlobalState,
        long startTime,
        long time
    ) {
        this.snapshot = Objects.requireNonNull(snapshot);
        this.state = Objects.requireNonNull(state);
        this.shards = Objects.requireNonNull(shards);
        this.includeGlobalState = includeGlobalState;
        shardsStats = new SnapshotShardsStats(shards);
        assert time >= 0 : "time must be >= 0 but received [" + time + "]";
        updateShardStats(startTime, time);
    }

    SnapshotStatus(
        Snapshot snapshot,
        State state,
        List<SnapshotIndexShardStatus> shards,
        Map<String, SnapshotIndexStatus> indicesStatus,
        SnapshotShardsStats shardsStats,
        SnapshotStats stats,
        Boolean includeGlobalState
    ) {
        this.snapshot = snapshot;
        this.state = state;
        this.shards = shards;
        this.indicesStatus = indicesStatus;
        this.shardsStats = shardsStats;
        this.stats = stats;
        this.includeGlobalState = includeGlobalState;
    }

    /**
     * Returns snapshot
     */
    public Snapshot getSnapshot() {
        return snapshot;
    }

    /**
     * Returns snapshot state
     */
    public State getState() {
        return state;
    }

    /**
     * Returns true if global state is included in the snapshot, false otherwise.
     * Can be null if this information is unknown.
     */
    public Boolean includeGlobalState() {
        return includeGlobalState;
    }

    /**
     * Returns list of snapshot shards
     */
    public List<SnapshotIndexShardStatus> getShards() {
        return shards;
    }

    public SnapshotShardsStats getShardsStats() {
        return shardsStats;
    }

    /**
     * Returns list of snapshot indices
     */
    public Map<String, SnapshotIndexStatus> getIndices() {
        var res = this.indicesStatus;
        if (res != null) {
            return res;
        }

        Map<String, List<SnapshotIndexShardStatus>> indices = new HashMap<>();
        for (SnapshotIndexShardStatus shard : shards) {
            indices.computeIfAbsent(shard.getIndex(), k -> new ArrayList<>()).add(shard);
        }
        Map<String, SnapshotIndexStatus> indicesStatus = Maps.newMapWithExpectedSize(indices.size());
        for (Map.Entry<String, List<SnapshotIndexShardStatus>> entry : indices.entrySet()) {
            indicesStatus.put(entry.getKey(), new SnapshotIndexStatus(entry.getKey(), entry.getValue()));
        }
        res = unmodifiableMap(indicesStatus);
        this.indicesStatus = res;
        return res;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        snapshot.writeTo(out);
        out.writeByte(state.value());
        out.writeCollection(shards);
        out.writeOptionalBoolean(includeGlobalState);
        out.writeLong(stats.getStartTime());
        out.writeLong(stats.getTime());
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, false);
    }

    /**
     * Returns number of files in the snapshot
     */
    public SnapshotStats getStats() {
        return stats;
    }

    static final String SNAPSHOT = "snapshot";
    static final String REPOSITORY = "repository";
    static final String UUID = "uuid";
    static final String STATE = "state";
    static final String INDICES = "indices";
    static final String INCLUDE_GLOBAL_STATE = "include_global_state";

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(Iterators.single((ToXContent) (b, p) -> {
            b.startObject()
                .field(SNAPSHOT, snapshot.getSnapshotId().getName())
                .field(REPOSITORY, snapshot.getRepository())
                .field(UUID, snapshot.getSnapshotId().getUUID())
                .field(STATE, state.name());
            if (includeGlobalState != null) {
                b.field(INCLUDE_GLOBAL_STATE, includeGlobalState);
            }
            return b.field(SnapshotShardsStats.Fields.SHARDS_STATS, shardsStats, p)
                .field(SnapshotStats.Fields.STATS, stats, p)
                .startObject(INDICES);
        }), getIndices().values().iterator(), Iterators.single((b, p) -> b.endObject().endObject()));
    }

    private void updateShardStats(long startTime, long time) {
        stats = new SnapshotStats(startTime, time, 0, 0, 0, 0, 0, 0);
        shardsStats = new SnapshotShardsStats(shards);
        for (SnapshotIndexShardStatus shard : shards) {
            // BWC: only update timestamps when we did not get a start time from an old node
            stats.add(shard.getStats(), startTime == 0L);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotStatus that = (SnapshotStatus) o;
        return Objects.equals(snapshot, that.snapshot)
            && state == that.state
            && Objects.equals(indicesStatus, that.indicesStatus)
            && Objects.equals(shardsStats, that.shardsStats)
            && Objects.equals(stats, that.stats)
            && Objects.equals(includeGlobalState, that.includeGlobalState);
    }

    @Override
    public int hashCode() {
        int result = snapshot != null ? snapshot.hashCode() : 0;
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (indicesStatus != null ? indicesStatus.hashCode() : 0);
        result = 31 * result + (shardsStats != null ? shardsStats.hashCode() : 0);
        result = 31 * result + (stats != null ? stats.hashCode() : 0);
        result = 31 * result + (includeGlobalState != null ? includeGlobalState.hashCode() : 0);
        return result;
    }
}
