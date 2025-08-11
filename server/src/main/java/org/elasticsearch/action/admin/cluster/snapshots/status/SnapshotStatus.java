/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Status of a snapshot
 */
public class SnapshotStatus implements ToXContentObject, Writeable {

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
        shards = Collections.unmodifiableList(in.readList(SnapshotIndexShardStatus::new));
        includeGlobalState = in.readOptionalBoolean();
        final long startTime;
        final long time;
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            startTime = in.readLong();
            time = in.readLong();
        } else {
            startTime = 0L;
            time = 0L;
        }
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

    private SnapshotStatus(
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
        Map<String, SnapshotIndexStatus> res = this.indicesStatus;
        if (res != null) {
            return res;
        }

        Map<String, List<SnapshotIndexShardStatus>> indices = new HashMap<>();
        for (SnapshotIndexShardStatus shard : shards) {
            indices.computeIfAbsent(shard.getIndex(), k -> new ArrayList<>()).add(shard);
        }
        Map<String, SnapshotIndexStatus> indicesStatus = new HashMap<>(indices.size());
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
        out.writeList(shards);
        out.writeOptionalBoolean(includeGlobalState);
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeLong(stats.getStartTime());
            out.writeLong(stats.getTime());
        }
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

    private static final String SNAPSHOT = "snapshot";
    private static final String REPOSITORY = "repository";
    private static final String UUID = "uuid";
    private static final String STATE = "state";
    private static final String INDICES = "indices";
    private static final String INCLUDE_GLOBAL_STATE = "include_global_state";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SNAPSHOT, snapshot.getSnapshotId().getName());
        builder.field(REPOSITORY, snapshot.getRepository());
        builder.field(UUID, snapshot.getSnapshotId().getUUID());
        builder.field(STATE, state.name());
        if (includeGlobalState != null) {
            builder.field(INCLUDE_GLOBAL_STATE, includeGlobalState);
        }
        builder.field(SnapshotShardsStats.Fields.SHARDS_STATS, shardsStats, params);
        builder.field(SnapshotStats.Fields.STATS, stats, params);
        builder.startObject(INDICES);
        for (SnapshotIndexStatus indexStatus : getIndices().values()) {
            indexStatus.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    static final ConstructingObjectParser<SnapshotStatus, Void> PARSER = new ConstructingObjectParser<>(
        "snapshot_status",
        true,
        (Object[] parsedObjects) -> {
            int i = 0;
            String name = (String) parsedObjects[i++];
            String repository = (String) parsedObjects[i++];
            String uuid = (String) parsedObjects[i++];
            String rawState = (String) parsedObjects[i++];
            Boolean includeGlobalState = (Boolean) parsedObjects[i++];
            SnapshotStats stats = ((SnapshotStats) parsedObjects[i++]);
            SnapshotShardsStats shardsStats = ((SnapshotShardsStats) parsedObjects[i++]);
            @SuppressWarnings("unchecked")
            List<SnapshotIndexStatus> indices = ((List<SnapshotIndexStatus>) parsedObjects[i]);

            Snapshot snapshot = new Snapshot(repository, new SnapshotId(name, uuid));
            SnapshotsInProgress.State state = SnapshotsInProgress.State.valueOf(rawState);
            Map<String, SnapshotIndexStatus> indicesStatus;
            List<SnapshotIndexShardStatus> shards;
            if (indices == null || indices.isEmpty()) {
                indicesStatus = emptyMap();
                shards = emptyList();
            } else {
                indicesStatus = new HashMap<>(indices.size());
                shards = new ArrayList<>();
                for (SnapshotIndexStatus index : indices) {
                    indicesStatus.put(index.getIndex(), index);
                    shards.addAll(index.getShards().values());
                }
            }
            return new SnapshotStatus(snapshot, state, shards, indicesStatus, shardsStats, stats, includeGlobalState);
        }
    );
    static {
        PARSER.declareString(constructorArg(), new ParseField(SNAPSHOT));
        PARSER.declareString(constructorArg(), new ParseField(REPOSITORY));
        PARSER.declareString(constructorArg(), new ParseField(UUID));
        PARSER.declareString(constructorArg(), new ParseField(STATE));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(INCLUDE_GLOBAL_STATE));
        PARSER.declareField(
            constructorArg(),
            SnapshotStats::fromXContent,
            new ParseField(SnapshotStats.Fields.STATS),
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareObject(constructorArg(), SnapshotShardsStats.PARSER, new ParseField(SnapshotShardsStats.Fields.SHARDS_STATS));
        PARSER.declareNamedObjects(constructorArg(), SnapshotIndexStatus.PARSER, new ParseField(INDICES));
    }

    public static SnapshotStatus fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
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
