/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.snapshots.SnapshotInfo.METADATA_FIELD_INTRODUCED;

/**
 * Meta data about snapshots that are currently executing
 */
public class SnapshotsInProgress extends AbstractNamedDiffable<Custom> implements Custom {
    public static final String TYPE = "snapshots";

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return entries.equals(((SnapshotsInProgress) o).entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("SnapshotsInProgress[");
        for (int i = 0; i < entries.size(); i++) {
            builder.append(entries.get(i).snapshot().getSnapshotId().getName());
            if (i + 1 < entries.size()) {
                builder.append(",");
            }
        }
        return builder.append("]").toString();
    }

    public static class Entry {
        private final State state;
        private final Snapshot snapshot;
        private final boolean includeGlobalState;
        private final boolean partial;
        private final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;
        private final List<IndexId> indices;
        private final ImmutableOpenMap<String, List<ShardId>> waitingIndices;
        private final long startTime;
        private final long repositoryStateId;
        @Nullable private final Map<String, Object> userMetadata;
        @Nullable private final String failure;

        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, List<IndexId> indices,
                     long startTime, long repositoryStateId, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards,
                     String failure, Map<String, Object> userMetadata) {
            this.state = state;
            this.snapshot = snapshot;
            this.includeGlobalState = includeGlobalState;
            this.partial = partial;
            this.indices = indices;
            this.startTime = startTime;
            if (shards == null) {
                this.shards = ImmutableOpenMap.of();
                this.waitingIndices = ImmutableOpenMap.of();
            } else {
                this.shards = shards;
                this.waitingIndices = findWaitingIndices(shards);
            }
            this.repositoryStateId = repositoryStateId;
            this.failure = failure;
            this.userMetadata = userMetadata;
        }

        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, List<IndexId> indices,
                     long startTime, long repositoryStateId, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards,
                     Map<String, Object> userMetadata) {
            this(snapshot, includeGlobalState, partial, state, indices, startTime, repositoryStateId, shards, null, userMetadata);
        }

        public Entry(Entry entry, State state, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            this(entry.snapshot, entry.includeGlobalState, entry.partial, state, entry.indices, entry.startTime,
                entry.repositoryStateId, shards, entry.failure, entry.userMetadata);
        }

        public Entry(Entry entry, State state, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, String failure) {
            this(entry.snapshot, entry.includeGlobalState, entry.partial, state, entry.indices, entry.startTime,
                 entry.repositoryStateId, shards, failure, entry.userMetadata);
        }

        public Entry(Entry entry, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            this(entry, entry.state, shards, entry.failure);
        }

        public Snapshot snapshot() {
            return this.snapshot;
        }

        public ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards() {
            return this.shards;
        }

        public State state() {
            return state;
        }

        public List<IndexId> indices() {
            return indices;
        }

        public ImmutableOpenMap<String, List<ShardId>> waitingIndices() {
            return waitingIndices;
        }

        public boolean includeGlobalState() {
            return includeGlobalState;
        }

        public Map<String, Object> userMetadata() {
            return userMetadata;
        }

        public boolean partial() {
            return partial;
        }

        public long startTime() {
            return startTime;
        }

        public long getRepositoryStateId() {
            return repositoryStateId;
        }

        public String failure() {
            return failure;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (includeGlobalState != entry.includeGlobalState) return false;
            if (partial != entry.partial) return false;
            if (startTime != entry.startTime) return false;
            if (!indices.equals(entry.indices)) return false;
            if (!shards.equals(entry.shards)) return false;
            if (!snapshot.equals(entry.snapshot)) return false;
            if (state != entry.state) return false;
            if (!waitingIndices.equals(entry.waitingIndices)) return false;
            if (repositoryStateId != entry.repositoryStateId) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = state.hashCode();
            result = 31 * result + snapshot.hashCode();
            result = 31 * result + (includeGlobalState ? 1 : 0);
            result = 31 * result + (partial ? 1 : 0);
            result = 31 * result + shards.hashCode();
            result = 31 * result + indices.hashCode();
            result = 31 * result + waitingIndices.hashCode();
            result = 31 * result + Long.hashCode(startTime);
            result = 31 * result + Long.hashCode(repositoryStateId);
            return result;
        }

        @Override
        public String toString() {
            return snapshot.toString();
        }

        private ImmutableOpenMap<String, List<ShardId>> findWaitingIndices(ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            Map<String, List<ShardId>> waitingIndicesMap = new HashMap<>();
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> entry : shards) {
                if (entry.value.state() == ShardState.WAITING) {
                    waitingIndicesMap.computeIfAbsent(entry.key.getIndexName(), k -> new ArrayList<>()).add(entry.key);
                }
            }
            if (waitingIndicesMap.isEmpty()) {
                return ImmutableOpenMap.of();
            }
            ImmutableOpenMap.Builder<String, List<ShardId>> waitingIndicesBuilder = ImmutableOpenMap.builder();
            for (Map.Entry<String, List<ShardId>> entry : waitingIndicesMap.entrySet()) {
                waitingIndicesBuilder.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
            }
            return waitingIndicesBuilder.build();
        }
    }

    /**
     * Checks if all shards in the list have completed
     *
     * @param shards list of shard statuses
     * @return true if all shards have completed (either successfully or failed), false otherwise
     */
    public static boolean completed(ObjectContainer<ShardSnapshotStatus> shards) {
        for (ObjectCursor<ShardSnapshotStatus> status : shards) {
            if (status.value.state().completed == false) {
                return false;
            }
        }
        return true;
    }

    public static class ShardSnapshotStatus {
        private final ShardState state;
        private final String nodeId;
        private final String reason;

        public ShardSnapshotStatus(String nodeId) {
            this(nodeId, ShardState.INIT);
        }

        public ShardSnapshotStatus(String nodeId, ShardState state) {
            this(nodeId, state, null);
        }

        public ShardSnapshotStatus(String nodeId, ShardState state, String reason) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
            // If the state is failed we have to have a reason for this failure
            assert state.failed() == false || reason != null;
        }

        public ShardSnapshotStatus(StreamInput in) throws IOException {
            nodeId = in.readOptionalString();
            state = ShardState.fromValue(in.readByte());
            reason = in.readOptionalString();
        }

        public ShardState state() {
            return state;
        }

        public String nodeId() {
            return nodeId;
        }

        public String reason() {
            return reason;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardSnapshotStatus status = (ShardSnapshotStatus) o;
            return Objects.equals(nodeId, status.nodeId) && Objects.equals(reason, status.reason) && state == status.state;

        }

        @Override
        public int hashCode() {
            int result = state != null ? state.hashCode() : 0;
            result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
            result = 31 * result + (reason != null ? reason.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ShardSnapshotStatus[state=" + state + ", nodeId=" + nodeId + ", reason=" + reason + "]";
        }
    }

    public enum State {
        INIT((byte) 0, false),
        STARTED((byte) 1, false),
        SUCCESS((byte) 2, true),
        FAILED((byte) 3, true),
        ABORTED((byte) 4, false),
        MISSING((byte) 5, true),
        WAITING((byte) 6, false);

        private final byte value;

        private final boolean completed;

        State(byte value, boolean completed) {
            this.value = value;
            this.completed = completed;
        }

        public byte value() {
            return value;
        }

        public boolean completed() {
            return completed;
        }

        public static State fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return STARTED;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILED;
                case 4:
                    return ABORTED;
                case 5:
                    return MISSING;
                case 6:
                    return WAITING;
                default:
                    throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
            }
        }
    }

    private final List<Entry> entries;

    public SnapshotsInProgress(List<Entry> entries) {
        this.entries = entries;
    }

    public SnapshotsInProgress(Entry... entries) {
        this.entries = Arrays.asList(entries);
    }

    public List<Entry> entries() {
        return this.entries;
    }

    public Entry snapshot(final Snapshot snapshot) {
        for (Entry entry : entries) {
            final Snapshot curr = entry.snapshot();
            if (curr.equals(snapshot)) {
                return entry;
            }
        }
        return null;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    public SnapshotsInProgress(StreamInput in) throws IOException {
        Entry[] entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            Snapshot snapshot = new Snapshot(in);
            boolean includeGlobalState = in.readBoolean();
            boolean partial = in.readBoolean();
            State state = State.fromValue(in.readByte());
            int indices = in.readVInt();
            List<IndexId> indexBuilder = new ArrayList<>();
            for (int j = 0; j < indices; j++) {
                indexBuilder.add(new IndexId(in.readString(), in.readString()));
            }
            long startTime = in.readLong();
            ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
            int shards = in.readVInt();
            for (int j = 0; j < shards; j++) {
                ShardId shardId = new ShardId(in);
                builder.put(shardId, new ShardSnapshotStatus(in));
            }
            long repositoryStateId = in.readLong();
            final String failure = in.readOptionalString();
            Map<String, Object> userMetadata = null;
            if (in.getVersion().onOrAfter(METADATA_FIELD_INTRODUCED)) {
                userMetadata = in.readMap();
            }
            entries[i] = new Entry(snapshot,
                                   includeGlobalState,
                                   partial,
                                   state,
                                   Collections.unmodifiableList(indexBuilder),
                                   startTime,
                                   repositoryStateId,
                                   builder.build(),
                                   failure,
                                   userMetadata
                );
        }
        this.entries = Arrays.asList(entries);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            entry.snapshot().writeTo(out);
            out.writeBoolean(entry.includeGlobalState());
            out.writeBoolean(entry.partial());
            out.writeByte(entry.state().value());
            out.writeVInt(entry.indices().size());
            for (IndexId index : entry.indices()) {
                index.writeTo(out);
            }
            out.writeLong(entry.startTime());
            out.writeVInt(entry.shards().size());
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : entry.shards()) {
                shardEntry.key.writeTo(out);
                shardEntry.value.writeTo(out);
            }
            out.writeLong(entry.repositoryStateId);
            out.writeOptionalString(entry.failure);
            if (out.getVersion().onOrAfter(METADATA_FIELD_INTRODUCED)) {
                out.writeMap(entry.userMetadata);
            }
        }
    }

    private static final String REPOSITORY = "repository";
    private static final String SNAPSHOTS = "snapshots";
    private static final String SNAPSHOT = "snapshot";
    private static final String UUID = "uuid";
    private static final String INCLUDE_GLOBAL_STATE = "include_global_state";
    private static final String PARTIAL = "partial";
    private static final String STATE = "state";
    private static final String INDICES = "indices";
    private static final String START_TIME_MILLIS = "start_time_millis";
    private static final String START_TIME = "start_time";
    private static final String REPOSITORY_STATE_ID = "repository_state_id";
    private static final String SHARDS = "shards";
    private static final String INDEX = "index";
    private static final String SHARD = "shard";
    private static final String NODE = "node";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray(SNAPSHOTS);
        for (Entry entry : entries) {
            toXContent(entry, builder, params);
        }
        builder.endArray();
        return builder;
    }

    public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(REPOSITORY, entry.snapshot().getRepository());
        builder.field(SNAPSHOT, entry.snapshot().getSnapshotId().getName());
        builder.field(UUID, entry.snapshot().getSnapshotId().getUUID());
        builder.field(INCLUDE_GLOBAL_STATE, entry.includeGlobalState());
        builder.field(PARTIAL, entry.partial());
        builder.field(STATE, entry.state());
        builder.startArray(INDICES);
        {
            for (IndexId index : entry.indices()) {
                index.toXContent(builder, params);
            }
        }
        builder.endArray();
        builder.humanReadableField(START_TIME_MILLIS, START_TIME, new TimeValue(entry.startTime()));
        builder.field(REPOSITORY_STATE_ID, entry.getRepositoryStateId());
        builder.startArray(SHARDS);
        {
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : entry.shards) {
                ShardId shardId = shardEntry.key;
                ShardSnapshotStatus status = shardEntry.value;
                builder.startObject();
                {
                    builder.field(INDEX, shardId.getIndex());
                    builder.field(SHARD, shardId.getId());
                    builder.field(STATE, status.state());
                    builder.field(NODE, status.nodeId());
                }
                builder.endObject();
            }
        }
        builder.endArray();
        builder.endObject();
    }

    public enum ShardState {
        INIT((byte) 0, false, false),
        SUCCESS((byte) 2, true, false),
        FAILED((byte) 3, true, true),
        ABORTED((byte) 4, false, true),
        MISSING((byte) 5, true, true),
        WAITING((byte) 6, false, false);

        private final byte value;

        private final boolean completed;

        private final boolean failed;

        ShardState(byte value, boolean completed, boolean failed) {
            this.value = value;
            this.completed = completed;
            this.failed = failed;
        }

        public boolean completed() {
            return completed;
        }

        public boolean failed() {
            return failed;
        }

        public static ShardState fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILED;
                case 4:
                    return ABORTED;
                case 5:
                    return MISSING;
                case 6:
                    return WAITING;
                default:
                    throw new IllegalArgumentException("No shard snapshot state for value [" + value + "]");
            }
        }
    }
}
