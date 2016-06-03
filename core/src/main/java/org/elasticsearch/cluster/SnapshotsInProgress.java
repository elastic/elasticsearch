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
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Meta data about snapshots that are currently executing
 */
public class SnapshotsInProgress extends AbstractDiffable<Custom> implements Custom {
    public static final String TYPE = "snapshots";

    public static final SnapshotsInProgress PROTO = new SnapshotsInProgress();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotsInProgress that = (SnapshotsInProgress) o;

        if (!entries.equals(that.entries)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    public static class Entry {
        private final State state;
        private final Snapshot snapshot;
        private final boolean includeGlobalState;
        private final boolean partial;
        private final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;
        private final List<String> indices;
        private final ImmutableOpenMap<String, List<ShardId>> waitingIndices;
        private final long startTime;

        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, List<String> indices, long startTime,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
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
        }

        public Entry(Entry entry, State state, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            this(entry.snapshot, entry.includeGlobalState, entry.partial, state, entry.indices, entry.startTime, shards);
        }

        public Entry(Entry entry, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            this(entry, entry.state, shards);
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

        public List<String> indices() {
            return indices;
        }

        public ImmutableOpenMap<String, List<ShardId>> waitingIndices() {
            return waitingIndices;
        }

        public boolean includeGlobalState() {
            return includeGlobalState;
        }

        public boolean partial() {
            return partial;
        }

        public long startTime() {
            return startTime;
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
            return result;
        }

        @Override
        public String toString() {
            return snapshot.toString();
        }

        private ImmutableOpenMap<String, List<ShardId>> findWaitingIndices(ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            Map<String, List<ShardId>> waitingIndicesMap = new HashMap<>();
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> entry : shards) {
                if (entry.value.state() == State.WAITING) {
                    List<ShardId> waitingShards = waitingIndicesMap.get(entry.key.getIndex());
                    if (waitingShards == null) {
                        waitingShards = new ArrayList<>();
                        waitingIndicesMap.put(entry.key.getIndexName(), waitingShards);
                    }
                    waitingShards.add(entry.key);
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
            if (status.value.state().completed() == false) {
                return false;
            }
        }
        return true;
    }


    public static class ShardSnapshotStatus {
        private State state;
        private String nodeId;
        private String reason;

        private ShardSnapshotStatus() {
        }

        public ShardSnapshotStatus(String nodeId) {
            this(nodeId, State.INIT);
        }

        public ShardSnapshotStatus(String nodeId, State state) {
            this(nodeId, state, null);
        }

        public ShardSnapshotStatus(String nodeId, State state, String reason) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
        }

        public State state() {
            return state;
        }

        public String nodeId() {
            return nodeId;
        }

        public String reason() {
            return reason;
        }

        public static ShardSnapshotStatus readShardSnapshotStatus(StreamInput in) throws IOException {
            ShardSnapshotStatus shardSnapshotStatus = new ShardSnapshotStatus();
            shardSnapshotStatus.readFrom(in);
            return shardSnapshotStatus;
        }

        public void readFrom(StreamInput in) throws IOException {
            nodeId = in.readOptionalString();
            state = State.fromValue(in.readByte());
            reason = in.readOptionalString();
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

            if (nodeId != null ? !nodeId.equals(status.nodeId) : status.nodeId != null) return false;
            if (reason != null ? !reason.equals(status.reason) : status.reason != null) return false;
            if (state != status.state) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = state != null ? state.hashCode() : 0;
            result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
            result = 31 * result + (reason != null ? reason.hashCode() : 0);
            return result;
        }
    }

    public enum State {
        INIT((byte) 0, false, false),
        STARTED((byte) 1, false, false),
        SUCCESS((byte) 2, true, false),
        FAILED((byte) 3, true, true),
        ABORTED((byte) 4, false, true),
        MISSING((byte) 5, true, true),
        WAITING((byte) 6, false, false);

        private byte value;

        private boolean completed;

        private boolean failed;

        State(byte value, boolean completed, boolean failed) {
            this.value = value;
            this.completed = completed;
            this.failed = failed;
        }

        public byte value() {
            return value;
        }

        public boolean completed() {
            return completed;
        }

        public boolean failed() {
            return failed;
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
    public String type() {
        return TYPE;
    }

    @Override
    public SnapshotsInProgress readFrom(StreamInput in) throws IOException {
        Entry[] entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            Snapshot snapshot = new Snapshot(in);
            boolean includeGlobalState = in.readBoolean();
            boolean partial = in.readBoolean();
            State state = State.fromValue(in.readByte());
            int indices = in.readVInt();
            List<String> indexBuilder = new ArrayList<>();
            for (int j = 0; j < indices; j++) {
                indexBuilder.add(in.readString());
            }
            long startTime = in.readLong();
            ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
            int shards = in.readVInt();
            for (int j = 0; j < shards; j++) {
                ShardId shardId = ShardId.readShardId(in);
                String nodeId = in.readOptionalString();
                State shardState = State.fromValue(in.readByte());
                builder.put(shardId, new ShardSnapshotStatus(nodeId, shardState));
            }
            entries[i] = new Entry(snapshot,
                                   includeGlobalState,
                                   partial,
                                   state,
                                   Collections.unmodifiableList(indexBuilder),
                                   startTime,
                                   builder.build());
        }
        return new SnapshotsInProgress(entries);
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
            for (String index : entry.indices()) {
                out.writeString(index);
            }
            out.writeLong(entry.startTime());
            out.writeVInt(entry.shards().size());
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : entry.shards()) {
                shardEntry.key.writeTo(out);
                out.writeOptionalString(shardEntry.value.nodeId());
                out.writeByte(shardEntry.value.state().value());
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
            for (String index : entry.indices()) {
                builder.value(index);
            }
        }
        builder.endArray();
        builder.timeValueField(START_TIME_MILLIS, START_TIME, entry.startTime());
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
}
