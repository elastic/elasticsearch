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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
        private final SnapshotId snapshotId;
        private final boolean includeGlobalState;
        private final ImmutableMap<ShardId, ShardSnapshotStatus> shards;
        private final List<String> indices;
        private final ImmutableMap<String, List<ShardId>> waitingIndices;
        private final long startTime;

        public Entry(SnapshotId snapshotId, boolean includeGlobalState, State state, List<String> indices, long startTime, ImmutableMap<ShardId, ShardSnapshotStatus> shards) {
            this.state = state;
            this.snapshotId = snapshotId;
            this.includeGlobalState = includeGlobalState;
            this.indices = indices;
            this.startTime = startTime;
            if (shards == null) {
                this.shards = ImmutableMap.of();
                this.waitingIndices = ImmutableMap.of();
            } else {
                this.shards = shards;
                this.waitingIndices = findWaitingIndices(shards);
            }
        }

        public Entry(Entry entry, State state, ImmutableMap<ShardId, ShardSnapshotStatus> shards) {
            this(entry.snapshotId, entry.includeGlobalState, state, entry.indices, entry.startTime, shards);
        }

        public Entry(Entry entry, ImmutableMap<ShardId, ShardSnapshotStatus> shards) {
            this(entry, entry.state, shards);
        }

        public SnapshotId snapshotId() {
            return this.snapshotId;
        }

        public ImmutableMap<ShardId, ShardSnapshotStatus> shards() {
            return this.shards;
        }

        public State state() {
            return state;
        }

        public List<String> indices() {
            return indices;
        }

        public ImmutableMap<String, List<ShardId>> waitingIndices() {
            return waitingIndices;
        }

        public boolean includeGlobalState() {
            return includeGlobalState;
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
            if (startTime != entry.startTime) return false;
            if (!indices.equals(entry.indices)) return false;
            if (!shards.equals(entry.shards)) return false;
            if (!snapshotId.equals(entry.snapshotId)) return false;
            if (state != entry.state) return false;
            if (!waitingIndices.equals(entry.waitingIndices)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = state.hashCode();
            result = 31 * result + snapshotId.hashCode();
            result = 31 * result + (includeGlobalState ? 1 : 0);
            result = 31 * result + shards.hashCode();
            result = 31 * result + indices.hashCode();
            result = 31 * result + waitingIndices.hashCode();
            result = 31 * result + (int) (startTime ^ (startTime >>> 32));
            return result;
        }

        private ImmutableMap<String, List<ShardId>> findWaitingIndices(ImmutableMap<ShardId, ShardSnapshotStatus> shards) {
            Map<String, List<ShardId>> waitingIndicesMap = new HashMap<>();
            for (ImmutableMap.Entry<ShardId, ShardSnapshotStatus> entry : shards.entrySet()) {
                if (entry.getValue().state() == State.WAITING) {
                    List<ShardId> waitingShards = waitingIndicesMap.get(entry.getKey().getIndex());
                    if (waitingShards == null) {
                        waitingShards = new ArrayList<>();
                        waitingIndicesMap.put(entry.getKey().getIndex(), waitingShards);
                    }
                    waitingShards.add(entry.getKey());
                }
            }
            if (!waitingIndicesMap.isEmpty()) {
                ImmutableMap.Builder<String, List<ShardId>> waitingIndicesBuilder = ImmutableMap.builder();
                for (Map.Entry<String, List<ShardId>> entry : waitingIndicesMap.entrySet()) {
                    waitingIndicesBuilder.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
                }
                return waitingIndicesBuilder.build();
            } else {
                return ImmutableMap.of();
            }

        }

    }

    /**
     * Checks if all shards in the list have completed
     *
     * @param shards list of shard statuses
     * @return true if all shards have completed (either successfully or failed), false otherwise
     */
    public static boolean completed(Collection<ShardSnapshotStatus> shards) {
        for (ShardSnapshotStatus status : shards) {
            if (status.state().completed() == false) {
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

    public static enum State {
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

    public Entry snapshot(SnapshotId snapshotId) {
        for (Entry entry : entries) {
            if (snapshotId.equals(entry.snapshotId())) {
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
            SnapshotId snapshotId = SnapshotId.readSnapshotId(in);
            boolean includeGlobalState = in.readBoolean();
            State state = State.fromValue(in.readByte());
            int indices = in.readVInt();
            List<String> indexBuilder = new ArrayList<>();
            for (int j = 0; j < indices; j++) {
                indexBuilder.add(in.readString());
            }
            long startTime = in.readLong();
            ImmutableMap.Builder<ShardId, ShardSnapshotStatus> builder = ImmutableMap.builder();
            int shards = in.readVInt();
            for (int j = 0; j < shards; j++) {
                ShardId shardId = ShardId.readShardId(in);
                String nodeId = in.readOptionalString();
                State shardState = State.fromValue(in.readByte());
                builder.put(shardId, new ShardSnapshotStatus(nodeId, shardState));
            }
            entries[i] = new Entry(snapshotId, includeGlobalState, state, Collections.unmodifiableList(indexBuilder), startTime, builder.build());
        }
        return new SnapshotsInProgress(entries);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            entry.snapshotId().writeTo(out);
            out.writeBoolean(entry.includeGlobalState());
            out.writeByte(entry.state().value());
            out.writeVInt(entry.indices().size());
            for (String index : entry.indices()) {
                out.writeString(index);
            }
            out.writeLong(entry.startTime());
            out.writeVInt(entry.shards().size());
            for (Map.Entry<ShardId, ShardSnapshotStatus> shardEntry : entry.shards().entrySet()) {
                shardEntry.getKey().writeTo(out);
                out.writeOptionalString(shardEntry.getValue().nodeId());
                out.writeByte(shardEntry.getValue().state().value());
            }
        }
    }

    static final class Fields {
        static final XContentBuilderString REPOSITORY = new XContentBuilderString("repository");
        static final XContentBuilderString SNAPSHOTS = new XContentBuilderString("snapshots");
        static final XContentBuilderString SNAPSHOT = new XContentBuilderString("snapshot");
        static final XContentBuilderString INCLUDE_GLOBAL_STATE = new XContentBuilderString("include_global_state");
        static final XContentBuilderString STATE = new XContentBuilderString("state");
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString START_TIME_MILLIS = new XContentBuilderString("start_time_millis");
        static final XContentBuilderString START_TIME = new XContentBuilderString("start_time");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString SHARD = new XContentBuilderString("shard");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray(Fields.SNAPSHOTS);
        for (Entry entry : entries) {
            toXContent(entry, builder, params);
        }
        builder.endArray();
        return builder;
    }

    public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.REPOSITORY, entry.snapshotId().getRepository());
        builder.field(Fields.SNAPSHOT, entry.snapshotId().getSnapshot());
        builder.field(Fields.INCLUDE_GLOBAL_STATE, entry.includeGlobalState());
        builder.field(Fields.STATE, entry.state());
        builder.startArray(Fields.INDICES);
        {
            for (String index : entry.indices()) {
                builder.value(index);
            }
        }
        builder.endArray();
        builder.timeValueField(Fields.START_TIME_MILLIS, Fields.START_TIME, entry.startTime());
        builder.startArray(Fields.SHARDS);
        {
            for (Map.Entry<ShardId, ShardSnapshotStatus> shardEntry : entry.shards.entrySet()) {
                ShardId shardId = shardEntry.getKey();
                ShardSnapshotStatus status = shardEntry.getValue();
                builder.startObject();
                {
                    builder.field(Fields.INDEX, shardId.getIndex());
                    builder.field(Fields.SHARD, shardId.getId());
                    builder.field(Fields.STATE, status.state());
                    builder.field(Fields.NODE, status.nodeId());
                }
                builder.endObject();
            }
        }
        builder.endArray();
        builder.endObject();
    }
}
