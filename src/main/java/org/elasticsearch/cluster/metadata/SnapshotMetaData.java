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

package org.elasticsearch.cluster.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Meta data about snapshots that are currently executing
 */
public class SnapshotMetaData implements MetaData.Custom {
    public static final String TYPE = "snapshots";

    public static final Factory FACTORY = new Factory();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotMetaData that = (SnapshotMetaData) o;

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
        private final ImmutableList<String> indices;
        private final ImmutableMap<String, ImmutableList<ShardId>> waitingIndices;
        private final long startTime;

        public Entry(SnapshotId snapshotId, boolean includeGlobalState, State state, ImmutableList<String> indices, long startTime, ImmutableMap<ShardId, ShardSnapshotStatus> shards) {
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

        public ImmutableList<String> indices() {
            return indices;
        }

        public ImmutableMap<String, ImmutableList<ShardId>> waitingIndices() {
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

        private ImmutableMap<String, ImmutableList<ShardId>> findWaitingIndices(ImmutableMap<ShardId, ShardSnapshotStatus> shards) {
            Map<String, ImmutableList.Builder<ShardId>> waitingIndicesMap = newHashMap();
            for (ImmutableMap.Entry<ShardId, ShardSnapshotStatus> entry : shards.entrySet()) {
                if (entry.getValue().state() == State.WAITING) {
                    ImmutableList.Builder<ShardId> waitingShards = waitingIndicesMap.get(entry.getKey().getIndex());
                    if (waitingShards == null) {
                        waitingShards = ImmutableList.builder();
                        waitingIndicesMap.put(entry.getKey().getIndex(), waitingShards);
                    }
                    waitingShards.add(entry.getKey());
                }
            }
            if (!waitingIndicesMap.isEmpty()) {
                ImmutableMap.Builder<String, ImmutableList<ShardId>> waitingIndicesBuilder = ImmutableMap.builder();
                for (Map.Entry<String, ImmutableList.Builder<ShardId>> entry : waitingIndicesMap.entrySet()) {
                    waitingIndicesBuilder.put(entry.getKey(), entry.getValue().build());
                }
                return waitingIndicesBuilder.build();
            } else {
                return ImmutableMap.of();
            }

        }

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
                    throw new ElasticsearchIllegalArgumentException("No snapshot state for value [" + value + "]");
            }
        }
    }

    private final ImmutableList<Entry> entries;


    public SnapshotMetaData(ImmutableList<Entry> entries) {
        this.entries = entries;
    }

    public SnapshotMetaData(Entry... entries) {
        this.entries = ImmutableList.copyOf(entries);
    }

    public ImmutableList<Entry> entries() {
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


    public static class Factory extends MetaData.Custom.Factory<SnapshotMetaData> {

        @Override
        public String type() {
            return TYPE;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public SnapshotMetaData readFrom(StreamInput in) throws IOException {
            Entry[] entries = new Entry[in.readVInt()];
            for (int i = 0; i < entries.length; i++) {
                SnapshotId snapshotId = SnapshotId.readSnapshotId(in);
                boolean includeGlobalState = in.readBoolean();
                State state = State.fromValue(in.readByte());
                int indices = in.readVInt();
                ImmutableList.Builder<String> indexBuilder = ImmutableList.builder();
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
                entries[i] = new Entry(snapshotId, includeGlobalState, state, indexBuilder.build(), startTime, builder.build());
            }
            return new SnapshotMetaData(entries);
        }

        @Override
        public void writeTo(SnapshotMetaData repositories, StreamOutput out) throws IOException {
            out.writeVInt(repositories.entries().size());
            for (Entry entry : repositories.entries()) {
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

        @Override
        public SnapshotMetaData fromXContent(XContentParser parser) throws IOException {
            throw new UnsupportedOperationException();
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
        public void toXContent(SnapshotMetaData customIndexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startArray(Fields.SNAPSHOTS);
            for (Entry entry : customIndexMetaData.entries()) {
                toXContent(entry, builder, params);
            }
            builder.endArray();
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


}
