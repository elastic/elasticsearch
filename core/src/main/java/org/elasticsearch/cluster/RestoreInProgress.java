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
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Meta data about restore processes that are currently executing
 */
public class RestoreInProgress extends AbstractDiffable<Custom> implements Custom {

    public static final String TYPE = "restore";

    public static final RestoreInProgress PROTO = new RestoreInProgress();

    private final List<Entry> entries;

    /**
     * Constructs new restore metadata
     *
     * @param entries list of currently running restore processes
     */
    public RestoreInProgress(List<Entry> entries) {
        this.entries = entries;
    }

    /**
     * Constructs new restore metadata
     *
     * @param entries list of currently running restore processes
     */
    public RestoreInProgress(Entry... entries) {
        this.entries = Arrays.asList(entries);
    }

    /**
     * Returns list of currently running restore processes
     *
     * @return list of currently running restore processes
     */
    public List<Entry> entries() {
        return this.entries;
    }

    /**
     * Returns currently running restore process with corresponding snapshot id or null if this snapshot is not being
     * restored
     *
     * @param snapshotId snapshot id
     * @return restore metadata or null
     */
    public Entry snapshot(SnapshotId snapshotId) {
        for (Entry entry : entries) {
            if (snapshotId.equals(entry.snapshotId())) {
                return entry;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RestoreInProgress that = (RestoreInProgress) o;

        if (!entries.equals(that.entries)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    /**
     * Restore metadata
     */
    public static class Entry {
        private final State state;
        private final SnapshotId snapshotId;
        private final ImmutableMap<ShardId, ShardRestoreStatus> shards;
        private final List<String> indices;

        /**
         * Creates new restore metadata
         *
         * @param snapshotId snapshot id
         * @param state      current state of the restore process
         * @param indices    list of indices being restored
         * @param shards     list of shards being restored and thier current restore status
         */
        public Entry(SnapshotId snapshotId, State state, List<String> indices, ImmutableMap<ShardId, ShardRestoreStatus> shards) {
            this.snapshotId = snapshotId;
            this.state = state;
            this.indices = indices;
            if (shards == null) {
                this.shards = ImmutableMap.of();
            } else {
                this.shards = shards;
            }
        }

        /**
         * Returns snapshot id
         *
         * @return snapshot id
         */
        public SnapshotId snapshotId() {
            return this.snapshotId;
        }

        /**
         * Returns list of shards that being restore and their status
         *
         * @return list of shards
         */
        public ImmutableMap<ShardId, ShardRestoreStatus> shards() {
            return this.shards;
        }

        /**
         * Returns current restore state
         *
         * @return restore state
         */
        public State state() {
            return state;
        }

        /**
         * Returns list of indices
         *
         * @return list of indices
         */
        public List<String> indices() {
            return indices;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (!indices.equals(entry.indices)) return false;
            if (!snapshotId.equals(entry.snapshotId)) return false;
            if (!shards.equals(entry.shards)) return false;
            if (state != entry.state) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = state.hashCode();
            result = 31 * result + snapshotId.hashCode();
            result = 31 * result + shards.hashCode();
            result = 31 * result + indices.hashCode();
            return result;
        }
    }

    /**
     * Represents status of a restored shard
     */
    public static class ShardRestoreStatus {
        private State state;
        private String nodeId;
        private String reason;

        private ShardRestoreStatus() {
        }

        /**
         * Constructs a new shard restore status in initializing state on the given node
         *
         * @param nodeId node id
         */
        public ShardRestoreStatus(String nodeId) {
            this(nodeId, State.INIT);
        }

        /**
         * Constructs a new shard restore status in with specified state on the given node
         *
         * @param nodeId node id
         * @param state  restore state
         */
        public ShardRestoreStatus(String nodeId, State state) {
            this(nodeId, state, null);
        }

        /**
         * Constructs a new shard restore status in with specified state on the given node with specified failure reason
         *
         * @param nodeId node id
         * @param state  restore state
         * @param reason failure reason
         */
        public ShardRestoreStatus(String nodeId, State state, String reason) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
        }

        /**
         * Returns current state
         *
         * @return current state
         */
        public State state() {
            return state;
        }

        /**
         * Returns node id of the node where shared is getting restored
         *
         * @return node id
         */
        public String nodeId() {
            return nodeId;
        }

        /**
         * Returns failure reason
         *
         * @return failure reason
         */
        public String reason() {
            return reason;
        }

        /**
         * Reads restore status from stream input
         *
         * @param in stream input
         * @return restore status
         * @throws IOException
         */
        public static ShardRestoreStatus readShardRestoreStatus(StreamInput in) throws IOException {
            ShardRestoreStatus shardSnapshotStatus = new ShardRestoreStatus();
            shardSnapshotStatus.readFrom(in);
            return shardSnapshotStatus;
        }

        /**
         * Reads restore status from stream input
         *
         * @param in stream input
         * @throws IOException
         */
        public void readFrom(StreamInput in) throws IOException {
            nodeId = in.readOptionalString();
            state = State.fromValue(in.readByte());
            reason = in.readOptionalString();
        }

        /**
         * Writes restore status to stream output
         *
         * @param out stream input
         * @throws IOException
         */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ShardRestoreStatus status = (ShardRestoreStatus) o;

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

    /**
     * Shard restore process state
     */
    public static enum State {
        /**
         * Initializing state
         */
        INIT((byte) 0),
        /**
         * Started state
         */
        STARTED((byte) 1),
        /**
         * Restore finished successfully
         */
        SUCCESS((byte) 2),
        /**
         * Restore failed
         */
        FAILURE((byte) 3);

        private byte value;

        /**
         * Constructs new state
         *
         * @param value state code
         */
        State(byte value) {
            this.value = value;
        }

        /**
         * Returns state code
         *
         * @return state code
         */
        public byte value() {
            return value;
        }

        /**
         * Returns true if restore process completed (either successfully or with failure)
         *
         * @return true if restore process completed
         */
        public boolean completed() {
            return this == SUCCESS || this == FAILURE;
        }

        /**
         * Returns state corresponding to state code
         *
         * @param value stat code
         * @return state
         */
        public static State fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return STARTED;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILURE;
                default:
                    throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String type() {
        return TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RestoreInProgress readFrom(StreamInput in) throws IOException {
        Entry[] entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            SnapshotId snapshotId = SnapshotId.readSnapshotId(in);
            State state = State.fromValue(in.readByte());
            int indices = in.readVInt();
            List<String> indexBuilder = new ArrayList<>();
            for (int j = 0; j < indices; j++) {
                indexBuilder.add(in.readString());
            }
            ImmutableMap.Builder<ShardId, ShardRestoreStatus> builder = ImmutableMap.<ShardId, ShardRestoreStatus>builder();
            int shards = in.readVInt();
            for (int j = 0; j < shards; j++) {
                ShardId shardId = ShardId.readShardId(in);
                ShardRestoreStatus shardState = ShardRestoreStatus.readShardRestoreStatus(in);
                builder.put(shardId, shardState);
            }
            entries[i] = new Entry(snapshotId, state, Collections.unmodifiableList(indexBuilder), builder.build());
        }
        return new RestoreInProgress(entries);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            entry.snapshotId().writeTo(out);
            out.writeByte(entry.state().value());
            out.writeVInt(entry.indices().size());
            for (String index : entry.indices()) {
                out.writeString(index);
            }
            out.writeVInt(entry.shards().size());
            for (Map.Entry<ShardId, ShardRestoreStatus> shardEntry : entry.shards().entrySet()) {
                shardEntry.getKey().writeTo(out);
                shardEntry.getValue().writeTo(out);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("snapshots");
        for (Entry entry : entries) {
            toXContent(entry, builder, params);
        }
        builder.endArray();
        return builder;
    }

    /**
     * Serializes single restore operation
     *
     * @param entry   restore operation metadata
     * @param builder XContent builder
     * @param params  serialization parameters
     * @throws IOException
     */
    public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("snapshot", entry.snapshotId().getSnapshot());
        builder.field("repository", entry.snapshotId().getRepository());
        builder.field("state", entry.state());
        builder.startArray("indices");
        {
            for (String index : entry.indices()) {
                builder.value(index);
            }
        }
        builder.endArray();
        builder.startArray("shards");
        {
            for (Map.Entry<ShardId, ShardRestoreStatus> shardEntry : entry.shards.entrySet()) {
                ShardId shardId = shardEntry.getKey();
                ShardRestoreStatus status = shardEntry.getValue();
                builder.startObject();
                {
                    builder.field("index", shardId.getIndex());
                    builder.field("shard", shardId.getId());
                    builder.field("state", status.state());
                }
                builder.endObject();
            }
        }

        builder.endArray();
        builder.endObject();
    }
}
