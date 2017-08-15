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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.common.collect.ImmutableOpenMap;
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
import java.util.Objects;

/**
 * Meta data about restore processes that are currently executing
 */
public class RestoreInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final String TYPE = "restore";

    private final List<Entry> entries;

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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("RestoreInProgress[");
        for (int i = 0; i < entries.size(); i++) {
            builder.append(entries.get(i).snapshot().getSnapshotId().getName());
            if (i + 1 < entries.size()) {
                builder.append(",");
            }
        }
        return builder.append("]").toString();
    }

    /**
     * Restore metadata
     */
    public static class Entry {
        private final State state;
        private final Snapshot snapshot;
        private final ImmutableOpenMap<ShardId, ShardRestoreStatus> shards;
        private final List<String> indices;

        /**
         * Creates new restore metadata
         *
         * @param snapshot   snapshot
         * @param state      current state of the restore process
         * @param indices    list of indices being restored
         * @param shards     map of shards being restored to their current restore status
         */
        public Entry(Snapshot snapshot, State state, List<String> indices, ImmutableOpenMap<ShardId, ShardRestoreStatus> shards) {
            this.snapshot = Objects.requireNonNull(snapshot);
            this.state = Objects.requireNonNull(state);
            this.indices = Objects.requireNonNull(indices);
            if (shards == null) {
                this.shards = ImmutableOpenMap.of();
            } else {
                this.shards = shards;
            }
        }

        /**
         * Returns snapshot
         *
         * @return snapshot
         */
        public Snapshot snapshot() {
            return this.snapshot;
        }

        /**
         * Returns list of shards that being restore and their status
         *
         * @return list of shards
         */
        public ImmutableOpenMap<ShardId, ShardRestoreStatus> shards() {
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
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            @SuppressWarnings("unchecked") Entry entry = (Entry) o;
            return snapshot.equals(entry.snapshot) &&
                       state == entry.state &&
                       indices.equals(entry.indices) &&
                       shards.equals(entry.shards);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, state, indices, shards);
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
         */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            @SuppressWarnings("unchecked") ShardRestoreStatus status = (ShardRestoreStatus) o;
            return state == status.state &&
                       Objects.equals(nodeId, status.nodeId) &&
                       Objects.equals(reason, status.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, nodeId, reason);
        }
    }

    /**
     * Shard restore process state
     */
    public enum State {
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
    public String getWriteableName() {
        return TYPE;
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    public RestoreInProgress(StreamInput in) throws IOException {
        Entry[] entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            Snapshot snapshot = new Snapshot(in);
            State state = State.fromValue(in.readByte());
            int indices = in.readVInt();
            List<String> indexBuilder = new ArrayList<>();
            for (int j = 0; j < indices; j++) {
                indexBuilder.add(in.readString());
            }
            ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> builder = ImmutableOpenMap.builder();
            int shards = in.readVInt();
            for (int j = 0; j < shards; j++) {
                ShardId shardId = ShardId.readShardId(in);
                ShardRestoreStatus shardState = ShardRestoreStatus.readShardRestoreStatus(in);
                builder.put(shardId, shardState);
            }
            entries[i] = new Entry(snapshot, state, Collections.unmodifiableList(indexBuilder), builder.build());
        }
        this.entries = Arrays.asList(entries);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            entry.snapshot().writeTo(out);
            out.writeByte(entry.state().value());
            out.writeVInt(entry.indices().size());
            for (String index : entry.indices()) {
                out.writeString(index);
            }
            out.writeVInt(entry.shards().size());
            for (ObjectObjectCursor<ShardId, ShardRestoreStatus> shardEntry : entry.shards()) {
                shardEntry.key.writeTo(out);
                shardEntry.value.writeTo(out);
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
     */
    public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("snapshot", entry.snapshot().getSnapshotId().getName());
        builder.field("repository", entry.snapshot().getRepository());
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
            for (ObjectObjectCursor<ShardId, ShardRestoreStatus> shardEntry : entry.shards) {
                ShardId shardId = shardEntry.key;
                ShardRestoreStatus status = shardEntry.value;
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
