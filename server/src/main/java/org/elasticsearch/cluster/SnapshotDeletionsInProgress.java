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
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A class that represents the snapshot deletions that are in progress in the cluster.
 */
public class SnapshotDeletionsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final String TYPE = "snapshot_deletions";

    // the list of snapshot deletion request entries
    private final List<Entry> entries;

    public SnapshotDeletionsInProgress() {
        this(Collections.emptyList());
    }

    public SnapshotDeletionsInProgress(List<Entry> entries) {
        this.entries = Collections.unmodifiableList(entries);
    }

    public SnapshotDeletionsInProgress(StreamInput in) throws IOException {
        this.entries = Collections.unmodifiableList(in.readList(Entry::new));
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} with the given
     * {@link Entry} added.
     */
    public static SnapshotDeletionsInProgress newInstance(Entry entry) {
        return new SnapshotDeletionsInProgress(Collections.singletonList(entry));
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} which adds
     * the given {@link Entry} to the invoking instance.
     */
    public SnapshotDeletionsInProgress withAddedEntry(Entry entry) {
        List<Entry> entries = new ArrayList<>(getEntries());
        entries.add(entry);
        return new SnapshotDeletionsInProgress(entries);
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} which removes
     * the given entry from the invoking instance.
     */
    public SnapshotDeletionsInProgress withRemovedEntry(Entry entry) {
        List<Entry> entries = new ArrayList<>(getEntries());
        entries.remove(entry);
        return new SnapshotDeletionsInProgress(entries);
    }

    /**
     * Returns an unmodifiable list of snapshot deletion entries.
     */
    public List<Entry> getEntries() {
        return entries;
    }

    /**
     * Returns {@code true} if there are snapshot deletions in progress in the cluster,
     * returns {@code false} otherwise.
     */
    public boolean hasDeletionsInProgress() {
        return entries.isEmpty() == false;
    }

    public Entry getEntry(final Snapshot snapshot) {
        for (Entry entry : entries) {
            final Snapshot curr = entry.getSnapshot();
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnapshotDeletionsInProgress that = (SnapshotDeletionsInProgress) o;
        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return 31 + entries.hashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TYPE);
        for (Entry entry : entries) {
            builder.startObject();
            {
                builder.field("repository", entry.snapshot.getRepository());
                builder.field("snapshot", entry.snapshot.getSnapshotId().getName());
                builder.humanReadableField("start_time_millis", "start_time", new TimeValue(entry.startTime));
                builder.field("repository_state_id", entry.repositoryStateId);
                builder.field("version", entry.version);
                builder.startArray("indices_to_cleanup");
                {
                    for (IndexId index : entry.indicesToCleanup) {
                        index.toXContent(builder, params);
                    }
                }
                builder.endArray();
                builder.startArray("shards");
                {
                    for (ObjectObjectCursor<Tuple<ShardId, String>, ShardSnapshotDeletionStatus> shardEntry : entry.shards) {
                        ShardId shardId = shardEntry.key.v1();
                        String snapshotIndexId = shardEntry.key.v2();
                        ShardSnapshotDeletionStatus status = shardEntry.value;
                        builder.startObject();
                        {
                            builder.field("index", shardId.getIndex());
                            builder.field("shard", shardId.getId());
                            builder.field("snapshot_index_id", snapshotIndexId);
                            builder.field("state", status.state());
                            builder.field("node", status.nodeId());
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("SnapshotDeletionsInProgress[");
        for (int i = 0; i < entries.size(); i++) {
            builder.append(entries.get(i).getSnapshot().getSnapshotId().getName());
            if (i + 1 < entries.size()) {
                builder.append(",");
            }
        }
        return builder.append("]").toString();
    }

    /**
     * A class representing a snapshot deletion request entry in the cluster state.
     */
    public static final class Entry implements Writeable {
        private final Snapshot snapshot;
        private final long startTime;
        private final long repositoryStateId;
        private final int version;
        private final List<IndexId> indicesToCleanup;
        private final ImmutableOpenMap<Tuple<ShardId, String>, ShardSnapshotDeletionStatus> shards;

        public Entry(Snapshot snapshot, long startTime, long repositoryStateId) {
            this(snapshot, startTime, repositoryStateId, 0, null, null);
        }

        public Entry(final Snapshot snapshot,
                     final long startTime,
                     final long repositoryStateId,
                     final int version,
                     final List<IndexId> indicesToCleanup,
                     final ImmutableOpenMap<Tuple<ShardId, String>, ShardSnapshotDeletionStatus> shards) {
            this.snapshot = snapshot;
            this.startTime = startTime;
            this.repositoryStateId = repositoryStateId;
            this.version = version;
            this.indicesToCleanup = indicesToCleanup == null ? Collections.emptyList() : indicesToCleanup;
            this.shards = shards == null ? ImmutableOpenMap.of() : shards;
        }

        public Entry(StreamInput in) throws IOException {
            this.snapshot = new Snapshot(in);
            this.startTime = in.readVLong();
            this.repositoryStateId = in.readLong();
            if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
                this.version = in.readInt();
                int indices = in.readVInt();
                List<IndexId> indexBuilder = new ArrayList<>();
                for (int i = 0; i < indices; i++) {
                    indexBuilder.add(new IndexId(in.readString(), in.readString()));
                }
                this.indicesToCleanup = indexBuilder;
                ImmutableOpenMap.Builder<Tuple<ShardId, String>, ShardSnapshotDeletionStatus> builder = ImmutableOpenMap.builder();
                int shards = in.readVInt();
                for (int i = 0; i < shards; i++) {
                    ShardId shardId = ShardId.readShardId(in);
                    String indexId = in.readString();
                    builder.put(new Tuple<>(shardId, indexId), new ShardSnapshotDeletionStatus(in));
                }
                this.shards = builder.build();
            } else {
                this.version = 0;
                this.indicesToCleanup = Collections.emptyList();
                this.shards = ImmutableOpenMap.of();
            }
        }

        public Entry(Entry entry, ImmutableOpenMap<Tuple<ShardId, String>, ShardSnapshotDeletionStatus> shards) {
            this(entry.snapshot, entry.startTime, entry.repositoryStateId, entry.version, entry.indicesToCleanup, shards);
        }

        /**
         * The snapshot to delete.
         */
        public Snapshot getSnapshot() {
            return snapshot;
        }

        /**
         * The start time in milliseconds for deleting the snapshots.
         */
        public long getStartTime() {
            return startTime;
        }

        public int getVersion() {
            return version;
        }

        /**
         * The repository state id at the time the snapshot deletion began.
         */
        public long getRepositoryStateId() {
            return repositoryStateId;
        }

        public List<IndexId> getIndicesToCleanup() {
            return indicesToCleanup;
        }

        public ImmutableOpenMap<Tuple<ShardId, String>, ShardSnapshotDeletionStatus> getShards() {
            return shards;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entry that = (Entry) o;
            return snapshot.equals(that.snapshot)
                       && startTime == that.startTime
                       && repositoryStateId == that.repositoryStateId
                       && version == that.version
                       && indicesToCleanup.equals(that.indicesToCleanup)
                       && shards.equals(that.shards);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, startTime, repositoryStateId, version, indicesToCleanup, shards);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            snapshot.writeTo(out);
            out.writeVLong(startTime);
            out.writeLong(repositoryStateId);
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                out.writeInt(version);
                out.writeVInt(indicesToCleanup.size());
                for (IndexId index : indicesToCleanup) {
                    index.writeTo(out);
                }
                out.writeVInt(shards.size());
                for (ObjectObjectCursor<Tuple<ShardId, String>, ShardSnapshotDeletionStatus> shardEntry : shards) {
                    shardEntry.key.v1().writeTo(out);
                    out.writeString(shardEntry.key.v2());
                    shardEntry.value.writeTo(out);
                }
            }
        }
    }

    /**
     * Checks if all shard deletions in the list have completed
     *
     * @param shards list of shard deletion statuses
     * @return true if all shard deletions have completed (either successfully or failed), false otherwise
     */
    public static boolean completed(ObjectContainer<ShardSnapshotDeletionStatus> shards) {
        for (ObjectCursor<ShardSnapshotDeletionStatus> status : shards) {
            if (status.value.state().completed() == false) {
                return false;
            }
        }
        return true;
    }

    public static class ShardSnapshotDeletionStatus {
        private final State state;
        private final String nodeId;
        private final String reason;

        public ShardSnapshotDeletionStatus(String nodeId, State state) {
            this(nodeId, state, null);
        }

        public ShardSnapshotDeletionStatus(String nodeId, State state, String reason) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
            // If the state is failed we have to have a reason for this failure
            assert state.failed() == false || reason != null;
        }

        public ShardSnapshotDeletionStatus(StreamInput in) throws IOException {
            nodeId = in.readOptionalString();
            state = State.fromValue(in.readByte());
            reason = in.readOptionalString();
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

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ShardSnapshotDeletionStatus status = (ShardSnapshotDeletionStatus) o;

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

        @Override
        public String toString() {
            return "ShardSnapshotDeletionStatus[state=" + state + ", nodeId=" + nodeId + ", reason=" + reason + "]";
        }
    }

    public enum State {
        INIT((byte) 0, false, false),
        STARTED((byte) 1, false, false),
        SUCCESS((byte) 2, true, false),
        FAILED((byte) 3, true, true);

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
                default:
                    throw new IllegalArgumentException("No snapshot deletion state for value [" + value + "]");
            }
        }
    }

}
