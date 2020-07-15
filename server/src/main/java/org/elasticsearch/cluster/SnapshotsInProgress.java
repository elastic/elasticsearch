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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.snapshots.SnapshotInfo.METADATA_FIELD_INTRODUCED;

import static org.elasticsearch.snapshots.SnapshotInfo.DATA_STREAMS_IN_SNAPSHOT;

/**
 * Meta data about snapshots that are currently executing
 */
public class SnapshotsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final SnapshotsInProgress EMPTY = new SnapshotsInProgress(Collections.emptyList());

    private static final Version VERSION_IN_SNAPSHOT_VERSION = Version.V_7_7_0;

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

    public static class Entry implements ToXContent, RepositoryOperation {
        private final State state;
        private final Snapshot snapshot;
        private final boolean includeGlobalState;
        private final boolean partial;
        private final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;
        private final List<IndexId> indices;
        private final List<String> dataStreams;
        private final long startTime;
        private final long repositoryStateId;
        // see #useShardGenerations
        private final Version version;
        @Nullable private final Map<String, Object> userMetadata;
        @Nullable private final String failure;

        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, List<IndexId> indices,
                     List<String> dataStreams, long startTime, long repositoryStateId,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, String failure, Map<String, Object> userMetadata,
                     Version version) {
            this.state = state;
            this.snapshot = snapshot;
            this.includeGlobalState = includeGlobalState;
            this.partial = partial;
            this.indices = indices;
            this.dataStreams = dataStreams;
            this.startTime = startTime;
            this.shards = shards;
            assert assertShardsConsistent(state, indices, shards);
            this.repositoryStateId = repositoryStateId;
            this.failure = failure;
            this.userMetadata = userMetadata;
            this.version = version;
        }

        private static boolean assertShardsConsistent(State state, List<IndexId> indices,
                                                      ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            if ((state == State.INIT || state == State.ABORTED) && shards.isEmpty()) {
                return true;
            }
            final Set<String> indexNames = indices.stream().map(IndexId::getName).collect(Collectors.toSet());
            final Set<String> indexNamesInShards = new HashSet<>();
            shards.keysIt().forEachRemaining(s -> indexNamesInShards.add(s.getIndexName()));
            assert indexNames.equals(indexNamesInShards)
                : "Indices in shards " + indexNamesInShards + " differ from expected indices " + indexNames + " for state [" + state + "]";
            return true;
        }

        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, List<IndexId> indices,
                     List<String> dataStreams, long startTime, long repositoryStateId,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, Map<String, Object> userMetadata, Version version) {
            this(snapshot, includeGlobalState, partial, state, indices, dataStreams, startTime, repositoryStateId, shards,
                null, userMetadata, version);
        }

        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, List<IndexId> indices,
                     long startTime, long repositoryStateId, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards,
                     Map<String, Object> userMetadata, Version version) {
            this(snapshot, includeGlobalState, partial, state, indices, Collections.emptyList(), startTime, repositoryStateId, shards,
                null, userMetadata, version);
        }

        public Entry(Entry entry, State state, List<IndexId> indices, long repositoryStateId,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, Version version, String failure) {
            this(entry.snapshot, entry.includeGlobalState, entry.partial, state, indices, entry.dataStreams, entry.startTime,
                repositoryStateId, shards, failure, entry.userMetadata, version);
        }

        public Entry(Entry entry, State state, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            this(entry.snapshot, entry.includeGlobalState, entry.partial, state, entry.indices, entry.dataStreams, entry.startTime,
                entry.repositoryStateId, shards, entry.failure, entry.userMetadata, entry.version);
        }

        public Entry(Entry entry, State state, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, String failure) {
            this(entry.snapshot, entry.includeGlobalState, entry.partial, state, entry.indices, entry.dataStreams, entry.startTime,
                 entry.repositoryStateId, shards, failure, entry.userMetadata, entry.version);
        }

        public Entry(Entry entry, ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            this(entry, entry.state, shards, entry.failure);
        }

        public Entry withRepoGen(long newRepoGen) {
            assert newRepoGen > repositoryStateId : "Updated repository generation [" + newRepoGen
                    + "] must be higher than current generation [" + repositoryStateId + "]";
            return new Entry(snapshot, includeGlobalState, partial, state, indices, dataStreams, startTime, newRepoGen, shards, failure,
                    userMetadata, version);
        }

        public Entry withShards(ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            return new Entry(snapshot, includeGlobalState, partial, state, indices, dataStreams, startTime, repositoryStateId, shards,
                    failure, userMetadata, version);
        }

        @Override
        public String repository() {
            return snapshot.getRepository();
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

        public List<String> dataStreams() {
            return dataStreams;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }

        public String failure() {
            return failure;
        }

        /**
         * What version of metadata to use for the snapshot in the repository
         */
        public Version version() {
            return version;
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
            if (repositoryStateId != entry.repositoryStateId) return false;
            if (version.equals(entry.version) == false) return false;

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
            result = 31 * result + Long.hashCode(startTime);
            result = 31 * result + Long.hashCode(repositoryStateId);
            result = 31 * result + version.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(REPOSITORY, snapshot.getRepository());
            builder.field(SNAPSHOT, snapshot.getSnapshotId().getName());
            builder.field(UUID, snapshot.getSnapshotId().getUUID());
            builder.field(INCLUDE_GLOBAL_STATE, includeGlobalState());
            builder.field(PARTIAL, partial);
            builder.field(STATE, state);
            builder.startArray(INDICES);
            {
                for (IndexId index : indices) {
                    index.toXContent(builder, params);
                }
            }
            builder.endArray();
            builder.humanReadableField(START_TIME_MILLIS, START_TIME, new TimeValue(startTime));
            builder.field(REPOSITORY_STATE_ID, repositoryStateId);
            builder.startArray(SHARDS);
            {
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : shards) {
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
            builder.array(DATA_STREAMS, dataStreams.toArray(new String[0]));
            builder.endObject();
            return builder;
        }

        @Override
        public boolean isFragment() {
            return false;
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

        /**
         * Shard snapshot status for shards that are waiting for another operation to finish before they can be assigned to a node.
         */
        public static final ShardSnapshotStatus UNASSIGNED_QUEUED =
                new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.QUEUED, null);

        private final ShardState state;

        @Nullable
        private final String nodeId;

        @Nullable
        private final String generation;

        @Nullable
        private final String reason;

        public ShardSnapshotStatus(String nodeId, String generation) {
            this(nodeId, ShardState.INIT, generation);
        }

        public ShardSnapshotStatus(@Nullable String nodeId, ShardState state, @Nullable String generation) {
            this(nodeId, state, null, generation);
        }

        public ShardSnapshotStatus(@Nullable String nodeId, ShardState state, String reason, @Nullable String generation) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
            this.generation = generation;
            assert assertConsistent();
        }

        private boolean assertConsistent() {
            // If the state is failed we have to have a reason for this failure
            assert state.failed() == false || reason != null;
            assert (state != ShardState.INIT && state != ShardState.WAITING) || nodeId != null : "Null node id for state [" + state + "]";
            return true;
        }

        public ShardSnapshotStatus(StreamInput in) throws IOException {
            nodeId = in.readOptionalString();
            state = ShardState.fromValue(in.readByte());
            if (SnapshotsService.useShardGenerations(in.getVersion())) {
                generation = in.readOptionalString();
            } else {
                generation = null;
            }
            reason = in.readOptionalString();
        }

        public ShardState state() {
            return state;
        }

        @Nullable
        public String nodeId() {
            return nodeId;
        }

        @Nullable
        public String generation() {
            return this.generation;
        }

        public String reason() {
            return reason;
        }

        /**
         * Checks if this shard snapshot is actively executing.
         * A shard is defined as actively executing if it either is in a state that may write to the repository
         * ({@link ShardState#INIT} or {@link ShardState#ABORTED}) or about to write to it in state {@link ShardState#WAITING}.
         */
        public boolean isActive() {
            return state == ShardState.INIT || state == ShardState.ABORTED || state == ShardState.WAITING;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            if (SnapshotsService.useShardGenerations(out.getVersion())) {
                out.writeOptionalString(generation);
            }
            out.writeOptionalString(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardSnapshotStatus status = (ShardSnapshotStatus) o;
            return Objects.equals(nodeId, status.nodeId) && Objects.equals(reason, status.reason)
                && Objects.equals(generation, status.generation) && state == status.state;
        }

        @Override
        public int hashCode() {
            int result = state != null ? state.hashCode() : 0;
            result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
            result = 31 * result + (reason != null ? reason.hashCode() : 0);
            result = 31 * result + (generation != null ? generation.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ShardSnapshotStatus[state=" + state + ", nodeId=" + nodeId + ", reason=" + reason + ", generation=" + generation + "]";
        }
    }

    public enum State {
        INIT((byte) 0, false),
        STARTED((byte) 1, false),
        SUCCESS((byte) 2, true),
        FAILED((byte) 3, true),
        ABORTED((byte) 4, false);

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
                default:
                    throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
            }
        }
    }

    private final List<Entry> entries;

    private static boolean assertConsistentEntries(List<Entry> entries) {
        final Map<String, Set<ShardId>> assignedShardsByRepo = new HashMap<>();
        for (Entry entry : entries) {
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                if (shard.value.isActive()) {
                    assert assignedShardsByRepo.computeIfAbsent(entry.repository(), k -> new HashSet<>()).add(shard.key) :
                            "Found duplicate shard assignments in " + entries;
                }
            }
        }
        return true;
    }

    public static SnapshotsInProgress of(List<Entry> entries) {
        if (entries.isEmpty()) {
            return EMPTY;
        }
        return new SnapshotsInProgress(Collections.unmodifiableList(entries));
    }

    private SnapshotsInProgress(List<Entry> entries) {
        this.entries = entries;
        assert assertConsistentEntries(entries);
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
            List<IndexId> indexBuilder = in.readList(IndexId::new);
            long startTime = in.readLong();
            int shards = in.readVInt();
            ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> builder = ImmutableOpenMap.builder(shards);
            for (int j = 0; j < shards; j++) {
                ShardId shardId = new ShardId(in);
                if (in.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
                    builder.put(shardId, new ShardSnapshotStatus(in));
                } else {
                    String nodeId = in.readOptionalString();
                    ShardState shardState = ShardState.fromValue(in.readByte());
                    // Workaround for https://github.com/elastic/elasticsearch/issues/25878
                    // Some old snapshot might still have null in shard failure reasons
                    String reason = shardState.failed() ? "" : null;
                    builder.put(shardId, new ShardSnapshotStatus(nodeId, shardState, reason));
                }
            }
            long repositoryStateId = in.readLong();
            final String failure;
            if (in.getVersion().onOrAfter(Version.V_6_7_0)) {
                failure = in.readOptionalString();
            } else {
                failure = null;
            }
            Map<String, Object> userMetadata = null;
            if (in.getVersion().onOrAfter(METADATA_FIELD_INTRODUCED)) {
                userMetadata = in.readMap();
            }
            final Version version;
            if (in.getVersion().onOrAfter(VERSION_IN_SNAPSHOT_VERSION)) {
                version = Version.readVersion(in);
            } else if (in.getVersion().onOrAfter(SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION)) {
                // If an older master informs us that shard generations are supported we use the minimum shard generation compatible
                // version. If shard generations are not supported yet we use a placeholder for a version that does not use shard
                // generations.
                version = in.readBoolean() ? SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION : SnapshotsService.OLD_SNAPSHOT_FORMAT;
            } else {
                version = SnapshotsService.OLD_SNAPSHOT_FORMAT;
            }

            List<String> dataStreams;
            if (in.getVersion().onOrAfter(DATA_STREAMS_IN_SNAPSHOT)) {
                dataStreams = in.readStringList();
            } else {
                dataStreams = Collections.emptyList();
            }
            entries[i] = new Entry(snapshot,
                                   includeGlobalState,
                                   partial,
                                   state,
                                   Collections.unmodifiableList(indexBuilder),
                                   dataStreams,
                                   startTime,
                                   repositoryStateId,
                                   builder.build(),
                                   failure,
                                   userMetadata,
                                   version
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
                if (out.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
                    shardEntry.value.writeTo(out);
                } else {
                    out.writeOptionalString(shardEntry.value.nodeId());
                    out.writeByte(shardEntry.value.state().value);
                }
            }
            out.writeLong(entry.repositoryStateId);
            if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
                out.writeOptionalString(entry.failure);
            }
            if (out.getVersion().onOrAfter(METADATA_FIELD_INTRODUCED)) {
                out.writeMap(entry.userMetadata);
            }
            if (out.getVersion().onOrAfter(VERSION_IN_SNAPSHOT_VERSION)) {
                Version.writeVersion(entry.version, out);
            } else if (out.getVersion().onOrAfter(SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION)) {
                out.writeBoolean(SnapshotsService.useShardGenerations(entry.version));
            }
            if (out.getVersion().onOrAfter(DATA_STREAMS_IN_SNAPSHOT)) {
                out.writeStringCollection(entry.dataStreams);
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
    private static final String DATA_STREAMS = "data_streams";
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
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public enum ShardState {
        INIT((byte) 0, false, false),
        SUCCESS((byte) 2, true, false),
        FAILED((byte) 3, true, true),
        ABORTED((byte) 4, false, true),
        MISSING((byte) 5, true, true),
        /**
         * Shard snapshot is waiting for the primary to snapshot to become available.
         */
        WAITING((byte) 6, false, false),
        /**
         * Shard snapshot is waiting for another shard snapshot for the same shard and to the same repository to finish.
         */
        QUEUED((byte) 7, false, false);

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
                case 7:
                    return QUEUED;
                default:
                    throw new IllegalArgumentException("No shard snapshot state for value [" + value + "]");
            }
        }
    }
}
