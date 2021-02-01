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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.snapshots.InFlightShardSnapshotStates;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Meta data about snapshots that are currently executing
 */
public class SnapshotsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final SnapshotsInProgress EMPTY = new SnapshotsInProgress(List.of());

    public static final String TYPE = "snapshots";

    public static final String ABORTED_FAILURE_TEXT = "Snapshot was aborted by deletion";

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

    /**
     * Creates the initial {@link Entry} when starting a snapshot, if no shard-level snapshot work is to be done the resulting entry
     * will be in state {@link State#SUCCESS} right away otherwise it will be in state {@link State#STARTED}.
     */
    public static Entry startedEntry(Snapshot snapshot, boolean includeGlobalState, boolean partial, List<IndexId> indices,
                                     List<String> dataStreams, long startTime, long repositoryStateId,
                                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, Map<String, Object> userMetadata,
                                     Version version) {
        return new SnapshotsInProgress.Entry(snapshot, includeGlobalState, partial,
                completed(shards.values()) ? State.SUCCESS : State.STARTED,
                indices, dataStreams, startTime, repositoryStateId, shards, null, userMetadata, version);
    }

    /**
     * Creates the initial snapshot clone entry
     *
     * @param snapshot snapshot to clone into
     * @param source   snapshot to clone from
     * @param indices  indices to clone
     * @param startTime start time
     * @param repositoryStateId repository state id that this clone is based on
     * @param version repository metadata version to write
     * @return snapshot clone entry
     */
    public static Entry startClone(Snapshot snapshot, SnapshotId source, List<IndexId> indices, long startTime,
                                   long repositoryStateId, Version version) {
        return new SnapshotsInProgress.Entry(snapshot, true, false, State.STARTED, indices, Collections.emptyList(),
                startTime, repositoryStateId, ImmutableOpenMap.of(), null, Collections.emptyMap(), version, source,
                ImmutableOpenMap.of());
    }

    public static class Entry implements Writeable, ToXContent, RepositoryOperation {
        private final State state;
        private final Snapshot snapshot;
        private final boolean includeGlobalState;
        private final boolean partial;
        /**
         * Map of {@link ShardId} to {@link ShardSnapshotStatus} tracking the state of each shard snapshot operation.
         */
        private final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;
        private final List<IndexId> indices;
        private final List<String> dataStreams;
        private final long startTime;
        private final long repositoryStateId;
        // see #useShardGenerations
        private final Version version;

        /**
         * Source snapshot if this is a clone operation or {@code null} if this is a snapshot.
         */
        @Nullable
        private final SnapshotId source;

        /**
         * Map of {@link RepositoryShardId} to {@link ShardSnapshotStatus} tracking the state of each shard clone operation in this entry
         * the same way {@link #shards} tracks the status of each shard snapshot operation in non-clone entries.
         */
        private final ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> clones;

        @Nullable private final Map<String, Object> userMetadata;
        @Nullable private final String failure;

        // visible for testing, use #startedEntry and copy constructors in production code
        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, List<IndexId> indices,
                     List<String> dataStreams, long startTime, long repositoryStateId,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, String failure, Map<String, Object> userMetadata,
                     Version version) {
            this(snapshot, includeGlobalState, partial, state, indices, dataStreams, startTime, repositoryStateId, shards, failure,
                    userMetadata, version, null, ImmutableOpenMap.of());
        }

        private Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, List<IndexId> indices,
                     List<String> dataStreams, long startTime, long repositoryStateId,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, String failure, Map<String, Object> userMetadata,
                     Version version, @Nullable SnapshotId source,
                     @Nullable ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> clones) {
            this.state = state;
            this.snapshot = snapshot;
            this.includeGlobalState = includeGlobalState;
            this.partial = partial;
            this.indices = indices;
            this.dataStreams = dataStreams;
            this.startTime = startTime;
            this.shards = shards;
            this.repositoryStateId = repositoryStateId;
            this.failure = failure;
            this.userMetadata = userMetadata;
            this.version = version;
            this.source = source;
            if (source == null) {
                assert clones == null || clones.isEmpty() : "Provided [" + clones + "] but no source";
                this.clones = ImmutableOpenMap.of();
            } else {
                this.clones = clones;
            }
            assert assertShardsConsistent(this.source, this.state, this.indices, this.shards, this.clones);
        }

        private Entry(StreamInput in) throws IOException {
            snapshot = new Snapshot(in);
            includeGlobalState = in.readBoolean();
            partial = in.readBoolean();
            state = State.fromValue(in.readByte());
            indices = in.readList(IndexId::new);
            startTime = in.readLong();
            shards = in.readImmutableMap(ShardId::new, ShardSnapshotStatus::readFrom);
            repositoryStateId = in.readLong();
            failure = in.readOptionalString();
            userMetadata = in.readMap();
            version = Version.readVersion(in);
            dataStreams = in.readStringList();
            if (in.getVersion().onOrAfter(SnapshotsService.CLONE_SNAPSHOT_VERSION)) {
                source = in.readOptionalWriteable(SnapshotId::new);
                clones = in.readImmutableMap(RepositoryShardId::new, ShardSnapshotStatus::readFrom);
            } else {
                source = null;
                clones = ImmutableOpenMap.of();
            }
        }

        private static boolean assertShardsConsistent(SnapshotId source, State state, List<IndexId> indices,
                                                      ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards,
                                                      ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> clones) {
            if ((state == State.INIT || state == State.ABORTED) && shards.isEmpty()) {
                return true;
            }
            final Set<String> indexNames = indices.stream().map(IndexId::getName).collect(Collectors.toSet());
            final Set<String> indexNamesInShards = new HashSet<>();
            shards.iterator().forEachRemaining(s -> {
                indexNamesInShards.add(s.key.getIndexName());
                assert source == null || s.value.nodeId == null :
                        "Shard snapshot must not be assigned to data node when copying from snapshot [" + source + "]";
            });
            assert source == null || indexNames.isEmpty() == false : "No empty snapshot clones allowed";
            assert source != null || indexNames.equals(indexNamesInShards)
                : "Indices in shards " + indexNamesInShards + " differ from expected indices " + indexNames + " for state [" + state + "]";
            final boolean shardsCompleted = completed(shards.values()) && completed(clones.values());
            // Check state consistency for normal snapshots and started clone operations
            if (source == null || clones.isEmpty() == false) {
                assert (state.completed() && shardsCompleted) || (state.completed() == false && shardsCompleted == false)
                        : "Completed state must imply all shards completed but saw state [" + state + "] and shards " + shards;
            }
            if (source != null && state.completed()) {
                assert hasFailures(clones) == false || state == State.FAILED
                        : "Failed shard clones in [" + clones + "] but state was [" + state + "]";
            }
            return true;
        }

        public Entry withRepoGen(long newRepoGen) {
            assert newRepoGen > repositoryStateId : "Updated repository generation [" + newRepoGen
                    + "] must be higher than current generation [" + repositoryStateId + "]";
            return new Entry(snapshot, includeGlobalState, partial, state, indices, dataStreams, startTime, newRepoGen, shards, failure,
                    userMetadata, version, source, clones);
        }

        public Entry withClones(ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> updatedClones) {
            if (updatedClones.equals(clones)) {
                return this;
            }
            return new Entry(snapshot, includeGlobalState, partial,
                    completed(updatedClones.values()) ? (hasFailures(updatedClones) ? State.FAILED : State.SUCCESS) :
                            state, indices, dataStreams, startTime, repositoryStateId, shards, failure, userMetadata, version, source,
                    updatedClones);
        }

        /**
         * Create a new instance by aborting this instance. Moving all in-progress shards to {@link ShardState#ABORTED} if assigned to a
         * data node or to {@link ShardState#FAILED} if not assigned to any data node.
         * If the instance had no in-progress shard snapshots assigned to data nodes it's moved to state {@link State#SUCCESS}, otherwise
         * it's moved to state {@link State#ABORTED}.
         * In the special case where this instance has not yet made any progress on any shard this method just returns
         * {@code null} since no abort is needed and the snapshot can simply be removed from the cluster state outright.
         *
         * @return aborted snapshot entry or {@code null} if entry can be removed from the cluster state directly
         */
        @Nullable
        public Entry abort() {
            final ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shardsBuilder = ImmutableOpenMap.builder();
            boolean completed = true;
            boolean allQueued = true;
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : shards) {
                ShardSnapshotStatus status = shardEntry.value;
                allQueued &= status.state() == ShardState.QUEUED;
                if (status.state().completed() == false) {
                    final String nodeId = status.nodeId();
                    status = new ShardSnapshotStatus(nodeId, nodeId == null ? ShardState.FAILED : ShardState.ABORTED,
                            "aborted by snapshot deletion", status.generation());
                }
                completed &= status.state().completed();
                shardsBuilder.put(shardEntry.key, status);
            }
            if (allQueued) {
                return null;
            }
            return fail(shardsBuilder.build(), completed ? State.SUCCESS : State.ABORTED, ABORTED_FAILURE_TEXT);
        }

        public Entry fail(ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, State state, String failure) {
            return new Entry(snapshot, includeGlobalState, partial, state, indices, dataStreams, startTime, repositoryStateId, shards,
                    failure, userMetadata, version, source, clones);
        }

        /**
         * Create a new instance that has its shard assignments replaced by the given shard assignment map.
         * If the given shard assignments show all shard snapshots in a completed state then the returned instance will be of state
         * {@link State#SUCCESS}, otherwise the state remains unchanged.
         *
         * @param shards new shard snapshot states
         * @return new snapshot entry
         */
        public Entry withShardStates(ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            if (completed(shards.values())) {
                return new Entry(snapshot, includeGlobalState, partial, State.SUCCESS, indices, dataStreams, startTime, repositoryStateId,
                        shards, failure, userMetadata, version);
            }
            return withStartedShards(shards);
        }

        /**
         * Same as {@link #withShardStates} but does not check if the snapshot completed and thus is only to be used when starting new
         * shard snapshots on data nodes for a running snapshot.
         */
        public Entry withStartedShards(ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            final SnapshotsInProgress.Entry updated = new Entry(snapshot, includeGlobalState, partial, state, indices, dataStreams,
                    startTime, repositoryStateId, shards, failure, userMetadata, version);
            assert updated.state().completed() == false && completed(updated.shards().values()) == false
                    : "Only running snapshots allowed but saw [" + updated + "]";
            return updated;
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

        @Nullable
        public SnapshotId source() {
            return source;
        }

        public boolean isClone() {
            return source != null;
        }

        public ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> clones() {
            return clones;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (includeGlobalState != entry.includeGlobalState) return false;
            if (partial != entry.partial) return false;
            if (startTime != entry.startTime) return false;
            if (indices.equals(entry.indices) == false) return false;
            if (dataStreams.equals(entry.dataStreams) == false) return false;
            if (shards.equals(entry.shards) == false) return false;
            if (snapshot.equals(entry.snapshot) == false) return false;
            if (state != entry.state) return false;
            if (repositoryStateId != entry.repositoryStateId) return false;
            if (Objects.equals(failure, ((Entry) o).failure) == false) return false;
            if (Objects.equals(userMetadata, ((Entry) o).userMetadata) == false) return false;
            if (version.equals(entry.version) == false) return false;
            if (Objects.equals(source, ((Entry) o).source) == false) return false;
            if (clones.equals(((Entry) o).clones) == false) return false;

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
            result = 31 * result + dataStreams.hashCode();
            result = 31 * result + Long.hashCode(startTime);
            result = 31 * result + Long.hashCode(repositoryStateId);
            result = 31 * result + (failure == null ? 0 : failure.hashCode());
            result = 31 * result + (userMetadata == null ? 0 : userMetadata.hashCode());
            result = 31 * result + version.hashCode();
            result = 31 * result + (source == null ? 0 : source.hashCode());
            result = 31 * result + clones.hashCode();
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
            if (isClone()) {
                builder.field(SOURCE, source);
                builder.startArray(CLONES);
                {
                    for (ObjectObjectCursor<RepositoryShardId, ShardSnapshotStatus> shardEntry : clones) {
                        RepositoryShardId shardId = shardEntry.key;
                        ShardSnapshotStatus status = shardEntry.value;
                        builder.startObject();
                        {
                            builder.field(INDEX, shardId.index());
                            builder.field(SHARD, shardId.shardId());
                            builder.field(STATE, status.state());
                            builder.field(NODE, status.nodeId());
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.array(DATA_STREAMS, dataStreams.toArray(new String[0]));
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            snapshot.writeTo(out);
            out.writeBoolean(includeGlobalState);
            out.writeBoolean(partial);
            out.writeByte(state.value());
            out.writeList(indices);
            out.writeLong(startTime);
            out.writeMap(shards);
            out.writeLong(repositoryStateId);
            out.writeOptionalString(failure);
            out.writeMap(userMetadata);
            Version.writeVersion(version, out);
            out.writeStringCollection(dataStreams);
            if (out.getVersion().onOrAfter(SnapshotsService.CLONE_SNAPSHOT_VERSION)) {
                out.writeOptionalWriteable(source);
                out.writeMap(clones);
            }
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

    private static boolean hasFailures(ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> clones) {
        for (ObjectCursor<ShardSnapshotStatus> value : clones.values()) {
            if (value.value.state().failed()) {
                return true;
            }
        }
        return false;
    }

    public static class ShardSnapshotStatus implements Writeable {

        /**
         * Shard snapshot status for shards that are waiting for another operation to finish before they can be assigned to a node.
         */
        public static final ShardSnapshotStatus UNASSIGNED_QUEUED =
                new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.QUEUED, null);

        /**
         * Shard snapshot status for shards that could not be snapshotted because their index was deleted from before the shard snapshot
         * started.
         */
        public static final ShardSnapshotStatus MISSING =
                new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "missing index", null);

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
            assert state != ShardState.QUEUED || (nodeId == null && generation == null && reason == null) :
                    "Found unexpected non-null values for queued state shard nodeId[" + nodeId + "][" + generation + "][" + reason + "]";
            return true;
        }

        public static ShardSnapshotStatus readFrom(StreamInput in) throws IOException {
            String nodeId = in.readOptionalString();
            final ShardState state = ShardState.fromValue(in.readByte());
            final String generation = in.readOptionalString();
            final String reason = in.readOptionalString();
            if (state == ShardState.QUEUED) {
                return UNASSIGNED_QUEUED;
            }
            return new ShardSnapshotStatus(nodeId, state, reason, generation);
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(generation);
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
        final Map<String, Set<Tuple<String, Integer>>> assignedShardsByRepo = new HashMap<>();
        final Map<String, Set<Tuple<String, Integer>>> queuedShardsByRepo = new HashMap<>();
        for (Entry entry : entries) {
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                final ShardId sid = shard.key;
                assert assertShardStateConsistent(entries, assignedShardsByRepo, queuedShardsByRepo, entry, sid.getIndexName(), sid.id(),
                        shard.value);
            }
            for (ObjectObjectCursor<RepositoryShardId, ShardSnapshotStatus> shard : entry.clones()) {
                final RepositoryShardId sid = shard.key;
                assert assertShardStateConsistent(entries, assignedShardsByRepo, queuedShardsByRepo, entry, sid.indexName(), sid.shardId(),
                        shard.value);
            }
        }
        for (String repoName : assignedShardsByRepo.keySet()) {
            // make sure in-flight-shard-states can be built cleanly for the entries without tripping assertions
            InFlightShardSnapshotStates.forRepo(repoName, entries);
        }
        return true;
    }

    private static boolean assertShardStateConsistent(List<Entry> entries, Map<String, Set<Tuple<String, Integer>>> assignedShardsByRepo,
                                                      Map<String, Set<Tuple<String, Integer>>> queuedShardsByRepo, Entry entry,
                                                      String indexName, int shardId, ShardSnapshotStatus shardSnapshotStatus) {
        if (shardSnapshotStatus.isActive()) {
            Tuple<String, Integer> plainShardId = Tuple.tuple(indexName, shardId);
            assert assignedShardsByRepo.computeIfAbsent(entry.repository(), k -> new HashSet<>())
                    .add(plainShardId) : "Found duplicate shard assignments in " + entries;
            assert queuedShardsByRepo.getOrDefault(entry.repository(), Collections.emptySet()).contains(plainShardId) == false
                    : "Found active shard assignments after queued shard assignments in " + entries;
        } else if (shardSnapshotStatus.state() == ShardState.QUEUED) {
            queuedShardsByRepo.computeIfAbsent(entry.repository(), k -> new HashSet<>()).add(Tuple.tuple(indexName, shardId));
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
        this.entries = in.readList(SnapshotsInProgress.Entry::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
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
    private static final String SOURCE = "source";
    private static final String CLONES = "clones";
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
