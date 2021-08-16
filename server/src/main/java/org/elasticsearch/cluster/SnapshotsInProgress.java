/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.snapshots.InFlightShardSnapshotStates;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotFeatureInfo;
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

import static org.elasticsearch.snapshots.SnapshotInfo.METADATA_FIELD_INTRODUCED;
import static org.elasticsearch.snapshots.SnapshotInfo.DATA_STREAMS_IN_SNAPSHOT;
import static org.elasticsearch.snapshots.SnapshotsService.FEATURE_STATES_VERSION;

/**
 * Meta data about snapshots that are currently executing
 */
public class SnapshotsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final SnapshotsInProgress EMPTY = new SnapshotsInProgress(Collections.emptyList());

    private static final Version VERSION_IN_SNAPSHOT_VERSION = Version.V_7_7_0;

    public static final String TYPE = "snapshots";

    public static final String ABORTED_FAILURE_TEXT = "Snapshot was aborted by deletion";

    private final List<Entry> entries;

    public static SnapshotsInProgress of(List<Entry> entries) {
        if (entries.isEmpty()) {
            return EMPTY;
        }
        return new SnapshotsInProgress(Collections.unmodifiableList(entries));
    }

    public SnapshotsInProgress(StreamInput in) throws IOException {
        this(in.readList(SnapshotsInProgress.Entry::readFrom));
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("snapshots");
        for (Entry entry : entries) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

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
    public static Entry startedEntry(Snapshot snapshot, boolean includeGlobalState, boolean partial, Map<String, IndexId> indices,
                                     List<String> dataStreams, long startTime, long repositoryStateId,
                                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards,
                                     Map<String, Object> userMetadata, Version version, List<SnapshotFeatureInfo> featureStates) {
        return new SnapshotsInProgress.Entry(snapshot, includeGlobalState, partial,
                completed(shards.values()) ? State.SUCCESS : State.STARTED,
                indices, dataStreams, featureStates, startTime, repositoryStateId, shards, null, userMetadata, version);
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
    public static Entry startClone(Snapshot snapshot, SnapshotId source, Map<String, IndexId> indices, long startTime,
                                   long repositoryStateId, Version version) {
        return new SnapshotsInProgress.Entry(snapshot, true, false, State.STARTED, indices, Collections.emptyList(),
            Collections.emptyList(), startTime, repositoryStateId, ImmutableOpenMap.of(), null, Collections.emptyMap(), version,
            source, ImmutableOpenMap.of());
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

    private static boolean assertConsistentEntries(List<Entry> entries) {
        final Map<String, Set<Tuple<String, Integer>>> assignedShardsByRepo = new HashMap<>();
        final Map<String, Set<Tuple<String, Integer>>> queuedShardsByRepo = new HashMap<>();
        for (Entry entry : entries) {
            for (ObjectObjectCursor<RepositoryShardId, ShardSnapshotStatus> shard : entry.shardsByRepoShardId()) {
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
        private final ShardGeneration generation;

        @Nullable
        private final String reason;

        @Nullable // only present in state SUCCESS; may be null even in SUCCESS if this state came over the wire from an older node
        private final ShardSnapshotResult shardSnapshotResult;

        public ShardSnapshotStatus(String nodeId, ShardGeneration generation) {
            this(nodeId, ShardState.INIT, generation);
        }

        public ShardSnapshotStatus(@Nullable String nodeId, ShardState state, @Nullable ShardGeneration generation) {
            this(nodeId, assertNotSuccess(state), null, generation);
        }

        public ShardSnapshotStatus(@Nullable String nodeId, ShardState state, String reason, @Nullable ShardGeneration generation) {
            this(nodeId, assertNotSuccess(state), reason, generation, null);
        }

        private ShardSnapshotStatus(
                @Nullable String nodeId,
                ShardState state,
                String reason,
                @Nullable ShardGeneration generation,
                @Nullable ShardSnapshotResult shardSnapshotResult) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
            this.generation = generation;
            this.shardSnapshotResult = shardSnapshotResult;
            assert assertConsistent();
        }

        private static ShardState assertNotSuccess(ShardState shardState) {
            assert shardState != ShardState.SUCCESS : "use ShardSnapshotStatus#success";
            return shardState;
        }

        public static ShardSnapshotStatus success(String nodeId, ShardSnapshotResult shardSnapshotResult) {
            return new ShardSnapshotStatus(nodeId, ShardState.SUCCESS, null, shardSnapshotResult.getGeneration(), shardSnapshotResult);
        }

        private boolean assertConsistent() {
            // If the state is failed we have to have a reason for this failure
            assert state.failed() == false || reason != null;
            assert (state != ShardState.INIT && state != ShardState.WAITING) || nodeId != null : "Null node id for state [" + state + "]";
            assert state == ShardState.SUCCESS || shardSnapshotResult == null;
            assert shardSnapshotResult == null || shardSnapshotResult.getGeneration().equals(generation)
                    : "generation [" + generation + "] does not match result generation [" + shardSnapshotResult.getGeneration() + "]";
            return true;
        }

        public static ShardSnapshotStatus readFrom(StreamInput in) throws IOException {
            String nodeId = in.readOptionalString();
            final ShardState state = ShardState.fromValue(in.readByte());
            final ShardGeneration generation;
            if (SnapshotsService.useShardGenerations(in.getVersion())) {
                generation = in.readOptionalWriteable(ShardGeneration::new);
            } else {
                generation = null;
            }
            final String reason = in.readOptionalString();
            final ShardSnapshotResult shardSnapshotResult;
            if (in.getVersion().onOrAfter(SnapshotsService.INDEX_DETAILS_INTRODUCED)) {
                shardSnapshotResult = in.readOptionalWriteable(ShardSnapshotResult::new);
            } else {
                shardSnapshotResult = null;
            }
            if (state == ShardState.QUEUED) {
                return UNASSIGNED_QUEUED;
            }
            return new ShardSnapshotStatus(nodeId, state, reason, generation, shardSnapshotResult);
        }

        public ShardState state() {
            return state;
        }

        @Nullable
        public String nodeId() {
            return nodeId;
        }

        @Nullable
        public ShardGeneration generation() {
            return this.generation;
        }

        public String reason() {
            return reason;
        }

        public ShardSnapshotStatus withUpdatedGeneration(ShardGeneration newGeneration) {
            assert state == ShardState.SUCCESS : "can't move generation in state " + state;
            return new ShardSnapshotStatus(nodeId, state, reason, newGeneration,
                    shardSnapshotResult == null ? null :
                            new ShardSnapshotResult(newGeneration, shardSnapshotResult.getSize(), shardSnapshotResult.getSegmentCount()));
        }

        @Nullable
        public ShardSnapshotResult shardSnapshotResult() {
            assert state == ShardState.SUCCESS : "result is unavailable in state " + state;
            return shardSnapshotResult;
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
            if (SnapshotsService.useShardGenerations(out.getVersion())) {
                out.writeOptionalWriteable(generation);
            }
            out.writeOptionalString(reason);
            if (out.getVersion().onOrAfter(SnapshotsService.INDEX_DETAILS_INTRODUCED)) {
                out.writeOptionalWriteable(shardSnapshotResult);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardSnapshotStatus status = (ShardSnapshotStatus) o;
            return Objects.equals(nodeId, status.nodeId)
                    && Objects.equals(reason, status.reason)
                    && Objects.equals(generation, status.generation)
                    && state == status.state
                    && Objects.equals(shardSnapshotResult, status.shardSnapshotResult);
        }

        @Override
        public int hashCode() {
            int result = state != null ? state.hashCode() : 0;
            result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
            result = 31 * result + (reason != null ? reason.hashCode() : 0);
            result = 31 * result + (generation != null ? generation.hashCode() : 0);
            result = 31 * result + (shardSnapshotResult != null ? shardSnapshotResult.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ShardSnapshotStatus[state=" + state + ", nodeId=" + nodeId + ", reason=" + reason + ", generation=" + generation +
                    ", shardSnapshotResult=" + shardSnapshotResult + "]";
        }
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
        /**
         * Map of index name to {@link IndexId}.
         */
        private final Map<String, IndexId> indices;

        private final Map<String, Index> snapshotIndices;

        private final List<String> dataStreams;
        private final List<SnapshotFeatureInfo> featureStates;
        private final long startTime;
        private final long repositoryStateId;
        private final Version version;

        /**
         * Source snapshot if this is a clone operation or {@code null} if this is a snapshot.
         */
        @Nullable
        private final SnapshotId source;

        /**
         * Map of {@link RepositoryShardId} to {@link ShardSnapshotStatus} tracking the state of each shard operation in this entry.
         */
        private final ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> shardStatusByRepoShardId;

        @Nullable private final Map<String, Object> userMetadata;
        @Nullable private final String failure;

        // visible for testing, use #startedEntry and copy constructors in production code
        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, Map<String, IndexId> indices,
                     List<String> dataStreams, List<SnapshotFeatureInfo> featureStates, long startTime, long repositoryStateId,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, String failure, Map<String, Object> userMetadata,
                     Version version) {
            this(snapshot, includeGlobalState, partial, state, indices, dataStreams, featureStates, startTime, repositoryStateId, shards,
                    failure, userMetadata, version, null, ImmutableOpenMap.of());
        }

        private Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, Map<String, IndexId> indices,
                      List<String> dataStreams, List<SnapshotFeatureInfo> featureStates, long startTime, long repositoryStateId,
                      ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, String failure, Map<String, Object> userMetadata,
                      Version version, @Nullable SnapshotId source,
                      @Nullable ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> shardStatusByRepoShardId) {
            this.state = state;
            this.snapshot = snapshot;
            this.includeGlobalState = includeGlobalState;
            this.partial = partial;
            this.indices = org.elasticsearch.core.Map.copyOf(indices);
            this.dataStreams = org.elasticsearch.core.List.copyOf(dataStreams);
            this.featureStates = org.elasticsearch.core.List.copyOf(featureStates);
            this.startTime = startTime;
            this.shards = shards;
            this.repositoryStateId = repositoryStateId;
            this.failure = failure;
            this.userMetadata = userMetadata == null ? null : org.elasticsearch.core.Map.copyOf(userMetadata);
            this.version = version;
            this.source = source;
            if (source == null) {
                assert shardStatusByRepoShardId == null || shardStatusByRepoShardId.isEmpty()
                        : "Provided explict repo shard id statuses [" + shardStatusByRepoShardId + "] but no source";
                final Map<String, Index> res = new HashMap<>(indices.size());
                final ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> byRepoShardIdBuilder =
                    ImmutableOpenMap.builder(shards.size());
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> entry : shards) {
                    final ShardId shardId = entry.key;
                    final IndexId indexId = indices.get(shardId.getIndexName());
                    final Index index = shardId.getIndex();
                    final Index existing = res.put(indexId.getName(), index);
                    assert existing == null || existing.equals(index) : "Conflicting indices [" + existing + "] and [" + index + "]";
                    byRepoShardIdBuilder.put(new RepositoryShardId(indexId, shardId.id()), entry.value);
                }
                this.shardStatusByRepoShardId = byRepoShardIdBuilder.build();
                snapshotIndices = org.elasticsearch.core.Map.copyOf(res);
            } else {
                assert shards.isEmpty();
                this.shardStatusByRepoShardId = shardStatusByRepoShardId;
                snapshotIndices = org.elasticsearch.core.Map.of();
            }
            assert assertShardsConsistent(this.source, this.state, this.indices, this.shards, this.shardStatusByRepoShardId);
        }

        private static Entry readFrom(StreamInput in) throws IOException {
            final Snapshot snapshot = new Snapshot(in);
            final boolean includeGlobalState = in.readBoolean();
            final boolean partial = in.readBoolean();
            final State state = State.fromValue(in.readByte());
            final int indexCount = in.readVInt();
            final Map<String, IndexId> indices;
            if (indexCount == 0) {
                indices = Collections.emptyMap();
            } else {
                final Map<String, IndexId> idx = new HashMap<>(indexCount);
                for (int i = 0; i < indexCount; i++) {
                    final IndexId indexId = new IndexId(in);
                    idx.put(indexId.getName(), indexId);
                }
                indices = Collections.unmodifiableMap(idx);
            }
            final long startTime = in.readLong();
            final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards = in.readImmutableMap(ShardId::new, ShardSnapshotStatus::readFrom);
            final long repositoryStateId = in.readLong();
            final String failure = in.readOptionalString();
            final Map<String, Object> userMetadata;
            if (in.getVersion().onOrAfter(METADATA_FIELD_INTRODUCED)) {
                userMetadata = in.readMap();
            } else {
                userMetadata = null;
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
            final List<String> dataStreams;
            if (in.getVersion().onOrAfter(DATA_STREAMS_IN_SNAPSHOT)) {
                dataStreams = in.readStringList();
            } else {
                dataStreams = Collections.emptyList();
            }
            final SnapshotId source;
            final ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> clones;
            if (in.getVersion().onOrAfter(SnapshotsService.CLONE_SNAPSHOT_VERSION)) {
                source = in.readOptionalWriteable(SnapshotId::new);
                clones = in.readImmutableMap(RepositoryShardId::new, ShardSnapshotStatus::readFrom);
            } else {
                source = null;
                clones = null;
            }
            final List<SnapshotFeatureInfo> featureStates;
            if (in.getVersion().onOrAfter(FEATURE_STATES_VERSION)) {
                featureStates = Collections.unmodifiableList(in.readList(SnapshotFeatureInfo::new));
            } else {
                featureStates = Collections.emptyList();
            }
            return new SnapshotsInProgress.Entry(
                    snapshot, includeGlobalState, partial, state, indices, dataStreams, featureStates, startTime, repositoryStateId,
                    shards, failure, userMetadata, version, source, clones);
        }

        private static boolean assertShardsConsistent(SnapshotId source, State state, Map<String, IndexId> indices,
                                                      ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards,
                                                      ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> statusByRepoShardId) {
            if ((state == State.INIT || state == State.ABORTED) && shards.isEmpty()) {
                return true;
            }
            final Set<String> indexNames = indices.keySet();
            final Set<String> indexNamesInShards = new HashSet<>();
            shards.iterator().forEachRemaining(s -> {
                indexNamesInShards.add(s.key.getIndexName());
                assert source == null || s.value.nodeId == null :
                        "Shard snapshot must not be assigned to data node when copying from snapshot [" + source + "]";
            });
            assert source == null || indexNames.isEmpty() == false : "No empty snapshot clones allowed";
            assert source != null || indexNames.equals(indexNamesInShards)
                : "Indices in shards " + indexNamesInShards + " differ from expected indices " + indexNames + " for state [" + state + "]";
            final boolean shardsCompleted = completed(shards.values()) && completed(statusByRepoShardId.values());
            // Check state consistency for normal snapshots and started clone operations
            if (source == null || statusByRepoShardId.isEmpty() == false) {
                assert (state.completed() && shardsCompleted) || (state.completed() == false && shardsCompleted == false)
                        : "Completed state must imply all shards completed but saw state [" + state + "] and shards " + shards;
            }
            if (source != null && state.completed()) {
                assert hasFailures(statusByRepoShardId) == false || state == State.FAILED
                        : "Failed shard clones in [" + statusByRepoShardId + "] but state was [" + state + "]";
            }
            if (source == null) {
                assert shards.size() == statusByRepoShardId.size();
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> entry : shards) {
                    final ShardId routingShardId = entry.key;
                    assert statusByRepoShardId.get(new RepositoryShardId(indices.get(routingShardId.getIndexName()), routingShardId.id()))
                            == entry.value : "found inconsistent values tracked by routing- and repository shard id";
                }
            }
            return true;
        }

        public Entry(Snapshot snapshot, boolean includeGlobalState, boolean partial, State state, Map<String, IndexId> indices,
                     List<String> dataStreams, long startTime, long repositoryStateId,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, Map<String, Object> userMetadata, Version version) {
            this(snapshot, includeGlobalState, partial, state, indices, dataStreams, Collections.emptyList(), startTime, repositoryStateId,
                shards, null, userMetadata, version);
        }

        public Entry(Entry entry, State state, Map<String, IndexId> indices, long repositoryStateId,
                     ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, Version version, String failure) {
            this(entry.snapshot, entry.includeGlobalState, entry.partial, state, indices, entry.dataStreams, entry.featureStates,
                entry.startTime, repositoryStateId, shards, failure, entry.userMetadata, version);
        }

        public Entry withRepoGen(long newRepoGen) {
            assert newRepoGen > repositoryStateId : "Updated repository generation [" + newRepoGen
                    + "] must be higher than current generation [" + repositoryStateId + "]";
            return new Entry(snapshot, includeGlobalState, partial, state, indices, dataStreams, featureStates, startTime, newRepoGen,
                    shards, failure, userMetadata, version, source, source == null ? ImmutableOpenMap.of() : shardStatusByRepoShardId);
        }

        public Entry withClones(ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> updatedClones) {
            if (updatedClones.equals(shardStatusByRepoShardId)) {
                return this;
            }
            assert shards.isEmpty();
            return new Entry(snapshot, includeGlobalState, partial,
                    completed(updatedClones.values()) ? (hasFailures(updatedClones) ? State.FAILED : State.SUCCESS) :
                            state, indices, dataStreams, featureStates, startTime, repositoryStateId, ImmutableOpenMap.of(), failure,
                            userMetadata, version, source, updatedClones);
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
            return new Entry(
                    snapshot,
                    includeGlobalState,
                    partial,
                    completed ? State.SUCCESS : State.ABORTED,
                    indices,
                    dataStreams,
                    featureStates,
                    startTime,
                    repositoryStateId,
                    shardsBuilder.build(),
                    ABORTED_FAILURE_TEXT,
                    userMetadata,
                    version,
                    source,
                    null
            );
        }

        public Entry fail(ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards, State state, String failure) {
            return new Entry(snapshot, includeGlobalState, partial, state, indices, dataStreams, featureStates, startTime,
                    repositoryStateId, shards, failure, userMetadata, version, source, null);
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
                return new Entry(snapshot, includeGlobalState, partial, State.SUCCESS, indices, dataStreams, featureStates,
                        startTime, repositoryStateId, shards, failure, userMetadata, version);
            }
            return withStartedShards(shards);
        }

        /**
         * Same as {@link #withShardStates} but does not check if the snapshot completed and thus is only to be used when starting new
         * shard snapshots on data nodes for a running snapshot.
         */
        public Entry withStartedShards(ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards) {
            final SnapshotsInProgress.Entry updated = new Entry(snapshot, includeGlobalState, partial, state, indices, dataStreams,
                    featureStates, startTime, repositoryStateId, shards, failure, userMetadata, version);
            assert updated.state().completed() == false && completed(updated.shardsByRepoShardId().values()) == false
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

        public ImmutableOpenMap<RepositoryShardId, ShardSnapshotStatus> shardsByRepoShardId() {
            return shardStatusByRepoShardId;
        }

        public Index indexByName(String name) {
            return snapshotIndices.get(name);
        }

        public ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards() {
            assert isClone() == false : "tried to get routing shards for clone entry [" + this + "]";
            return this.shards;
        }

        public ShardId shardId(RepositoryShardId repositoryShardId) {
            assert isClone() == false : "must not be called for clone [" + this + "]";
            return new ShardId(indexByName(repositoryShardId.indexName()), repositoryShardId.shardId());
        }

        public State state() {
            return state;
        }

        public Map<String, IndexId> indices() {
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

        public List<SnapshotFeatureInfo> featureStates() {
            return featureStates;
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
            if (shardStatusByRepoShardId.equals(((Entry) o).shardStatusByRepoShardId) == false) return false;
            if (featureStates.equals(entry.featureStates) == false) return false;

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
            result = 31 * result + shardStatusByRepoShardId.hashCode();
            result = 31 * result + featureStates.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("repository", snapshot.getRepository());
            builder.field("snapshot", snapshot.getSnapshotId().getName());
            builder.field("uuid", snapshot.getSnapshotId().getUUID());
            builder.field("include_global_state", includeGlobalState());
            builder.field("partial", partial);
            builder.field("state", state);
            builder.startArray("indices");
            {
                for (IndexId index : indices.values()) {
                    index.toXContent(builder, params);
                }
            }
            builder.endArray();
            builder.timeField("start_time_millis", "start_time", startTime);
            builder.field("repository_state_id", repositoryStateId);
            builder.startArray("shards");
            {
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : shards) {
                    ShardId shardId = shardEntry.key;
                    writeShardSnapshotStatus(builder, shardId.getIndex(), shardId.getId(), shardEntry.value);
                }
            }
            builder.endArray();
            builder.startArray("feature_states");
            {
                for (SnapshotFeatureInfo featureState : featureStates) {
                    featureState.toXContent(builder, params);
                }
            }
            builder.endArray();
            if (isClone()) {
                builder.field("source", source);
                builder.startArray("clones");
                {
                    for (ObjectObjectCursor<RepositoryShardId, ShardSnapshotStatus> shardEntry : shardStatusByRepoShardId) {
                        RepositoryShardId shardId = shardEntry.key;
                        writeShardSnapshotStatus(builder, shardId.index(), shardId.shardId(), shardEntry.value);
                    }
                }
                builder.endArray();
            }
            builder.array("data_streams", dataStreams.toArray(new String[0]));
            builder.endObject();
            return builder;
        }

        private void writeShardSnapshotStatus(XContentBuilder builder, ToXContent indexId, int shardId,
                                              ShardSnapshotStatus status) throws IOException {
            builder.startObject();
            builder.field("index", indexId);
            builder.field("shard", shardId);
            builder.field("state", status.state());
            builder.field("generation", status.generation());
            builder.field("node", status.nodeId());

            if (status.state() == ShardState.SUCCESS) {
                final ShardSnapshotResult result = status.shardSnapshotResult();
                builder.startObject("result");
                builder.field("generation", result.getGeneration());
                builder.humanReadableField("size_in_bytes", "size", result.getSize());
                builder.field("segments", result.getSegmentCount());
                builder.endObject();
            }

            if (status.reason() != null) {
                builder.field("reason", status.reason());
            }

            builder.endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            snapshot.writeTo(out);
            out.writeBoolean(includeGlobalState);
            out.writeBoolean(partial);
            out.writeByte(state.value());
            out.writeCollection(indices.values());
            out.writeLong(startTime);
            out.writeMap(shards);
            out.writeLong(repositoryStateId);
            out.writeOptionalString(failure);
            if (out.getVersion().onOrAfter(METADATA_FIELD_INTRODUCED)) {
                out.writeMap(userMetadata);
            }
            if (out.getVersion().onOrAfter(VERSION_IN_SNAPSHOT_VERSION)) {
                Version.writeVersion(version, out);
            } else if (out.getVersion().onOrAfter(SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION)) {
                out.writeBoolean(SnapshotsService.useShardGenerations(version));
            }
            if (out.getVersion().onOrAfter(DATA_STREAMS_IN_SNAPSHOT)) {
                out.writeStringCollection(dataStreams);
            }
            if (out.getVersion().onOrAfter(SnapshotsService.CLONE_SNAPSHOT_VERSION)) {
                out.writeOptionalWriteable(source);
                if (source == null) {
                    out.writeMap(ImmutableOpenMap.of());
                } else {
                    out.writeMap(shardStatusByRepoShardId);
                }
            }
            if (out.getVersion().onOrAfter(FEATURE_STATES_VERSION)) {
                out.writeList(featureStates);
            }
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }
}
