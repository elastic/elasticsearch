/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
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
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Meta data about snapshots that are currently executing
 */
public class SnapshotsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final SnapshotsInProgress EMPTY = new SnapshotsInProgress(Map.of());

    public static final String TYPE = "snapshots";

    public static final String ABORTED_FAILURE_TEXT = "Snapshot was aborted by deletion";

    // keyed by repository name
    private final Map<String, ByRepo> entries;

    public SnapshotsInProgress(StreamInput in) throws IOException {
        this(collectByRepo(in));
    }

    private static Map<String, ByRepo> collectByRepo(StreamInput in) throws IOException {
        final int count = in.readVInt();
        if (count == 0) {
            return Map.of();
        }
        final Map<String, List<Entry>> entriesByRepo = new HashMap<>();
        for (int i = 0; i < count; i++) {
            final Entry entry = Entry.readFrom(in);
            entriesByRepo.computeIfAbsent(entry.repository(), repo -> new ArrayList<>()).add(entry);
        }
        final Map<String, ByRepo> res = Maps.newMapWithExpectedSize(entriesByRepo.size());
        for (Map.Entry<String, List<Entry>> entryForRepo : entriesByRepo.entrySet()) {
            res.put(entryForRepo.getKey(), new ByRepo(entryForRepo.getValue()));
        }
        return res;
    }

    private SnapshotsInProgress(Map<String, ByRepo> entries) {
        this.entries = Map.copyOf(entries);
        assert assertConsistentEntries(this.entries);
    }

    public SnapshotsInProgress withUpdatedEntriesForRepo(String repository, List<Entry> updatedEntries) {
        if (updatedEntries.equals(forRepo(repository))) {
            return this;
        }
        final Map<String, ByRepo> copy = new HashMap<>(this.entries);
        if (updatedEntries.isEmpty()) {
            copy.remove(repository);
            if (copy.isEmpty()) {
                return EMPTY;
            }
        } else {
            copy.put(repository, new ByRepo(updatedEntries));
        }
        return new SnapshotsInProgress(copy);
    }

    public SnapshotsInProgress withAddedEntry(Entry entry) {
        final List<Entry> forRepo = new ArrayList<>(forRepo(entry.repository()));
        forRepo.add(entry);
        return withUpdatedEntriesForRepo(entry.repository(), forRepo);
    }

    public List<Entry> forRepo(String repository) {
        return entries.getOrDefault(repository, ByRepo.EMPTY).entries;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int count() {
        int count = 0;
        for (ByRepo byRepo : entries.values()) {
            count += byRepo.entries.size();
        }
        return count;
    }

    public Iterable<List<Entry>> entriesByRepo() {
        return () -> entries.values().stream().map(byRepo -> byRepo.entries).iterator();
    }

    public Stream<Entry> asStream() {
        return entries.values().stream().flatMap(t -> t.entries.stream());
    }

    @Nullable
    public Entry snapshot(final Snapshot snapshot) {
        return findInList(snapshot, forRepo(snapshot.getRepository()));
    }

    @Nullable
    private static Entry findInList(Snapshot snapshot, List<Entry> forRepo) {
        for (Entry entry : forRepo) {
            final Snapshot curr = entry.snapshot();
            if (curr.equals(snapshot)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Computes a map of repository shard id to set of generations, containing all shard generations that became obsolete and may be
     * deleted from the repository as the cluster state moved from the given {@code old} value of {@link SnapshotsInProgress} to this
     * instance.
     */
    public Map<RepositoryShardId, Set<ShardGeneration>> obsoleteGenerations(String repository, SnapshotsInProgress old) {
        final Map<RepositoryShardId, Set<ShardGeneration>> obsoleteGenerations = new HashMap<>();
        final List<Entry> updatedSnapshots = forRepo(repository);
        for (Entry entry : old.forRepo(repository)) {
            final Entry updatedEntry = findInList(entry.snapshot(), updatedSnapshots);
            if (updatedEntry == null || updatedEntry == entry) {
                continue;
            }
            for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> oldShardAssignment : entry.shardsByRepoShardId().entrySet()) {
                final RepositoryShardId repositoryShardId = oldShardAssignment.getKey();
                final ShardSnapshotStatus oldStatus = oldShardAssignment.getValue();
                final ShardSnapshotStatus newStatus = updatedEntry.shardsByRepoShardId().get(repositoryShardId);
                if (oldStatus.state == ShardState.SUCCESS
                    && oldStatus.generation() != null
                    && newStatus != null
                    && newStatus.state() == ShardState.SUCCESS
                    && newStatus.generation() != null
                    && oldStatus.generation().equals(newStatus.generation()) == false) {
                    // We moved from a non-null generation successful generation to a different non-null successful generation
                    // so the original generation is clearly obsolete because it was in-flight before and is now unreferenced everywhere.
                    obsoleteGenerations.computeIfAbsent(repositoryShardId, ignored -> new HashSet<>()).add(oldStatus.generation());
                }
            }
        }
        return Map.copyOf(obsoleteGenerations);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    private static final Version DIFFABLE_VERSION = Version.V_8_5_0;

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(DIFFABLE_VERSION)) {
            return new SnapshotInProgressDiff(in);
        }
        return readDiffFrom(Custom.class, TYPE, in);
    }

    @Override
    public Diff<Custom> diff(Custom previousState) {
        return new SnapshotInProgressDiff((SnapshotsInProgress) previousState, this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(count());
        final Iterator<Entry> iterator = asStream().iterator();
        while (iterator.hasNext()) {
            iterator.next().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("snapshots");
        final Iterator<Entry> iterator = asStream().iterator();
        while (iterator.hasNext()) {
            iterator.next().toXContent(builder, params);
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
        final Iterator<SnapshotsInProgress.Entry> entryList = asStream().iterator();
        boolean firstEntry = true;
        while (entryList.hasNext()) {
            if (firstEntry == false) {
                builder.append(",");
            }
            builder.append(entryList.next().snapshot().getSnapshotId().getName());
            firstEntry = false;
        }
        return builder.append("]").toString();
    }

    /**
     * Creates the initial {@link Entry} when starting a snapshot, if no shard-level snapshot work is to be done the resulting entry
     * will be in state {@link State#SUCCESS} right away otherwise it will be in state {@link State#STARTED}.
     */
    public static Entry startedEntry(
        Snapshot snapshot,
        boolean includeGlobalState,
        boolean partial,
        Map<String, IndexId> indices,
        List<String> dataStreams,
        long startTime,
        long repositoryStateId,
        Map<ShardId, ShardSnapshotStatus> shards,
        Map<String, Object> userMetadata,
        Version version,
        List<SnapshotFeatureInfo> featureStates
    ) {
        return Entry.snapshot(
            snapshot,
            includeGlobalState,
            partial,
            completed(shards.values()) ? State.SUCCESS : State.STARTED,
            indices,
            dataStreams,
            featureStates,
            startTime,
            repositoryStateId,
            shards,
            null,
            userMetadata,
            version
        );
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
    public static Entry startClone(
        Snapshot snapshot,
        SnapshotId source,
        Map<String, IndexId> indices,
        long startTime,
        long repositoryStateId,
        Version version
    ) {
        return Entry.createClone(snapshot, State.STARTED, indices, startTime, repositoryStateId, null, version, source, Map.of());
    }

    /**
     * Checks if all shards in the list have completed
     *
     * @param shards list of shard statuses
     * @return true if all shards have completed (either successfully or failed), false otherwise
     */
    public static boolean completed(Collection<ShardSnapshotStatus> shards) {
        for (ShardSnapshotStatus status : shards) {
            if (status.state().completed == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasFailures(Map<RepositoryShardId, ShardSnapshotStatus> clones) {
        for (ShardSnapshotStatus value : clones.values()) {
            if (value.state().failed()) {
                return true;
            }
        }
        return false;
    }

    private static boolean assertConsistentEntries(Map<String, ByRepo> entries) {
        for (Map.Entry<String, ByRepo> repoEntries : entries.entrySet()) {
            final Set<Tuple<String, Integer>> assignedShards = new HashSet<>();
            final Set<Tuple<String, Integer>> queuedShards = new HashSet<>();
            final List<Entry> entriesForRepository = repoEntries.getValue().entries;
            final String repository = repoEntries.getKey();
            assert entriesForRepository.isEmpty() == false : "found empty list of snapshots for " + repository + " in " + entries;
            for (Entry entry : entriesForRepository) {
                assert entry.repository().equals(repository) : "mismatched repository " + entry + " tracked under " + repository;
                for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> shard : entry.shardsByRepoShardId().entrySet()) {
                    final RepositoryShardId sid = shard.getKey();
                    assert assertShardStateConsistent(
                        entriesForRepository,
                        assignedShards,
                        queuedShards,
                        sid.indexName(),
                        sid.shardId(),
                        shard.getValue()
                    );
                }
            }
            // make sure in-flight-shard-states can be built cleanly for the entries without tripping assertions
            InFlightShardSnapshotStates.forEntries(entriesForRepository);
        }
        return true;
    }

    private static boolean assertShardStateConsistent(
        List<Entry> entries,
        Set<Tuple<String, Integer>> assignedShards,
        Set<Tuple<String, Integer>> queuedShards,
        String indexName,
        int shardId,
        ShardSnapshotStatus shardSnapshotStatus
    ) {
        if (shardSnapshotStatus.isActive()) {
            Tuple<String, Integer> plainShardId = Tuple.tuple(indexName, shardId);
            assert assignedShards.add(plainShardId) : plainShardId + " is assigned twice in " + entries;
            assert queuedShards.contains(plainShardId) == false : plainShardId + " is queued then assigned in " + entries;
        } else if (shardSnapshotStatus.state() == ShardState.QUEUED) {
            queuedShards.add(Tuple.tuple(indexName, shardId));
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
            return switch (value) {
                case 0 -> INIT;
                case 2 -> SUCCESS;
                case 3 -> FAILED;
                case 4 -> ABORTED;
                case 5 -> MISSING;
                case 6 -> WAITING;
                case 7 -> QUEUED;
                default -> throw new IllegalArgumentException("No shard snapshot state for value [" + value + "]");
            };
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
            return switch (value) {
                case 0 -> INIT;
                case 1 -> STARTED;
                case 2 -> SUCCESS;
                case 3 -> FAILED;
                case 4 -> ABORTED;
                default -> throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
            };
        }
    }

    public static class ShardSnapshotStatus implements Writeable {

        /**
         * Shard snapshot status for shards that are waiting for another operation to finish before they can be assigned to a node.
         */
        public static final ShardSnapshotStatus UNASSIGNED_QUEUED = new SnapshotsInProgress.ShardSnapshotStatus(
            null,
            ShardState.QUEUED,
            null
        );

        /**
         * Shard snapshot status for shards that could not be snapshotted because their index was deleted from before the shard snapshot
         * started.
         */
        public static final ShardSnapshotStatus MISSING = new SnapshotsInProgress.ShardSnapshotStatus(
            null,
            ShardState.MISSING,
            "missing index",
            null
        );

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
            @Nullable ShardSnapshotResult shardSnapshotResult
        ) {
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
            assert state != ShardState.QUEUED || (nodeId == null && generation == null && reason == null)
                : "Found unexpected non-null values for queued state shard nodeId[" + nodeId + "][" + generation + "][" + reason + "]";
            assert state == ShardState.SUCCESS || shardSnapshotResult == null;
            assert shardSnapshotResult == null || shardSnapshotResult.getGeneration().equals(generation)
                : "generation [" + generation + "] does not match result generation [" + shardSnapshotResult.getGeneration() + "]";
            return true;
        }

        public static ShardSnapshotStatus readFrom(StreamInput in) throws IOException {
            String nodeId = in.readOptionalString();
            final ShardState state = ShardState.fromValue(in.readByte());
            final ShardGeneration generation = in.readOptionalWriteable(ShardGeneration::new);
            final String reason = in.readOptionalString();
            final ShardSnapshotResult shardSnapshotResult = in.readOptionalWriteable(ShardSnapshotResult::new);
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
            return new ShardSnapshotStatus(
                nodeId,
                state,
                reason,
                newGeneration,
                shardSnapshotResult == null
                    ? null
                    : new ShardSnapshotResult(newGeneration, shardSnapshotResult.getSize(), shardSnapshotResult.getSegmentCount())
            );
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
            out.writeOptionalWriteable(generation);
            out.writeOptionalString(reason);
            out.writeOptionalWriteable(shardSnapshotResult);
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
            return "ShardSnapshotStatus[state="
                + state
                + ", nodeId="
                + nodeId
                + ", reason="
                + reason
                + ", generation="
                + generation
                + ", shardSnapshotResult="
                + shardSnapshotResult
                + "]";
        }
    }

    public static class Entry implements Writeable, ToXContent, RepositoryOperation, Diffable<Entry> {
        private final State state;
        private final Snapshot snapshot;
        private final boolean includeGlobalState;
        private final boolean partial;
        /**
         * Map of {@link ShardId} to {@link ShardSnapshotStatus} tracking the state of each shard snapshot operation.
         */
        private final Map<ShardId, ShardSnapshotStatus> shards;
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
        private final Map<RepositoryShardId, ShardSnapshotStatus> shardStatusByRepoShardId;

        @Nullable
        private final Map<String, Object> userMetadata;
        @Nullable
        private final String failure;

        // visible for testing, use #startedEntry and copy constructors in production code
        public static Entry snapshot(
            Snapshot snapshot,
            boolean includeGlobalState,
            boolean partial,
            State state,
            Map<String, IndexId> indices,
            List<String> dataStreams,
            List<SnapshotFeatureInfo> featureStates,
            long startTime,
            long repositoryStateId,
            Map<ShardId, ShardSnapshotStatus> shards,
            String failure,
            Map<String, Object> userMetadata,
            Version version
        ) {
            final Map<String, Index> res = Maps.newMapWithExpectedSize(indices.size());
            final Map<RepositoryShardId, ShardSnapshotStatus> byRepoShardIdBuilder = Maps.newHashMapWithExpectedSize(shards.size());
            for (Map.Entry<ShardId, ShardSnapshotStatus> entry : shards.entrySet()) {
                final ShardId shardId = entry.getKey();
                final IndexId indexId = indices.get(shardId.getIndexName());
                final Index index = shardId.getIndex();
                final Index existing = res.put(indexId.getName(), index);
                assert existing == null || existing.equals(index) : "Conflicting indices [" + existing + "] and [" + index + "]";
                byRepoShardIdBuilder.put(new RepositoryShardId(indexId, shardId.id()), entry.getValue());
            }
            return new Entry(
                snapshot,
                includeGlobalState,
                partial,
                state,
                indices,
                dataStreams,
                featureStates,
                startTime,
                repositoryStateId,
                shards,
                failure,
                userMetadata,
                version,
                null,
                byRepoShardIdBuilder,
                res
            );
        }

        private static Entry createClone(
            Snapshot snapshot,
            State state,
            Map<String, IndexId> indices,
            long startTime,
            long repositoryStateId,
            String failure,
            Version version,
            SnapshotId source,
            Map<RepositoryShardId, ShardSnapshotStatus> shardStatusByRepoShardId
        ) {
            return new Entry(
                snapshot,
                true,
                false,
                state,
                indices,
                List.of(),
                List.of(),
                startTime,
                repositoryStateId,
                Map.of(),
                failure,
                Map.of(),
                version,
                source,
                shardStatusByRepoShardId,
                Map.of()
            );
        }

        private Entry(
            Snapshot snapshot,
            boolean includeGlobalState,
            boolean partial,
            State state,
            Map<String, IndexId> indices,
            List<String> dataStreams,
            List<SnapshotFeatureInfo> featureStates,
            long startTime,
            long repositoryStateId,
            Map<ShardId, ShardSnapshotStatus> shards,
            String failure,
            Map<String, Object> userMetadata,
            Version version,
            @Nullable SnapshotId source,
            Map<RepositoryShardId, ShardSnapshotStatus> shardStatusByRepoShardId,
            Map<String, Index> snapshotIndices
        ) {
            this.state = state;
            this.snapshot = snapshot;
            this.includeGlobalState = includeGlobalState;
            this.partial = partial;
            this.indices = Map.copyOf(indices);
            this.dataStreams = List.copyOf(dataStreams);
            this.featureStates = List.copyOf(featureStates);
            this.startTime = startTime;
            this.shards = shards;
            this.repositoryStateId = repositoryStateId;
            this.failure = failure;
            this.userMetadata = userMetadata == null ? null : Map.copyOf(userMetadata);
            this.version = version;
            this.source = source;
            this.shardStatusByRepoShardId = Map.copyOf(shardStatusByRepoShardId);
            this.snapshotIndices = snapshotIndices;
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
                final Map<String, IndexId> idx = Maps.newMapWithExpectedSize(indexCount);
                for (int i = 0; i < indexCount; i++) {
                    final IndexId indexId = new IndexId(in);
                    idx.put(indexId.getName(), indexId);
                }
                indices = Collections.unmodifiableMap(idx);
            }
            final long startTime = in.readLong();
            final Map<ShardId, ShardSnapshotStatus> shards = in.readImmutableMap(ShardId::new, ShardSnapshotStatus::readFrom);
            final long repositoryStateId = in.readLong();
            final String failure = in.readOptionalString();
            final Map<String, Object> userMetadata = in.readMap();
            final Version version = Version.readVersion(in);
            final List<String> dataStreams = in.readStringList();
            final SnapshotId source = in.readOptionalWriteable(SnapshotId::new);
            final Map<RepositoryShardId, ShardSnapshotStatus> clones = in.readImmutableMap(
                RepositoryShardId::new,
                ShardSnapshotStatus::readFrom
            );
            final List<SnapshotFeatureInfo> featureStates = in.readImmutableList(SnapshotFeatureInfo::new);
            if (source == null) {
                return snapshot(
                    snapshot,
                    includeGlobalState,
                    partial,
                    state,
                    indices,
                    dataStreams,
                    featureStates,
                    startTime,
                    repositoryStateId,
                    shards,
                    failure,
                    userMetadata,
                    version
                );
            }
            assert shards.isEmpty();
            return Entry.createClone(snapshot, state, indices, startTime, repositoryStateId, failure, version, source, clones);
        }

        private static boolean assertShardsConsistent(
            SnapshotId source,
            State state,
            Map<String, IndexId> indices,
            Map<ShardId, ShardSnapshotStatus> shards,
            Map<RepositoryShardId, ShardSnapshotStatus> statusByRepoShardId
        ) {
            if ((state == State.INIT || state == State.ABORTED) && shards.isEmpty()) {
                return true;
            }
            final Set<String> indexNames = indices.keySet();
            final Set<String> indexNamesInShards = new HashSet<>();
            shards.entrySet().forEach(s -> {
                indexNamesInShards.add(s.getKey().getIndexName());
                assert source == null || s.getValue().nodeId == null
                    : "Shard snapshot must not be assigned to data node when copying from snapshot [" + source + "]";
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
                for (Map.Entry<ShardId, ShardSnapshotStatus> entry : shards.entrySet()) {
                    final ShardId routingShardId = entry.getKey();
                    assert statusByRepoShardId.get(
                        new RepositoryShardId(indices.get(routingShardId.getIndexName()), routingShardId.id())
                    ) == entry.getValue() : "found inconsistent values tracked by routing- and repository shard id";
                }
            }
            return true;
        }

        public Entry withRepoGen(long newRepoGen) {
            assert newRepoGen > repositoryStateId
                : "Updated repository generation [" + newRepoGen + "] must be higher than current generation [" + repositoryStateId + "]";
            return new Entry(
                snapshot,
                includeGlobalState,
                partial,
                state,
                indices,
                dataStreams,
                featureStates,
                startTime,
                newRepoGen,
                shards,
                failure,
                userMetadata,
                version,
                source,
                shardStatusByRepoShardId,
                snapshotIndices
            );
        }

        /**
         * Reassigns all {@link IndexId} in a snapshot that can be found as keys in the given {@code updates} to the {@link IndexId} value
         * that they map to.
         * This method is used in an edge case of removing a {@link SnapshotDeletionsInProgress.Entry} from the cluster state at the
         * end of a delete. If the delete removed the last use of a certain {@link IndexId} from the repository then we do not want to
         * reuse that {@link IndexId} because the implementation of {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository}
         * assumes that a given {@link IndexId} will never be reused if it went from referenced to unreferenced in the
         * {@link org.elasticsearch.repositories.RepositoryData} in a delete.
         *
         * @param updates map of existing {@link IndexId} to updated {@link IndexId}
         * @return a new instance with updated index ids or this instance if unchanged
         */
        public Entry withUpdatedIndexIds(Map<IndexId, IndexId> updates) {
            if (isClone()) {
                assert indices.values().stream().noneMatch(updates::containsKey)
                    : "clone index ids can not be updated but saw tried to update " + updates + " on " + this;
                return this;
            }
            Map<String, IndexId> updatedIndices = null;
            for (IndexId existingIndexId : indices.values()) {
                final IndexId updatedIndexId = updates.get(existingIndexId);
                if (updatedIndexId != null) {
                    if (updatedIndices == null) {
                        updatedIndices = new HashMap<>(indices);
                    }
                    updatedIndices.put(updatedIndexId.getName(), updatedIndexId);
                }
            }
            if (updatedIndices != null) {
                return snapshot(
                    snapshot,
                    includeGlobalState,
                    partial,
                    state,
                    updatedIndices,
                    dataStreams,
                    featureStates,
                    startTime,
                    repositoryStateId,
                    shards,
                    failure,
                    userMetadata,
                    version
                );
            }
            return this;
        }

        public Entry withClones(Map<RepositoryShardId, ShardSnapshotStatus> updatedClones) {
            if (updatedClones.equals(shardStatusByRepoShardId)) {
                return this;
            }
            assert shards.isEmpty();
            return Entry.createClone(
                snapshot,
                completed(updatedClones.values()) ? (hasFailures(updatedClones) ? State.FAILED : State.SUCCESS) : state,
                indices,
                startTime,
                repositoryStateId,
                failure,
                version,
                source,
                updatedClones
            );
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
            final Map<ShardId, ShardSnapshotStatus> shardsBuilder = new HashMap<>();
            boolean completed = true;
            boolean allQueued = true;
            for (Map.Entry<ShardId, ShardSnapshotStatus> shardEntry : shards.entrySet()) {
                ShardSnapshotStatus status = shardEntry.getValue();
                allQueued &= status.state() == ShardState.QUEUED;
                if (status.state().completed() == false) {
                    final String nodeId = status.nodeId();
                    status = new ShardSnapshotStatus(
                        nodeId,
                        nodeId == null ? ShardState.FAILED : ShardState.ABORTED,
                        "aborted by snapshot deletion",
                        status.generation()
                    );
                }
                completed &= status.state().completed();
                shardsBuilder.put(shardEntry.getKey(), status);
            }
            if (allQueued) {
                return null;
            }
            return Entry.snapshot(
                snapshot,
                includeGlobalState,
                partial,
                completed ? State.SUCCESS : State.ABORTED,
                indices,
                dataStreams,
                featureStates,
                startTime,
                repositoryStateId,
                Map.copyOf(shardsBuilder),
                ABORTED_FAILURE_TEXT,
                userMetadata,
                version
            );
        }

        /**
         * Create a new instance that has its shard assignments replaced by the given shard assignment map.
         * If the given shard assignments show all shard snapshots in a completed state then the returned instance will be of state
         * {@link State#SUCCESS}, otherwise the state remains unchanged.
         *
         * @param shards new shard snapshot states
         * @return new snapshot entry
         */
        public Entry withShardStates(Map<ShardId, ShardSnapshotStatus> shards) {
            if (completed(shards.values())) {
                return Entry.snapshot(
                    snapshot,
                    includeGlobalState,
                    partial,
                    State.SUCCESS,
                    indices,
                    dataStreams,
                    featureStates,
                    startTime,
                    repositoryStateId,
                    shards,
                    failure,
                    userMetadata,
                    version
                );
            }
            return withStartedShards(shards);
        }

        /**
         * Same as {@link #withShardStates} but does not check if the snapshot completed and thus is only to be used when starting new
         * shard snapshots on data nodes for a running snapshot.
         */
        public Entry withStartedShards(Map<ShardId, ShardSnapshotStatus> shards) {
            final SnapshotsInProgress.Entry updated = Entry.snapshot(
                snapshot,
                includeGlobalState,
                partial,
                state,
                indices,
                dataStreams,
                featureStates,
                startTime,
                repositoryStateId,
                shards,
                failure,
                userMetadata,
                version
            );
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

        public Map<RepositoryShardId, ShardSnapshotStatus> shardsByRepoShardId() {
            return shardStatusByRepoShardId;
        }

        public Index indexByName(String name) {
            assert isClone() == false : "tried to get routing index for clone entry [" + this + "]";
            return snapshotIndices.get(name);
        }

        public Map<ShardId, ShardSnapshotStatus> shards() {
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
                for (Map.Entry<ShardId, ShardSnapshotStatus> shardEntry : shards.entrySet()) {
                    ShardId shardId = shardEntry.getKey();
                    writeShardSnapshotStatus(builder, shardId.getIndex(), shardId.getId(), shardEntry.getValue());
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
                    for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> shardEntry : shardStatusByRepoShardId.entrySet()) {
                        RepositoryShardId shardId = shardEntry.getKey();
                        writeShardSnapshotStatus(builder, shardId.index(), shardId.shardId(), shardEntry.getValue());
                    }
                }
                builder.endArray();
            }
            builder.array("data_streams", dataStreams.toArray(new String[0]));
            builder.endObject();
            return builder;
        }

        private static void writeShardSnapshotStatus(XContentBuilder builder, ToXContent indexId, int shardId, ShardSnapshotStatus status)
            throws IOException {
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
            out.writeGenericMap(userMetadata);
            Version.writeVersion(version, out);
            out.writeStringCollection(dataStreams);
            out.writeOptionalWriteable(source);
            if (source == null) {
                out.writeMap(Map.of());
            } else {
                out.writeMap(shardStatusByRepoShardId);
            }
            out.writeList(featureStates);
        }

        @Override
        public boolean isFragment() {
            return false;
        }

        @Override
        public Diff<Entry> diff(Entry previousState) {
            return new EntryDiff(previousState, this);
        }
    }

    private static final class EntryDiff implements Diff<Entry> {

        private static final DiffableUtils.NonDiffableValueSerializer<String, IndexId> INDEX_ID_VALUE_SERIALIZER =
            new DiffableUtils.NonDiffableValueSerializer<>() {
                @Override
                public void write(IndexId value, StreamOutput out) throws IOException {
                    out.writeString(value.getId());
                }

                @Override
                public IndexId read(StreamInput in, String key) throws IOException {
                    return new IndexId(key, in.readString());
                }
            };

        private static final DiffableUtils.NonDiffableValueSerializer<?, ShardSnapshotStatus> SHARD_SNAPSHOT_STATUS_VALUE_SERIALIZER =
            new DiffableUtils.NonDiffableValueSerializer<>() {
                @Override
                public void write(ShardSnapshotStatus value, StreamOutput out) throws IOException {
                    value.writeTo(out);
                }

                @Override
                public ShardSnapshotStatus read(StreamInput in, Object key) throws IOException {
                    return ShardSnapshotStatus.readFrom(in);
                }
            };

        private static final DiffableUtils.KeySerializer<ShardId> SHARD_ID_KEY_SERIALIZER = new DiffableUtils.KeySerializer<>() {
            @Override
            public void writeKey(ShardId key, StreamOutput out) throws IOException {
                key.writeTo(out);
            }

            @Override
            public ShardId readKey(StreamInput in) throws IOException {
                return new ShardId(in);
            }
        };

        private static final DiffableUtils.KeySerializer<RepositoryShardId> REPO_SHARD_ID_KEY_SERIALIZER =
            new DiffableUtils.KeySerializer<>() {
                @Override
                public void writeKey(RepositoryShardId key, StreamOutput out) throws IOException {
                    key.writeTo(out);
                }

                @Override
                public RepositoryShardId readKey(StreamInput in) throws IOException {
                    return new RepositoryShardId(in);
                }
            };

        private final DiffableUtils.MapDiff<String, IndexId, Map<String, IndexId>> indexByIndexNameDiff;

        private final DiffableUtils.MapDiff<ShardId, ShardSnapshotStatus, Map<ShardId, ShardSnapshotStatus>> shardsByShardIdDiff;

        @Nullable
        private final DiffableUtils.MapDiff<
            RepositoryShardId,
            ShardSnapshotStatus,
            Map<RepositoryShardId, ShardSnapshotStatus>> shardsByRepoShardIdDiff;

        @Nullable
        private final List<String> updatedDataStreams;

        @Nullable
        private final String updatedFailure;

        private final long updatedRepositoryStateId;

        private final State updatedState;

        @SuppressWarnings("unchecked")
        EntryDiff(StreamInput in) throws IOException {
            this.indexByIndexNameDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), INDEX_ID_VALUE_SERIALIZER);
            this.updatedState = State.fromValue(in.readByte());
            this.updatedRepositoryStateId = in.readLong();
            this.updatedDataStreams = in.readOptionalStringList();
            this.updatedFailure = in.readOptionalString();
            this.shardsByShardIdDiff = DiffableUtils.readJdkMapDiff(
                in,
                SHARD_ID_KEY_SERIALIZER,
                (DiffableUtils.ValueSerializer<ShardId, ShardSnapshotStatus>) SHARD_SNAPSHOT_STATUS_VALUE_SERIALIZER
            );
            shardsByRepoShardIdDiff = in.readOptionalWriteable(
                i -> DiffableUtils.readJdkMapDiff(
                    i,
                    REPO_SHARD_ID_KEY_SERIALIZER,
                    (DiffableUtils.ValueSerializer<RepositoryShardId, ShardSnapshotStatus>) SHARD_SNAPSHOT_STATUS_VALUE_SERIALIZER
                )
            );
        }

        @SuppressWarnings("unchecked")
        EntryDiff(Entry before, Entry after) {
            try {
                verifyDiffable(before, after);
            } catch (Exception e) {
                final IllegalArgumentException ex = new IllegalArgumentException("Cannot diff [" + before + "] and [" + after + "]");
                assert false : ex;
                throw ex;
            }
            this.indexByIndexNameDiff = DiffableUtils.diff(
                before.indices,
                after.indices,
                DiffableUtils.getStringKeySerializer(),
                INDEX_ID_VALUE_SERIALIZER
            );
            this.updatedDataStreams = before.dataStreams.equals(after.dataStreams) ? null : after.dataStreams;
            this.updatedState = after.state;
            this.updatedRepositoryStateId = after.repositoryStateId;
            this.updatedFailure = after.failure;
            this.shardsByShardIdDiff = DiffableUtils.diff(
                before.shards,
                after.shards,
                SHARD_ID_KEY_SERIALIZER,
                (DiffableUtils.ValueSerializer<ShardId, ShardSnapshotStatus>) SHARD_SNAPSHOT_STATUS_VALUE_SERIALIZER
            );
            if (before.isClone()) {
                this.shardsByRepoShardIdDiff = DiffableUtils.diff(
                    before.shardStatusByRepoShardId,
                    after.shardStatusByRepoShardId,
                    REPO_SHARD_ID_KEY_SERIALIZER,
                    (DiffableUtils.ValueSerializer<RepositoryShardId, ShardSnapshotStatus>) SHARD_SNAPSHOT_STATUS_VALUE_SERIALIZER
                );
            } else {
                this.shardsByRepoShardIdDiff = null;
            }
        }

        private static void verifyDiffable(Entry before, Entry after) {
            if (before.snapshot().equals(after.snapshot()) == false) {
                throw new IllegalArgumentException("snapshot changed from [" + before.snapshot() + "] to [" + after.snapshot() + "]");
            }
            if (before.startTime() != after.startTime()) {
                throw new IllegalArgumentException("start time changed from [" + before.startTime() + "] to [" + after.startTime() + "]");
            }
            if (Objects.equals(before.source(), after.source()) == false) {
                throw new IllegalArgumentException("source changed from [" + before.source() + "] to [" + after.source() + "]");
            }
            if (before.includeGlobalState() != after.includeGlobalState()) {
                throw new IllegalArgumentException(
                    "include global state changed from [" + before.includeGlobalState() + "] to [" + after.includeGlobalState() + "]"
                );
            }
            if (before.partial() != after.partial()) {
                throw new IllegalArgumentException("partial changed from [" + before.partial() + "] to [" + after.partial() + "]");
            }
            if (before.featureStates().equals(after.featureStates()) == false) {
                throw new IllegalArgumentException(
                    "feature states changed from " + before.featureStates() + " to " + after.featureStates()
                );
            }
            if (Objects.equals(before.userMetadata(), after.userMetadata()) == false) {
                throw new IllegalArgumentException("user metadata changed from " + before.userMetadata() + " to " + after.userMetadata());
            }
            if (before.version().equals(after.version()) == false) {
                throw new IllegalArgumentException("version changed from " + before.version() + " to " + after.version());
            }
        }

        @Override
        public Entry apply(Entry part) {
            final var updatedIndices = indexByIndexNameDiff.apply(part.indices);
            final var updatedStateByShard = shardsByShardIdDiff.apply(part.shards);
            if (part.isClone() == false && updatedIndices == part.indices && updatedStateByShard == part.shards) {
                // fast path for normal snapshots that avoid rebuilding the by-repo-id map if nothing changed about shard status
                return new Entry(
                    part.snapshot,
                    part.includeGlobalState,
                    part.partial,
                    updatedState,
                    updatedIndices,
                    updatedDataStreams == null ? part.dataStreams : updatedDataStreams,
                    part.featureStates,
                    part.startTime,
                    updatedRepositoryStateId,
                    updatedStateByShard,
                    updatedFailure,
                    part.userMetadata,
                    part.version,
                    null,
                    part.shardStatusByRepoShardId,
                    part.snapshotIndices
                );
            }
            if (part.isClone()) {
                return Entry.createClone(
                    part.snapshot,
                    updatedState,
                    updatedIndices,
                    part.startTime,
                    updatedRepositoryStateId,
                    updatedFailure,
                    part.version,
                    part.source,
                    shardsByRepoShardIdDiff.apply(part.shardStatusByRepoShardId)
                );
            }
            return Entry.snapshot(
                part.snapshot,
                part.includeGlobalState,
                part.partial,
                updatedState,
                updatedIndices,
                updatedDataStreams == null ? part.dataStreams : updatedDataStreams,
                part.featureStates,
                part.startTime,
                updatedRepositoryStateId,
                updatedStateByShard,
                updatedFailure,
                part.userMetadata,
                part.version
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.indexByIndexNameDiff.writeTo(out);
            out.writeByte(this.updatedState.value());
            out.writeLong(this.updatedRepositoryStateId);
            out.writeOptionalStringCollection(updatedDataStreams);
            out.writeOptionalString(updatedFailure);
            shardsByShardIdDiff.writeTo(out);
            out.writeOptionalWriteable(shardsByRepoShardIdDiff);
        }
    }

    private static final class SnapshotInProgressDiff implements NamedDiff<Custom> {

        private final SnapshotsInProgress after;

        private final DiffableUtils.MapDiff<String, ByRepo, Map<String, ByRepo>> mapDiff;

        SnapshotInProgressDiff(SnapshotsInProgress before, SnapshotsInProgress after) {
            this.mapDiff = DiffableUtils.diff(before.entries, after.entries, DiffableUtils.getStringKeySerializer());
            this.after = after;
        }

        SnapshotInProgressDiff(StreamInput in) throws IOException {
            this.mapDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                i -> new ByRepo(i.readList(Entry::readFrom)),
                i -> new ByRepo.ByRepoDiff(
                    DiffableUtils.readJdkMapDiff(i, DiffableUtils.getStringKeySerializer(), Entry::readFrom, EntryDiff::new),
                    DiffableUtils.readJdkMapDiff(i, DiffableUtils.getStringKeySerializer(), ByRepo.INT_DIFF_VALUE_SERIALIZER)
                )
            );
            this.after = null;
        }

        @Override
        public SnapshotsInProgress apply(Custom part) {
            return new SnapshotsInProgress(mapDiff.apply(((SnapshotsInProgress) part).entries));
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert after != null : "should only write instances that were diffed from this node's state";
            if (out.getVersion().onOrAfter(DIFFABLE_VERSION)) {
                mapDiff.writeTo(out);
            } else {
                new SimpleDiffable.CompleteDiff<>(after).writeTo(out);
            }
        }
    }

    /**
     * Wrapper for the list of snapshots per repository to allow for diffing changes in individual entries as well as position changes
     * of entries in the list.
     *
     * @param entries all snapshots executing for a single repository
     */
    private record ByRepo(List<Entry> entries) implements Diffable<ByRepo> {

        static final ByRepo EMPTY = new ByRepo(List.of());
        private static final DiffableUtils.NonDiffableValueSerializer<String, Integer> INT_DIFF_VALUE_SERIALIZER =
            new DiffableUtils.NonDiffableValueSerializer<>() {
                @Override
                public void write(Integer value, StreamOutput out) throws IOException {
                    out.writeVInt(value);
                }

                @Override
                public Integer read(StreamInput in, String key) throws IOException {
                    return in.readVInt();
                }
            };

        private ByRepo(List<Entry> entries) {
            this.entries = List.copyOf(entries);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(entries);
        }

        @Override
        public Diff<ByRepo> diff(ByRepo previousState) {
            return new ByRepoDiff(
                DiffableUtils.diff(toMapByUUID(previousState), toMapByUUID(this), DiffableUtils.getStringKeySerializer()),
                DiffableUtils.diff(
                    toPositionMap(previousState),
                    toPositionMap(this),
                    DiffableUtils.getStringKeySerializer(),
                    INT_DIFF_VALUE_SERIALIZER
                )
            );
        }

        public static Map<String, Integer> toPositionMap(ByRepo part) {
            final Map<String, Integer> res = Maps.newMapWithExpectedSize(part.entries.size());
            for (int i = 0; i < part.entries.size(); i++) {
                final String snapshotUUID = part.entries.get(i).snapshot().getSnapshotId().getUUID();
                assert res.containsKey(snapshotUUID) == false;
                res.put(snapshotUUID, i);
            }
            return res;
        }

        public static Map<String, Entry> toMapByUUID(ByRepo part) {
            final Map<String, Entry> res = Maps.newMapWithExpectedSize(part.entries.size());
            for (Entry entry : part.entries) {
                final String snapshotUUID = entry.snapshot().getSnapshotId().getUUID();
                assert res.containsKey(snapshotUUID) == false;
                res.put(snapshotUUID, entry);
            }
            return res;
        }

        /**
         * @param diffBySnapshotUUID diff of a map of snapshot UUID to snapshot entry
         * @param positionDiff diff of a map with snapshot UUID keys and positions in {@link ByRepo#entries} as values. Used to efficiently
         *                     diff an entry moving to another index in the list
         */
        private record ByRepoDiff(
            DiffableUtils.MapDiff<String, Entry, Map<String, Entry>> diffBySnapshotUUID,
            DiffableUtils.MapDiff<String, Integer, Map<String, Integer>> positionDiff
        ) implements Diff<ByRepo> {

            @Override
            public ByRepo apply(ByRepo part) {
                final var updated = diffBySnapshotUUID.apply(toMapByUUID(part));
                final var updatedPositions = positionDiff.apply(toPositionMap(part));
                final Entry[] arr = new Entry[updated.size()];
                updatedPositions.forEach((uuid, position) -> arr[position] = updated.get(uuid));
                return new ByRepo(List.of(arr));
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                diffBySnapshotUUID.writeTo(out);
                positionDiff.writeTo(out);
            }
        }
    }
}
