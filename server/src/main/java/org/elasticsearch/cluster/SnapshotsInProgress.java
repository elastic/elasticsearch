/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import org.elasticsearch.xcontent.ToXContentObject;
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

    private static final Logger logger = LogManager.getLogger(SnapshotsInProgress.class);

    public static final SnapshotsInProgress EMPTY = new SnapshotsInProgress(Map.of(), Set.of());

    public static final String TYPE = "snapshots";

    public static final String ABORTED_FAILURE_TEXT = "Snapshot was aborted by deletion";

    /** Maps repository name to list of snapshots in that repository */
    private final Map<String, ByRepo> entries;

    /**
     * IDs of nodes which are marked for removal, or which were previously marked for removal and still have running shard snapshots.
     */
    // When a node is marked for removal it pauses all its shard snapshots as promptly as possible. When each shard snapshot pauses it
    // enters state PAUSED_FOR_NODE_REMOVAL to allow the shard to move to a different node where its snapshot can resume. However, if the
    // removal marker is deleted before the node shuts down then we need to make sure to resume the snapshots of any remaining shards, which
    // we do by moving all those PAUSED_FOR_NODE_REMOVAL shards back to state INIT. The problem is that the data node needs to be able to
    // distinguish an INIT shard whose snapshot was successfully paused and now needs to be resumed from an INIT shard whose move to state
    // PAUSED_FOR_NODE_REMOVAL has not yet been processed on the master: the latter kind of shard will move back to PAUSED_FOR_NODE_REMOVAL
    // in a subsequent update and so shouldn't be resumed. The solution is to wait for all the shards on the previously-shutting-down node
    // to finish pausing before resuming any of them. We do this by tracking the nodes in this field, avoiding moving any shards back to
    // state INIT while the node appears in this set and, conversely, we only remove nodes from this set when none of their shards are in
    // INIT state.
    private final Set<String> nodesIdsForRemoval;

    /**
     * Returns the SnapshotInProgress metadata present within the given cluster state.
     */
    public static SnapshotsInProgress get(ClusterState state) {
        return state.custom(TYPE, EMPTY);
    }

    public SnapshotsInProgress(StreamInput in) throws IOException {
        this(collectByRepo(in), readNodeIdsForRemoval(in));
    }

    private static Set<String> readNodeIdsForRemoval(StreamInput in) throws IOException {
        return in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)
            ? in.readCollectionAsImmutableSet(StreamInput::readString)
            : Set.of();
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

    private SnapshotsInProgress(Map<String, ByRepo> entries, Set<String> nodesIdsForRemoval) {
        this.entries = Map.copyOf(entries);
        this.nodesIdsForRemoval = nodesIdsForRemoval;
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
        return new SnapshotsInProgress(copy, nodesIdsForRemoval);
    }

    public SnapshotsInProgress withAddedEntry(Entry entry) {
        final List<Entry> forRepo = new ArrayList<>(forRepo(entry.repository()));
        forRepo.add(entry);
        return withUpdatedEntriesForRepo(entry.repository(), forRepo);
    }

    /**
     * Returns the list of snapshots in the specified repository.
     */
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
        return () -> Iterators.map(entries.values().iterator(), byRepo -> byRepo.entries);
    }

    public Stream<Entry> asStream() {
        return entries.values().stream().flatMap(t -> t.entries.stream());
    }

    @Nullable
    public Entry snapshot(final Snapshot snapshot) {
        return findSnapshotInList(snapshot, forRepo(snapshot.getRepository()));
    }

    /**
     * Searches for a particular {@code snapshotToFind} in the given snapshot list.
     * @return a matching snapshot entry or null.
     */
    @Nullable
    private static Entry findSnapshotInList(Snapshot snapshotToFind, List<Entry> forRepo) {
        for (Entry entry : forRepo) {
            final Snapshot snapshot = entry.snapshot();
            if (snapshot.equals(snapshotToFind)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Computes a map of repository shard id to set of shard generations, containing all shard generations that became obsolete and may be
     * deleted from the repository as the cluster state moves from the given old value of {@link SnapshotsInProgress} to this instance.
     * <p>
     * An unique shard generation is created for every in-progress shard snapshot. The shard generation file contains information about all
     * the files needed by pre-existing and any new shard snapshots that were in-progress. When a shard snapshot is finalized, its file list
     * is promoted to the official shard snapshot list for the index shard. This final list will contain metadata about any other
     * in-progress shard snapshots that were not yet finalized when it began. All these other in-progress shard snapshot lists are scheduled
     * for deletion now.
     */
    public Map<RepositoryShardId, Set<ShardGeneration>> obsoleteGenerations(
        String repository,
        SnapshotsInProgress oldClusterStateSnapshots
    ) {
        final Map<RepositoryShardId, Set<ShardGeneration>> obsoleteGenerations = new HashMap<>();
        final List<Entry> latestSnapshots = forRepo(repository);

        for (Entry oldEntry : oldClusterStateSnapshots.forRepo(repository)) {
            final Entry matchingLatestEntry = findSnapshotInList(oldEntry.snapshot(), latestSnapshots);
            if (matchingLatestEntry == null || matchingLatestEntry == oldEntry) {
                // The snapshot progress has not changed.
                continue;
            }
            for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> oldShardAssignment : oldEntry.shardSnapshotStatusByRepoShardId()
                .entrySet()) {
                final RepositoryShardId repositoryShardId = oldShardAssignment.getKey();
                final ShardSnapshotStatus oldStatus = oldShardAssignment.getValue();
                final ShardSnapshotStatus newStatus = matchingLatestEntry.shardSnapshotStatusByRepoShardId().get(repositoryShardId);
                if (oldStatus.state == ShardState.SUCCESS
                    && oldStatus.generation() != null
                    && newStatus != null
                    && newStatus.state() == ShardState.SUCCESS
                    && newStatus.generation() != null
                    && oldStatus.generation().equals(newStatus.generation()) == false) {
                    // We moved from a non-null successful generation to a different non-null successful generation
                    // so the original generation is obsolete because it was in-flight before and is now unreferenced.
                    obsoleteGenerations.computeIfAbsent(repositoryShardId, ignored -> new HashSet<>()).add(oldStatus.generation());
                    logger.debug(
                        """
                            Marking shard generation [{}] file for cleanup. The finalized shard generation is now [{}], for shard \
                            snapshot [{}] with shard ID [{}] on node [{}]
                            """,
                        oldStatus.generation(),
                        newStatus.generation(),
                        oldEntry.snapshot(),
                        repositoryShardId.shardId(),
                        oldStatus.nodeId()
                    );
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
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
    }

    private static final TransportVersion DIFFABLE_VERSION = TransportVersions.V_8_5_0;

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(DIFFABLE_VERSION)) {
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeStringCollection(nodesIdsForRemoval);
        } else {
            assert nodesIdsForRemoval.isEmpty() : nodesIdsForRemoval;
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            ChunkedToXContentHelper.startArray("snapshots"),
            asStream().iterator(),
            ChunkedToXContentHelper.endArray(),
            ChunkedToXContentHelper.startArray("node_ids_for_removal"),
            Iterators.map(nodesIdsForRemoval.iterator(), s -> (builder, params) -> builder.value(s)),
            ChunkedToXContentHelper.endArray()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final var other = (SnapshotsInProgress) o;
        return nodesIdsForRemoval.equals(other.nodesIdsForRemoval) && entries.equals(other.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries, nodesIdsForRemoval);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("SnapshotsInProgress[entries=[");
        final Iterator<SnapshotsInProgress.Entry> entryList = asStream().iterator();
        boolean firstEntry = true;
        while (entryList.hasNext()) {
            if (firstEntry == false) {
                builder.append(",");
            }
            builder.append(entryList.next().snapshot().getSnapshotId().getName());
            firstEntry = false;
        }
        return builder.append("],nodeIdsForRemoval=").append(nodesIdsForRemoval).append("]").toString();
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
        IndexVersion version,
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
        IndexVersion version
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

    public boolean isNodeIdForRemoval(String nodeId) {
        return nodeId != null && nodesIdsForRemoval.contains(nodeId);
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
                for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> shard : entry.shardSnapshotStatusByRepoShardId().entrySet()) {
                    final RepositoryShardId sid = shard.getKey();
                    final ShardSnapshotStatus shardSnapshotStatus = shard.getValue();
                    assert assertShardStateConsistent(
                        entriesForRepository,
                        assignedShards,
                        queuedShards,
                        sid.indexName(),
                        sid.shardId(),
                        shardSnapshotStatus
                    );

                    assert entry.state() != State.ABORTED
                        || shardSnapshotStatus.state == ShardState.ABORTED
                        || shardSnapshotStatus.state().completed()
                        : sid + " is in state " + shardSnapshotStatus.state() + " in aborted snapshot " + entry.snapshot;
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

    /**
     * Adds any new node IDs to {@link #nodesIdsForRemoval}, and removes any node IDs that are no longer marked for shutdown if they have no
     * running shard snapshots.
     */
    public SnapshotsInProgress withUpdatedNodeIdsForRemoval(ClusterState clusterState) {
        assert clusterState.getMinTransportVersion().onOrAfter(TransportVersions.V_8_13_0);

        final var updatedNodeIdsForRemoval = new HashSet<>(nodesIdsForRemoval);

        final var nodeIdsMarkedForRemoval = getNodesIdsMarkedForRemoval(clusterState);

        // add any nodes newly marked for removal
        updatedNodeIdsForRemoval.addAll(nodeIdsMarkedForRemoval);

        // remove any nodes which are no longer marked for shutdown if they have no running shard snapshots
        var restoredNodeIds = getObsoleteNodeIdsForRemoval(nodeIdsMarkedForRemoval);
        updatedNodeIdsForRemoval.removeAll(restoredNodeIds);
        logger.debug("Resuming shard snapshots on nodes [{}]", restoredNodeIds);

        if (updatedNodeIdsForRemoval.equals(nodesIdsForRemoval)) {
            return this;
        } else {
            return new SnapshotsInProgress(entries, Collections.unmodifiableSet(updatedNodeIdsForRemoval));
        }
    }

    private static Set<String> getNodesIdsMarkedForRemoval(ClusterState clusterState) {
        final var nodesShutdownMetadata = clusterState.metadata().nodeShutdowns();
        final var shutdownMetadataCount = nodesShutdownMetadata.getAllNodeIds().size();
        if (shutdownMetadataCount == 0) {
            return Set.of();
        }

        final Set<String> result = Sets.newHashSetWithExpectedSize(shutdownMetadataCount);
        for (final var entry : nodesShutdownMetadata.getAll().entrySet()) {
            if (entry.getValue().getType() != SingleNodeShutdownMetadata.Type.RESTART) {
                // Only pause the snapshot when the node is being removed (to let shards vacate) and not when it is restarting in place. If
                // it is restarting and there are replicas to promote then we need #71333 to move the shard snapshot over; if there are no
                // replicas then we do not expect the restart to be graceful so a PARTIAL or FAILED snapshot is ok.
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * Identifies any nodes that are no longer marked for removal AND have no running shard snapshots.
     * @param latestNodeIdsMarkedForRemoval the current nodes marked for removal in the cluster state.
     */
    private Set<String> getObsoleteNodeIdsForRemoval(Set<String> latestNodeIdsMarkedForRemoval) {
        // Find any nodes no longer marked for removal.
        final var nodeIdsNoLongerMarkedForRemoval = new HashSet<>(nodesIdsForRemoval);
        nodeIdsNoLongerMarkedForRemoval.removeIf(latestNodeIdsMarkedForRemoval::contains);
        if (nodeIdsNoLongerMarkedForRemoval.isEmpty()) {
            return Set.of();
        }
        // If any nodes have INIT state shard snapshots, then the node's snapshots are not concurrency safe to resume yet. All shard
        // snapshots on a newly revived node (no longer marked for shutdown) must finish moving to paused before any can resume.
        for (final var byRepo : entries.values()) {
            for (final var entry : byRepo.entries()) {
                if (entry.state() == State.STARTED && entry.hasShardsInInitState()) {
                    for (final var shardSnapshotStatus : entry.shards().values()) {
                        if (shardSnapshotStatus.state() == ShardState.INIT) {
                            nodeIdsNoLongerMarkedForRemoval.remove(shardSnapshotStatus.nodeId());
                            if (nodeIdsNoLongerMarkedForRemoval.isEmpty()) {
                                return Set.of();
                            }
                        }
                    }
                }
            }
        }
        return nodeIdsNoLongerMarkedForRemoval;
    }

    public boolean nodeIdsForRemovalChanged(SnapshotsInProgress other) {
        return nodesIdsForRemoval.equals(other.nodesIdsForRemoval) == false;
    }

    /**
     * The current stage/phase of the shard snapshot, and whether it has completed or failed.
     */
    public enum ShardState {
        INIT((byte) 0, false, false),
        SUCCESS((byte) 2, true, false),
        FAILED((byte) 3, true, true),
        ABORTED((byte) 4, false, true),
        /**
         * Shard primary is unassigned and shard cannot be snapshotted.
         */
        MISSING((byte) 5, true, true),
        /**
         * Shard snapshot is waiting for the primary to snapshot to become available.
         */
        WAITING((byte) 6, false, false),
        /**
         * Shard snapshot is waiting for another shard snapshot for the same shard and to the same repository to finish.
         */
        QUEUED((byte) 7, false, false),
        /**
         * Primary shard is assigned to a node which is marked for removal from the cluster (or which was previously marked for removal and
         * we're still waiting for its other shards to pause).
         */
        PAUSED_FOR_NODE_REMOVAL((byte) 8, false, false);

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
                case 8 -> PAUSED_FOR_NODE_REMOVAL;
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

    /**
     * @param nodeId node snapshotting the shard
     * @param state the current phase of the snapshot
     * @param generation shard generation ID identifying a particular snapshot of a shard
     * @param reason what initiated the shard snapshot
     * @param shardSnapshotResult only set if the snapshot has been successful, contains information for the shard finalization phase
     */
    public record ShardSnapshotStatus(
        @Nullable String nodeId,
        ShardState state,
        @Nullable ShardGeneration generation,
        @Nullable String reason,
        @Nullable // only present in state SUCCESS; may be null even in SUCCESS if this state came over the wire from an older node
        ShardSnapshotResult shardSnapshotResult
    ) implements Writeable {

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
            null,
            "missing index"
        );

        /**
         * Initializes status with state {@link ShardState#INIT}.
         */
        public ShardSnapshotStatus(String nodeId, ShardGeneration generation) {
            this(nodeId, ShardState.INIT, generation);
        }

        public ShardSnapshotStatus(@Nullable String nodeId, ShardState state, @Nullable ShardGeneration generation) {
            this(nodeId, assertNotSuccess(state), generation, null);
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public ShardSnapshotStatus(@Nullable String nodeId, ShardState state, @Nullable ShardGeneration generation, String reason) {
            this(nodeId, assertNotSuccess(state), generation, reason, null);
        }

        private static ShardState assertNotSuccess(ShardState shardState) {
            assert shardState != ShardState.SUCCESS : "use ShardSnapshotStatus#success";
            return shardState;
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public static ShardSnapshotStatus success(String nodeId, ShardSnapshotResult shardSnapshotResult) {
            return new ShardSnapshotStatus(nodeId, ShardState.SUCCESS, shardSnapshotResult.getGeneration(), null, shardSnapshotResult);
        }

        public ShardSnapshotStatus(
            @Nullable String nodeId,
            ShardState state,
            @Nullable ShardGeneration generation,
            String reason,
            @Nullable ShardSnapshotResult shardSnapshotResult
        ) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
            this.generation = generation;
            this.shardSnapshotResult = shardSnapshotResult;
            assert assertConsistent();
        }

        private boolean assertConsistent() {
            // If the state is failed we have to have a reason for this failure
            assert state.failed() == false || reason != null;
            assert (state != ShardState.INIT && state != ShardState.WAITING && state != ShardState.PAUSED_FOR_NODE_REMOVAL)
                || nodeId != null : "Null node id for state [" + state + "]";
            assert state != ShardState.QUEUED || (nodeId == null && generation == null && reason == null)
                : "Found unexpected non-null values for queued state shard nodeId[" + nodeId + "][" + generation + "][" + reason + "]";
            assert state == ShardState.SUCCESS || shardSnapshotResult == null;
            assert shardSnapshotResult == null || shardSnapshotResult.getGeneration().equals(generation)
                : "generation [" + generation + "] does not match result generation [" + shardSnapshotResult.getGeneration() + "]";
            return true;
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public static ShardSnapshotStatus readFrom(StreamInput in) throws IOException {
            final String nodeId = DiscoveryNode.deduplicateNodeIdentifier(in.readOptionalString());
            final ShardState state = ShardState.fromValue(in.readByte());
            final ShardGeneration generation = in.readOptionalWriteable(ShardGeneration::new);
            final String reason = in.readOptionalString();
            final ShardSnapshotResult shardSnapshotResult = in.readOptionalWriteable(ShardSnapshotResult::new);
            if (state == ShardState.QUEUED) {
                return UNASSIGNED_QUEUED;
            }
            return new ShardSnapshotStatus(nodeId, state, generation, reason, shardSnapshotResult);
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public ShardSnapshotStatus withUpdatedGeneration(ShardGeneration newGeneration) {
            assert state == ShardState.SUCCESS : "can't move generation in state " + state;
            return new ShardSnapshotStatus(
                nodeId,
                state,
                newGeneration,
                reason,
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
         * ({@link ShardState#INIT} or {@link ShardState#ABORTED}) or about to write to it in state {@link ShardState#WAITING} or
         * {@link ShardState#PAUSED_FOR_NODE_REMOVAL}.
         */
        public boolean isActive() {
            return switch (state) {
                case INIT, ABORTED, WAITING, PAUSED_FOR_NODE_REMOVAL -> true;
                case SUCCESS, FAILED, MISSING, QUEUED -> false;
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalWriteable(generation);
            out.writeOptionalString(reason);
            out.writeOptionalWriteable(shardSnapshotResult);
        }
    }

    public static class Entry implements Writeable, ToXContentObject, RepositoryOperation, Diffable<Entry> {
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
        private final IndexVersion version;

        /**
         * Source snapshot if this is a clone operation or {@code null} if this is a snapshot.
         */
        @Nullable
        private final SnapshotId source;

        /**
         * Map of {@link RepositoryShardId} to {@link ShardSnapshotStatus} tracking the state of each shard operation in this snapshot.
         */
        private final Map<RepositoryShardId, ShardSnapshotStatus> shardStatusByRepoShardId;

        @Nullable
        private final Map<String, Object> userMetadata;
        @Nullable
        private final String failure;

        /**
         * Flag set to true in case any of the shard snapshots in {@link #shards} are in state {@link ShardState#INIT}.
         * This is used by data nodes to determine if there is any work to be done on a snapshot by them without having to iterate
         * the full {@link #shards} map.
         */
        private final boolean hasShardsInInitState;

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
            IndexVersion version
        ) {
            final Map<String, Index> res = Maps.newMapWithExpectedSize(indices.size());
            final Map<RepositoryShardId, ShardSnapshotStatus> byRepoShardIdBuilder = Maps.newHashMapWithExpectedSize(shards.size());
            boolean hasInitStateShards = false;
            for (Map.Entry<ShardId, ShardSnapshotStatus> entry : shards.entrySet()) {
                final ShardId shardId = entry.getKey();
                final IndexId indexId = indices.get(shardId.getIndexName());
                final Index index = shardId.getIndex();
                final Index existing = res.put(indexId.getName(), index);
                assert existing == null || existing.equals(index) : "Conflicting indices [" + existing + "] and [" + index + "]";
                final var shardSnapshotStatus = entry.getValue();
                hasInitStateShards |= shardSnapshotStatus.state() == ShardState.INIT;
                byRepoShardIdBuilder.put(new RepositoryShardId(indexId, shardId.id()), shardSnapshotStatus);
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
                res,
                hasInitStateShards
            );
        }

        private static Entry createClone(
            Snapshot snapshot,
            State state,
            Map<String, IndexId> indices,
            long startTime,
            long repositoryStateId,
            String failure,
            IndexVersion version,
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
                Map.of(),
                false
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
            IndexVersion version,
            @Nullable SnapshotId source,
            Map<RepositoryShardId, ShardSnapshotStatus> shardStatusByRepoShardId,
            Map<String, Index> snapshotIndices,
            boolean hasShardsInInitState
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
            this.hasShardsInInitState = hasShardsInInitState;
            assert assertShardsConsistent(
                this.source,
                this.state,
                this.indices,
                this.shards,
                this.shardStatusByRepoShardId,
                this.hasShardsInInitState
            );
        }

        private static Entry readFrom(StreamInput in) throws IOException {
            final Snapshot snapshot = new Snapshot(in);
            final boolean includeGlobalState = in.readBoolean();
            final boolean partial = in.readBoolean();
            final State state = State.fromValue(in.readByte());
            final Map<String, IndexId> indices = in.readMapValues(IndexId::new, IndexId::getName);
            final long startTime = in.readLong();
            final Map<ShardId, ShardSnapshotStatus> shards = in.readImmutableMap(ShardId::new, ShardSnapshotStatus::readFrom);
            final long repositoryStateId = in.readLong();
            final String failure = in.readOptionalString();
            final Map<String, Object> userMetadata = in.readGenericMap();
            final IndexVersion version = IndexVersion.readVersion(in);
            final List<String> dataStreams = in.readStringCollectionAsImmutableList();
            final SnapshotId source = in.readOptionalWriteable(SnapshotId::new);
            final Map<RepositoryShardId, ShardSnapshotStatus> clones = in.readImmutableMap(
                RepositoryShardId::readFrom,
                ShardSnapshotStatus::readFrom
            );
            final List<SnapshotFeatureInfo> featureStates = in.readCollectionAsImmutableList(SnapshotFeatureInfo::new);
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
            Map<RepositoryShardId, ShardSnapshotStatus> statusByRepoShardId,
            boolean hasInitStateShards
        ) {
            if ((state == State.INIT || state == State.ABORTED) && shards.isEmpty()) {
                return true;
            }
            if (hasInitStateShards) {
                assert state == State.STARTED : "shouldn't have INIT-state shards in state " + state;
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
                boolean foundInitStateShard = false;
                for (Map.Entry<ShardId, ShardSnapshotStatus> entry : shards.entrySet()) {
                    foundInitStateShard |= entry.getValue().state() == ShardState.INIT;
                    final ShardId routingShardId = entry.getKey();
                    assert statusByRepoShardId.get(
                        new RepositoryShardId(indices.get(routingShardId.getIndexName()), routingShardId.id())
                    ) == entry.getValue() : "found inconsistent values tracked by routing- and repository shard id";
                }
                assert foundInitStateShard == hasInitStateShards : "init shard state flag does not match shard states";
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
                snapshotIndices,
                hasShardsInInitState
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
                        status.generation(),
                        "aborted by snapshot deletion"
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
            assert updated.state().completed() == false && completed(updated.shardSnapshotStatusByRepoShardId().values()) == false
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

        /**
         * Returns a map of shards to their snapshot status.
         */
        public Map<RepositoryShardId, ShardSnapshotStatus> shardSnapshotStatusByRepoShardId() {
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

        /**
         * See {@link #hasShardsInInitState}.
         * @return true if this entry can contain shard snapshots that have yet to be started on a data node.
         */
        public boolean hasShardsInInitState() {
            return hasShardsInInitState;
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
        public IndexVersion version() {
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
            IndexVersion.writeVersion(version, out);
            out.writeStringCollection(dataStreams);
            out.writeOptionalWriteable(source);
            if (source == null) {
                out.writeMap(Map.of());
            } else {
                out.writeMap(shardStatusByRepoShardId);
            }
            out.writeCollection(featureStates);
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
                    return RepositoryShardId.readFrom(in);
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
            this.updatedDataStreams = in.readOptionalStringCollectionAsList();
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
                    part.snapshotIndices,
                    part.hasShardsInInitState
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
        private final Set<String> nodeIdsForRemoval;

        SnapshotInProgressDiff(SnapshotsInProgress before, SnapshotsInProgress after) {
            this.mapDiff = DiffableUtils.diff(before.entries, after.entries, DiffableUtils.getStringKeySerializer());
            this.nodeIdsForRemoval = after.nodesIdsForRemoval;
            this.after = after;
        }

        SnapshotInProgressDiff(StreamInput in) throws IOException {
            this.mapDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                i -> new ByRepo(i.readCollectionAsImmutableList(Entry::readFrom)),
                i -> new ByRepo.ByRepoDiff(
                    DiffableUtils.readJdkMapDiff(i, DiffableUtils.getStringKeySerializer(), Entry::readFrom, EntryDiff::new),
                    DiffableUtils.readJdkMapDiff(i, DiffableUtils.getStringKeySerializer(), ByRepo.INT_DIFF_VALUE_SERIALIZER)
                )
            );
            this.nodeIdsForRemoval = readNodeIdsForRemoval(in);
            this.after = null;
        }

        @Override
        public SnapshotsInProgress apply(Custom part) {
            final var snapshotsInProgress = (SnapshotsInProgress) part;
            return new SnapshotsInProgress(mapDiff.apply(snapshotsInProgress.entries), this.nodeIdsForRemoval);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.MINIMUM_COMPATIBLE;
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert after != null : "should only write instances that were diffed from this node's state";
            if (out.getTransportVersion().onOrAfter(DIFFABLE_VERSION)) {
                mapDiff.writeTo(out);
            } else {
                new SimpleDiffable.CompleteDiff<>(after).writeTo(out);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                out.writeStringCollection(nodeIdsForRemoval);
            } else {
                assert nodeIdsForRemoval.isEmpty() : nodeIdsForRemoval;
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
            out.writeCollection(entries);
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
