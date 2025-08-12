/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.repositories.ProjectRepo.projectRepoString;
import static org.elasticsearch.snapshots.SnapshotsService.FILE_INFO_WRITER_UUIDS_IN_SHARD_DATA_VERSION;
import static org.elasticsearch.snapshots.SnapshotsService.INDEX_GEN_IN_REPO_DATA_VERSION;
import static org.elasticsearch.snapshots.SnapshotsService.OLD_SNAPSHOT_FORMAT;
import static org.elasticsearch.snapshots.SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION;
import static org.elasticsearch.snapshots.SnapshotsService.UUIDS_IN_REPO_DATA_VERSION;

/**
 * A utility class for static snapshotting methods.
 */
public class SnapshotsServiceUtils {
    private static final Logger logger = LogManager.getLogger(SnapshotsServiceUtils.class);

    public static void ensureSnapshotNameNotRunning(
        SnapshotsInProgress runningSnapshots,
        ProjectId projectId,
        String repositoryName,
        String snapshotName
    ) {
        if (runningSnapshots.forRepo(projectId, repositoryName)
            .stream()
            .anyMatch(s -> s.snapshot().getSnapshotId().getName().equals(snapshotName))) {
            throw new SnapshotNameAlreadyInUseException(repositoryName, snapshotName, "snapshot with the same name is already in-progress");
        }
    }

    /**
     * Checks the cluster state for any in-progress repository cleanup tasks ({@link RepositoryCleanupInProgress}).
     * Note that repository cleanup is intentionally cluster wide exclusive.
     */
    public static void ensureNoCleanupInProgress(
        final ClusterState currentState,
        final String repositoryName,
        final String snapshotName,
        final String reason
    ) {
        final RepositoryCleanupInProgress repositoryCleanupInProgress = RepositoryCleanupInProgress.get(currentState);
        if (repositoryCleanupInProgress.hasCleanupInProgress()) {
            throw new ConcurrentSnapshotExecutionException(
                repositoryName,
                snapshotName,
                "cannot "
                    + reason
                    + " while a repository cleanup is in-progress in "
                    + repositoryCleanupInProgress.entries()
                        .stream()
                        .map(RepositoryCleanupInProgress.Entry::repository)
                        .collect(Collectors.toSet())
            );
        }
    }

    public static void ensureNotReadOnly(final ProjectMetadata projectMetadata, final String repositoryName) {
        final var repositoryMetadata = RepositoriesMetadata.get(projectMetadata).repository(repositoryName);
        if (RepositoriesService.isReadOnly(repositoryMetadata.settings())) {
            throw new RepositoryException(repositoryMetadata.name(), "repository is readonly");
        }
    }

    public static void ensureSnapshotNameAvailableInRepo(RepositoryData repositoryData, String snapshotName, Repository repository) {
        // check if the snapshot name already exists in the repository
        if (repositoryData.getSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
            throw new SnapshotNameAlreadyInUseException(
                repository.getMetadata().name(),
                snapshotName,
                "snapshot with the same name already exists"
            );
        }
    }

    /**
     * Throws {@link RepositoryMissingException} if no repository by the given name is found in the given cluster state.
     */
    public static void ensureRepositoryExists(String repoName, ProjectMetadata projectMetadata) {
        if (RepositoriesMetadata.get(projectMetadata).repository(repoName) == null) {
            throw new RepositoryMissingException(repoName);
        }
    }

    /**
     * Validates snapshot request
     *
     * @param repositoryName repository name
     * @param snapshotName snapshot name
     * @param projectMetadata   current project metadata
     */
    public static void validate(String repositoryName, String snapshotName, ProjectMetadata projectMetadata) {
        if (RepositoriesMetadata.get(projectMetadata).repository(repositoryName) == null) {
            throw new RepositoryMissingException(repositoryName);
        }
        validate(repositoryName, snapshotName);
    }

    public static void validate(final String repositoryName, final String snapshotName) {
        if (Strings.hasLength(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "cannot be empty");
        }
        if (snapshotName.contains(" ")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain whitespace");
        }
        if (snapshotName.contains(",")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain ','");
        }
        if (snapshotName.contains("#")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain '#'");
        }
        if (snapshotName.charAt(0) == '_') {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not start with '_'");
        }
        if (snapshotName.toLowerCase(Locale.ROOT).equals(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must be lowercase");
        }
        if (Strings.validFileName(snapshotName) == false) {
            throw new InvalidSnapshotNameException(
                repositoryName,
                snapshotName,
                "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS
            );
        }
    }

    /**
     * Assert that there are no snapshots that have a shard that is waiting to be assigned even though the cluster state would allow for it
     * to be assigned
     */
    public static boolean assertNoDanglingSnapshots(ClusterState state) {
        final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(state);
        final SnapshotDeletionsInProgress snapshotDeletionsInProgress = SnapshotDeletionsInProgress.get(state);
        final Set<ProjectRepo> reposWithRunningDelete = snapshotDeletionsInProgress.getEntries()
            .stream()
            .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
            .map(entry -> new ProjectRepo(entry.projectId(), entry.repository()))
            .collect(Collectors.toSet());
        for (List<SnapshotsInProgress.Entry> repoEntry : snapshotsInProgress.entriesByRepo()) {
            final SnapshotsInProgress.Entry entry = repoEntry.get(0);
            for (SnapshotsInProgress.ShardSnapshotStatus value : entry.shardSnapshotStatusByRepoShardId().values()) {
                if (value.equals(SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)) {
                    assert reposWithRunningDelete.contains(new ProjectRepo(entry.projectId(), entry.repository()))
                        : "Found shard snapshot waiting to be assigned in [" + entry + "] but it is not blocked by any running delete";
                } else if (value.isActive()) {
                    assert reposWithRunningDelete.contains(new ProjectRepo(entry.projectId(), entry.repository())) == false
                        : "Found shard snapshot actively executing in ["
                            + entry
                            + "] when it should be blocked by a running delete ["
                            + Strings.toString(snapshotDeletionsInProgress)
                            + "]";
                }
            }
        }
        return true;
    }

    /**
     * Checks whether the metadata version supports writing {@link ShardGenerations} to the repository.
     *
     * @param repositoryMetaVersion version to check
     * @return true if version supports {@link ShardGenerations}
     */
    public static boolean useShardGenerations(IndexVersion repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(SHARD_GEN_IN_REPO_DATA_VERSION);
    }

    /**
     * Checks whether the metadata version supports writing {@link ShardGenerations} to the repository.
     *
     * @param repositoryMetaVersion version to check
     * @return true if version supports {@link ShardGenerations}
     */
    public static boolean useIndexGenerations(IndexVersion repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(INDEX_GEN_IN_REPO_DATA_VERSION);
    }

    /**
     * Checks whether the metadata version supports writing the cluster- and repository-uuid to the repository.
     *
     * @param repositoryMetaVersion version to check
     * @return true if version supports writing cluster- and repository-uuid to the repository
     */
    public static boolean includesUUIDs(IndexVersion repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(UUIDS_IN_REPO_DATA_VERSION);
    }

    public static boolean includeFileInfoWriterUUID(IndexVersion repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(FILE_INFO_WRITER_UUIDS_IN_SHARD_DATA_VERSION);
    }

    public static boolean supportsNodeRemovalTracking(ClusterState clusterState) {
        return clusterState.getMinTransportVersion().onOrAfter(TransportVersions.V_8_13_0);
    }

    /**
     * Checks if the given {@link SnapshotsInProgress.Entry} is currently writing to the repository.
     *
     * @param entry snapshot entry
     * @return true if entry is currently writing to the repository
     */
    public static boolean isWritingToRepository(SnapshotsInProgress.Entry entry) {
        if (entry.state().completed()) {
            // Entry is writing to the repo because it's finalizing on master
            return true;
        }
        for (SnapshotsInProgress.ShardSnapshotStatus value : entry.shardSnapshotStatusByRepoShardId().values()) {
            if (value.isActive()) {
                // Entry is writing to the repo because it's writing to a shard on a data node or waiting to do so for a concrete shard
                return true;
            }
        }
        return false;
    }

    public static boolean isQueued(@Nullable SnapshotsInProgress.ShardSnapshotStatus status) {
        return status != null && status.state() == SnapshotsInProgress.ShardState.QUEUED;
    }

    public static FinalizeSnapshotContext.UpdatedShardGenerations buildGenerations(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        ShardGenerations.Builder builder = ShardGenerations.builder();
        ShardGenerations.Builder deletedBuilder = null;
        if (snapshot.isClone()) {
            snapshot.shardSnapshotStatusByRepoShardId().forEach((key, value) -> builder.put(key.index(), key.shardId(), value));
        } else {
            for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> entry : snapshot.shardSnapshotStatusByRepoShardId()
                .entrySet()) {
                RepositoryShardId key = entry.getKey();
                SnapshotsInProgress.ShardSnapshotStatus value = entry.getValue();
                final Index index = snapshot.indexByName(key.indexName());
                if (metadata.getProject(snapshot.projectId()).hasIndex(index) == false) {
                    assert snapshot.partial() : "Index [" + index + "] was deleted during a snapshot but snapshot was not partial.";
                    if (deletedBuilder == null) {
                        deletedBuilder = ShardGenerations.builder();
                    }
                    deletedBuilder.put(key.index(), key.shardId(), value);
                    continue;
                }
                builder.put(key.index(), key.shardId(), value);
            }
        }
        return new FinalizeSnapshotContext.UpdatedShardGenerations(
            builder.build(),
            deletedBuilder == null ? ShardGenerations.EMPTY : deletedBuilder.build()
        );
    }

    public static ProjectMetadata projectForSnapshot(SnapshotsInProgress.Entry snapshot, ProjectMetadata project) {
        final ProjectMetadata.Builder builder;
        if (snapshot.includeGlobalState() == false) {
            // Create a new project state that only includes the index data
            builder = ProjectMetadata.builder(project.id());
            for (IndexId index : snapshot.indices().values()) {
                final IndexMetadata indexMetadata = project.index(index.getName());
                if (indexMetadata == null) {
                    assert snapshot.partial() : "Index [" + index + "] was deleted during a snapshot but snapshot was not partial.";
                } else {
                    builder.put(indexMetadata, false);
                }
            }
        } else {
            builder = ProjectMetadata.builder(project);
        }
        // Only keep those data streams in the metadata that were actually requested by the initial snapshot create operation and that have
        // all their indices contained in the snapshot
        final Map<String, DataStream> dataStreams = new HashMap<>();
        final Set<String> indicesInSnapshot = snapshot.indices().keySet();
        for (String dataStreamName : snapshot.dataStreams()) {
            DataStream dataStream = project.dataStreams().get(dataStreamName);
            if (dataStream == null) {
                assert snapshot.partial()
                    : "Data stream [" + dataStreamName + "] was deleted during a snapshot but snapshot was not partial.";
            } else {
                final DataStream reconciled = dataStream.snapshot(indicesInSnapshot, builder);
                if (reconciled != null) {
                    dataStreams.put(dataStreamName, reconciled);
                }
            }
        }
        return builder.dataStreams(dataStreams, filterDataStreamAliases(dataStreams, project.dataStreamAliases())).build();
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on master node
     * </p>
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param projectId           project to look for the repository
     * @param repository          repository id
     * @param snapshots           list of snapshots that will be used as a filter, empty list means no snapshots are filtered
     * @return list of metadata for currently running snapshots
     */
    public static List<SnapshotsInProgress.Entry> currentSnapshots(
        @Nullable SnapshotsInProgress snapshotsInProgress,
        ProjectId projectId,
        String repository,
        List<String> snapshots
    ) {
        if (snapshotsInProgress == null || snapshotsInProgress.isEmpty()) {
            return Collections.emptyList();
        }
        if ("_all".equals(repository)) {
            return snapshotsInProgress.asStream(projectId).toList();
        }
        if (snapshots.isEmpty()) {
            return snapshotsInProgress.forRepo(projectId, repository);
        }
        List<SnapshotsInProgress.Entry> builder = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(projectId, repository)) {
            for (String snapshot : snapshots) {
                if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                    builder.add(entry);
                    break;
                }
            }
        }
        return unmodifiableList(builder);
    }

    /**
     * Walks through the snapshot entries' shard snapshots and creates applies updates from looking at removed nodes or indexes and known
     * failed shard snapshots on the same shard IDs.
     *
     * @param nodeIdRemovalPredicate identify any nodes that are marked for removal / in shutdown mode
     * @param knownFailures already known failed shard snapshots, but more may be found in this method
     * @return an updated map of shard statuses
     */
    public static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> processWaitingShardsAndRemovedNodes(
        SnapshotsInProgress.Entry snapshotEntry,
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        Predicate<String> nodeIdRemovalPredicate,
        Map<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> knownFailures
    ) {
        assert snapshotEntry.isClone() == false : "clones take a different path";
        boolean snapshotChanged = false;
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
        for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> shardSnapshotEntry : snapshotEntry
            .shardSnapshotStatusByRepoShardId()
            .entrySet()) {
            SnapshotsInProgress.ShardSnapshotStatus shardStatus = shardSnapshotEntry.getValue();
            ShardId shardId = snapshotEntry.shardId(shardSnapshotEntry.getKey());
            if (shardStatus.equals(SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)) {
                // this shard snapshot is waiting for a previous snapshot to finish execution for this shard
                final SnapshotsInProgress.ShardSnapshotStatus knownFailure = knownFailures.get(shardSnapshotEntry.getKey());
                if (knownFailure == null) {
                    final IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                    if (indexShardRoutingTable == null) {
                        // shard became unassigned while queued after a delete or clone operation so we can fail as missing here
                        assert snapshotEntry.partial();
                        snapshotChanged = true;
                        logger.debug("failing snapshot of shard [{}] because index got deleted", shardId);
                        shards.put(shardId, SnapshotsInProgress.ShardSnapshotStatus.MISSING);
                        knownFailures.put(shardSnapshotEntry.getKey(), SnapshotsInProgress.ShardSnapshotStatus.MISSING);
                    } else {
                        // if no failure is known for the shard we keep waiting
                        shards.put(shardId, shardStatus);
                    }
                } else {
                    // If a failure is known for an execution we waited on for this shard then we fail with the same exception here
                    // as well
                    snapshotChanged = true;
                    shards.put(shardId, knownFailure);
                }
            } else if (shardStatus.state() == SnapshotsInProgress.ShardState.WAITING
                || shardStatus.state() == SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL) {
                    // The shard primary wasn't assigned, or the shard snapshot was paused because the node was shutting down.
                    IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                    if (indexShardRoutingTable != null) {
                        IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                        if (shardRouting != null) {
                            final var primaryNodeId = shardRouting.primaryShard().currentNodeId();
                            if (nodeIdRemovalPredicate.test(primaryNodeId)) {
                                if (shardStatus.state() == SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL) {
                                    // Shard that we are waiting for is on a node marked for removal, keep it as PAUSED_FOR_REMOVAL
                                    shards.put(shardId, shardStatus);
                                } else {
                                    // Shard that we are waiting for is on a node marked for removal, move it to PAUSED_FOR_REMOVAL
                                    snapshotChanged = true;
                                    shards.put(
                                        shardId,
                                        new SnapshotsInProgress.ShardSnapshotStatus(
                                            primaryNodeId,
                                            SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL,
                                            shardStatus.generation()
                                        )
                                    );
                                }
                                continue;
                            } else if (shardRouting.primaryShard().started()) {
                                // Shard that we were waiting for has started on a node, let's process it
                                snapshotChanged = true;
                                logger.debug("""
                                    Starting shard [{}] with shard generation [{}] that we were waiting to start on node [{}]. Previous \
                                    shard state [{}]
                                    """, shardId, shardStatus.generation(), shardStatus.nodeId(), shardStatus.state());
                                shards.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(primaryNodeId, shardStatus.generation()));
                                continue;
                            } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                                // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                                shards.put(shardId, shardStatus);
                                continue;
                            }
                        }
                    }
                    // Shard that we were waiting for went into unassigned state or disappeared (index or shard is gone) - giving up
                    snapshotChanged = true;
                    logger.warn("failing snapshot of shard [{}] on node [{}] because shard is unassigned", shardId, shardStatus.nodeId());
                    final SnapshotsInProgress.ShardSnapshotStatus failedState = new SnapshotsInProgress.ShardSnapshotStatus(
                        shardStatus.nodeId(),
                        SnapshotsInProgress.ShardState.FAILED,
                        shardStatus.generation(),
                        "shard is unassigned"
                    );
                    shards.put(shardId, failedState);
                    knownFailures.put(shardSnapshotEntry.getKey(), failedState);
                } else if (shardStatus.state().completed() == false && shardStatus.nodeId() != null) {
                    if (nodes.nodeExists(shardStatus.nodeId())) {
                        shards.put(shardId, shardStatus);
                    } else {
                        // TODO: Restart snapshot on another node?
                        snapshotChanged = true;
                        logger.warn("failing snapshot of shard [{}] on departed node [{}]", shardId, shardStatus.nodeId());
                        final SnapshotsInProgress.ShardSnapshotStatus failedState = new SnapshotsInProgress.ShardSnapshotStatus(
                            shardStatus.nodeId(),
                            SnapshotsInProgress.ShardState.FAILED,
                            shardStatus.generation(),
                            "node left the cluster during snapshot"
                        );
                        shards.put(shardId, failedState);
                        knownFailures.put(shardSnapshotEntry.getKey(), failedState);
                    }
                } else {
                    shards.put(shardId, shardStatus);
                }
        }
        if (snapshotChanged) {
            return shards.build();
        } else {
            return null;
        }
    }

    public static boolean waitingShardsStartedOrUnassigned(SnapshotsInProgress snapshotsInProgress, ClusterChangedEvent event) {
        for (List<SnapshotsInProgress.Entry> entries : snapshotsInProgress.entriesByRepo()) {
            for (SnapshotsInProgress.Entry entry : entries) {
                if (entry.state() == SnapshotsInProgress.State.STARTED && entry.isClone() == false) {
                    final ProjectId projectId = entry.projectId();
                    for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> shardStatus : entry
                        .shardSnapshotStatusByRepoShardId()
                        .entrySet()) {
                        final SnapshotsInProgress.ShardState state = shardStatus.getValue().state();
                        if (state != SnapshotsInProgress.ShardState.WAITING
                            && state != SnapshotsInProgress.ShardState.QUEUED
                            && state != SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL) {
                            continue;
                        }
                        final RepositoryShardId shardId = shardStatus.getKey();
                        final Index index = entry.indexByName(shardId.indexName());
                        if (event.indexRoutingTableChanged(index)) {
                            IndexRoutingTable indexShardRoutingTable = event.state().routingTable(projectId).index(index);
                            if (indexShardRoutingTable == null) {
                                // index got removed concurrently and we have to fail WAITING, QUEUED and PAUSED_FOR_REMOVAL state shards
                                return true;
                            }
                            ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.shardId()).primaryShard();
                            if (shardRouting.started() && snapshotsInProgress.isNodeIdForRemoval(shardRouting.currentNodeId()) == false
                                || shardRouting.unassigned()) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    public static boolean removedNodesCleanupNeeded(SnapshotsInProgress snapshotsInProgress, List<DiscoveryNode> removedNodes) {
        if (removedNodes.isEmpty()) {
            // Nothing to do, no nodes removed
            return false;
        }
        final Set<String> removedNodeIds = removedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        return snapshotsInProgress.asStream().anyMatch(snapshot -> {
            if (snapshot.state().completed() || snapshot.isClone()) {
                // nothing to do for already completed snapshots or clones that run on master anyways
                return false;
            }
            for (SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus : snapshot.shardSnapshotStatusByRepoShardId().values()) {
                if (shardSnapshotStatus.state().completed() == false && removedNodeIds.contains(shardSnapshotStatus.nodeId())) {
                    // Snapshot had an incomplete shard running on a removed node so we need to adjust that shard's snapshot status
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Removes all feature states which have missing or failed shards, as they are no longer safely restorable.
     * @param entry The "in progress" entry with a list of feature states and one or more failed shards.
     * @param finalIndices The final list of indices in the snapshot, after any indices that were concurrently deleted are removed.
     * @return The list of feature states which were completed successfully in the given entry.
     */
    public static List<SnapshotFeatureInfo> onlySuccessfulFeatureStates(SnapshotsInProgress.Entry entry, List<String> finalIndices) {
        assert entry.partial() : "should not try to filter feature states from a non-partial entry";

        // Figure out which indices have unsuccessful shards
        Set<String> indicesWithUnsuccessfulShards = new HashSet<>();
        entry.shardSnapshotStatusByRepoShardId().forEach((key, value) -> {
            final SnapshotsInProgress.ShardState shardState = value.state();
            if (shardState.failed() || shardState.completed() == false) {
                indicesWithUnsuccessfulShards.add(key.indexName());
            }
        });

        // Now remove any feature states which contain any of those indices, as the feature state is not intact and not safely restorable
        return entry.featureStates()
            .stream()
            .filter(stateInfo -> finalIndices.containsAll(stateInfo.getIndices()))
            .filter(stateInfo -> stateInfo.getIndices().stream().anyMatch(indicesWithUnsuccessfulShards::contains) == false)
            .toList();
    }

    /**
     * Finds snapshot delete operations that are ready to execute in the given {@link ClusterState} and computes a new cluster state that
     * has all executable deletes marked as executing. Returns a {@link Tuple} of the updated cluster state and all executable deletes.
     * This can either be {@link SnapshotDeletionsInProgress.Entry} that were already in state
     * {@link SnapshotDeletionsInProgress.State#STARTED} or waiting entries in state {@link SnapshotDeletionsInProgress.State#WAITING}
     * that were moved to {@link SnapshotDeletionsInProgress.State#STARTED} in the returned updated cluster state.
     *
     * @param projectId the project for repositories where deletions should be prepared. {@code null} means all projects
     * @param currentState current cluster state
     * @return tuple of an updated cluster state and currently executable snapshot delete operations
     */
    public static Tuple<ClusterState, List<SnapshotDeletionsInProgress.Entry>> readyDeletions(
        ClusterState currentState,
        @Nullable ProjectId projectId
    ) {
        final SnapshotDeletionsInProgress deletions = SnapshotDeletionsInProgress.get(currentState);
        if (deletions.hasDeletionsInProgress() == false || (projectId != null && deletions.hasDeletionsInProgress(projectId) == false)) {
            return Tuple.tuple(currentState, List.of());
        }
        final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE);
        assert snapshotsInProgress != null;
        final Set<ProjectRepo> repositoriesSeen = new HashSet<>();
        boolean changed = false;
        final ArrayList<SnapshotDeletionsInProgress.Entry> readyDeletions = new ArrayList<>();
        final List<SnapshotDeletionsInProgress.Entry> newDeletes = new ArrayList<>();
        for (SnapshotDeletionsInProgress.Entry entry : deletions.getEntries()) {
            if (projectId != null && projectId.equals(entry.projectId()) == false) {
                // Not the target project, keep the entry as is
                newDeletes.add(entry);
                continue;
            }
            final var projectRepo = new ProjectRepo(entry.projectId(), entry.repository());
            if (repositoriesSeen.add(projectRepo)
                && entry.state() == SnapshotDeletionsInProgress.State.WAITING
                && snapshotsInProgress.forRepo(projectRepo).stream().noneMatch(SnapshotsServiceUtils::isWritingToRepository)) {
                changed = true;
                final SnapshotDeletionsInProgress.Entry newEntry = entry.started();
                readyDeletions.add(newEntry);
                newDeletes.add(newEntry);
            } else {
                newDeletes.add(entry);
            }
        }
        return Tuple.tuple(
            changed
                ? ClusterState.builder(currentState)
                    .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(newDeletes))
                    .build()
                : currentState,
            readyDeletions
        );
    }

    /**
     * Computes the cluster state resulting from removing a given snapshot create operation from the given state. This method will update
     * the shard generations of snapshots that the given snapshot depended on so that finalizing them will not cause rolling back to an
     * outdated shard generation.
     * <p>
     * For example, shard snapshot X can be taken, but not finalized yet. Shard snapshot Y can then depend upon shard snapshot X. Then shard
     * snapshot Y may finalize before shard snapshot X, but including X. However, X does not include Y. Therefore we update X to use Y's
     * shard generation file (list of snapshots and dependencies) to avoid overwriting with X's file that is missing Y.
     *
     * @param state    current cluster state
     * @param snapshot snapshot for which to remove the snapshot operation
     * @return updated cluster state
     */
    public static ClusterState stateWithoutSnapshot(
        ClusterState state,
        Snapshot snapshot,
        FinalizeSnapshotContext.UpdatedShardGenerations updatedShardGenerations
    ) {
        final SnapshotsInProgress inProgressSnapshots = SnapshotsInProgress.get(state);
        ClusterState result = state;
        int indexOfEntry = -1;
        // Find the in-progress snapshot entry that matches {@code snapshot}.
        final ProjectId projectId = snapshot.getProjectId();
        final String repository = snapshot.getRepository();
        final List<SnapshotsInProgress.Entry> entryList = inProgressSnapshots.forRepo(projectId, repository);
        for (int i = 0; i < entryList.size(); i++) {
            SnapshotsInProgress.Entry entry = entryList.get(i);
            if (entry.snapshot().equals(snapshot)) {
                indexOfEntry = i;
                break;
            }
        }
        if (indexOfEntry >= 0) {
            final List<SnapshotsInProgress.Entry> updatedEntries = new ArrayList<>(entryList.size() - 1);
            final SnapshotsInProgress.Entry removedEntry = entryList.get(indexOfEntry);
            for (int i = 0; i < indexOfEntry; i++) {
                final SnapshotsInProgress.Entry previousEntry = entryList.get(i);
                if (removedEntry.isClone()) {
                    if (previousEntry.isClone()) {
                        ImmutableOpenMap.Builder<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> updatedShardAssignments = null;
                        for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> finishedShardEntry : removedEntry
                            .shardSnapshotStatusByRepoShardId()
                            .entrySet()) {
                            final SnapshotsInProgress.ShardSnapshotStatus shardState = finishedShardEntry.getValue();
                            if (shardState.state() == SnapshotsInProgress.ShardState.SUCCESS) {
                                updatedShardAssignments = maybeAddUpdatedAssignment(
                                    updatedShardAssignments,
                                    shardState,
                                    finishedShardEntry.getKey(),
                                    previousEntry.shardSnapshotStatusByRepoShardId()
                                );
                            }
                        }
                        addCloneEntry(updatedEntries, previousEntry, updatedShardAssignments);
                    } else {
                        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> updatedShardAssignments = null;
                        for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> finishedShardEntry : removedEntry
                            .shardSnapshotStatusByRepoShardId()
                            .entrySet()) {
                            final SnapshotsInProgress.ShardSnapshotStatus shardState = finishedShardEntry.getValue();
                            final RepositoryShardId repositoryShardId = finishedShardEntry.getKey();
                            if (shardState.state() != SnapshotsInProgress.ShardState.SUCCESS
                                || previousEntry.shardSnapshotStatusByRepoShardId().containsKey(repositoryShardId) == false) {
                                continue;
                            }
                            updatedShardAssignments = maybeAddUpdatedAssignment(
                                updatedShardAssignments,
                                shardState,
                                previousEntry.shardId(repositoryShardId),
                                previousEntry.shards()
                            );

                        }
                        addSnapshotEntry(updatedEntries, previousEntry, updatedShardAssignments);
                    }
                } else {
                    if (previousEntry.isClone()) {
                        ImmutableOpenMap.Builder<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> updatedShardAssignments = null;
                        for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> finishedShardEntry : removedEntry
                            .shardSnapshotStatusByRepoShardId()
                            .entrySet()) {
                            final SnapshotsInProgress.ShardSnapshotStatus shardState = finishedShardEntry.getValue();
                            final RepositoryShardId repositoryShardId = finishedShardEntry.getKey();
                            if (shardState.state() != SnapshotsInProgress.ShardState.SUCCESS
                                || previousEntry.shardSnapshotStatusByRepoShardId().containsKey(repositoryShardId) == false
                                || updatedShardGenerations.hasShardGen(finishedShardEntry.getKey()) == false) {
                                continue;
                            }
                            updatedShardAssignments = maybeAddUpdatedAssignment(
                                updatedShardAssignments,
                                shardState,
                                repositoryShardId,
                                previousEntry.shardSnapshotStatusByRepoShardId()
                            );
                        }
                        addCloneEntry(updatedEntries, previousEntry, updatedShardAssignments);
                    } else {
                        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> updatedShardAssignments = null;
                        for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> finishedShardEntry : removedEntry
                            .shardSnapshotStatusByRepoShardId()
                            .entrySet()) {
                            final SnapshotsInProgress.ShardSnapshotStatus shardState = finishedShardEntry.getValue();
                            if (shardState.state() == SnapshotsInProgress.ShardState.SUCCESS
                                && previousEntry.shardSnapshotStatusByRepoShardId().containsKey(finishedShardEntry.getKey())
                                && updatedShardGenerations.hasShardGen(finishedShardEntry.getKey())) {
                                updatedShardAssignments = maybeAddUpdatedAssignment(
                                    updatedShardAssignments,
                                    shardState,
                                    previousEntry.shardId(finishedShardEntry.getKey()),
                                    previousEntry.shards()
                                );
                            }
                        }
                        addSnapshotEntry(updatedEntries, previousEntry, updatedShardAssignments);
                    }
                }
            }
            for (int i = indexOfEntry + 1; i < entryList.size(); i++) {
                updatedEntries.add(entryList.get(i));
            }
            result = ClusterState.builder(state)
                .putCustom(
                    SnapshotsInProgress.TYPE,
                    inProgressSnapshots.createCopyWithUpdatedEntriesForRepo(projectId, repository, updatedEntries)
                )
                .build();
        }
        return readyDeletions(result, projectId).v1();
    }

    public static void addSnapshotEntry(
        List<SnapshotsInProgress.Entry> entries,
        SnapshotsInProgress.Entry entryToUpdate,
        @Nullable ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> updatedShardAssignments
    ) {
        if (updatedShardAssignments == null) {
            entries.add(entryToUpdate);
        } else {
            final ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> updatedStatus = ImmutableOpenMap.builder(
                entryToUpdate.shards()
            );
            updatedStatus.putAllFromMap(updatedShardAssignments.build());
            entries.add(entryToUpdate.withShardStates(updatedStatus.build()));
        }
    }

    public static void addCloneEntry(
        List<SnapshotsInProgress.Entry> entries,
        SnapshotsInProgress.Entry entryToUpdate,
        @Nullable ImmutableOpenMap.Builder<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> updatedShardAssignments
    ) {
        if (updatedShardAssignments == null) {
            entries.add(entryToUpdate);
        } else {
            final ImmutableOpenMap.Builder<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> updatedStatus = ImmutableOpenMap
                .builder(entryToUpdate.shardSnapshotStatusByRepoShardId());
            updatedStatus.putAllFromMap(updatedShardAssignments.build());
            entries.add(entryToUpdate.withClones(updatedStatus.build()));
        }
    }

    @Nullable
    public static <T> ImmutableOpenMap.Builder<T, SnapshotsInProgress.ShardSnapshotStatus> maybeAddUpdatedAssignment(
        @Nullable ImmutableOpenMap.Builder<T, SnapshotsInProgress.ShardSnapshotStatus> updatedShardAssignments,
        SnapshotsInProgress.ShardSnapshotStatus finishedShardState,
        T shardId,
        Map<T, SnapshotsInProgress.ShardSnapshotStatus> statesToUpdate
    ) {
        final ShardGeneration newGeneration = finishedShardState.generation();
        final SnapshotsInProgress.ShardSnapshotStatus stateToUpdate = statesToUpdate.get(shardId);
        if (stateToUpdate != null
            && stateToUpdate.state() == SnapshotsInProgress.ShardState.SUCCESS
            && Objects.equals(newGeneration, stateToUpdate.generation()) == false) {
            if (updatedShardAssignments == null) {
                updatedShardAssignments = ImmutableOpenMap.builder();
            }
            updatedShardAssignments.put(shardId, stateToUpdate.withUpdatedGeneration(newGeneration));
        }
        return updatedShardAssignments;
    }

    /**
     * Remove the given {@link SnapshotId}s for the given {@code repository} from an instance of {@link SnapshotDeletionsInProgress}.
     * If no deletion contained any of the snapshot ids to remove then return {@code null}.
     *
     * @param deletions   snapshot deletions to update
     * @param snapshotIds snapshot ids to remove
     * @param projectId   project for the repository
     * @param repository  repository that the snapshot ids belong to
     * @return            updated {@link SnapshotDeletionsInProgress} or {@code null} if unchanged
     */
    @Nullable
    public static SnapshotDeletionsInProgress deletionsWithoutSnapshots(
        SnapshotDeletionsInProgress deletions,
        Collection<SnapshotId> snapshotIds,
        ProjectId projectId,
        String repository
    ) {
        boolean changed = false;
        List<SnapshotDeletionsInProgress.Entry> updatedEntries = new ArrayList<>(deletions.getEntries().size());
        for (SnapshotDeletionsInProgress.Entry entry : deletions.getEntries()) {
            if (entry.projectId().equals(projectId) && entry.repository().equals(repository)) {
                final List<SnapshotId> updatedSnapshotIds = new ArrayList<>(entry.snapshots());
                if (updatedSnapshotIds.removeAll(snapshotIds)) {
                    changed = true;
                    updatedEntries.add(entry.withSnapshots(updatedSnapshotIds));
                } else {
                    updatedEntries.add(entry);
                }
            } else {
                updatedEntries.add(entry);
            }
        }
        return changed ? SnapshotDeletionsInProgress.of(updatedEntries) : null;
    }

    /**
     * Determines the minimum {@link IndexVersion} that the snapshot repository must be compatible with
     * from the current nodes in the cluster and the contents of the repository.
     * The minimum version is determined as the lowest version found across all snapshots in the
     * repository and all nodes in the cluster.
     *
     * @param minNodeVersion minimum node version in the cluster
     * @param repositoryData current {@link RepositoryData} of that repository
     * @param excluded       snapshot id to ignore when computing the minimum version
     *                       (used to use newer metadata version after a snapshot delete)
     * @return minimum node version that must still be able to read the repository metadata
     */
    public static IndexVersion minCompatibleVersion(
        IndexVersion minNodeVersion,
        RepositoryData repositoryData,
        @Nullable Collection<SnapshotId> excluded
    ) {
        IndexVersion minCompatVersion = minNodeVersion;
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        for (SnapshotId snapshotId : snapshotIds.stream()
            .filter(excluded == null ? Predicates.always() : Predicate.not(excluded::contains))
            .toList()) {
            final IndexVersion known = repositoryData.getVersion(snapshotId);
            // If we don't have the version cached in the repository data yet we load it from the snapshot info blobs
            if (known == null) {
                assert repositoryData.shardGenerations().totalShards() == 0
                    : "Saw shard generations ["
                        + repositoryData.shardGenerations()
                        + "] but did not have versions tracked for snapshot ["
                        + snapshotId
                        + "]";
                return OLD_SNAPSHOT_FORMAT;
            } else {
                minCompatVersion = IndexVersion.min(minCompatVersion, known);
            }
        }
        return minCompatVersion;
    }

    /**
     * Shortcut to build new {@link ClusterState} from the current state and updated values of {@link SnapshotsInProgress} and
     * {@link SnapshotDeletionsInProgress}.
     *
     * @param state                       current cluster state
     * @param snapshotsInProgress         new value for {@link SnapshotsInProgress} or {@code null} if it's unchanged
     * @param snapshotDeletionsInProgress new value for {@link SnapshotDeletionsInProgress} or {@code null} if it's unchanged
     * @return updated cluster state
     */
    public static ClusterState updateWithSnapshots(
        ClusterState state,
        @Nullable SnapshotsInProgress snapshotsInProgress,
        @Nullable SnapshotDeletionsInProgress snapshotDeletionsInProgress
    ) {
        if (snapshotsInProgress == null && snapshotDeletionsInProgress == null) {
            return state;
        }
        ClusterState.Builder builder = ClusterState.builder(state);
        if (snapshotsInProgress != null) {
            builder.putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress);
        }
        if (snapshotDeletionsInProgress != null) {
            builder.putCustom(SnapshotDeletionsInProgress.TYPE, snapshotDeletionsInProgress);
        }
        return builder.build();
    }

    public static <T> void failListenersIgnoringException(@Nullable List<ActionListener<T>> listeners, Exception failure) {
        if (listeners != null) {
            try {
                ActionListener.onFailure(listeners, failure);
            } catch (Exception ex) {
                assert false : new AssertionError(ex);
                logger.warn("Failed to notify listeners", ex);
            }
        }
    }

    public static <T> void completeListenersIgnoringException(@Nullable List<ActionListener<T>> listeners, T result) {
        if (listeners != null) {
            try {
                ActionListener.onResponse(listeners, result);
            } catch (Exception ex) {
                assert false : new AssertionError(ex);
                logger.warn("Failed to notify listeners", ex);
            }
        }
    }

    /**
     * Calculates the assignment of shards to data nodes for a new snapshot based on the given cluster state and the
     * indices that should be included in the snapshot.
     *
     * @param indices             Indices to snapshot
     * @param useShardGenerations whether to write {@link ShardGenerations} during the snapshot
     * @return map of shard-id to snapshot-status of all shards included into current snapshot
     */
    public static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(
        SnapshotsInProgress snapshotsInProgress,
        SnapshotDeletionsInProgress deletionsInProgress,
        ProjectState currentState,
        Collection<IndexId> indices,
        boolean useShardGenerations,
        RepositoryData repositoryData,
        String repoName
    ) {
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();
        final InFlightShardSnapshotStates inFlightShardStates = InFlightShardSnapshotStates.forEntries(
            snapshotsInProgress.forRepo(currentState.projectId(), repoName)
        );
        final boolean readyToExecute = deletionsInProgress.hasExecutingDeletion(currentState.projectId(), repoName) == false;
        for (IndexId index : indices) {
            final String indexName = index.getName();
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetadata indexMetadata = currentState.metadata().index(indexName);
            if (indexMetadata == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                builder.put(new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 0), SnapshotsInProgress.ShardSnapshotStatus.MISSING);
            } else {
                final IndexRoutingTable indexRoutingTable = currentState.routingTable().index(indexName);
                assert indexRoutingTable != null;
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    final ShardId shardId = indexRoutingTable.shard(i).shardId();
                    final ShardGeneration shardRepoGeneration;
                    if (useShardGenerations) {
                        final ShardGeneration inFlightGeneration = inFlightShardStates.generationForShard(
                            index,
                            shardId.id(),
                            shardGenerations
                        );
                        if (inFlightGeneration == null && isNewIndex) {
                            assert shardGenerations.getShardGen(index, shardId.getId()) == null
                                : "Found shard generation for new index [" + index + "]";
                            shardRepoGeneration = ShardGenerations.NEW_SHARD_GEN;
                        } else {
                            shardRepoGeneration = inFlightGeneration;
                        }
                    } else {
                        shardRepoGeneration = null;
                    }
                    final SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus;
                    if (readyToExecute == false || inFlightShardStates.isActive(shardId.getIndexName(), shardId.id())) {
                        shardSnapshotStatus = SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED;
                    } else {
                        shardSnapshotStatus = initShardSnapshotStatus(
                            shardRepoGeneration,
                            indexRoutingTable.shard(i).primaryShard(),
                            snapshotsInProgress::isNodeIdForRemoval
                        );
                    }
                    builder.put(shardId, shardSnapshotStatus);
                }
            }
        }

        return builder.build();
    }

    /**
     * Compute the snapshot status for a given shard based on the current primary routing entry for the shard.
     *
     * @param shardRepoGeneration    repository generation of the shard in the repository
     * @param primary                primary routing entry for the shard
     * @param nodeIdRemovalPredicate tests whether a node ID is currently marked for removal from the cluster
     * @return                       shard snapshot status
     */
    public static SnapshotsInProgress.ShardSnapshotStatus initShardSnapshotStatus(
        ShardGeneration shardRepoGeneration,
        ShardRouting primary,
        Predicate<String> nodeIdRemovalPredicate
    ) {
        SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus;
        if (primary == null || primary.assignedToNode() == false) {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(
                null,
                SnapshotsInProgress.ShardState.MISSING,
                shardRepoGeneration,
                "primary shard is not allocated"
            );
        } else if (primary.relocating() || primary.initializing()) {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(
                primary.currentNodeId(),
                SnapshotsInProgress.ShardState.WAITING,
                shardRepoGeneration
            );
        } else if (nodeIdRemovalPredicate.test(primary.currentNodeId())) {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(
                primary.currentNodeId(),
                SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL,
                shardRepoGeneration
            );
        } else if (primary.started() == false) {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(
                primary.currentNodeId(),
                SnapshotsInProgress.ShardState.MISSING,
                shardRepoGeneration,
                "primary shard hasn't been started yet"
            );
        } else {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), shardRepoGeneration);
        }
        return shardSnapshotStatus;
    }

    /**
     * Returns the data streams that are currently being snapshotted (with partial == false) and that are contained in the
     * indices-to-check set.
     */
    public static Set<String> snapshottingDataStreams(final ProjectState projectState, final Set<String> dataStreamsToCheck) {
        Map<String, DataStream> dataStreams = projectState.metadata().dataStreams();
        return SnapshotsInProgress.get(projectState.cluster())
            .asStream(projectState.projectId())
            .filter(e -> e.partial() == false)
            .flatMap(e -> e.dataStreams().stream())
            .filter(ds -> dataStreams.containsKey(ds) && dataStreamsToCheck.contains(ds))
            .collect(Collectors.toSet());
    }

    /**
     * Returns the indices that are currently being snapshotted (with partial == false) and that are contained in the indices-to-check set.
     */
    public static Set<Index> snapshottingIndices(final ProjectState projectState, final Set<Index> indicesToCheck) {
        final Set<Index> indices = new HashSet<>();
        for (List<SnapshotsInProgress.Entry> snapshotsInRepo : SnapshotsInProgress.get(projectState.cluster())
            .entriesByRepo(projectState.projectId())) {
            for (final SnapshotsInProgress.Entry entry : snapshotsInRepo) {
                if (entry.partial() == false && entry.isClone() == false) {
                    for (String indexName : entry.indices().keySet()) {
                        IndexMetadata indexMetadata = projectState.metadata().index(indexName);
                        if (indexMetadata != null && indicesToCheck.contains(indexMetadata.getIndex())) {
                            indices.add(indexMetadata.getIndex());
                        }
                    }
                }
            }
        }
        return indices;
    }

    /**
     * Filters out the aliases that refer to data streams to do not exist in the provided data streams.
     * Also rewrites the list of data streams an alias point to to only contain data streams that exist in the provided data streams.
     * <p>
     * The purpose of this method is to capture the relevant data stream aliases based on the data streams
     * that will be included in a snapshot.
     *
     * @param dataStreams       The provided data streams, which will be included in a snapshot.
     * @param dataStreamAliases The data streams aliases that may contain aliases that refer to data streams
     *                          that don't exist in the provided data streams.
     * @return                  The filtered data streams aliases only referring to data streams in the provided data streams.
     */
    public static Map<String, DataStreamAlias> filterDataStreamAliases(
        Map<String, DataStream> dataStreams,
        Map<String, DataStreamAlias> dataStreamAliases
    ) {
        return dataStreamAliases.values()
            .stream()
            .filter(alias -> alias.getDataStreams().stream().anyMatch(dataStreams::containsKey))
            .map(alias -> alias.intersect(dataStreams::containsKey))
            .collect(Collectors.toMap(DataStreamAlias::getName, Function.identity()));
    }

    public static void logSnapshotFailure(String operation, Snapshot snapshot, Exception e) {
        final var logLevel = snapshotFailureLogLevel(e);
        if (logLevel == Level.INFO && logger.isDebugEnabled() == false) {
            // suppress stack trace at INFO unless extra verbosity is configured
            logger.info(
                format(
                    "%s[%s] failed to %s snapshot: %s",
                    projectRepoString(snapshot.getProjectId(), snapshot.getRepository()),
                    snapshot.getSnapshotId().getName(),
                    operation,
                    e.getMessage()
                )
            );
        } else {
            logger.log(
                logLevel,
                () -> format("[%s][%s] failed to %s snapshot", snapshot.getRepository(), snapshot.getSnapshotId().getName(), operation),
                e
            );
        }
    }

    public static Level snapshotFailureLogLevel(Exception e) {
        if (MasterService.isPublishFailureException(e)) {
            // no action needed, the new master will take things from here
            return Level.INFO;
        } else if (e instanceof InvalidSnapshotNameException) {
            // no action needed, typically ILM-related, or a user error
            return Level.INFO;
        } else if (e instanceof IndexNotFoundException) {
            // not worrying, most likely a user error
            return Level.INFO;
        } else if (e instanceof SnapshotException) {
            if (e.getMessage().contains(ReferenceDocs.UNASSIGNED_SHARDS.toString())) {
                // non-partial snapshot requested but cluster health is not yellow or green; the health is tracked elsewhere so no need to
                // make more noise here
                return Level.INFO;
            }
        } else if (e instanceof IllegalArgumentException) {
            // some other user error
            return Level.INFO;
        }
        return Level.WARN;
    }
}
