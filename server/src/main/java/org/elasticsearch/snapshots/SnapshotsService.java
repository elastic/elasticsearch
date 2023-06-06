/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;
import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
import static org.elasticsearch.core.Strings.format;

/**
 * Service responsible for creating snapshots. This service runs all the steps executed on the master node during snapshot creation and
 * deletion.
 * See package level documentation of {@link org.elasticsearch.snapshots} for details.
 */
public class SnapshotsService extends AbstractLifecycleComponent implements ClusterStateApplier {

    public static final Version SHARD_GEN_IN_REPO_DATA_VERSION = Version.V_7_6_0;

    public static final Version INDEX_GEN_IN_REPO_DATA_VERSION = Version.V_7_9_0;

    public static final Version UUIDS_IN_REPO_DATA_VERSION = Version.V_7_12_0;
    public static final TransportVersion UUIDS_IN_REPO_DATA_TRANSPORT_VERSION = TransportVersion.V_7_12_0;

    public static final Version FILE_INFO_WRITER_UUIDS_IN_SHARD_DATA_VERSION = Version.V_7_16_0;

    public static final Version OLD_SNAPSHOT_FORMAT = Version.V_7_5_0;

    public static final String POLICY_ID_METADATA_FIELD = "policy";

    private static final Logger logger = LogManager.getLogger(SnapshotsService.class);

    public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME = "internal:cluster/snapshot/update_snapshot_status";

    public static final String NO_FEATURE_STATES_VALUE = "none";

    private final ClusterService clusterService;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final RepositoriesService repositoriesService;

    private final ThreadPool threadPool;

    private final Map<Snapshot, List<ActionListener<SnapshotInfo>>> snapshotCompletionListeners = new ConcurrentHashMap<>();

    /**
     * Listeners for snapshot deletion keyed by delete uuid as returned from {@link SnapshotDeletionsInProgress.Entry#uuid()}
     */
    private final Map<String, List<ActionListener<Void>>> snapshotDeletionListeners = new HashMap<>();

    // Set of repositories currently running either a snapshot finalization or a snapshot delete.
    private final Set<String> currentlyFinalizing = Collections.synchronizedSet(new HashSet<>());

    // Set of snapshots that are currently being ended by this node
    private final Set<Snapshot> endingSnapshots = Collections.synchronizedSet(new HashSet<>());

    // Set of currently initializing clone operations
    private final Set<Snapshot> initializingClones = Collections.synchronizedSet(new HashSet<>());

    private final UpdateSnapshotStatusAction updateSnapshotStatusHandler;

    private final TransportService transportService;

    private final OngoingRepositoryOperations repositoryOperations = new OngoingRepositoryOperations();

    private final SystemIndices systemIndices;

    private final MasterServiceTaskQueue<SnapshotTask> masterServiceTaskQueue;

    /**
     * Setting that specifies the maximum number of allowed concurrent snapshot create and delete operations in the
     * cluster state. The number of concurrent operations in a cluster state is defined as the sum of
     * {@link SnapshotsInProgress#count()} and the size of {@link SnapshotDeletionsInProgress#getEntries()}.
     */
    public static final Setting<Integer> MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING = Setting.intSetting(
        "snapshot.max_concurrent_operations",
        1000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile int maxConcurrentOperations;

    public SnapshotsService(
        Settings settings,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RepositoriesService repositoriesService,
        TransportService transportService,
        ActionFilters actionFilters,
        SystemIndices systemIndices
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.repositoriesService = repositoriesService;
        this.threadPool = transportService.getThreadPool();
        this.transportService = transportService;

        // The constructor of UpdateSnapshotStatusAction will register itself to the TransportService.
        this.updateSnapshotStatusHandler = new UpdateSnapshotStatusAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        if (DiscoveryNode.isMasterNode(settings)) {
            // addLowPriorityApplier to make sure that Repository will be created before snapshot
            clusterService.addLowPriorityApplier(this);
            maxConcurrentOperations = MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING.get(settings);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING, i -> maxConcurrentOperations = i);
        }
        this.systemIndices = systemIndices;

        this.masterServiceTaskQueue = clusterService.createTaskQueue("snapshots-service", Priority.NORMAL, new SnapshotTaskExecutor());
    }

    /**
     * Same as {@link #createSnapshot(CreateSnapshotRequest, ActionListener)} but invokes its callback on completion of
     * the snapshot.
     *
     * @param request snapshot request
     * @param listener snapshot completion listener
     */
    public void executeSnapshot(final CreateSnapshotRequest request, final ActionListener<SnapshotInfo> listener) {
        createSnapshot(request, listener.delegateFailureAndWrap((l, snapshot) -> addListener(snapshot, l)));
    }

    /**
     * Initializes the snapshotting process.
     * <p>
     * This method is used by clients to start snapshot. It makes sure that there is no snapshots are currently running and
     * creates a snapshot record in cluster state metadata.
     *
     * @param request  snapshot request
     * @param listener snapshot creation listener
     */
    public void createSnapshot(final CreateSnapshotRequest request, final ActionListener<Snapshot> listener) {
        final String repositoryName = request.repository();
        final String snapshotName = IndexNameExpressionResolver.resolveDateMathExpression(request.snapshot());
        validate(repositoryName, snapshotName);
        // TODO: create snapshot UUID in CreateSnapshotRequest and make this operation idempotent to cleanly deal with transport layer
        // retries
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID()); // new UUID for the snapshot
        Repository repository = repositoriesService.repository(request.repository());
        if (repository.isReadOnly()) {
            listener.onFailure(new RepositoryException(repository.getMetadata().name(), "cannot create snapshot in a readonly repository"));
            return;
        }
        submitCreateSnapshotRequest(request, listener, repository, new Snapshot(repositoryName, snapshotId), repository.getMetadata());
    }

    private void submitCreateSnapshotRequest(
        CreateSnapshotRequest request,
        ActionListener<Snapshot> listener,
        Repository repository,
        Snapshot snapshot,
        RepositoryMetadata initialRepositoryMetadata
    ) {
        repository.getRepositoryData(
            listener.delegateFailure(
                (l, repositoryData) -> masterServiceTaskQueue.submitTask(
                    "create_snapshot [" + snapshot.getSnapshotId().getName() + ']',
                    new CreateSnapshotTask(repository, repositoryData, l, snapshot, request, initialRepositoryMetadata),
                    request.masterNodeTimeout()
                )
            )
        );
    }

    private static void ensureSnapshotNameNotRunning(SnapshotsInProgress runningSnapshots, String repositoryName, String snapshotName) {
        if (runningSnapshots.forRepo(repositoryName).stream().anyMatch(s -> s.snapshot().getSnapshotId().getName().equals(snapshotName))) {
            throw new SnapshotNameAlreadyInUseException(repositoryName, snapshotName, "snapshot with the same name is already in-progress");
        }
    }

    // TODO: It is worth revisiting the design choice of creating a placeholder entry in snapshots-in-progress here once we have a cache
    // for repository metadata and loading it has predictable performance
    public void cloneSnapshot(CloneSnapshotRequest request, ActionListener<Void> listener) {
        final String repositoryName = request.repository();
        Repository repository = repositoriesService.repository(repositoryName);
        if (repository.isReadOnly()) {
            listener.onFailure(new RepositoryException(repositoryName, "cannot create snapshot in a readonly repository"));
            return;
        }
        final String snapshotName = IndexNameExpressionResolver.resolveDateMathExpression(request.target());
        validate(repositoryName, snapshotName);
        // TODO: create snapshot UUID in CloneSnapshotRequest and make this operation idempotent to cleanly deal with transport layer
        // retries
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID());
        final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);
        initializingClones.add(snapshot);
        executeConsistentStateUpdate(repository, repositoryData -> new ClusterStateUpdateTask(request.masterNodeTimeout()) {

            private SnapshotsInProgress.Entry newEntry;

            @Override
            public ClusterState execute(ClusterState currentState) {
                ensureRepositoryExists(repositoryName, currentState);
                ensureSnapshotNameAvailableInRepo(repositoryData, snapshotName, repository);
                ensureNoCleanupInProgress(currentState, repositoryName, snapshotName, "clone snapshot");
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                ensureSnapshotNameNotRunning(snapshots, repositoryName, snapshotName);
                validate(repositoryName, snapshotName, currentState);

                final SnapshotId sourceSnapshotId = repositoryData.getSnapshotIds()
                    .stream()
                    .filter(src -> src.getName().equals(request.source()))
                    .findAny()
                    .orElseThrow(() -> new SnapshotMissingException(repositoryName, request.source()));
                final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );
                if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(sourceSnapshotId))) {
                    throw new ConcurrentSnapshotExecutionException(
                        repositoryName,
                        sourceSnapshotId.getName(),
                        "cannot clone from snapshot that is being deleted"
                    );
                }
                ensureBelowConcurrencyLimit(repositoryName, snapshotName, snapshots, deletionsInProgress);
                final List<String> indicesForSnapshot = new ArrayList<>();
                for (IndexId indexId : repositoryData.getIndices().values()) {
                    if (repositoryData.getSnapshots(indexId).contains(sourceSnapshotId)) {
                        indicesForSnapshot.add(indexId.getName());
                    }
                }
                final List<String> matchingIndices = SnapshotUtils.filterIndices(
                    indicesForSnapshot,
                    request.indices(),
                    request.indicesOptions()
                );
                if (matchingIndices.isEmpty()) {
                    throw new SnapshotException(
                        new Snapshot(repositoryName, sourceSnapshotId),
                        "No indices in the source snapshot ["
                            + sourceSnapshotId
                            + "] matched requested pattern ["
                            + Strings.arrayToCommaDelimitedString(request.indices())
                            + "]"
                    );
                }
                newEntry = SnapshotsInProgress.startClone(
                    snapshot,
                    sourceSnapshotId,
                    repositoryData.resolveIndices(matchingIndices),
                    threadPool.absoluteTimeInMillis(),
                    repositoryData.getGenId(),
                    minCompatibleVersion(currentState.nodes().getMinNodeVersion(), repositoryData, null)
                );
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots.withAddedEntry(newEntry)).build();
            }

            @Override
            public void onFailure(Exception e) {
                initializingClones.remove(snapshot);
                logger.warn(() -> format("[%s][%s] failed to clone snapshot", repositoryName, snapshotName), e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, final ClusterState newState) {
                logger.info("snapshot clone [{}] started", snapshot);
                addListener(snapshot, listener.delegateFailureAndWrap((l, r) -> l.onResponse(null)));
                startCloning(repository, newEntry);
            }
        }, "clone_snapshot [" + request.source() + "][" + snapshotName + ']', listener::onFailure);
    }

    private static void ensureNoCleanupInProgress(
        final ClusterState currentState,
        final String repositoryName,
        final String snapshotName,
        final String reason
    ) {
        final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(
            RepositoryCleanupInProgress.TYPE,
            RepositoryCleanupInProgress.EMPTY
        );
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

    private static void ensureSnapshotNameAvailableInRepo(RepositoryData repositoryData, String snapshotName, Repository repository) {
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
     * Determine the number of shards in each index of a clone operation and update the cluster state accordingly.
     *
     * @param repository     repository to run operation on
     * @param cloneEntry     clone operation in the cluster state
     */
    private void startCloning(Repository repository, SnapshotsInProgress.Entry cloneEntry) {
        final Collection<IndexId> indices = cloneEntry.indices().values();
        final SnapshotId sourceSnapshot = cloneEntry.source();
        final Snapshot targetSnapshot = cloneEntry.snapshot();

        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        // Exception handler for IO exceptions with loading index and repo metadata
        final Consumer<Exception> onFailure = e -> {
            endingSnapshots.add(targetSnapshot);
            initializingClones.remove(targetSnapshot);
            logger.info(() -> "Failed to start snapshot clone [" + cloneEntry + "]", e);
            removeFailedSnapshotFromClusterState(targetSnapshot, e, null);
        };

        // 1. step, load SnapshotInfo to make sure that source snapshot was successful for the indices we want to clone
        // TODO: we could skip this step for snapshots with state SUCCESS
        final StepListener<SnapshotInfo> snapshotInfoListener = new StepListener<>();
        repository.getSnapshotInfo(sourceSnapshot, snapshotInfoListener);

        final StepListener<Collection<Tuple<IndexId, Integer>>> allShardCountsListener = new StepListener<>();
        final GroupedActionListener<Tuple<IndexId, Integer>> shardCountListener = new GroupedActionListener<>(
            indices.size(),
            allShardCountsListener
        );
        snapshotInfoListener.whenComplete(snapshotInfo -> {
            for (IndexId indexId : indices) {
                if (RestoreService.failed(snapshotInfo, indexId.getName())) {
                    throw new SnapshotException(
                        targetSnapshot,
                        "Can't clone index [" + indexId + "] because its snapshot was not successful."
                    );
                }
            }
            // 2. step, load the number of shards we have in each index to be cloned from the index metadata.
            repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
                for (IndexId index : indices) {
                    executor.execute(ActionRunnable.supply(shardCountListener, () -> {
                        final IndexMetadata metadata = repository.getSnapshotIndexMetaData(repositoryData, sourceSnapshot, index);
                        return Tuple.tuple(index, metadata.getNumberOfShards());
                    }));
                }
            }, onFailure));
        }, onFailure);

        // 3. step, we have all the shard counts, now update the cluster state to have clone jobs in the snap entry
        allShardCountsListener.whenComplete(counts -> executeConsistentStateUpdate(repository, repoData -> new ClusterStateUpdateTask() {

            private SnapshotsInProgress.Entry updatedEntry;

            @Override
            public ClusterState execute(ClusterState currentState) {
                final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final String repoName = cloneEntry.repository();
                final List<SnapshotsInProgress.Entry> existingEntries = snapshotsInProgress.forRepo(repoName);
                final List<SnapshotsInProgress.Entry> updatedEntries = new ArrayList<>(existingEntries.size());
                final String localNodeId = currentState.nodes().getLocalNodeId();
                final ShardGenerations shardGenerations = repoData.shardGenerations();
                for (SnapshotsInProgress.Entry existing : existingEntries) {
                    if (cloneEntry.snapshot().getSnapshotId().equals(existing.snapshot().getSnapshotId())) {
                        final ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> clonesBuilder = ImmutableOpenMap.builder();
                        final boolean readyToExecute = currentState.custom(
                            SnapshotDeletionsInProgress.TYPE,
                            SnapshotDeletionsInProgress.EMPTY
                        ).hasExecutingDeletion(repoName) == false;
                        final InFlightShardSnapshotStates inFlightShardStates;
                        if (readyToExecute) {
                            inFlightShardStates = InFlightShardSnapshotStates.forEntries(snapshotsInProgress.forRepo(repoName));
                        } else {
                            // no need to compute these, we'll mark all shards as queued anyway because we wait for the delete
                            inFlightShardStates = null;
                        }
                        for (Tuple<IndexId, Integer> count : counts) {
                            for (int shardId = 0; shardId < count.v2(); shardId++) {
                                final RepositoryShardId repoShardId = new RepositoryShardId(count.v1(), shardId);
                                final String indexName = repoShardId.indexName();
                                if (readyToExecute == false || inFlightShardStates.isActive(indexName, shardId)) {
                                    clonesBuilder.put(repoShardId, ShardSnapshotStatus.UNASSIGNED_QUEUED);
                                } else {
                                    clonesBuilder.put(
                                        repoShardId,
                                        new ShardSnapshotStatus(
                                            localNodeId,
                                            inFlightShardStates.generationForShard(repoShardId.index(), shardId, shardGenerations)
                                        )
                                    );
                                }
                            }
                        }
                        updatedEntry = cloneEntry.withClones(clonesBuilder.build());
                    } else {
                        updatedEntries.add(existing);
                    }
                }
                if (updatedEntry != null) {
                    // Move the now ready to execute clone operation to the back of the snapshot operations order because its
                    // shard snapshot state was based on all previous existing operations in progress
                    // TODO: If we could eventually drop the snapshot clone init phase we don't need this any longer
                    updatedEntries.add(updatedEntry);
                    return updateWithSnapshots(currentState, snapshotsInProgress.withUpdatedEntriesForRepo(repoName, updatedEntries), null);
                }
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                initializingClones.remove(targetSnapshot);
                logger.info(() -> "Failed to start snapshot clone [" + cloneEntry + "]", e);
                failAllListenersOnMasterFailOver(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                initializingClones.remove(targetSnapshot);
                if (updatedEntry != null) {
                    final Snapshot target = updatedEntry.snapshot();
                    final SnapshotId sourceSnapshot = updatedEntry.source();
                    for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> indexClone : updatedEntry.shardsByRepoShardId().entrySet()) {
                        final ShardSnapshotStatus shardStatusBefore = indexClone.getValue();
                        if (shardStatusBefore.state() != ShardState.INIT) {
                            continue;
                        }
                        final RepositoryShardId repoShardId = indexClone.getKey();
                        runReadyClone(target, sourceSnapshot, shardStatusBefore, repoShardId, repository);
                    }
                } else {
                    // Extremely unlikely corner case of master failing over between between starting the clone and
                    // starting shard clones.
                    logger.warn("Did not find expected entry [{}] in the cluster state", cloneEntry);
                }
            }
        }, "start snapshot clone", onFailure), onFailure);
    }

    private final Set<RepositoryShardId> currentlyCloning = Collections.synchronizedSet(new HashSet<>());

    private void runReadyClone(
        Snapshot target,
        SnapshotId sourceSnapshot,
        ShardSnapshotStatus shardStatusBefore,
        RepositoryShardId repoShardId,
        Repository repository
    ) {
        final SnapshotId targetSnapshot = target.getSnapshotId();
        final String localNodeId = clusterService.localNode().getId();
        if (currentlyCloning.add(repoShardId)) {
            repository.cloneShardSnapshot(
                sourceSnapshot,
                targetSnapshot,
                repoShardId,
                shardStatusBefore.generation(),
                ActionListener.wrap(
                    shardSnapshotResult -> innerUpdateSnapshotState(
                        target,
                        null,
                        repoShardId,
                        ShardSnapshotStatus.success(localNodeId, shardSnapshotResult),
                        ActionListener.runBefore(
                            ActionListener.wrap(
                                v -> logger.trace(
                                    "Marked [{}] as successfully cloned from [{}] to [{}]",
                                    repoShardId,
                                    sourceSnapshot,
                                    targetSnapshot
                                ),
                                e -> {
                                    logger.warn("Cluster state update after successful shard clone [{}] failed", repoShardId);
                                    failAllListenersOnMasterFailOver(e);
                                }
                            ),
                            () -> currentlyCloning.remove(repoShardId)
                        )
                    ),
                    e -> innerUpdateSnapshotState(
                        target,
                        null,
                        repoShardId,
                        new ShardSnapshotStatus(
                            localNodeId,
                            ShardState.FAILED,
                            "failed to clone shard snapshot",
                            shardStatusBefore.generation()
                        ),
                        ActionListener.runBefore(
                            ActionListener.wrap(
                                v -> logger.trace(
                                    "Marked [{}] as failed clone from [{}] to [{}]",
                                    repoShardId,
                                    sourceSnapshot,
                                    targetSnapshot
                                ),
                                ex -> {
                                    logger.warn("Cluster state update after failed shard clone [{}] failed", repoShardId);
                                    failAllListenersOnMasterFailOver(ex);
                                }
                            ),
                            () -> currentlyCloning.remove(repoShardId)
                        )
                    )
                )
            );
        }
    }

    private void ensureBelowConcurrencyLimit(
        String repository,
        String name,
        SnapshotsInProgress snapshotsInProgress,
        SnapshotDeletionsInProgress deletionsInProgress
    ) {
        final int inProgressOperations = snapshotsInProgress.count() + deletionsInProgress.getEntries().size();
        final int maxOps = maxConcurrentOperations;
        if (inProgressOperations >= maxOps) {
            throw new ConcurrentSnapshotExecutionException(
                repository,
                name,
                "Cannot start another operation, already running ["
                    + inProgressOperations
                    + "] operations and the current"
                    + " limit for concurrent snapshot operations is set to ["
                    + maxOps
                    + "]"
            );
        }
    }

    /**
     * Throws {@link RepositoryMissingException} if no repository by the given name is found in the given cluster state.
     */
    public static void ensureRepositoryExists(String repoName, ClusterState state) {
        if (state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY).repository(repoName) == null) {
            throw new RepositoryMissingException(repoName);
        }
    }

    /**
     * Validates snapshot request
     *
     * @param repositoryName repository name
     * @param snapshotName snapshot name
     * @param state   current cluster state
     */
    private static void validate(String repositoryName, String snapshotName, ClusterState state) {
        RepositoriesMetadata repositoriesMetadata = state.getMetadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        if (repositoriesMetadata.repository(repositoryName) == null) {
            throw new RepositoryMissingException(repositoryName);
        }
        validate(repositoryName, snapshotName);
    }

    private static void validate(final String repositoryName, final String snapshotName) {
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

    private static ShardGenerations buildGenerations(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        ShardGenerations.Builder builder = ShardGenerations.builder();
        if (snapshot.isClone()) {
            snapshot.shardsByRepoShardId().entrySet().forEach(c -> builder.put(c.getKey().index(), c.getKey().shardId(), c.getValue()));
        } else {
            snapshot.shardsByRepoShardId().entrySet().forEach(c -> {
                final Index index = snapshot.indexByName(c.getKey().indexName());
                if (metadata.index(index) == null) {
                    assert snapshot.partial() : "Index [" + index + "] was deleted during a snapshot but snapshot was not partial.";
                    return;
                }
                builder.put(c.getKey().index(), c.getKey().shardId(), c.getValue());
            });
        }
        return builder.build();
    }

    private static Metadata metadataForSnapshot(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        final Metadata.Builder builder;
        if (snapshot.includeGlobalState() == false) {
            // Remove global state from the cluster state
            builder = Metadata.builder();
            for (IndexId index : snapshot.indices().values()) {
                final IndexMetadata indexMetadata = metadata.index(index.getName());
                if (indexMetadata == null) {
                    assert snapshot.partial() : "Index [" + index + "] was deleted during a snapshot but snapshot was not partial.";
                } else {
                    builder.put(indexMetadata, false);
                }
            }
        } else {
            builder = Metadata.builder(metadata);
        }
        // Only keep those data streams in the metadata that were actually requested by the initial snapshot create operation and that have
        // all their indices contained in the snapshot
        final Map<String, DataStream> dataStreams = new HashMap<>();
        final Set<String> indicesInSnapshot = snapshot.indices().keySet();
        for (String dataStreamName : snapshot.dataStreams()) {
            DataStream dataStream = metadata.dataStreams().get(dataStreamName);
            if (dataStream == null) {
                assert snapshot.partial()
                    : "Data stream [" + dataStreamName + "] was deleted during a snapshot but snapshot was not partial.";
            } else {
                boolean missingIndex = false;
                for (Index index : dataStream.getIndices()) {
                    final String indexName = index.getName();
                    if (builder.get(indexName) == null || indicesInSnapshot.contains(indexName) == false) {
                        missingIndex = true;
                        break;
                    }
                }
                final DataStream reconciled = missingIndex ? dataStream.snapshot(indicesInSnapshot) : dataStream;
                if (reconciled != null) {
                    dataStreams.put(dataStreamName, reconciled);
                }
            }
        }
        return builder.dataStreams(dataStreams, filterDataStreamAliases(dataStreams, metadata.dataStreamAliases())).build();
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on master node
     * </p>
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repository          repository id
     * @param snapshots           list of snapshots that will be used as a filter, empty list means no snapshots are filtered
     * @return list of metadata for currently running snapshots
     */
    public static List<SnapshotsInProgress.Entry> currentSnapshots(
        @Nullable SnapshotsInProgress snapshotsInProgress,
        String repository,
        List<String> snapshots
    ) {
        if (snapshotsInProgress == null || snapshotsInProgress.isEmpty()) {
            return Collections.emptyList();
        }
        if ("_all".equals(repository)) {
            return snapshotsInProgress.asStream().toList();
        }
        if (snapshots.isEmpty()) {
            return snapshotsInProgress.forRepo(repository);
        }
        List<SnapshotsInProgress.Entry> builder = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(repository)) {
            for (String snapshot : snapshots) {
                if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                    builder.add(entry);
                    break;
                }
            }
        }
        return unmodifiableList(builder);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                // We don't remove old master when master flips anymore. So, we need to check for change in master
                SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final boolean newMaster = event.previousState().nodes().isLocalNodeElectedMaster() == false;
                processExternalChanges(
                    newMaster || removedNodesCleanupNeeded(snapshotsInProgress, event.nodesDelta().removedNodes()),
                    event.routingTableChanged() && waitingShardsStartedOrUnassigned(snapshotsInProgress, event)
                );
            } else {
                if (snapshotCompletionListeners.isEmpty() == false) {
                    // We have snapshot listeners but are not the master any more. Fail all waiting listeners except for those that already
                    // have their snapshots finalizing (those that are already finalizing will fail on their own from to update the cluster
                    // state).
                    for (Snapshot snapshot : Set.copyOf(snapshotCompletionListeners.keySet())) {
                        if (endingSnapshots.add(snapshot)) {
                            failSnapshotCompletionListeners(snapshot, new SnapshotException(snapshot, "no longer master"));
                            assert endingSnapshots.contains(snapshot) == false : snapshot;
                        }
                    }
                }
                if (snapshotDeletionListeners.isEmpty() == false) {
                    final Exception e = new NotMasterException("no longer master");
                    for (String delete : Set.copyOf(snapshotDeletionListeners.keySet())) {
                        failListenersIgnoringException(snapshotDeletionListeners.remove(delete), e);
                    }
                }
            }
        } catch (Exception e) {
            assert false : new AssertionError(e);
            logger.warn("Failed to update snapshot state ", e);
        }
        assert assertConsistentWithClusterState(event.state());
        assert assertNoDanglingSnapshots(event.state());
    }

    private boolean assertConsistentWithClusterState(ClusterState state) {
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        if (snapshotsInProgress.isEmpty() == false) {
            synchronized (endingSnapshots) {
                final Set<Snapshot> runningSnapshots = Stream.concat(
                    snapshotsInProgress.asStream().map(SnapshotsInProgress.Entry::snapshot),
                    endingSnapshots.stream()
                ).collect(Collectors.toSet());
                final Set<Snapshot> snapshotListenerKeys = snapshotCompletionListeners.keySet();
                assert runningSnapshots.containsAll(snapshotListenerKeys)
                    : "Saw completion listeners for unknown snapshots in "
                        + snapshotListenerKeys
                        + " but running snapshots are "
                        + runningSnapshots;
            }
        }
        final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        );
        if (snapshotDeletionsInProgress.hasDeletionsInProgress()) {
            synchronized (repositoryOperations.runningDeletions) {
                final Set<String> runningDeletes = Stream.concat(
                    snapshotDeletionsInProgress.getEntries().stream().map(SnapshotDeletionsInProgress.Entry::uuid),
                    repositoryOperations.runningDeletions.stream()
                ).collect(Collectors.toSet());
                final Set<String> deleteListenerKeys = snapshotDeletionListeners.keySet();
                assert runningDeletes.containsAll(deleteListenerKeys)
                    : "Saw deletions listeners for unknown uuids in " + deleteListenerKeys + " but running deletes are " + runningDeletes;
            }
        }
        return true;
    }

    // Assert that there are no snapshots that have a shard that is waiting to be assigned even though the cluster state would allow for it
    // to be assigned
    private static boolean assertNoDanglingSnapshots(ClusterState state) {
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        );
        final Set<String> reposWithRunningDelete = snapshotDeletionsInProgress.getEntries()
            .stream()
            .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
            .map(SnapshotDeletionsInProgress.Entry::repository)
            .collect(Collectors.toSet());
        for (List<SnapshotsInProgress.Entry> repoEntry : snapshotsInProgress.entriesByRepo()) {
            final SnapshotsInProgress.Entry entry = repoEntry.get(0);
            for (ShardSnapshotStatus value : entry.shardsByRepoShardId().values()) {
                if (value.equals(ShardSnapshotStatus.UNASSIGNED_QUEUED)) {
                    assert reposWithRunningDelete.contains(entry.repository())
                        : "Found shard snapshot waiting to be assigned in [" + entry + "] but it is not blocked by any running delete";
                } else if (value.isActive()) {
                    assert reposWithRunningDelete.contains(entry.repository()) == false
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
     * Updates the state of in-progress snapshots in reaction to a change in the configuration of the cluster nodes (master fail-over or
     * disconnect of a data node that was executing a snapshot) or a routing change that started shards whose snapshot state is
     * {@link SnapshotsInProgress.ShardState#WAITING}.
     *
     * @param changedNodes true iff either a master fail-over occurred or a data node that was doing snapshot work got removed from the
     *                     cluster
     * @param startShards  true iff any waiting shards were started due to a routing change
     */
    private void processExternalChanges(boolean changedNodes, boolean startShards) {
        if (changedNodes == false && startShards == false) {
            // nothing to do, no relevant external change happened
            return;
        }
        final String source = "update snapshot after shards started ["
            + startShards
            + "] or node configuration changed ["
            + changedNodes
            + "]";
        submitUnbatchedTask(source, new ClusterStateUpdateTask() {

            private final Collection<SnapshotsInProgress.Entry> finishedSnapshots = new ArrayList<>();

            private final Collection<SnapshotDeletionsInProgress.Entry> deletionsToExecute = new ArrayList<>();

            @Override
            public ClusterState execute(ClusterState currentState) {
                RoutingTable routingTable = currentState.routingTable();
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final SnapshotDeletionsInProgress deletes = currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );
                DiscoveryNodes nodes = currentState.nodes();
                final EnumSet<State> statesToUpdate;
                // If we are reacting to a change in the cluster node configuration we have to update the shard states of both started
                // and
                // aborted snapshots to potentially fail shards running on the removed nodes
                if (changedNodes) {
                    statesToUpdate = EnumSet.of(State.STARTED, State.ABORTED);
                } else {
                    // We are reacting to shards that started only so which only affects the individual shard states of started
                    // snapshots
                    statesToUpdate = EnumSet.of(State.STARTED);
                }

                // We keep a cache of shards that failed in this map. If we fail a shardId for a given repository because of
                // a node leaving or shard becoming unassigned for one snapshot, we will also fail it for all subsequent enqueued
                // snapshots for the same repository
                // TODO: the code in this state update duplicates large chunks of the logic in #SHARD_STATE_EXECUTOR.
                // We should refactor it to ideally also go through #SHARD_STATE_EXECUTOR by hand-crafting shard state updates
                // that encapsulate nodes leaving or indices having been deleted and passing them to the executor instead.
                SnapshotsInProgress updated = snapshots;
                for (final List<SnapshotsInProgress.Entry> snapshotsInRepo : snapshots.entriesByRepo()) {
                    boolean changed = false;
                    final List<SnapshotsInProgress.Entry> updatedEntriesForRepo = new ArrayList<>();
                    final Map<RepositoryShardId, ShardSnapshotStatus> knownFailures = new HashMap<>();
                    final String repository = snapshotsInRepo.get(0).repository();
                    for (SnapshotsInProgress.Entry snapshot : snapshotsInRepo) {
                        if (statesToUpdate.contains(snapshot.state())) {
                            if (snapshot.isClone()) {
                                if (snapshot.shardsByRepoShardId().isEmpty()) {
                                    // Currently initializing clone
                                    if (initializingClones.contains(snapshot.snapshot())) {
                                        updatedEntriesForRepo.add(snapshot);
                                    } else {
                                        logger.debug("removing not yet start clone operation [{}]", snapshot);
                                        changed = true;
                                    }
                                } else {
                                    // see if any clones may have had a shard become available for execution because of failures
                                    if (deletes.hasExecutingDeletion(repository)) {
                                        // Currently executing a delete for this repo, no need to try and update any clone operations.
                                        // The logic for finishing the delete will update running clones with the latest changes.
                                        updatedEntriesForRepo.add(snapshot);
                                        continue;
                                    }
                                    ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> clones = null;
                                    InFlightShardSnapshotStates inFlightShardSnapshotStates = null;
                                    for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> failureEntry : knownFailures.entrySet()) {
                                        final RepositoryShardId repositoryShardId = failureEntry.getKey();
                                        final ShardSnapshotStatus existingStatus = snapshot.shardsByRepoShardId().get(repositoryShardId);
                                        if (ShardSnapshotStatus.UNASSIGNED_QUEUED.equals(existingStatus)) {
                                            if (inFlightShardSnapshotStates == null) {
                                                inFlightShardSnapshotStates = InFlightShardSnapshotStates.forEntries(updatedEntriesForRepo);
                                            }
                                            if (inFlightShardSnapshotStates.isActive(
                                                repositoryShardId.indexName(),
                                                repositoryShardId.shardId()
                                            )) {
                                                // we already have this shard assigned to another task
                                                continue;
                                            }
                                            if (clones == null) {
                                                clones = ImmutableOpenMap.builder(snapshot.shardsByRepoShardId());
                                            }
                                            // We can use the generation from the shard failure to start the clone operation here
                                            // because #processWaitingShardsAndRemovedNodes adds generations to failure statuses that
                                            // allow us to start another clone.
                                            // The usual route via InFlightShardSnapshotStates is not viable here because it would
                                            // require a consistent view of the RepositoryData which we don't have here because this
                                            // state update runs over all repositories at once.
                                            clones.put(
                                                repositoryShardId,
                                                new ShardSnapshotStatus(nodes.getLocalNodeId(), failureEntry.getValue().generation())
                                            );
                                        }
                                    }
                                    if (clones != null) {
                                        changed = true;
                                        updatedEntriesForRepo.add(snapshot.withClones(clones.build()));
                                    } else {
                                        updatedEntriesForRepo.add(snapshot);
                                    }
                                }
                            } else {
                                ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards = processWaitingShardsAndRemovedNodes(
                                    snapshot,
                                    routingTable,
                                    nodes,
                                    knownFailures
                                );
                                if (shards != null) {
                                    final SnapshotsInProgress.Entry updatedSnapshot = snapshot.withShardStates(shards);
                                    changed = true;
                                    if (updatedSnapshot.state().completed()) {
                                        finishedSnapshots.add(updatedSnapshot);
                                    }
                                    updatedEntriesForRepo.add(updatedSnapshot);
                                } else {
                                    updatedEntriesForRepo.add(snapshot);
                                }
                            }
                        } else if (snapshot.repositoryStateId() == RepositoryData.UNKNOWN_REPO_GEN) {
                            // BwC path, older versions could create entries with unknown repo GEN in INIT or ABORTED state that did not
                            // yet write anything to the repository physically. This means we can simply remove these from the cluster
                            // state without having to do any additional cleanup.
                            changed = true;
                            logger.debug("[{}] was found in dangling INIT or ABORTED state", snapshot);
                        } else {
                            if (snapshot.state().completed() || completed(snapshot.shardsByRepoShardId().values())) {
                                finishedSnapshots.add(snapshot);
                            }
                            updatedEntriesForRepo.add(snapshot);
                        }
                    }
                    if (changed) {
                        updated = updated.withUpdatedEntriesForRepo(repository, updatedEntriesForRepo);
                    }
                }
                final ClusterState res = readyDeletions(
                    updated != snapshots
                        ? ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, updated).build()
                        : currentState
                ).v1();
                for (SnapshotDeletionsInProgress.Entry delete : res.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                ).getEntries()) {
                    if (delete.state() == SnapshotDeletionsInProgress.State.STARTED) {
                        deletionsToExecute.add(delete);
                    }
                }
                return res;
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> format("failed to update snapshot state after shards started or nodes removed from [%s] ", source), e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                final SnapshotDeletionsInProgress snapshotDeletionsInProgress = newState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );
                if (finishedSnapshots.isEmpty() == false) {
                    // If we found snapshots that should be finalized as a result of the CS update we try to initiate finalization for
                    // them
                    // unless there is an executing snapshot delete already. If there is an executing snapshot delete we don't have to
                    // enqueue the snapshot finalizations here because the ongoing delete will take care of that when removing the
                    // delete
                    // from the cluster state
                    final Set<String> reposWithRunningDeletes = snapshotDeletionsInProgress.getEntries()
                        .stream()
                        .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
                        .map(SnapshotDeletionsInProgress.Entry::repository)
                        .collect(Collectors.toSet());
                    for (SnapshotsInProgress.Entry entry : finishedSnapshots) {
                        if (reposWithRunningDeletes.contains(entry.repository()) == false) {
                            endSnapshot(entry, newState.metadata(), null);
                        }
                    }
                }
                startExecutableClones(newState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY), null);
                // run newly ready deletes
                for (SnapshotDeletionsInProgress.Entry entry : deletionsToExecute) {
                    if (tryEnterRepoLoop(entry.repository())) {
                        deleteSnapshotsFromRepository(entry, newState.nodes().getMinNodeVersion());
                    }
                }
            }
        });
    }

    private static ImmutableOpenMap<ShardId, ShardSnapshotStatus> processWaitingShardsAndRemovedNodes(
        SnapshotsInProgress.Entry entry,
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        Map<RepositoryShardId, ShardSnapshotStatus> knownFailures
    ) {
        assert entry.isClone() == false : "clones take a different path";
        boolean snapshotChanged = false;
        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
        for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> shardEntry : entry.shardsByRepoShardId().entrySet()) {
            ShardSnapshotStatus shardStatus = shardEntry.getValue();
            ShardId shardId = entry.shardId(shardEntry.getKey());
            if (shardStatus.equals(ShardSnapshotStatus.UNASSIGNED_QUEUED)) {
                // this shard snapshot is waiting for a previous snapshot to finish execution for this shard
                final ShardSnapshotStatus knownFailure = knownFailures.get(shardEntry.getKey());
                if (knownFailure == null) {
                    final IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                    if (indexShardRoutingTable == null) {
                        // shard became unassigned while queued after a delete or clone operation so we can fail as missing here
                        assert entry.partial();
                        snapshotChanged = true;
                        logger.debug("failing snapshot of shard [{}] because index got deleted", shardId);
                        shards.put(shardId, ShardSnapshotStatus.MISSING);
                        knownFailures.put(shardEntry.getKey(), ShardSnapshotStatus.MISSING);
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
            } else if (shardStatus.state() == ShardState.WAITING) {
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        if (shardRouting.primaryShard().started()) {
                            // Shard that we were waiting for has started on a node, let's process it
                            snapshotChanged = true;
                            logger.trace("starting shard that we were waiting for [{}] on node [{}]", shardId, shardStatus.nodeId());
                            shards.put(
                                shardId,
                                new ShardSnapshotStatus(shardRouting.primaryShard().currentNodeId(), shardStatus.generation())
                            );
                            continue;
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                            shards.put(shardId, shardStatus);
                            continue;
                        }
                    }
                }
                // Shard that we were waiting for went into unassigned state or disappeared - giving up
                snapshotChanged = true;
                logger.warn("failing snapshot of shard [{}] on unassigned shard [{}]", shardId, shardStatus.nodeId());
                final ShardSnapshotStatus failedState = new ShardSnapshotStatus(
                    shardStatus.nodeId(),
                    ShardState.FAILED,
                    "shard is unassigned",
                    shardStatus.generation()
                );
                shards.put(shardId, failedState);
                knownFailures.put(shardEntry.getKey(), failedState);
            } else if (shardStatus.state().completed() == false && shardStatus.nodeId() != null) {
                if (nodes.nodeExists(shardStatus.nodeId())) {
                    shards.put(shardId, shardStatus);
                } else {
                    // TODO: Restart snapshot on another node?
                    snapshotChanged = true;
                    logger.warn("failing snapshot of shard [{}] on departed node [{}]", shardId, shardStatus.nodeId());
                    final ShardSnapshotStatus failedState = new ShardSnapshotStatus(
                        shardStatus.nodeId(),
                        ShardState.FAILED,
                        "node left the cluster during snapshot",
                        shardStatus.generation()
                    );
                    shards.put(shardId, failedState);
                    knownFailures.put(shardEntry.getKey(), failedState);
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

    private static boolean waitingShardsStartedOrUnassigned(SnapshotsInProgress snapshotsInProgress, ClusterChangedEvent event) {
        for (List<SnapshotsInProgress.Entry> entries : snapshotsInProgress.entriesByRepo()) {
            for (SnapshotsInProgress.Entry entry : entries) {
                if (entry.state() == State.STARTED && entry.isClone() == false) {
                    for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> shardStatus : entry.shardsByRepoShardId().entrySet()) {
                        final ShardState state = shardStatus.getValue().state();
                        if (state != ShardState.WAITING && state != ShardState.QUEUED) {
                            continue;
                        }
                        final RepositoryShardId shardId = shardStatus.getKey();
                        if (event.indexRoutingTableChanged(shardId.indexName())) {
                            IndexRoutingTable indexShardRoutingTable = event.state()
                                .getRoutingTable()
                                .index(entry.indexByName(shardId.indexName()));
                            if (indexShardRoutingTable == null) {
                                // index got removed concurrently and we have to fail WAITING or QUEUED state shards
                                return true;
                            }
                            ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.shardId()).primaryShard();
                            if (shardRouting != null && (shardRouting.started() || shardRouting.unassigned())) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private static boolean removedNodesCleanupNeeded(SnapshotsInProgress snapshotsInProgress, List<DiscoveryNode> removedNodes) {
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
            for (ShardSnapshotStatus shardSnapshotStatus : snapshot.shardsByRepoShardId().values()) {
                if (shardSnapshotStatus.state().completed() == false && removedNodeIds.contains(shardSnapshotStatus.nodeId())) {
                    // Snapshot had an incomplete shard running on a removed node so we need to adjust that shard's snapshot status
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Finalizes the snapshot in the repository.
     *
     * @param entry snapshot
     */
    private void endSnapshot(SnapshotsInProgress.Entry entry, Metadata metadata, @Nullable RepositoryData repositoryData) {
        final Snapshot snapshot = entry.snapshot();
        final boolean newFinalization = endingSnapshots.add(snapshot);
        if (entry.isClone() && entry.state() == State.FAILED) {
            logger.debug("Removing failed snapshot clone [{}] from cluster state", entry);
            if (newFinalization) {
                removeFailedSnapshotFromClusterState(snapshot, new SnapshotException(snapshot, entry.failure()), null);
            }
            return;
        }
        final String repoName = snapshot.getRepository();
        if (tryEnterRepoLoop(repoName)) {
            if (repositoryData == null) {
                repositoriesService.repository(repoName).getRepositoryData(new ActionListener<>() {
                    @Override
                    public void onResponse(RepositoryData repositoryData) {
                        finalizeSnapshotEntry(snapshot, metadata, repositoryData);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        submitUnbatchedTask("fail repo tasks for [" + repoName + "]", new FailPendingRepoTasksTask(repoName, e));
                    }
                });
            } else {
                finalizeSnapshotEntry(snapshot, metadata, repositoryData);
            }
        } else {
            if (newFinalization) {
                repositoryOperations.addFinalization(snapshot, metadata);
            }
        }
    }

    /**
     * Try starting to run a snapshot finalization or snapshot delete for the given repository. If this method returns
     * {@code true} then snapshot finalizations and deletions for the repo may be executed. Once no more operations are
     * ready for the repository {@link #leaveRepoLoop(String)} should be invoked so that a subsequent state change that
     * causes another operation to become ready can execute.
     *
     * @return true if a finalization or snapshot delete may be started at this point
     */
    private boolean tryEnterRepoLoop(String repository) {
        return currentlyFinalizing.add(repository);
    }

    /**
     * Stop polling for ready snapshot finalizations or deletes in state {@link SnapshotDeletionsInProgress.State#STARTED} to execute
     * for the given repository.
     */
    private void leaveRepoLoop(String repository) {
        final boolean removed = currentlyFinalizing.remove(repository);
        assert removed;
    }

    private void finalizeSnapshotEntry(Snapshot snapshot, Metadata metadata, RepositoryData repositoryData) {
        assert currentlyFinalizing.contains(snapshot.getRepository());
        try {
            SnapshotsInProgress.Entry entry = clusterService.state()
                .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .snapshot(snapshot);
            final String failure = entry.failure();
            logger.trace("[{}] finalizing snapshot in repository, state: [{}], failure[{}]", snapshot, entry.state(), failure);
            final ShardGenerations shardGenerations = buildGenerations(entry, metadata);
            final List<String> finalIndices = shardGenerations.indices().stream().map(IndexId::getName).toList();
            final Set<String> indexNames = new HashSet<>(finalIndices);
            ArrayList<SnapshotShardFailure> shardFailures = new ArrayList<>();
            for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> shardStatus : entry.shardsByRepoShardId().entrySet()) {
                RepositoryShardId shardId = shardStatus.getKey();
                if (indexNames.contains(shardId.indexName()) == false) {
                    assert entry.partial() : "only ignoring shard failures for concurrently deleted indices for partial snapshots";
                    continue;
                }
                ShardSnapshotStatus status = shardStatus.getValue();
                final ShardState state = status.state();
                if (state.failed()) {
                    shardFailures.add(new SnapshotShardFailure(status.nodeId(), entry.shardId(shardId), status.reason()));
                } else if (state.completed() == false) {
                    shardFailures.add(new SnapshotShardFailure(status.nodeId(), entry.shardId(shardId), "skipped"));
                } else {
                    assert state == ShardState.SUCCESS;
                }
            }
            final String repository = snapshot.getRepository();
            final StepListener<Metadata> metadataListener = new StepListener<>();
            final Repository repo = repositoriesService.repository(snapshot.getRepository());
            if (entry.isClone()) {
                threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.supply(metadataListener, () -> {
                    final Metadata existing = repo.getSnapshotGlobalMetadata(entry.source());
                    final Metadata.Builder metaBuilder = Metadata.builder(existing);
                    final Set<Index> existingIndices = new HashSet<>();
                    for (IndexId index : entry.indices().values()) {
                        final IndexMetadata indexMetadata = repo.getSnapshotIndexMetaData(repositoryData, entry.source(), index);
                        existingIndices.add(indexMetadata.getIndex());
                        metaBuilder.put(indexMetadata, false);
                    }
                    // remove those data streams from metadata for which we are missing indices
                    Map<String, DataStream> dataStreamsToCopy = new HashMap<>();
                    for (Map.Entry<String, DataStream> dataStreamEntry : existing.dataStreams().entrySet()) {
                        if (existingIndices.containsAll(dataStreamEntry.getValue().getIndices())) {
                            dataStreamsToCopy.put(dataStreamEntry.getKey(), dataStreamEntry.getValue());
                        }
                    }
                    Map<String, DataStreamAlias> dataStreamAliasesToCopy = filterDataStreamAliases(
                        dataStreamsToCopy,
                        existing.dataStreamAliases()
                    );
                    metaBuilder.dataStreams(dataStreamsToCopy, dataStreamAliasesToCopy);
                    return metaBuilder.build();
                }));
            } else {
                metadataListener.onResponse(metadata);
            }
            metadataListener.whenComplete(meta -> {
                final Metadata metaForSnapshot = metadataForSnapshot(entry, meta);

                final Map<String, SnapshotInfo.IndexSnapshotDetails> indexSnapshotDetails = Maps.newMapWithExpectedSize(
                    finalIndices.size()
                );
                for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> shardEntry : entry.shardsByRepoShardId().entrySet()) {
                    indexSnapshotDetails.compute(shardEntry.getKey().indexName(), (indexName, current) -> {
                        if (current == SnapshotInfo.IndexSnapshotDetails.SKIPPED) {
                            // already found an unsuccessful shard in this index, skip this shard
                            return current;
                        }

                        final ShardSnapshotStatus shardSnapshotStatus = shardEntry.getValue();
                        if (shardSnapshotStatus.state() != ShardState.SUCCESS) {
                            // first unsuccessful shard in this index found, record that this index should be skipped
                            return SnapshotInfo.IndexSnapshotDetails.SKIPPED;
                        }

                        final ShardSnapshotResult result = shardSnapshotStatus.shardSnapshotResult();
                        if (result == null) {
                            // detailed result not recorded, skip this index
                            return SnapshotInfo.IndexSnapshotDetails.SKIPPED;
                        }

                        if (current == null) {
                            return new SnapshotInfo.IndexSnapshotDetails(1, result.getSize(), result.getSegmentCount());
                        } else {
                            return new SnapshotInfo.IndexSnapshotDetails(
                                current.getShardCount() + 1,
                                ByteSizeValue.ofBytes(current.getSize().getBytes() + result.getSize().getBytes()),
                                Math.max(current.getMaxSegmentsPerShard(), result.getSegmentCount())
                            );
                        }
                    });
                }
                indexSnapshotDetails.entrySet().removeIf(e -> e.getValue().getShardCount() == 0);

                final SnapshotInfo snapshotInfo = new SnapshotInfo(
                    snapshot,
                    finalIndices,
                    entry.dataStreams().stream().filter(metaForSnapshot.dataStreams()::containsKey).toList(),
                    entry.partial() ? onlySuccessfulFeatureStates(entry, finalIndices) : entry.featureStates(),
                    failure,
                    threadPool.absoluteTimeInMillis(),
                    entry.partial() ? shardGenerations.totalShards() : entry.shardsByRepoShardId().size(),
                    shardFailures,
                    entry.includeGlobalState(),
                    entry.userMetadata(),
                    entry.startTime(),
                    indexSnapshotDetails
                );
                final ListenableFuture<List<ActionListener<SnapshotInfo>>> snapshotListeners = new ListenableFuture<>();
                repo.finalizeSnapshot(
                    new FinalizeSnapshotContext(
                        shardGenerations,
                        repositoryData.getGenId(),
                        metaForSnapshot,
                        snapshotInfo,
                        entry.version(),
                        ActionListener.wrap(updatedRepositoryData -> {
                            // get a hold of the listeners for this snapshot here and store them in the future so they can be used
                            // by the snapshot info callback below and won't be failed needlessly if #runNextQueuedOperation runs into
                            // a fatal like e.g. this node stopped being the master node
                            snapshotListeners.onResponse(endAndGetListenersToResolve(snapshot));
                            runNextQueuedOperation(updatedRepositoryData, repository, true);
                        }, e -> handleFinalizationFailure(e, snapshot, repositoryData)),
                        snInfo -> snapshotListeners.addListener(new ActionListener<>() {
                            @Override
                            public void onResponse(List<ActionListener<SnapshotInfo>> actionListeners) {
                                completeListenersIgnoringException(actionListeners, snInfo);
                                logger.info("snapshot [{}] completed with state [{}]", snapshot, snInfo.state());
                            }

                            @Override
                            public void onFailure(Exception e) {
                                // never fails
                                assert false : e;
                            }
                        })
                    )
                );
            }, e -> handleFinalizationFailure(e, snapshot, repositoryData));
        } catch (Exception e) {
            assert false : new AssertionError(e);
            handleFinalizationFailure(e, snapshot, repositoryData);
        }
    }

    /**
     * Removes all feature states which have missing or failed shards, as they are no longer safely restorable.
     * @param entry The "in progress" entry with a list of feature states and one or more failed shards.
     * @param finalIndices The final list of indices in the snapshot, after any indices that were concurrently deleted are removed.
     * @return The list of feature states which were completed successfully in the given entry.
     */
    private static List<SnapshotFeatureInfo> onlySuccessfulFeatureStates(SnapshotsInProgress.Entry entry, List<String> finalIndices) {
        assert entry.partial() : "should not try to filter feature states from a non-partial entry";

        // Figure out which indices have unsuccessful shards
        Set<String> indicesWithUnsuccessfulShards = new HashSet<>();
        entry.shardsByRepoShardId().entrySet().forEach(shard -> {
            final ShardState shardState = shard.getValue().state();
            if (shardState.failed() || shardState.completed() == false) {
                indicesWithUnsuccessfulShards.add(shard.getKey().indexName());
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
     * Remove a snapshot from {@link #endingSnapshots} set and return its completion listeners that must be resolved.
     */
    private List<ActionListener<SnapshotInfo>> endAndGetListenersToResolve(Snapshot snapshot) {
        // get listeners before removing from the ending snapshots set to not trip assertion in #assertConsistentWithClusterState that
        // makes sure we don't have listeners for snapshots that aren't tracked in any internal state of this class
        final List<ActionListener<SnapshotInfo>> listenersToComplete = snapshotCompletionListeners.remove(snapshot);
        endingSnapshots.remove(snapshot);
        return listenersToComplete;
    }

    /**
     * Handles failure to finalize a snapshot. If the exception indicates that this node was unable to publish a cluster state and stopped
     * being the master node, then fail all snapshot create and delete listeners executing on this node by delegating to
     * {@link #failAllListenersOnMasterFailOver}. Otherwise, i.e. as a result of failing to write to the snapshot repository for some
     * reason, remove the snapshot's {@link SnapshotsInProgress.Entry} from the cluster state and move on with other queued snapshot
     * operations if there are any.
     *
     * @param e              exception encountered
     * @param snapshot       snapshot that failed to finalize
     * @param repositoryData current repository data for the snapshot's repository
     */
    private void handleFinalizationFailure(Exception e, Snapshot snapshot, RepositoryData repositoryData) {
        if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class) != null) {
            // Failure due to not being master any more, don't try to remove snapshot from cluster state the next master
            // will try ending this snapshot again
            logger.debug(() -> "[" + snapshot + "] failed to update cluster state during snapshot finalization", e);
            failSnapshotCompletionListeners(
                snapshot,
                new SnapshotException(snapshot, "Failed to update cluster state during snapshot finalization", e)
            );
            failAllListenersOnMasterFailOver(e);
        } else {
            logger.warn(() -> "[" + snapshot + "] failed to finalize snapshot", e);
            removeFailedSnapshotFromClusterState(snapshot, e, repositoryData);
        }
    }

    /**
     * Run the next queued up repository operation for the given repository name.
     *
     * @param repositoryData current repository data
     * @param repository     repository name
     * @param attemptDelete  whether to try and run delete operations that are ready in the cluster state if no
     *                       snapshot create operations remain to execute
     */
    private void runNextQueuedOperation(RepositoryData repositoryData, String repository, boolean attemptDelete) {
        assert currentlyFinalizing.contains(repository);
        final Tuple<Snapshot, Metadata> nextFinalization = repositoryOperations.pollFinalization(repository);
        if (nextFinalization == null) {
            if (attemptDelete) {
                runReadyDeletions(repositoryData, repository);
            } else {
                leaveRepoLoop(repository);
            }
        } else {
            logger.trace("Moving on to finalizing next snapshot [{}]", nextFinalization);
            finalizeSnapshotEntry(nextFinalization.v1(), nextFinalization.v2(), repositoryData);
        }
    }

    /**
     * Runs a cluster state update that checks whether we have outstanding snapshot deletions that can be executed and executes them.
     *
     * TODO: optimize this to execute in a single CS update together with finalizing the latest snapshot
     */
    private void runReadyDeletions(RepositoryData repositoryData, String repository) {
        submitUnbatchedTask("Run ready deletions", new ClusterStateUpdateTask() {

            private SnapshotDeletionsInProgress.Entry deletionToRun;

            @Override
            public ClusterState execute(ClusterState currentState) {
                assert readyDeletions(currentState).v1() == currentState
                    : "Deletes should have been set to ready by finished snapshot deletes and finalizations";
                for (SnapshotDeletionsInProgress.Entry entry : currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                ).getEntries()) {
                    if (entry.repository().equals(repository) && entry.state() == SnapshotDeletionsInProgress.State.STARTED) {
                        deletionToRun = entry;
                        break;
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to run ready delete operations", e);
                failAllListenersOnMasterFailOver(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                if (deletionToRun == null) {
                    runNextQueuedOperation(repositoryData, repository, false);
                } else {
                    deleteSnapshotsFromRepository(deletionToRun, repositoryData, newState.nodes().getMinNodeVersion());
                }
            }
        });
    }

    /**
     * Finds snapshot delete operations that are ready to execute in the given {@link ClusterState} and computes a new cluster state that
     * has all executable deletes marked as executing. Returns a {@link Tuple} of the updated cluster state and all executable deletes.
     * This can either be {@link SnapshotDeletionsInProgress.Entry} that were already in state
     * {@link SnapshotDeletionsInProgress.State#STARTED} or waiting entries in state {@link SnapshotDeletionsInProgress.State#WAITING}
     * that were moved to {@link SnapshotDeletionsInProgress.State#STARTED} in the returned updated cluster state.
     *
     * @param currentState current cluster state
     * @return tuple of an updated cluster state and currently executable snapshot delete operations
     */
    private static Tuple<ClusterState, List<SnapshotDeletionsInProgress.Entry>> readyDeletions(ClusterState currentState) {
        final SnapshotDeletionsInProgress deletions = currentState.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        );
        if (deletions.hasDeletionsInProgress() == false) {
            return Tuple.tuple(currentState, List.of());
        }
        final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE);
        assert snapshotsInProgress != null;
        final Set<String> repositoriesSeen = new HashSet<>();
        boolean changed = false;
        final ArrayList<SnapshotDeletionsInProgress.Entry> readyDeletions = new ArrayList<>();
        final List<SnapshotDeletionsInProgress.Entry> newDeletes = new ArrayList<>();
        for (SnapshotDeletionsInProgress.Entry entry : deletions.getEntries()) {
            final String repo = entry.repository();
            if (repositoriesSeen.add(entry.repository())
                && entry.state() == SnapshotDeletionsInProgress.State.WAITING
                && snapshotsInProgress.forRepo(repo).stream().noneMatch(SnapshotsService::isWritingToRepository)) {
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
     *
     * @param state    current cluster state
     * @param snapshot snapshot for which to remove the snapshot operation
     * @return updated cluster state
     */
    public static ClusterState stateWithoutSnapshot(ClusterState state, Snapshot snapshot) {
        final SnapshotsInProgress snapshots = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        ClusterState result = state;
        int indexOfEntry = -1;
        final List<SnapshotsInProgress.Entry> entryList = snapshots.forRepo(snapshot.getRepository());
        for (int i = 0; i < entryList.size(); i++) {
            SnapshotsInProgress.Entry entry = entryList.get(i);
            if (entry.snapshot().equals(snapshot)) {
                indexOfEntry = i;
                break;
            }
        }
        if (indexOfEntry >= 0) {
            final List<SnapshotsInProgress.Entry> entries = new ArrayList<>(entryList.size() - 1);
            final SnapshotsInProgress.Entry removedEntry = entryList.get(indexOfEntry);
            for (int i = 0; i < indexOfEntry; i++) {
                final SnapshotsInProgress.Entry previousEntry = entryList.get(i);
                if (removedEntry.isClone()) {
                    if (previousEntry.isClone()) {
                        ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> updatedShardAssignments = null;
                        for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> finishedShardEntry : removedEntry.shardsByRepoShardId()
                            .entrySet()) {
                            final ShardSnapshotStatus shardState = finishedShardEntry.getValue();
                            if (shardState.state() == ShardState.SUCCESS) {
                                updatedShardAssignments = maybeAddUpdatedAssignment(
                                    updatedShardAssignments,
                                    shardState,
                                    finishedShardEntry.getKey(),
                                    previousEntry.shardsByRepoShardId()
                                );
                            }
                        }
                        addCloneEntry(entries, previousEntry, updatedShardAssignments);
                    } else {
                        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> updatedShardAssignments = null;
                        for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> finishedShardEntry : removedEntry.shardsByRepoShardId()
                            .entrySet()) {
                            final ShardSnapshotStatus shardState = finishedShardEntry.getValue();
                            final RepositoryShardId repositoryShardId = finishedShardEntry.getKey();
                            if (shardState.state() != ShardState.SUCCESS
                                || previousEntry.shardsByRepoShardId().containsKey(repositoryShardId) == false) {
                                continue;
                            }
                            updatedShardAssignments = maybeAddUpdatedAssignment(
                                updatedShardAssignments,
                                shardState,
                                previousEntry.shardId(repositoryShardId),
                                previousEntry.shards()
                            );

                        }
                        addSnapshotEntry(entries, previousEntry, updatedShardAssignments);
                    }
                } else {
                    if (previousEntry.isClone()) {
                        ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> updatedShardAssignments = null;
                        for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> finishedShardEntry : removedEntry.shardsByRepoShardId()
                            .entrySet()) {
                            final ShardSnapshotStatus shardState = finishedShardEntry.getValue();
                            final RepositoryShardId repositoryShardId = finishedShardEntry.getKey();
                            if (shardState.state() != ShardState.SUCCESS
                                || previousEntry.shardsByRepoShardId().containsKey(repositoryShardId) == false) {
                                continue;
                            }
                            updatedShardAssignments = maybeAddUpdatedAssignment(
                                updatedShardAssignments,
                                shardState,
                                repositoryShardId,
                                previousEntry.shardsByRepoShardId()
                            );
                        }
                        addCloneEntry(entries, previousEntry, updatedShardAssignments);
                    } else {
                        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> updatedShardAssignments = null;
                        for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> finishedShardEntry : removedEntry.shardsByRepoShardId()
                            .entrySet()) {
                            final ShardSnapshotStatus shardState = finishedShardEntry.getValue();
                            if (shardState.state() == ShardState.SUCCESS
                                && previousEntry.shardsByRepoShardId().containsKey(finishedShardEntry.getKey())) {
                                updatedShardAssignments = maybeAddUpdatedAssignment(
                                    updatedShardAssignments,
                                    shardState,
                                    previousEntry.shardId(finishedShardEntry.getKey()),
                                    previousEntry.shards()
                                );
                            }
                        }
                        addSnapshotEntry(entries, previousEntry, updatedShardAssignments);
                    }
                }
            }
            for (int i = indexOfEntry + 1; i < entryList.size(); i++) {
                entries.add(entryList.get(i));
            }
            result = ClusterState.builder(state)
                .putCustom(SnapshotsInProgress.TYPE, snapshots.withUpdatedEntriesForRepo(snapshot.getRepository(), entries))
                .build();
        }
        return readyDeletions(result).v1();
    }

    private static void addSnapshotEntry(
        List<SnapshotsInProgress.Entry> entries,
        SnapshotsInProgress.Entry entryToUpdate,
        @Nullable ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> updatedShardAssignments
    ) {
        if (updatedShardAssignments == null) {
            entries.add(entryToUpdate);
        } else {
            final ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> updatedStatus = ImmutableOpenMap.builder(entryToUpdate.shards());
            updatedStatus.putAllFromMap(updatedShardAssignments.build());
            entries.add(entryToUpdate.withShardStates(updatedStatus.build()));
        }
    }

    private static void addCloneEntry(
        List<SnapshotsInProgress.Entry> entries,
        SnapshotsInProgress.Entry entryToUpdate,
        @Nullable ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> updatedShardAssignments
    ) {
        if (updatedShardAssignments == null) {
            entries.add(entryToUpdate);
        } else {
            final ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> updatedStatus = ImmutableOpenMap.builder(
                entryToUpdate.shardsByRepoShardId()
            );
            updatedStatus.putAllFromMap(updatedShardAssignments.build());
            entries.add(entryToUpdate.withClones(updatedStatus.build()));
        }
    }

    @Nullable
    private static <T> ImmutableOpenMap.Builder<T, ShardSnapshotStatus> maybeAddUpdatedAssignment(
        @Nullable ImmutableOpenMap.Builder<T, ShardSnapshotStatus> updatedShardAssignments,
        ShardSnapshotStatus finishedShardState,
        T shardId,
        Map<T, ShardSnapshotStatus> statesToUpdate
    ) {
        final ShardGeneration newGeneration = finishedShardState.generation();
        final ShardSnapshotStatus stateToUpdate = statesToUpdate.get(shardId);
        if (stateToUpdate != null
            && stateToUpdate.state() == ShardState.SUCCESS
            && Objects.equals(newGeneration, stateToUpdate.generation()) == false) {
            if (updatedShardAssignments == null) {
                updatedShardAssignments = ImmutableOpenMap.builder();
            }
            updatedShardAssignments.put(shardId, stateToUpdate.withUpdatedGeneration(newGeneration));
        }
        return updatedShardAssignments;
    }

    /**
     * Removes record of running snapshot from cluster state and notifies the listener when this action is complete. This method is only
     * used when the snapshot fails for some reason. During normal operation the snapshot repository will remove the
     * {@link SnapshotsInProgress.Entry} from the cluster state once it's done finalizing the snapshot.
     *
     * @param snapshot       snapshot that failed
     * @param failure        exception that failed the snapshot
     * @param repositoryData repository data if the next finalization operation on the repository should be attempted or {@code null} if
     *                       no further actions should be executed
     */
    private void removeFailedSnapshotFromClusterState(Snapshot snapshot, Exception failure, @Nullable RepositoryData repositoryData) {
        assert failure != null : "Failure must be supplied";
        submitUnbatchedTask(REMOVE_SNAPSHOT_METADATA_TASK_SOURCE, new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                final ClusterState updatedState = stateWithoutSnapshot(currentState, snapshot);
                assert updatedState == currentState || endingSnapshots.contains(snapshot)
                    : "did not track [" + snapshot + "] in ending snapshots while removing it from the cluster state";
                // now check if there are any delete operations that refer to the just failed snapshot and remove the snapshot from them
                return updateWithSnapshots(
                    updatedState,
                    null,
                    deletionsWithoutSnapshots(
                        updatedState.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY),
                        Collections.singletonList(snapshot.getSnapshotId()),
                        snapshot.getRepository()
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof NotMasterException) {
                    failure.addSuppressed(new SnapshotException(snapshot, "no longer master"));
                }
                logger.log(
                    MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN,
                    () -> "[" + snapshot + "] failed to remove snapshot metadata",
                    e
                );
                failSnapshotCompletionListeners(
                    snapshot,
                    new SnapshotException(snapshot, "Failed to remove snapshot from cluster state", e)
                );
                failAllListenersOnMasterFailOver(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                failSnapshotCompletionListeners(snapshot, failure);
                if (repositoryData != null) {
                    runNextQueuedOperation(repositoryData, snapshot.getRepository(), true);
                }
            }
        });
    }

    private static final String REMOVE_SNAPSHOT_METADATA_TASK_SOURCE = "remove snapshot metadata";

    /**
     * Remove the given {@link SnapshotId}s for the given {@code repository} from an instance of {@link SnapshotDeletionsInProgress}.
     * If no deletion contained any of the snapshot ids to remove then return {@code null}.
     *
     * @param deletions   snapshot deletions to update
     * @param snapshotIds snapshot ids to remove
     * @param repository  repository that the snapshot ids belong to
     * @return            updated {@link SnapshotDeletionsInProgress} or {@code null} if unchanged
     */
    @Nullable
    private static SnapshotDeletionsInProgress deletionsWithoutSnapshots(
        SnapshotDeletionsInProgress deletions,
        Collection<SnapshotId> snapshotIds,
        String repository
    ) {
        boolean changed = false;
        List<SnapshotDeletionsInProgress.Entry> updatedEntries = new ArrayList<>(deletions.getEntries().size());
        for (SnapshotDeletionsInProgress.Entry entry : deletions.getEntries()) {
            if (entry.repository().equals(repository)) {
                final List<SnapshotId> updatedSnapshotIds = new ArrayList<>(entry.getSnapshots());
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

    private void failSnapshotCompletionListeners(Snapshot snapshot, Exception e) {
        failListenersIgnoringException(endAndGetListenersToResolve(snapshot), e);
        assert repositoryOperations.assertNotQueued(snapshot);
    }

    /**
     * Deletes snapshots from the repository. In-progress snapshots matched by the delete will be aborted before deleting them.
     *
     * @param request         delete snapshot request
     * @param listener        listener
     */
    public void deleteSnapshots(final DeleteSnapshotRequest request, final ActionListener<Void> listener) {
        final String repositoryName = request.repository();
        final String[] snapshotNames = request.snapshots();
        logger.info(
            () -> format("deleting snapshots [%s] from repository [%s]", arrayToCommaDelimitedString(snapshotNames), repositoryName)
        );

        final Repository repository = repositoriesService.repository(repositoryName);
        executeConsistentStateUpdate(repository, repositoryData -> new ClusterStateUpdateTask(request.masterNodeTimeout()) {

            private SnapshotDeletionsInProgress.Entry newDelete = null;

            private boolean reusedExistingDelete = false;

            // Snapshots that had all of their shard snapshots in queued state and thus were removed from the
            // cluster state right away
            private final Collection<Snapshot> completedNoCleanup = new ArrayList<>();

            // Snapshots that were aborted and that already wrote data to the repository and now have to be deleted
            // from the repository after the cluster state update
            private final Collection<SnapshotsInProgress.Entry> completedWithCleanup = new ArrayList<>();

            @Override
            public ClusterState execute(ClusterState currentState) {
                ensureRepositoryExists(repositoryName, currentState);
                final Set<SnapshotId> snapshotIds = new HashSet<>();

                // find in-progress snapshots to delete in cluster state
                final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(repositoryName)) {
                    final SnapshotId snapshotId = entry.snapshot().getSnapshotId();
                    if (Regex.simpleMatch(snapshotNames, snapshotId.getName())) {
                        snapshotIds.add(snapshotId);
                    }
                }

                // find snapshots to delete in repository data
                final Map<String, SnapshotId> snapshotsIdsInRepository = repositoryData.getSnapshotIds()
                    .stream()
                    .collect(Collectors.toMap(SnapshotId::getName, Function.identity()));
                for (String snapshotOrPattern : snapshotNames) {
                    if (Regex.isSimpleMatchPattern(snapshotOrPattern)) {
                        for (Map.Entry<String, SnapshotId> entry : snapshotsIdsInRepository.entrySet()) {
                            if (Regex.simpleMatch(snapshotOrPattern, entry.getKey())) {
                                snapshotIds.add(entry.getValue());
                            }
                        }
                    } else {
                        final SnapshotId foundId = snapshotsIdsInRepository.get(snapshotOrPattern);
                        if (foundId == null) {
                            if (snapshotIds.stream().noneMatch(snapshotId -> snapshotId.getName().equals(snapshotOrPattern))) {
                                throw new SnapshotMissingException(repositoryName, snapshotOrPattern);
                            }
                        } else {
                            snapshotIds.add(foundId);
                        }
                    }
                }

                if (snapshotIds.isEmpty()) {
                    return currentState;
                }

                final Set<SnapshotId> activeCloneSources = snapshotsInProgress.asStream()
                    .filter(SnapshotsInProgress.Entry::isClone)
                    .map(SnapshotsInProgress.Entry::source)
                    .collect(Collectors.toSet());
                for (SnapshotId snapshotId : snapshotIds) {
                    if (activeCloneSources.contains(snapshotId)) {
                        throw new ConcurrentSnapshotExecutionException(
                            new Snapshot(repositoryName, snapshotId),
                            "cannot delete snapshot while it is being cloned"
                        );
                    }
                }

                ensureNoCleanupInProgress(
                    currentState,
                    repositoryName,
                    snapshotIds.stream().findFirst().get().getName(),
                    "delete snapshot"
                );

                final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );

                final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
                // don't allow snapshot deletions while a restore is taking place,
                // otherwise we could end up deleting a snapshot that is being restored
                // and the files the restore depends on would all be gone

                for (RestoreInProgress.Entry entry : restoreInProgress) {
                    if (repositoryName.equals(entry.snapshot().getRepository()) && snapshotIds.contains(entry.snapshot().getSnapshotId())) {
                        throw new ConcurrentSnapshotExecutionException(
                            new Snapshot(repositoryName, snapshotIds.stream().findFirst().get()),
                            "cannot delete snapshot during a restore in progress in [" + restoreInProgress + "]"
                        );
                    }
                }
                // Snapshot ids that will have to be physically deleted from the repository
                final Set<SnapshotId> snapshotIdsRequiringCleanup = new HashSet<>(snapshotIds);
                final SnapshotsInProgress updatedSnapshots = snapshotsInProgress.withUpdatedEntriesForRepo(
                    repositoryName,
                    snapshotsInProgress.forRepo(repositoryName).stream().map(existing -> {
                        if (existing.state() == State.STARTED
                            && snapshotIdsRequiringCleanup.contains(existing.snapshot().getSnapshotId())) {
                            // snapshot is started - mark every non completed shard as aborted
                            final SnapshotsInProgress.Entry abortedEntry = existing.abort();
                            if (abortedEntry == null) {
                                // No work has been done for this snapshot yet so we remove it from the cluster state directly
                                final Snapshot existingNotYetStartedSnapshot = existing.snapshot();
                                // Adding the snapshot to #endingSnapshots since we still have to resolve its listeners to not trip
                                // any leaked listener assertions
                                if (endingSnapshots.add(existingNotYetStartedSnapshot)) {
                                    completedNoCleanup.add(existingNotYetStartedSnapshot);
                                }
                                snapshotIdsRequiringCleanup.remove(existingNotYetStartedSnapshot.getSnapshotId());
                            } else if (abortedEntry.state().completed()) {
                                completedWithCleanup.add(abortedEntry);
                            }
                            return abortedEntry;
                        }
                        return existing;
                    }).filter(Objects::nonNull).toList()
                );
                if (snapshotIdsRequiringCleanup.isEmpty()) {
                    // We only saw snapshots that could be removed from the cluster state right away, no need to update the deletions
                    return updateWithSnapshots(currentState, updatedSnapshots, null);
                }
                // add the snapshot deletion to the cluster state
                final SnapshotDeletionsInProgress.Entry replacedEntry = deletionsInProgress.getEntries()
                    .stream()
                    .filter(entry -> entry.repository().equals(repositoryName))
                    .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.WAITING)
                    .findFirst()
                    .orElse(null);
                if (replacedEntry == null) {
                    final Optional<SnapshotDeletionsInProgress.Entry> foundDuplicate = deletionsInProgress.getEntries()
                        .stream()
                        .filter(
                            entry -> entry.repository().equals(repositoryName)
                                && entry.state() == SnapshotDeletionsInProgress.State.STARTED
                                && entry.getSnapshots().containsAll(snapshotIds)
                        )
                        .findFirst();
                    if (foundDuplicate.isPresent()) {
                        newDelete = foundDuplicate.get();
                        reusedExistingDelete = true;
                        return currentState;
                    }
                    newDelete = new SnapshotDeletionsInProgress.Entry(
                        List.copyOf(snapshotIdsRequiringCleanup),
                        repositoryName,
                        threadPool.absoluteTimeInMillis(),
                        repositoryData.getGenId(),
                        updatedSnapshots.forRepo(repositoryName).stream().noneMatch(SnapshotsService::isWritingToRepository)
                            && deletionsInProgress.hasExecutingDeletion(repositoryName) == false
                                ? SnapshotDeletionsInProgress.State.STARTED
                                : SnapshotDeletionsInProgress.State.WAITING
                    );
                } else {
                    newDelete = replacedEntry.withAddedSnapshots(snapshotIdsRequiringCleanup);
                }
                return updateWithSnapshots(
                    currentState,
                    updatedSnapshots,
                    (replacedEntry == null ? deletionsInProgress : deletionsInProgress.withRemovedEntry(replacedEntry.uuid()))
                        .withAddedEntry(newDelete)
                );
            }

            @Override
            public void onFailure(Exception e) {
                endingSnapshots.removeAll(completedNoCleanup);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                if (completedNoCleanup.isEmpty() == false) {
                    logger.info("snapshots {} aborted", completedNoCleanup);
                }
                for (Snapshot snapshot : completedNoCleanup) {
                    failSnapshotCompletionListeners(snapshot, new SnapshotException(snapshot, SnapshotsInProgress.ABORTED_FAILURE_TEXT));
                }
                if (newDelete == null) {
                    listener.onResponse(null);
                } else {
                    addDeleteListener(newDelete.uuid(), listener);
                    if (reusedExistingDelete) {
                        return;
                    }
                    if (newDelete.state() == SnapshotDeletionsInProgress.State.STARTED) {
                        if (tryEnterRepoLoop(repositoryName)) {
                            deleteSnapshotsFromRepository(newDelete, repositoryData, newState.nodes().getMinNodeVersion());
                        } else {
                            logger.trace("Delete [{}] could not execute directly and was queued", newDelete);
                        }
                    } else {
                        for (SnapshotsInProgress.Entry completedSnapshot : completedWithCleanup) {
                            endSnapshot(completedSnapshot, newState.metadata(), repositoryData);
                        }
                    }
                }
            }
        }, "delete snapshot [" + repository + "]" + Arrays.toString(snapshotNames), listener::onFailure);
    }

    /**
     * Checks if the given {@link SnapshotsInProgress.Entry} is currently writing to the repository.
     *
     * @param entry snapshot entry
     * @return true if entry is currently writing to the repository
     */
    private static boolean isWritingToRepository(SnapshotsInProgress.Entry entry) {
        if (entry.state().completed()) {
            // Entry is writing to the repo because it's finalizing on master
            return true;
        }
        for (ShardSnapshotStatus value : entry.shardsByRepoShardId().values()) {
            if (value.isActive()) {
                // Entry is writing to the repo because it's writing to a shard on a data node or waiting to do so for a concrete shard
                return true;
            }
        }
        return false;
    }

    private void addDeleteListener(String deleteUUID, ActionListener<Void> listener) {
        snapshotDeletionListeners.computeIfAbsent(deleteUUID, k -> new CopyOnWriteArrayList<>())
            .add(ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext()));
    }

    /**
     * Determines the minimum {@link Version} that the snapshot repository must be compatible with from the current nodes in the cluster
     * and the contents of the repository. The minimum version is determined as the lowest version found across all snapshots in the
     * repository and all nodes in the cluster.
     *
     * @param minNodeVersion minimum node version in the cluster
     * @param repositoryData current {@link RepositoryData} of that repository
     * @param excluded       snapshot id to ignore when computing the minimum version
     *                       (used to use newer metadata version after a snapshot delete)
     * @return minimum node version that must still be able to read the repository metadata
     */
    public static Version minCompatibleVersion(
        Version minNodeVersion,
        RepositoryData repositoryData,
        @Nullable Collection<SnapshotId> excluded
    ) {
        Version minCompatVersion = minNodeVersion;
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        for (SnapshotId snapshotId : snapshotIds.stream()
            .filter(excluded == null ? sn -> true : Predicate.not(excluded::contains))
            .toList()) {
            final Version known = repositoryData.getVersion(snapshotId);
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
                minCompatVersion = minCompatVersion.before(known) ? minCompatVersion : known;
            }
        }
        return minCompatVersion;
    }

    /**
     * Checks whether the metadata version supports writing {@link ShardGenerations} to the repository.
     *
     * @param repositoryMetaVersion version to check
     * @return true if version supports {@link ShardGenerations}
     */
    public static boolean useShardGenerations(Version repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(SHARD_GEN_IN_REPO_DATA_VERSION);
    }

    /**
     * Checks whether the metadata version supports writing {@link ShardGenerations} to the repository.
     *
     * @param repositoryMetaVersion version to check
     * @return true if version supports {@link ShardGenerations}
     */
    public static boolean useIndexGenerations(Version repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(INDEX_GEN_IN_REPO_DATA_VERSION);
    }

    /**
     * Checks whether the metadata version supports writing the cluster- and repository-uuid to the repository.
     *
     * @param repositoryMetaVersion version to check
     * @return true if version supports writing cluster- and repository-uuid to the repository
     */
    public static boolean includesUUIDs(Version repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(UUIDS_IN_REPO_DATA_VERSION);
    }

    public static boolean includeFileInfoWriterUUID(Version repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(FILE_INFO_WRITER_UUIDS_IN_SHARD_DATA_VERSION);
    }

    /** Deletes snapshot from repository
     *
     * @param deleteEntry       delete entry in cluster state
     * @param minNodeVersion    minimum node version in the cluster
     */
    private void deleteSnapshotsFromRepository(SnapshotDeletionsInProgress.Entry deleteEntry, Version minNodeVersion) {
        final long expectedRepoGen = deleteEntry.repositoryStateId();
        repositoriesService.getRepositoryData(deleteEntry.repository(), new ActionListener<>() {
            @Override
            public void onResponse(RepositoryData repositoryData) {
                assert repositoryData.getGenId() == expectedRepoGen
                    : "Repository generation should not change as long as a ready delete is found in the cluster state but found ["
                        + expectedRepoGen
                        + "] in cluster state and ["
                        + repositoryData.getGenId()
                        + "] in the repository";
                deleteSnapshotsFromRepository(deleteEntry, repositoryData, minNodeVersion);
            }

            @Override
            public void onFailure(Exception e) {
                submitUnbatchedTask(
                    "fail repo tasks for [" + deleteEntry.repository() + "]",
                    new FailPendingRepoTasksTask(deleteEntry.repository(), e)
                );
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    /**
     * Execute a cluster state update with a consistent view of the current {@link RepositoryData}. The {@link ClusterState} passed to the
     * task generated through {@code createUpdateTask} is guaranteed to point at the same state for this repository as the did the state
     * at the time the {@code RepositoryData} was loaded.
     * This allows for operations on the repository that need a consistent view of both the cluster state and the repository contents at
     * one point in time like for example, checking if a snapshot is in the repository before adding the delete operation for it to the
     * cluster state.
     *
     * @param repository       repository to execute update for
     * @param createUpdateTask function to supply cluster state update task
     * @param source           the source of the cluster state update task
     * @param onFailure        error handler invoked on failure to get a consistent view of the current {@link RepositoryData}
     */
    private void executeConsistentStateUpdate(
        Repository repository,
        Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
        String source,
        Consumer<Exception> onFailure
    ) {
        final RepositoryMetadata repositoryMetadataStart = repository.getMetadata();
        repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
            final ClusterStateUpdateTask updateTask = createUpdateTask.apply(repositoryData);
            submitUnbatchedTask(source, new ClusterStateUpdateTask(updateTask.priority(), updateTask.timeout()) {

                private boolean executedTask = false;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    // Comparing the full metadata here on purpose instead of simply comparing the safe generation.
                    // If the safe generation has changed, then we have to reload repository data and start over.
                    // If the pending generation has changed we are in the midst of a write operation and might pick up the
                    // updated repository data and state on the retry. We don't want to wait for the write to finish though
                    // because it could fail for any number of reasons so we just retry instead of waiting on the cluster state
                    // to change in any form.
                    if (repositoryMetadataStart.equals(
                        currentState.getMetadata()
                            .<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE)
                            .repository(repository.getMetadata().name())
                    )) {
                        executedTask = true;
                        return updateTask.execute(currentState);
                    }
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    if (executedTask) {
                        updateTask.onFailure(e);
                    } else {
                        onFailure.accept(e);
                    }
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    if (executedTask) {
                        updateTask.clusterStateProcessed(oldState, newState);
                    } else {
                        executeConsistentStateUpdate(repository, createUpdateTask, source, onFailure);
                    }
                }
            });
        }, onFailure));
    }

    /** Deletes snapshot from repository
     *
     * @param deleteEntry       delete entry in cluster state
     * @param repositoryData    the {@link RepositoryData} of the repository to delete from
     * @param minNodeVersion    minimum node version in the cluster
     */
    private void deleteSnapshotsFromRepository(
        SnapshotDeletionsInProgress.Entry deleteEntry,
        RepositoryData repositoryData,
        Version minNodeVersion
    ) {
        if (repositoryOperations.startDeletion(deleteEntry.uuid())) {
            assert currentlyFinalizing.contains(deleteEntry.repository());
            final List<SnapshotId> snapshotIds = deleteEntry.getSnapshots();
            assert deleteEntry.state() == SnapshotDeletionsInProgress.State.STARTED : "incorrect state for entry [" + deleteEntry + "]";
            if (snapshotIds.isEmpty()) {
                // this deletion overlapped one or more deletions that were successfully processed and there is no remaining snapshot to
                // delete now, we can avoid reaching to the repository and can complete the deletion.
                // TODO we should complete the deletion and resolve the listeners of SnapshotDeletionsInProgress with no snapshot sooner,
                // that would save some cluster state updates.
                removeSnapshotDeletionFromClusterState(
                    deleteEntry,
                    repositoryData,
                    listeners -> completeListenersIgnoringException(listeners, null)
                );
                return;
            }
            repositoriesService.repository(deleteEntry.repository())
                .deleteSnapshots(
                    snapshotIds,
                    repositoryData.getGenId(),
                    minCompatibleVersion(minNodeVersion, repositoryData, snapshotIds),
                    new SnapshotDeleteListener() {

                        private final ListenableFuture<Void> doneFuture = new ListenableFuture<>();

                        @Override
                        public void onDone() {
                            logger.info("snapshots {} deleted", snapshotIds);
                            doneFuture.onResponse(null);
                        }

                        @Override
                        public void onRepositoryDataWritten(RepositoryData updatedRepoData) {
                            removeSnapshotDeletionFromClusterState(
                                deleteEntry,
                                updatedRepoData,
                                listeners -> doneFuture.addListener(new ActionListener<>() {
                                    @Override
                                    public void onResponse(Void unused) {
                                        completeListenersIgnoringException(listeners, null);
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        // this should never be called, once updated repository metadata has been written to the
                                        // repository and the delete been removed from the cluster state, we ignore any further failures
                                        // and always complete the delete successfully
                                        assert false : e;
                                    }
                                })
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            submitUnbatchedTask(
                                "remove snapshot deletion metadata after failed delete",
                                new RemoveSnapshotDeletionAndContinueTask(deleteEntry, repositoryData) {
                                    @Override
                                    protected void handleListeners(List<ActionListener<Void>> deleteListeners) {
                                        failListenersIgnoringException(deleteListeners, e);
                                    }
                                }
                            );
                        }
                    }
                );
        }
    }

    /**
     * Removes a {@link SnapshotDeletionsInProgress.Entry} from {@link SnapshotDeletionsInProgress} in the cluster state after it executed
     * on the repository.
     *
     * @param deleteEntry    delete entry to remove from the cluster state
     * @param repositoryData current {@link RepositoryData} for the repository we just ran the delete on.
     * @param listenersHandler consumer that gets passed a list of all listeners that had their delete entry successfully removed from the
     *                         cluster state
     */
    private void removeSnapshotDeletionFromClusterState(
        final SnapshotDeletionsInProgress.Entry deleteEntry,
        final RepositoryData repositoryData,
        final Consumer<List<ActionListener<Void>>> listenersHandler
    ) {
        // We remove all snapshot ids that the delete successfully removed from the repository from enqueued snapshot delete entries during
        // the cluster state update. After the cluster state update we pass the list of listeners that had their entry removed from the
        // cluster state to the given handler
        submitUnbatchedTask("remove snapshot deletion metadata", new RemoveSnapshotDeletionAndContinueTask(deleteEntry, repositoryData) {
            @Override
            protected SnapshotDeletionsInProgress filterDeletions(SnapshotDeletionsInProgress deletions) {
                final SnapshotDeletionsInProgress updatedDeletions = deletionsWithoutSnapshots(
                    deletions,
                    deleteEntry.getSnapshots(),
                    deleteEntry.repository()
                );
                return updatedDeletions == null ? deletions : updatedDeletions;
            }

            @Override
            protected void handleListeners(List<ActionListener<Void>> deleteListeners) {
                assert repositoryData.getSnapshotIds().stream().noneMatch(deleteEntry.getSnapshots()::contains)
                    : "Repository data contained snapshot ids "
                        + repositoryData.getSnapshotIds()
                        + " that should should been deleted by ["
                        + deleteEntry
                        + "]";
                listenersHandler.accept(deleteListeners);
            }
        });
    }

    /**
     * Handle snapshot or delete failure due to not being master any more so we don't try to do run additional cluster state updates.
     * The next master will try handling the missing operations. All we can do is fail all the listeners on this master node so that
     * transport requests return and we don't leak listeners.
     *
     * @param e exception that caused us to realize we are not master any longer
     */
    private void failAllListenersOnMasterFailOver(Exception e) {
        logger.debug("Failing all snapshot operation listeners because this node is not master any longer", e);
        synchronized (currentlyFinalizing) {
            if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class) != null) {
                repositoryOperations.clear();
                for (Snapshot snapshot : Set.copyOf(snapshotCompletionListeners.keySet())) {
                    failSnapshotCompletionListeners(snapshot, new SnapshotException(snapshot, "no longer master"));
                }
                final Exception wrapped = new RepositoryException("_all", "Failed to update cluster state during repository operation", e);
                for (Iterator<List<ActionListener<Void>>> iterator = snapshotDeletionListeners.values().iterator(); iterator.hasNext();) {
                    final List<ActionListener<Void>> listeners = iterator.next();
                    iterator.remove();
                    failListenersIgnoringException(listeners, wrapped);
                }
                assert snapshotDeletionListeners.isEmpty() : "No new listeners should have been added but saw " + snapshotDeletionListeners;
            } else {
                assert false
                    : new AssertionError("Modifying snapshot state should only ever fail because we failed to publish new state", e);
                logger.error("Unexpected failure during cluster state update", e);
            }
            currentlyFinalizing.clear();
        }
    }

    /**
     * A cluster state update that will remove a given {@link SnapshotDeletionsInProgress.Entry} from the cluster state
     * and trigger running the next snapshot-delete or -finalization operation available to execute if there is one
     * ready in the cluster state as a result of this state update.
     */
    private abstract class RemoveSnapshotDeletionAndContinueTask extends ClusterStateUpdateTask {

        // Snapshots that can be finalized after the delete operation has been removed from the cluster state
        protected final List<SnapshotsInProgress.Entry> newFinalizations = new ArrayList<>();

        private List<SnapshotDeletionsInProgress.Entry> readyDeletions = Collections.emptyList();

        protected final SnapshotDeletionsInProgress.Entry deleteEntry;

        private final RepositoryData repositoryData;

        RemoveSnapshotDeletionAndContinueTask(SnapshotDeletionsInProgress.Entry deleteEntry, RepositoryData repositoryData) {
            this.deleteEntry = deleteEntry;
            this.repositoryData = repositoryData;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            final SnapshotDeletionsInProgress deletions = currentState.custom(SnapshotDeletionsInProgress.TYPE);
            assert deletions != null : "We only run this if there were deletions in the cluster state before";
            final SnapshotDeletionsInProgress updatedDeletions = deletions.withRemovedEntry(deleteEntry.uuid());
            if (updatedDeletions == deletions) {
                return currentState;
            }
            final SnapshotDeletionsInProgress newDeletions = filterDeletions(updatedDeletions);
            final Tuple<ClusterState, List<SnapshotDeletionsInProgress.Entry>> res = readyDeletions(
                updateWithSnapshots(currentState, updatedSnapshotsInProgress(currentState, newDeletions), newDeletions)
            );
            readyDeletions = res.v2();
            return res.v1();
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(() -> format("%s failed to remove snapshot deletion metadata", deleteEntry), e);
            repositoryOperations.finishDeletion(deleteEntry.uuid());
            failAllListenersOnMasterFailOver(e);
        }

        protected SnapshotDeletionsInProgress filterDeletions(SnapshotDeletionsInProgress deletions) {
            return deletions;
        }

        @Override
        public final void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            repositoryOperations.finishDeletion(deleteEntry.uuid());
            final List<ActionListener<Void>> deleteListeners = snapshotDeletionListeners.remove(deleteEntry.uuid());
            handleListeners(deleteListeners);
            if (newFinalizations.isEmpty()) {
                if (readyDeletions.isEmpty()) {
                    leaveRepoLoop(deleteEntry.repository());
                } else {
                    for (SnapshotDeletionsInProgress.Entry readyDeletion : readyDeletions) {
                        deleteSnapshotsFromRepository(readyDeletion, repositoryData, newState.nodes().getMinNodeVersion());
                    }
                }
            } else {
                leaveRepoLoop(deleteEntry.repository());
                assert readyDeletions.stream().noneMatch(entry -> entry.repository().equals(deleteEntry.repository()))
                    : "New finalizations " + newFinalizations + " added even though deletes " + readyDeletions + " are ready";
                for (SnapshotsInProgress.Entry entry : newFinalizations) {
                    endSnapshot(entry, newState.metadata(), repositoryData);
                }
            }
            // TODO: be more efficient here, we could collect newly ready shard clones as we compute them and then directly start them
            // instead of looping over all possible clones to execute
            startExecutableClones(newState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY), null);
        }

        /**
         * Invoke snapshot delete listeners for {@link #deleteEntry}.
         *
         * @param deleteListeners delete snapshot listeners or {@code null} if there weren't any for {@link #deleteEntry}.
         */
        protected abstract void handleListeners(@Nullable List<ActionListener<Void>> deleteListeners);

        /**
         * Computes an updated {@link SnapshotsInProgress} that takes into account an updated version of
         * {@link SnapshotDeletionsInProgress} that has a {@link SnapshotDeletionsInProgress.Entry} removed from it
         * relative to the {@link SnapshotDeletionsInProgress} found in {@code initialState}.
         * The removal of a delete from the cluster state can trigger two possible actions on in-progress snapshots:
         * <ul>
         *     <li>Snapshots that had unfinished shard snapshots in state {@link ShardSnapshotStatus#UNASSIGNED_QUEUED} that
         *     could not be started because the delete was running can have those started.</li>
         *     <li>Snapshots that had all their shards reach a completed state while a delete was running (e.g. as a result of
         *     nodes dropping out of the cluster or another incoming delete aborting them) need not be updated in the cluster
         *     state but need to have their finalization triggered now that it's possible with the removal of the delete
         *     from the state.</li>
         * </ul>
         *
         * @param currentState     current cluster state
         * @param updatedDeletions deletions with removed entry
         * @return updated snapshot in progress instance or {@code null} if there are no changes to it
         */
        @Nullable
        private SnapshotsInProgress updatedSnapshotsInProgress(ClusterState currentState, SnapshotDeletionsInProgress updatedDeletions) {
            final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            final List<SnapshotsInProgress.Entry> snapshotEntries = new ArrayList<>();

            // Keep track of shardIds that we started snapshots for as a result of removing this delete so we don't assign
            // them to multiple snapshots by accident
            final Set<RepositoryShardId> reassignedShardIds = new HashSet<>();

            boolean changed = false;

            final String localNodeId = currentState.nodes().getLocalNodeId();
            final String repoName = deleteEntry.repository();
            InFlightShardSnapshotStates inFlightShardStates = null;
            // Keep track of IndexId values that may have gone unreferenced due to the delete entry just executed.
            // See org.elasticsearch.cluster.SnapshotsInProgress.Entry#withUpdatedIndexIds for details.
            final Set<IndexId> newIndexIdsToRefresh = new HashSet<>();
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(repoName)) {
                if (entry.state().completed() == false) {
                    // TODO: dry up redundant computation and code between clone and non-clone case, in particular reuse
                    // `inFlightShardStates` across both clone and standard snapshot code
                    if (entry.isClone()) {
                        // Collect waiting shards from that entry that we can assign now that we are done with the deletion
                        final List<RepositoryShardId> canBeUpdated = new ArrayList<>();
                        for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> value : entry.shardsByRepoShardId().entrySet()) {
                            if (value.getValue().equals(ShardSnapshotStatus.UNASSIGNED_QUEUED)
                                && reassignedShardIds.contains(value.getKey()) == false) {
                                canBeUpdated.add(value.getKey());
                            }
                        }
                        // TODO: the below logic is very similar to that in #startCloning and both could be dried up against each other
                        // also the code for standard snapshots could make use of this breakout as well
                        if (canBeUpdated.isEmpty() || updatedDeletions.hasExecutingDeletion(repoName)) {
                            // No shards can be updated in this snapshot so we just add it as is again
                            snapshotEntries.add(entry);
                        } else {
                            if (inFlightShardStates == null) {
                                inFlightShardStates = InFlightShardSnapshotStates.forEntries(snapshotsInProgress.forRepo(repoName));
                            }
                            final ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> updatedAssignmentsBuilder =
                                ImmutableOpenMap.builder(entry.shardsByRepoShardId());
                            for (RepositoryShardId shardId : canBeUpdated) {
                                if (inFlightShardStates.isActive(shardId.indexName(), shardId.shardId()) == false) {
                                    markShardReassigned(shardId, reassignedShardIds);
                                    updatedAssignmentsBuilder.put(
                                        shardId,
                                        new ShardSnapshotStatus(
                                            localNodeId,
                                            inFlightShardStates.generationForShard(
                                                shardId.index(),
                                                shardId.shardId(),
                                                repositoryData.shardGenerations()
                                            )
                                        )
                                    );
                                }
                            }
                            snapshotEntries.add(entry.withClones(updatedAssignmentsBuilder.build()));
                            changed = true;
                        }
                    } else {
                        // Collect waiting shards that in entry that we can assign now that we are done with the deletion
                        final List<RepositoryShardId> canBeUpdated = new ArrayList<>();
                        for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> value : entry.shardsByRepoShardId().entrySet()) {
                            final RepositoryShardId repositoryShardId = value.getKey();
                            if (value.getValue().equals(ShardSnapshotStatus.UNASSIGNED_QUEUED)
                                && reassignedShardIds.contains(repositoryShardId) == false) {
                                canBeUpdated.add(repositoryShardId);
                                if (repositoryData.hasIndex(repositoryShardId.indexName()) == false) {
                                    newIndexIdsToRefresh.add(repositoryShardId.index());
                                }
                            }
                        }
                        if (canBeUpdated.isEmpty()) {
                            // No shards can be updated in this snapshot so we just add it as is again
                            snapshotEntries.add(entry);
                        } else {
                            final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shardAssignments = shards(
                                snapshotsInProgress,
                                updatedDeletions,
                                currentState,
                                entry.indices().values(),
                                entry.version().onOrAfter(SHARD_GEN_IN_REPO_DATA_VERSION),
                                repositoryData,
                                repoName
                            );
                            final ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> updatedAssignmentsBuilder = ImmutableOpenMap
                                .builder(entry.shards());
                            for (RepositoryShardId shardId : canBeUpdated) {
                                final ShardId sid = entry.shardId(shardId);
                                final ShardSnapshotStatus updated = shardAssignments.get(sid);
                                if (updated == null) {
                                    // We don't have a new assignment for this shard because its index was concurrently deleted
                                    assert currentState.routingTable().hasIndex(sid.getIndex()) == false
                                        : "Missing assignment for [" + sid + "]";
                                    updatedAssignmentsBuilder.put(sid, ShardSnapshotStatus.MISSING);
                                } else {
                                    if (updated.isActive()) {
                                        markShardReassigned(shardId, reassignedShardIds);
                                    }
                                    updatedAssignmentsBuilder.put(sid, updated);
                                }
                            }
                            final SnapshotsInProgress.Entry updatedEntry = entry.withShardStates(updatedAssignmentsBuilder.build());
                            snapshotEntries.add(updatedEntry);
                            changed = true;
                            if (updatedEntry.state().completed()) {
                                newFinalizations.add(entry);
                            }
                        }
                    }
                } else {
                    // Entry is already completed so we will finalize it now that the delete doesn't block us after
                    // this CS update finishes
                    newFinalizations.add(entry);
                    snapshotEntries.add(entry);
                }
            }
            if (changed && newIndexIdsToRefresh.isEmpty() == false) {
                final Map<IndexId, IndexId> updatedIndexIds = Maps.newMapWithExpectedSize(newIndexIdsToRefresh.size());
                for (IndexId indexIdToRefresh : newIndexIdsToRefresh) {
                    updatedIndexIds.put(indexIdToRefresh, new IndexId(indexIdToRefresh.getName(), UUIDs.randomBase64UUID()));
                }
                for (int i = 0; i < snapshotEntries.size(); i++) {
                    snapshotEntries.set(i, snapshotEntries.get(i).withUpdatedIndexIds(updatedIndexIds));
                }
            }
            return changed ? snapshotsInProgress.withUpdatedEntriesForRepo(repoName, snapshotEntries) : null;
        }

        private static void markShardReassigned(RepositoryShardId shardId, Set<RepositoryShardId> reassignments) {
            final boolean added = reassignments.add(shardId);
            assert added : "should only ever reassign each shard once but assigned [" + shardId + "] multiple times";
        }
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

    private static <T> void failListenersIgnoringException(@Nullable List<ActionListener<T>> listeners, Exception failure) {
        if (listeners != null) {
            try {
                ActionListener.onFailure(listeners, failure);
            } catch (Exception ex) {
                assert false : new AssertionError(ex);
                logger.warn("Failed to notify listeners", ex);
            }
        }
    }

    private static <T> void completeListenersIgnoringException(@Nullable List<ActionListener<T>> listeners, T result) {
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
     * @return list of shard to be included into current snapshot
     */
    private static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(
        SnapshotsInProgress snapshotsInProgress,
        SnapshotDeletionsInProgress deletionsInProgress,
        ClusterState currentState,
        Collection<IndexId> indices,
        boolean useShardGenerations,
        RepositoryData repositoryData,
        String repoName
    ) {
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();
        final InFlightShardSnapshotStates inFlightShardStates = InFlightShardSnapshotStates.forEntries(
            snapshotsInProgress.forRepo(repoName)
        );
        final boolean readyToExecute = deletionsInProgress.hasExecutingDeletion(repoName) == false;
        for (IndexId index : indices) {
            final String indexName = index.getName();
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetadata indexMetadata = currentState.metadata().index(indexName);
            if (indexMetadata == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                builder.put(new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 0), ShardSnapshotStatus.MISSING);
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
                    final ShardSnapshotStatus shardSnapshotStatus;
                    if (readyToExecute == false || inFlightShardStates.isActive(shardId.getIndexName(), shardId.id())) {
                        shardSnapshotStatus = ShardSnapshotStatus.UNASSIGNED_QUEUED;
                    } else {
                        shardSnapshotStatus = initShardSnapshotStatus(shardRepoGeneration, indexRoutingTable.shard(i).primaryShard());
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
     * @param shardRepoGeneration repository generation of the shard in the repository
     * @param primary             primary routing entry for the shard
     * @return                    shard snapshot status
     */
    private static ShardSnapshotStatus initShardSnapshotStatus(ShardGeneration shardRepoGeneration, ShardRouting primary) {
        ShardSnapshotStatus shardSnapshotStatus;
        if (primary == null || primary.assignedToNode() == false) {
            shardSnapshotStatus = new ShardSnapshotStatus(null, ShardState.MISSING, "primary shard is not allocated", shardRepoGeneration);
        } else if (primary.relocating() || primary.initializing()) {
            shardSnapshotStatus = new ShardSnapshotStatus(primary.currentNodeId(), ShardState.WAITING, shardRepoGeneration);
        } else if (primary.started() == false) {
            shardSnapshotStatus = new ShardSnapshotStatus(
                primary.currentNodeId(),
                ShardState.MISSING,
                "primary shard hasn't been started yet",
                shardRepoGeneration
            );
        } else {
            shardSnapshotStatus = new ShardSnapshotStatus(primary.currentNodeId(), shardRepoGeneration);
        }
        return shardSnapshotStatus;
    }

    /**
     * Returns the data streams that are currently being snapshotted (with partial == false) and that are contained in the
     * indices-to-check set.
     */
    public static Set<String> snapshottingDataStreams(final ClusterState currentState, final Set<String> dataStreamsToCheck) {
        Map<String, DataStream> dataStreams = currentState.metadata().dataStreams();
        return currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
            .asStream()
            .filter(e -> e.partial() == false)
            .flatMap(e -> e.dataStreams().stream())
            .filter(ds -> dataStreams.containsKey(ds) && dataStreamsToCheck.contains(ds))
            .collect(Collectors.toSet());
    }

    /**
     * Returns the indices that are currently being snapshotted (with partial == false) and that are contained in the indices-to-check set.
     */
    public static Set<Index> snapshottingIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final Set<Index> indices = new HashSet<>();
        for (List<SnapshotsInProgress.Entry> snapshotsInRepo : currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
            .entriesByRepo()) {
            for (final SnapshotsInProgress.Entry entry : snapshotsInRepo) {
                if (entry.partial() == false && entry.isClone() == false) {
                    for (String indexName : entry.indices().keySet()) {
                        IndexMetadata indexMetadata = currentState.metadata().index(indexName);
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
     *
     * The purpose of this method is to capture the relevant data stream aliases based on the data streams
     * that will be included in a snapshot.
     *
     * @param dataStreams       The provided data streams, which will be included in a snapshot.
     * @param dataStreamAliases The data streams aliases that may contain aliases that refer to data streams
     *                          that don't exist in the provided data streams.
     * @return                  The filtered data streams aliases only referring to data streams in the provided data streams.
     */
    static Map<String, DataStreamAlias> filterDataStreamAliases(
        Map<String, DataStream> dataStreams,
        Map<String, DataStreamAlias> dataStreamAliases
    ) {

        return dataStreamAliases.values()
            .stream()
            .filter(alias -> alias.getDataStreams().stream().anyMatch(dataStreams::containsKey))
            .map(alias -> alias.intersect(dataStreams::containsKey))
            .collect(Collectors.toMap(DataStreamAlias::getName, Function.identity()));
    }

    /**
     * Adds snapshot completion listener
     *
     * @param snapshot Snapshot to listen for
     * @param listener listener
     */
    private void addListener(Snapshot snapshot, ActionListener<SnapshotInfo> listener) {
        snapshotCompletionListeners.computeIfAbsent(snapshot, k -> new CopyOnWriteArrayList<>())
            .add(ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext()));
    }

    @Override
    protected void doStart() {
        assert this.updateSnapshotStatusHandler != null;
        assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME) != null;
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.removeApplier(this);
    }

    /**
     * Assert that no in-memory state for any running snapshot-create or -delete operation exists in this instance.
     */
    public boolean assertAllListenersResolved() {
        final DiscoveryNode localNode = clusterService.localNode();
        assert endingSnapshots.isEmpty() : "Found leaked ending snapshots " + endingSnapshots + " on [" + localNode + "]";
        assert snapshotCompletionListeners.isEmpty()
            : "Found leaked snapshot completion listeners " + snapshotCompletionListeners + " on [" + localNode + "]";
        assert currentlyFinalizing.isEmpty() : "Found leaked finalizations " + currentlyFinalizing + " on [" + localNode + "]";
        assert snapshotDeletionListeners.isEmpty()
            : "Found leaked snapshot delete listeners " + snapshotDeletionListeners + " on [" + localNode + "]";
        assert repositoryOperations.isEmpty() : "Found leaked snapshots to finalize " + repositoryOperations + " on [" + localNode + "]";
        return true;
    }

    private static boolean isQueued(@Nullable ShardSnapshotStatus status) {
        return status != null && status.state() == ShardState.QUEUED;
    }

    /**
     * State machine for updating existing {@link SnapshotsInProgress.Entry} by applying a given list of {@link ShardSnapshotUpdate} to
     * them. The algorithm implemented below works as described
     * below:
     * Every shard snapshot or clone state update can result in multiple snapshots being updated. In order to determine whether or not a
     * shard update has an effect we use an outer loop over all current executing snapshot operations that iterates over them in the order
     * they were started in and an inner loop over the list of shard update tasks.
     *
     * If the inner loop finds that a shard update task applies to a given snapshot and either a shard-snapshot or shard-clone operation in
     * it then it will update the state of the snapshot entry accordingly. If that update was a noop, then the task is removed from the
     * iteration as it was already applied before and likely just arrived on the master node again due to retries upstream.
     * If the update was not a noop, then it means that the shard it applied to is now available for another snapshot or clone operation
     * to be re-assigned if there is another snapshot operation that is waiting for the shard to become available. We therefore record the
     * fact that a task was executed by adding it to a collection of executed tasks. If a subsequent execution of the outer loop finds that
     * a task in the executed tasks collection applied to a shard it was waiting for to become available, then the shard snapshot operation
     * will be started for that snapshot entry and the task removed from the collection of tasks that need to be applied to snapshot
     * entries since it can not have any further effects.
     *
     * Package private for testing.
     */
    static final class SnapshotShardsUpdateContext {

        // number of updated shard snapshot states as a result of applying updates to the snapshot entries seen so far
        private int changedCount = 0;

        // number of started tasks as a result of applying updates to the snapshot entries seen so far
        private int startedCount = 0;

        // batch execution context
        private final ClusterStateTaskExecutor.BatchExecutionContext<SnapshotTask> batchExecutionContext;

        // initial cluster state for update computation
        private final ClusterState initialState;

        // updates outstanding to be applied to existing snapshot entries
        private final Map<String, List<ShardSnapshotUpdate>> updatesByRepo;

        // updates that were used to update an existing in-progress shard snapshot
        private final Set<ShardSnapshotUpdate> executedUpdates = new HashSet<>();

        SnapshotShardsUpdateContext(ClusterStateTaskExecutor.BatchExecutionContext<SnapshotTask> batchExecutionContext) {
            this.batchExecutionContext = batchExecutionContext;
            this.initialState = batchExecutionContext.initialState();
            this.updatesByRepo = new HashMap<>();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                if (taskContext.getTask() instanceof ShardSnapshotUpdate task) {
                    updatesByRepo.computeIfAbsent(task.snapshot.getRepository(), r -> new ArrayList<>()).add(task);
                }
            }
        }

        SnapshotsInProgress computeUpdatedState() {
            final SnapshotsInProgress existing = initialState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            SnapshotsInProgress updated = existing;
            for (Map.Entry<String, List<ShardSnapshotUpdate>> updates : updatesByRepo.entrySet()) {
                final String repoName = updates.getKey();
                final List<SnapshotsInProgress.Entry> oldEntries = existing.forRepo(repoName);
                if (oldEntries.isEmpty()) {
                    continue;
                }
                final List<SnapshotsInProgress.Entry> newEntries = new ArrayList<>(oldEntries.size());
                for (SnapshotsInProgress.Entry entry : oldEntries) {
                    newEntries.add(applyToEntry(entry, updates.getValue()));
                }
                updated = updated.withUpdatedEntriesForRepo(repoName, newEntries);
            }

            if (changedCount > 0) {
                logger.trace(
                    "changed cluster state triggered by [{}] snapshot state updates and resulted in starting " + "[{}] shard snapshots",
                    changedCount,
                    startedCount
                );
                return updated;
            }
            return existing;
        }

        void completeWithUpdatedState(SnapshotsInProgress snapshotsInProgress) {
            if (updatesByRepo.isEmpty() == false) {
                final var result = new ShardSnapshotUpdateResult(initialState.metadata(), snapshotsInProgress);
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    if (taskContext.getTask() instanceof ShardSnapshotUpdate task) {
                        taskContext.success(() -> task.listener.onResponse(result));
                    }
                }
            }
        }

        private SnapshotsInProgress.Entry applyToEntry(SnapshotsInProgress.Entry entry, List<ShardSnapshotUpdate> updates) {
            // Completed snapshots do not require any updates so we just add them to the output list and keep going.
            // Also we short circuit if there are no more unconsumed updates to apply.
            if (entry.state().completed() || updates.isEmpty()) {
                return entry;
            }
            return new EntryContext(entry, updates).computeUpdatedEntry();
        }

        // Per snapshot entry state
        private final class EntryContext {

            private final SnapshotsInProgress.Entry entry;

            // iterator containing the updates yet to be applied to #entry
            private final Iterator<ShardSnapshotUpdate> iterator;

            // builder for updated shard snapshot status mappings if any could be computed
            private ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shardsBuilder = null;

            // builder for updated shard clone status mappings if any could be computed
            private ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> clonesBuilder = null;

            EntryContext(SnapshotsInProgress.Entry entry, List<ShardSnapshotUpdate> updates) {
                this.entry = entry;
                this.iterator = updates.iterator();
            }

            SnapshotsInProgress.Entry computeUpdatedEntry() {
                assert shardsBuilder == null && clonesBuilder == null : "update context was already used";

                // loop over all the shard updates that are potentially applicable to the current snapshot entry
                while (iterator.hasNext()) {
                    final ShardSnapshotUpdate update = iterator.next();
                    if (entry.snapshot().getSnapshotId().equals(update.snapshot.getSnapshotId())) {
                        // update a currently running shard level operation
                        if (update.isClone()) {
                            executeShardSnapshotUpdate(entry.shardsByRepoShardId(), this::clonesBuilder, update, update.repoShardId);
                        } else {
                            executeShardSnapshotUpdate(entry.shards(), this::shardsBuilder, update, update.shardId);
                        }
                    } else if (executedUpdates.contains(update)) {
                        // try starting a new shard level operation because one has completed
                        if (update.isClone()) {
                            tryStartNextTaskAfterCloneUpdated(update.repoShardId, update.updatedState);
                        } else {
                            tryStartNextTaskAfterSnapshotUpdated(update.shardId, update.updatedState);
                        }
                    }
                }

                if (shardsBuilder != null) {
                    assert clonesBuilder == null
                        : "Should not have updated clones when updating shard snapshots but saw "
                            + clonesBuilder
                            + " as well as "
                            + shardsBuilder;
                    return entry.withShardStates(shardsBuilder.build());
                } else if (clonesBuilder != null) {
                    return entry.withClones(clonesBuilder.build());
                } else {
                    return entry;
                }
            }

            /**
             * Start shard level operation for given {@code shardId}.
             *
             * @param newStates   builder for updated shard states mapping
             * @param nodeId      node id to execute started operation on
             * @param generation  shard generation to base started operation on
             * @param shardId     shard identifier of shard to start operation for
             * @param <T>         either {@link ShardId} for snapshots or {@link RepositoryShardId} for clones
             */
            private <T> void startShardOperation(
                ImmutableOpenMap.Builder<T, ShardSnapshotStatus> newStates,
                String nodeId,
                ShardGeneration generation,
                T shardId
            ) {
                startShardOperation(newStates, shardId, new ShardSnapshotStatus(nodeId, generation));
            }

            /**
             * Start shard level operation for given {@code shardId}.
             *
             * @param newStates builder for updated shard states mapping
             * @param shardId   shard identifier of shard to start operation for
             * @param newState  new shard task state for operation to start
             * @param <T>       either {@link ShardId} for snapshots or {@link RepositoryShardId} for clones
             */
            private <T> void startShardOperation(
                ImmutableOpenMap.Builder<T, ShardSnapshotStatus> newStates,
                T shardId,
                ShardSnapshotStatus newState
            ) {
                logger.trace(
                    "[{}] Starting [{}] on [{}] with generation [{}]",
                    entry.snapshot(),
                    shardId,
                    newState.nodeId(),
                    newState.generation()
                );
                newStates.put(shardId, newState);
                iterator.remove();
                startedCount++;
            }

            private <T> void executeShardSnapshotUpdate(
                Map<T, ShardSnapshotStatus> existingStates,
                Supplier<ImmutableOpenMap.Builder<T, ShardSnapshotStatus>> newStates,
                ShardSnapshotUpdate updateSnapshotState,
                T updatedShard
            ) {
                assert updateSnapshotState.snapshot.equals(entry.snapshot());
                final ShardSnapshotStatus existing = existingStates.get(updatedShard);
                if (existing == null) {
                    logger.warn("Received shard snapshot status update [{}] but this shard is not tracked in [{}]", updatedShard, entry);
                    assert false : "This should never happen, should only receive updates for expected shards";
                    return;
                }

                if (existing.state().completed()) {
                    // No point in doing noop updates that might happen if data nodes resends shard status after a disconnect.
                    iterator.remove();
                    return;
                }

                logger.trace(
                    "[{}] Updating shard [{}] with status [{}]",
                    updateSnapshotState.snapshot,
                    updatedShard,
                    updateSnapshotState.updatedState.state()
                );
                changedCount++;
                newStates.get().put(updatedShard, updateSnapshotState.updatedState);
                executedUpdates.add(updateSnapshotState);
            }

            private void tryStartNextTaskAfterCloneUpdated(RepositoryShardId repoShardId, ShardSnapshotStatus updatedState) {
                // the update was already executed on the clone operation it applied to, now we check if it may be possible to
                // start a shard snapshot or clone operation on the current entry
                if (entry.isClone() == false) {
                    tryStartSnapshotAfterCloneFinish(repoShardId, updatedState.generation());
                } else if (isQueued(entry.shardsByRepoShardId().get(repoShardId))) {
                    final String localNodeId = initialState.nodes().getLocalNodeId();
                    assert updatedState.nodeId().equals(localNodeId)
                        : "Clone updated with node id [" + updatedState.nodeId() + "] but local node id is [" + localNodeId + "]";
                    startShardOperation(clonesBuilder(), localNodeId, updatedState.generation(), repoShardId);
                }
            }

            private void tryStartNextTaskAfterSnapshotUpdated(ShardId shardId, ShardSnapshotStatus updatedState) {
                // We applied the update for a shard snapshot state to its snapshot entry, now check if we can update
                // either a clone or a snapshot
                final IndexId indexId = entry.indices().get(shardId.getIndexName());
                if (indexId != null) {
                    final RepositoryShardId repoShardId = new RepositoryShardId(indexId, shardId.id());
                    if (isQueued(entry.shardsByRepoShardId().get(repoShardId))) {
                        if (entry.isClone()) {
                            // shard snapshot was completed, we check if we can start a clone operation for the same repo shard
                            startShardOperation(
                                clonesBuilder(),
                                initialState.nodes().getLocalNodeId(),
                                updatedState.generation(),
                                repoShardId
                            );
                        } else {
                            startShardSnapshot(repoShardId, updatedState.generation());
                        }
                    }
                }
            }

            private void tryStartSnapshotAfterCloneFinish(RepositoryShardId repoShardId, ShardGeneration generation) {
                assert entry.source() == null;
                // current entry is a snapshot operation so we must translate the repository shard id to a routing shard id
                if (isQueued(entry.shardsByRepoShardId().get(repoShardId))) {
                    startShardSnapshot(repoShardId, generation);
                }
            }

            private void startShardSnapshot(RepositoryShardId repoShardId, ShardGeneration generation) {
                final Index index = entry.indexByName(repoShardId.indexName());
                assert index != null
                    : "index ["
                        + repoShardId.index()
                        + "] must exist in snapshot entry ["
                        + entry
                        + "] because it's a normal snapshot but did not";
                // work out the node to run the snapshot task on as it might have changed from the previous operation if it was a clone
                // or there was a primary failover
                final IndexRoutingTable indexRouting = initialState.routingTable().index(index);
                final ShardRouting shardRouting;
                if (indexRouting == null) {
                    shardRouting = null;
                } else {
                    shardRouting = indexRouting.shard(repoShardId.shardId()).primaryShard();
                }
                final ShardSnapshotStatus shardSnapshotStatus = initShardSnapshotStatus(generation, shardRouting);
                final ShardId routingShardId = shardRouting != null ? shardRouting.shardId() : new ShardId(index, repoShardId.shardId());
                if (shardSnapshotStatus.isActive()) {
                    startShardOperation(shardsBuilder(), routingShardId, shardSnapshotStatus);
                } else {
                    // update to queued snapshot did not result in an actual update execution so we just record it but keep applying
                    // the update to e.g. fail all snapshots for a given shard if the primary for the shard went away
                    shardsBuilder().put(routingShardId, shardSnapshotStatus);
                }
            }

            private ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> clonesBuilder() {
                assert shardsBuilder == null;
                if (clonesBuilder == null) {
                    clonesBuilder = ImmutableOpenMap.builder(entry.shardsByRepoShardId());
                }
                return clonesBuilder;
            }

            private ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shardsBuilder() {
                assert clonesBuilder == null;
                if (shardsBuilder == null) {
                    shardsBuilder = ImmutableOpenMap.builder(entry.shards());
                }
                return shardsBuilder;
            }
        }
    }

    /**
     * The result of a {@link ShardSnapshotUpdate}, capturing the info needed to finalize the relevant snapshot if appropriate.
     */
    record ShardSnapshotUpdateResult(Metadata metadata, SnapshotsInProgress snapshotsInProgress) {}

    /**
     * An update to the snapshot state of a shard.
     *
     * Package private for testing
     */
    static final class ShardSnapshotUpdate implements SnapshotTask {

        private final Snapshot snapshot;
        private final ShardId shardId;
        private final RepositoryShardId repoShardId;
        private final ShardSnapshotStatus updatedState;
        private final ActionListener<ShardSnapshotUpdateResult> listener;

        ShardSnapshotUpdate(
            Snapshot snapshot,
            ShardId shardId,
            RepositoryShardId repoShardId,
            ShardSnapshotStatus updatedState,
            ActionListener<ShardSnapshotUpdateResult> listener
        ) {
            assert shardId != null ^ repoShardId != null;
            this.snapshot = snapshot;
            this.shardId = shardId;
            this.repoShardId = repoShardId;
            this.updatedState = updatedState;
            this.listener = listener;
        }

        public boolean isClone() {
            return repoShardId != null;
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if ((other instanceof ShardSnapshotUpdate) == false) {
                return false;
            }
            final ShardSnapshotUpdate that = (ShardSnapshotUpdate) other;
            return this.snapshot.equals(that.snapshot)
                && Objects.equals(this.shardId, that.shardId)
                && Objects.equals(this.repoShardId, that.repoShardId)
                && this.updatedState == that.updatedState;
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, shardId, updatedState, repoShardId);
        }

        @Override
        public String toString() {
            return "ShardSnapshotUpdate{"
                + "snapshot="
                + snapshot
                + ", shardId="
                + shardId
                + ", repoShardId="
                + repoShardId
                + ", updatedState="
                + updatedState
                + '}';
        }
    }

    private void innerUpdateSnapshotState(
        Snapshot snapshot,
        ShardId shardId,
        RepositoryShardId repoShardId,
        ShardSnapshotStatus updatedState,
        ActionListener<Void> listener
    ) {
        var update = new ShardSnapshotUpdate(snapshot, shardId, repoShardId, updatedState, listener.delegateFailure((delegate, result) -> {
            try {
                delegate.onResponse(null);
            } finally {
                // Maybe this state update completed the snapshot. If we are not already ending it because of a concurrent
                // state update we check if its state is completed and end it if it is.
                final SnapshotsInProgress snapshotsInProgress = result.snapshotsInProgress();
                if (endingSnapshots.contains(snapshot) == false) {
                    final SnapshotsInProgress.Entry updatedEntry = snapshotsInProgress.snapshot(snapshot);
                    // If the entry is still in the cluster state and is completed, try finalizing the snapshot in the repo
                    if (updatedEntry != null && updatedEntry.state().completed()) {
                        endSnapshot(updatedEntry, result.metadata(), null);
                    }
                }
                startExecutableClones(snapshotsInProgress, snapshot.getRepository());
            }
        }));
        logger.trace("received updated snapshot restore state [{}]", update);
        masterServiceTaskQueue.submitTask("update snapshot state", update, null);
    }

    private void startExecutableClones(SnapshotsInProgress snapshotsInProgress, @Nullable String repoName) {
        if (repoName == null) {
            for (List<SnapshotsInProgress.Entry> entries : snapshotsInProgress.entriesByRepo()) {
                startExecutableClones(entries);
            }
        } else {
            startExecutableClones(snapshotsInProgress.forRepo(repoName));
        }
    }

    private void startExecutableClones(List<SnapshotsInProgress.Entry> entries) {
        for (SnapshotsInProgress.Entry entry : entries) {
            if (entry.isClone() && entry.state() == State.STARTED) {
                // this is a clone, see if new work is ready
                for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> clone : entry.shardsByRepoShardId().entrySet()) {
                    if (clone.getValue().state() == ShardState.INIT) {
                        runReadyClone(
                            entry.snapshot(),
                            entry.source(),
                            clone.getValue(),
                            clone.getKey(),
                            repositoriesService.repository(entry.repository())
                        );
                    }
                }
            }
        }
    }

    private class UpdateSnapshotStatusAction extends TransportMasterNodeAction<
        UpdateIndexShardSnapshotStatusRequest,
        ActionResponse.Empty> {
        UpdateSnapshotStatusAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
                false,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                UpdateIndexShardSnapshotStatusRequest::new,
                indexNameExpressionResolver,
                in -> ActionResponse.Empty.INSTANCE,
                ThreadPool.Names.SAME
            );
        }

        @Override
        protected void masterOperation(
            Task task,
            UpdateIndexShardSnapshotStatusRequest request,
            ClusterState state,
            ActionListener<ActionResponse.Empty> listener
        ) {
            innerUpdateSnapshotState(
                request.snapshot(),
                request.shardId(),
                null,
                request.status(),
                listener.map(v -> ActionResponse.Empty.INSTANCE)
            );
        }

        @Override
        protected ClusterBlockException checkBlock(UpdateIndexShardSnapshotStatusRequest request, ClusterState state) {
            return null;
        }
    }

    /**
     * Cluster state update task that removes all {@link SnapshotsInProgress.Entry} and {@link SnapshotDeletionsInProgress.Entry} for a
     * given repository from the cluster state and afterwards fails all relevant listeners in {@link #snapshotCompletionListeners} and
     * {@link #snapshotDeletionListeners}.
     */
    private final class FailPendingRepoTasksTask extends ClusterStateUpdateTask {

        // Snapshots to fail after the state update
        private final List<Snapshot> snapshotsToFail = new ArrayList<>();

        // Delete uuids to fail because after the state update
        private final List<String> deletionsToFail = new ArrayList<>();

        // Failure that caused the decision to fail all snapshots and deletes for a repo
        private final Exception failure;

        private final String repository;

        FailPendingRepoTasksTask(String repository, Exception failure) {
            this.repository = repository;
            this.failure = failure;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                SnapshotDeletionsInProgress.TYPE,
                SnapshotDeletionsInProgress.EMPTY
            );
            boolean changed = false;
            final List<SnapshotDeletionsInProgress.Entry> remainingEntries = deletionsInProgress.getEntries();
            List<SnapshotDeletionsInProgress.Entry> updatedEntries = new ArrayList<>(remainingEntries.size());
            for (SnapshotDeletionsInProgress.Entry entry : remainingEntries) {
                if (entry.repository().equals(repository)) {
                    changed = true;
                    deletionsToFail.add(entry.uuid());
                } else {
                    updatedEntries.add(entry);
                }
            }
            final SnapshotDeletionsInProgress updatedDeletions = changed ? SnapshotDeletionsInProgress.of(updatedEntries) : null;
            final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            boolean changedSnapshots = false;
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(repository)) {
                // We failed to read repository data for this delete, it is not the job of SnapshotsService to
                // retry these kinds of issues so we fail all the pending snapshots
                snapshotsToFail.add(entry.snapshot());
                changedSnapshots = true;
            }
            final SnapshotsInProgress updatedSnapshotsInProgress = changedSnapshots
                ? snapshotsInProgress.withUpdatedEntriesForRepo(repository, List.of())
                : null;
            return updateWithSnapshots(currentState, updatedSnapshotsInProgress, updatedDeletions);
        }

        @Override
        public void onFailure(Exception e) {
            logger.info(() -> "Failed to remove all snapshot tasks for repo [" + repository + "] from cluster state", e);
            failAllListenersOnMasterFailOver(e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            logger.warn(
                () -> format("Removed all snapshot tasks for repository [%s] from cluster state, now failing listeners", repository),
                failure
            );
            synchronized (currentlyFinalizing) {
                Tuple<Snapshot, Metadata> finalization;
                while ((finalization = repositoryOperations.pollFinalization(repository)) != null) {
                    assert snapshotsToFail.contains(finalization.v1())
                        : "[" + finalization.v1() + "] not found in snapshots to fail " + snapshotsToFail;
                }
                leaveRepoLoop(repository);
                for (Snapshot snapshot : snapshotsToFail) {
                    failSnapshotCompletionListeners(snapshot, failure);
                }
                for (String delete : deletionsToFail) {
                    failListenersIgnoringException(snapshotDeletionListeners.remove(delete), failure);
                    repositoryOperations.finishDeletion(delete);
                }
            }
        }
    }

    private static final class OngoingRepositoryOperations {

        /**
         * Map of repository name to a deque of {@link Snapshot} that need to be finalized for the repository and the
         * {@link Metadata to use when finalizing}.
         */
        private final Map<String, Deque<Snapshot>> snapshotsToFinalize = new HashMap<>();

        /**
         * Set of delete operations currently being executed against the repository. The values in this set are the delete UUIDs returned
         * by {@link SnapshotDeletionsInProgress.Entry#uuid()}.
         */
        private final Set<String> runningDeletions = Collections.synchronizedSet(new HashSet<>());

        @Nullable
        private Metadata latestKnownMetaData;

        @Nullable
        synchronized Tuple<Snapshot, Metadata> pollFinalization(String repository) {
            assertConsistent();
            final Snapshot nextEntry;
            final Deque<Snapshot> queued = snapshotsToFinalize.get(repository);
            if (queued == null) {
                return null;
            }
            nextEntry = queued.pollFirst();
            assert nextEntry != null;
            final Tuple<Snapshot, Metadata> res = Tuple.tuple(nextEntry, latestKnownMetaData);
            if (queued.isEmpty()) {
                snapshotsToFinalize.remove(repository);
            }
            if (snapshotsToFinalize.isEmpty()) {
                latestKnownMetaData = null;
            }
            assert assertConsistent();
            return res;
        }

        boolean startDeletion(String deleteUUID) {
            return runningDeletions.add(deleteUUID);
        }

        void finishDeletion(String deleteUUID) {
            runningDeletions.remove(deleteUUID);
        }

        synchronized void addFinalization(Snapshot snapshot, Metadata metadata) {
            snapshotsToFinalize.computeIfAbsent(snapshot.getRepository(), k -> new LinkedList<>()).add(snapshot);
            this.latestKnownMetaData = metadata;
            assertConsistent();
        }

        /**
         * Clear all state associated with running snapshots. To be used on master-failover if the current node stops
         * being master.
         */
        synchronized void clear() {
            snapshotsToFinalize.clear();
            runningDeletions.clear();
            latestKnownMetaData = null;
        }

        synchronized boolean isEmpty() {
            return snapshotsToFinalize.isEmpty();
        }

        synchronized boolean assertNotQueued(Snapshot snapshot) {
            assert snapshotsToFinalize.getOrDefault(snapshot.getRepository(), new LinkedList<>())
                .stream()
                .noneMatch(entry -> entry.equals(snapshot)) : "Snapshot [" + snapshot + "] is still in finalization queue";
            return true;
        }

        synchronized boolean assertConsistent() {
            assert (latestKnownMetaData == null && snapshotsToFinalize.isEmpty())
                || (latestKnownMetaData != null && snapshotsToFinalize.isEmpty() == false)
                : "Should not hold on to metadata if there are no more queued snapshots";
            assert snapshotsToFinalize.values().stream().noneMatch(Collection::isEmpty) : "Found empty queue in " + snapshotsToFinalize;
            return true;
        }
    }

    interface SnapshotTask extends ClusterStateTaskListener {}

    private record CreateSnapshotTask(
        Repository repository,
        RepositoryData repositoryData,
        ActionListener<Snapshot> listener,
        Snapshot snapshot,
        CreateSnapshotRequest createSnapshotRequest,
        RepositoryMetadata initialRepositoryMetadata
    ) implements SnapshotTask {

        @Override
        public void onFailure(Exception e) {
            logger.warn(
                () -> format("[%s][%s] failed to create snapshot", snapshot.getRepository(), snapshot.getSnapshotId().getName()),
                e
            );
            listener.onFailure(e);
        }
    }

    private class SnapshotTaskExecutor implements ClusterStateTaskExecutor<SnapshotTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<SnapshotTask> batchExecutionContext) throws Exception {
            final ClusterState state = batchExecutionContext.initialState();
            final SnapshotShardsUpdateContext shardsUpdateContext = new SnapshotShardsUpdateContext(batchExecutionContext);
            final SnapshotsInProgress initialSnapshots = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            SnapshotsInProgress snapshotsInProgress = shardsUpdateContext.computeUpdatedState();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                if (taskContext.getTask() instanceof CreateSnapshotTask task) {
                    try {
                        final var repoMeta = state.metadata()
                            .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY)
                            .repository(task.snapshot.getRepository());
                        if (Objects.equals(task.initialRepositoryMetadata, repoMeta)) {
                            snapshotsInProgress = createSnapshot(task, taskContext, state, snapshotsInProgress);
                        } else {
                            // repository data changed in between starting the task and executing this cluster state update so try again
                            taskContext.success(
                                () -> submitCreateSnapshotRequest(
                                    task.createSnapshotRequest,
                                    task.listener,
                                    task.repository,
                                    task.snapshot,
                                    repoMeta
                                )
                            );
                        }
                    } catch (Exception e) {
                        taskContext.onFailure(e);
                    }
                }
            }
            shardsUpdateContext.completeWithUpdatedState(snapshotsInProgress);
            if (snapshotsInProgress == initialSnapshots) {
                return state;
            }
            return ClusterState.builder(state).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build();
        }

        private SnapshotsInProgress createSnapshot(
            CreateSnapshotTask createSnapshotTask,
            TaskContext<SnapshotTask> taskContext,
            ClusterState currentState,
            SnapshotsInProgress snapshotsInProgress
        ) {
            final RepositoryData repositoryData = createSnapshotTask.repositoryData;
            final Snapshot snapshot = createSnapshotTask.snapshot;
            final String repositoryName = snapshot.getRepository();
            final String snapshotName = snapshot.getSnapshotId().getName();
            ensureRepositoryExists(repositoryName, currentState);
            final Repository repository = createSnapshotTask.repository;
            ensureSnapshotNameAvailableInRepo(repositoryData, snapshotName, repository);
            ensureSnapshotNameNotRunning(snapshotsInProgress, repositoryName, snapshotName);
            validate(repositoryName, snapshotName, currentState);
            final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                SnapshotDeletionsInProgress.TYPE,
                SnapshotDeletionsInProgress.EMPTY
            );
            ensureNoCleanupInProgress(currentState, repositoryName, snapshotName, "create snapshot");
            ensureBelowConcurrencyLimit(repositoryName, snapshotName, snapshotsInProgress, deletionsInProgress);

            final CreateSnapshotRequest request = createSnapshotTask.createSnapshotRequest;
            // Store newSnapshot here to be processed in clusterStateProcessed
            Map<Boolean, List<String>> requestedIndices = Arrays.stream(
                indexNameExpressionResolver.concreteIndexNames(currentState, request)
            ).collect(Collectors.partitioningBy(systemIndices::isSystemIndex));

            List<String> requestedSystemIndices = requestedIndices.get(true);
            if (requestedSystemIndices.isEmpty() == false) {
                Set<String> explicitlyRequestedSystemIndices = new HashSet<>(requestedSystemIndices);
                explicitlyRequestedSystemIndices.retainAll(Arrays.asList(request.indices()));
                if (explicitlyRequestedSystemIndices.isEmpty() == false) {
                    throw new IllegalArgumentException(
                        format(
                            "the [indices] parameter includes system indices %s; to include or exclude system indices from a "
                                + "snapshot, use the [include_global_state] or [feature_states] parameters",
                            explicitlyRequestedSystemIndices
                        )
                    );
                }
            }

            List<String> indices = requestedIndices.get(false);

            // We should only use the feature states logic if we're sure we'll be able to finish the snapshot without a lower-version
            // node taking over and causing problems. Therefore, if we're in a mixed cluster with versions that don't know how to handle
            // feature states, skip all feature states logic, and if `feature_states` is explicitly configured, throw an exception.
            final List<String> requestedStates = Arrays.asList(request.featureStates());
            final Set<String> featureStatesSet;
            if (request.includeGlobalState() || requestedStates.isEmpty() == false) {
                if (request.includeGlobalState() && requestedStates.isEmpty()) {
                    // If we're including global state and feature states aren't specified, include all of them
                    featureStatesSet = systemIndices.getFeatureNames();
                } else if (requestedStates.size() == 1 && NO_FEATURE_STATES_VALUE.equalsIgnoreCase(requestedStates.get(0))) {
                    // If there's exactly one value and it's "none", include no states
                    featureStatesSet = Collections.emptySet();
                } else {
                    // Otherwise, check for "none" then use the list of requested states
                    if (requestedStates.contains(NO_FEATURE_STATES_VALUE)) {
                        throw new IllegalArgumentException(
                            "the feature_states value ["
                                + SnapshotsService.NO_FEATURE_STATES_VALUE
                                + "] indicates that no feature states should be snapshotted, "
                                + "but other feature states were requested: "
                                + requestedStates
                        );
                    }
                    featureStatesSet = new HashSet<>(requestedStates);
                    featureStatesSet.retainAll(systemIndices.getFeatureNames());
                }
            } else {
                featureStatesSet = Collections.emptySet();
            }

            final Set<SnapshotFeatureInfo> featureStates = new HashSet<>();
            final Set<String> systemDataStreamNames = new HashSet<>();
            // if we have any feature states in the snapshot, we add their required indices to the snapshot indices if they haven't
            // been requested by the request directly
            final Set<String> indexNames = new HashSet<>(indices);
            for (String featureName : featureStatesSet) {
                SystemIndices.Feature feature = systemIndices.getFeature(featureName);

                Set<String> featureSystemIndices = feature.getIndexDescriptors()
                    .stream()
                    .flatMap(descriptor -> descriptor.getMatchingIndices(currentState.metadata()).stream())
                    .collect(Collectors.toSet());
                Set<String> featureAssociatedIndices = feature.getAssociatedIndexDescriptors()
                    .stream()
                    .flatMap(descriptor -> descriptor.getMatchingIndices(currentState.metadata()).stream())
                    .collect(Collectors.toSet());

                Set<String> featureSystemDataStreams = new HashSet<>();
                Set<String> featureDataStreamBackingIndices = new HashSet<>();
                for (SystemDataStreamDescriptor sdd : feature.getDataStreamDescriptors()) {
                    List<String> backingIndexNames = sdd.getBackingIndexNames(currentState.metadata());
                    if (backingIndexNames.size() > 0) {
                        featureDataStreamBackingIndices.addAll(backingIndexNames);
                        featureSystemDataStreams.add(sdd.getDataStreamName());
                    }
                }

                if (featureSystemIndices.size() > 0 || featureAssociatedIndices.size() > 0 || featureDataStreamBackingIndices.size() > 0) {

                    featureStates.add(new SnapshotFeatureInfo(featureName, List.copyOf(featureSystemIndices)));
                    indexNames.addAll(featureSystemIndices);
                    indexNames.addAll(featureAssociatedIndices);
                    indexNames.addAll(featureDataStreamBackingIndices);
                    systemDataStreamNames.addAll(featureSystemDataStreams);
                }
                indices = List.copyOf(indexNames);
            }

            logger.trace("[{}][{}] creating snapshot for indices [{}]", repositoryName, snapshotName, indices);

            final Map<String, IndexId> allIndices = new HashMap<>();
            for (SnapshotsInProgress.Entry runningSnapshot : snapshotsInProgress.forRepo(repositoryName)) {
                allIndices.putAll(runningSnapshot.indices());
            }
            final Map<String, IndexId> indexIds = repositoryData.resolveNewIndices(indices, allIndices);
            final Version version = minCompatibleVersion(currentState.nodes().getMinNodeVersion(), repositoryData, null);
            ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards = shards(
                snapshotsInProgress,
                deletionsInProgress,
                currentState,
                indexIds.values(),
                useShardGenerations(version),
                repositoryData,
                repositoryName
            );
            if (request.partial() == false) {
                Set<String> missing = new HashSet<>();
                for (Map.Entry<ShardId, ShardSnapshotStatus> entry : shards.entrySet()) {
                    if (entry.getValue().state() == ShardState.MISSING) {
                        missing.add(entry.getKey().getIndex().getName());
                    }
                }
                if (missing.isEmpty() == false) {
                    throw new SnapshotException(snapshot, "Indices don't have primary shards " + missing);
                }
            }
            final var newEntry = SnapshotsInProgress.startedEntry(
                snapshot,
                request.includeGlobalState(),
                request.partial(),
                indexIds,
                CollectionUtils.concatLists(
                    indexNameExpressionResolver.dataStreamNames(currentState, request.indicesOptions(), request.indices()),
                    systemDataStreamNames
                ),
                threadPool.absoluteTimeInMillis(),
                repositoryData.getGenId(),
                shards,
                request.userMetadata(),
                version,
                List.copyOf(featureStates)
            );
            final var res = snapshotsInProgress.withAddedEntry(newEntry);
            taskContext.success(() -> {
                logger.info("snapshot [{}] started", snapshot);
                createSnapshotTask.listener.onResponse(snapshot);
                if (newEntry.state().completed()) {
                    endSnapshot(newEntry, currentState.metadata(), createSnapshotTask.repositoryData);
                }
            });
            return res;
        }
    }
}
