/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Repository cleanup action for repository implementations based on {@link BlobStoreRepository}.
 *
 * The steps taken by the repository cleanup operation are as follows:
 * <ol>
 *     <li>Check that there are no running repository cleanup, snapshot create, or snapshot delete actions
 *     and add an entry for the repository that is to be cleaned up to {@link RepositoryCleanupInProgress}</li>
 *     <li>Run cleanup actions on the repository. Note, these are executed exclusively on the master node.
 *     For the precise operations execute see {@link BlobStoreRepository#cleanup}</li>
 *     <li>Remove the entry in {@link RepositoryCleanupInProgress} in the first step.</li>
 * </ol>
 *
 * On master failover during the cleanup operation it is simply removed from the cluster state. This is safe because the logic in
 * {@link BlobStoreRepository#cleanup} ensures that the repository state id has not changed between creation of the cluster state entry
 * and any delete/write operations. TODO: This will not work if we also want to clean up at the shard level as those will involve writes
 *                                        as well as deletes.
 */
public final class TransportCleanupRepositoryAction extends TransportMasterNodeAction<CleanupRepositoryRequest, CleanupRepositoryResponse> {

    public static final ActionType<CleanupRepositoryResponse> TYPE = new ActionType<>("cluster:admin/repository/_cleanup");
    private static final Logger logger = LogManager.getLogger(TransportCleanupRepositoryAction.class);

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportCleanupRepositoryAction(
        TransportService transportService,
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CleanupRepositoryRequest::readFrom,
            CleanupRepositoryResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.repositoriesService = repositoriesService;
        // We add a state applier that will remove any dangling repository cleanup actions on master failover.
        // This is safe to do since cleanups will increment the repository state id before executing any operations to prevent concurrent
        // operations from corrupting the repository. This is the same safety mechanism used by snapshot deletes.
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            addClusterStateApplier(clusterService);
        }
    }

    private static void addClusterStateApplier(ClusterService clusterService) {
        clusterService.addStateApplier(event -> {
            if (event.localNodeMaster() && event.previousState().nodes().isLocalNodeElectedMaster() == false) {
                final RepositoryCleanupInProgress repositoryCleanupInProgress = RepositoryCleanupInProgress.get(event.state());
                if (repositoryCleanupInProgress.hasCleanupInProgress() == false) {
                    return;
                }
                submitUnbatchedTask(clusterService, "clean up repository cleanup task after master failover", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return removeInProgressCleanup(currentState);
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                        logger.debug("Removed repository cleanup task [{}] from cluster state", repositoryCleanupInProgress);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Failed to remove repository cleanup task [{}] from cluster state", repositoryCleanupInProgress);
                    }
                });
            }
        });
    }

    private static ClusterState removeInProgressCleanup(final ClusterState currentState) {
        return RepositoryCleanupInProgress.get(currentState).hasCleanupInProgress()
            ? ClusterState.builder(currentState).putCustom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY).build()
            : currentState;
    }

    @Override
    protected void masterOperation(
        Task task,
        CleanupRepositoryRequest request,
        ClusterState state,
        ActionListener<CleanupRepositoryResponse> listener
    ) {
        cleanupRepo(request.name(), listener.map(CleanupRepositoryResponse::new));
    }

    @Override
    protected ClusterBlockException checkBlock(CleanupRepositoryRequest request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    /**
     * Runs cleanup operations on the given repository.
     * @param repositoryName Repository to clean up
     * @param listener Listener for cleanup result
     */
    private void cleanupRepo(String repositoryName, ActionListener<RepositoryCleanupResult> listener) {
        final Repository repository = repositoriesService.repository(repositoryName);
        if (repository instanceof BlobStoreRepository == false) {
            listener.onFailure(new IllegalArgumentException("Repository [" + repositoryName + "] does not support repository cleanup"));
            return;
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final ListenableFuture<RepositoryData> repositoryDataListener = new ListenableFuture<>();
        repository.getRepositoryData(
            EsExecutors.DIRECT_EXECUTOR_SERVICE, // Listener is lightweight, only submits a cluster state update task, no need to fork
            repositoryDataListener
        );
        repositoryDataListener.addListener(listener.delegateFailureAndWrap((delegate, repositoryData) -> {
            final long repositoryStateId = repositoryData.getGenId();
            logger.info("Running cleanup operations on repository [{}][{}]", repositoryName, repositoryStateId);
            submitUnbatchedTask(
                clusterService,
                "cleanup repository [" + repositoryName + "][" + repositoryStateId + ']',
                new ClusterStateUpdateTask() {

                    private boolean startedCleanup = false;

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        SnapshotsService.ensureRepositoryExists(repositoryName, currentState);
                        SnapshotsService.ensureNotReadOnly(currentState, repositoryName);
                        final RepositoryCleanupInProgress repositoryCleanupInProgress = RepositoryCleanupInProgress.get(currentState);
                        if (repositoryCleanupInProgress.hasCleanupInProgress()) {
                            throw new IllegalStateException(
                                "Cannot cleanup ["
                                    + repositoryName
                                    + "] - a repository cleanup is already in-progress in ["
                                    + repositoryCleanupInProgress
                                    + "]"
                            );
                        }
                        final SnapshotDeletionsInProgress deletionsInProgress = SnapshotDeletionsInProgress.get(currentState);
                        if (deletionsInProgress.hasDeletionsInProgress()) {
                            throw new IllegalStateException(
                                "Cannot cleanup ["
                                    + repositoryName
                                    + "] - a snapshot is currently being deleted in ["
                                    + deletionsInProgress
                                    + "]"
                            );
                        }
                        SnapshotsInProgress snapshots = SnapshotsInProgress.get(currentState);
                        if (snapshots.isEmpty() == false) {
                            throw new IllegalStateException(
                                "Cannot cleanup [" + repositoryName + "] - a snapshot is currently running in [" + snapshots + "]"
                            );
                        }
                        return ClusterState.builder(currentState)
                            .putCustom(
                                RepositoryCleanupInProgress.TYPE,
                                new RepositoryCleanupInProgress(
                                    List.of(RepositoryCleanupInProgress.startedEntry(repositoryName, repositoryStateId))
                                )
                            )
                            .build();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        after(e, null);
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                        startedCleanup = true;
                        logger.debug("Initialized repository cleanup in cluster state for [{}][{}]", repositoryName, repositoryStateId);
                        ActionListener.run(
                            ActionListener.<DeleteResult>wrap(
                                result -> after(null, new RepositoryCleanupResult(result)),
                                e -> after(e, null)
                            ),
                            l -> blobStoreRepository.cleanup(repositoryStateId, newState.nodes().getMaxDataNodeCompatibleIndexVersion(), l)
                        );
                    }

                    private void after(@Nullable Exception failure, @Nullable RepositoryCleanupResult result) {
                        if (failure == null) {
                            logger.debug("Finished repository cleanup operations on [{}][{}]", repositoryName, repositoryStateId);
                        } else {
                            logger.debug(
                                () -> "Failed to finish repository cleanup operations on ["
                                    + repositoryName
                                    + "]["
                                    + repositoryStateId
                                    + "]",
                                failure
                            );
                        }
                        assert failure != null || result != null;
                        if (startedCleanup == false) {
                            logger.debug("No cleanup task to remove from cluster state because we failed to start one", failure);
                            delegate.onFailure(failure);
                            return;
                        }
                        submitUnbatchedTask(
                            clusterService,
                            "remove repository cleanup task [" + repositoryName + "][" + repositoryStateId + ']',
                            new ClusterStateUpdateTask() {
                                @Override
                                public ClusterState execute(ClusterState currentState) {
                                    return removeInProgressCleanup(currentState);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    if (failure != null) {
                                        e.addSuppressed(failure);
                                    }
                                    logger.warn(() -> "[" + repositoryName + "] failed to remove repository cleanup task", e);
                                    delegate.onFailure(e);
                                }

                                @Override
                                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                                    if (failure == null) {
                                        logger.info(
                                            "Done with repository cleanup on [{}][{}] with result [{}]",
                                            repositoryName,
                                            repositoryStateId,
                                            result
                                        );
                                        delegate.onResponse(result);
                                    } else {
                                        logger.warn(
                                            () -> "Failed to run repository cleanup operations on ["
                                                + repositoryName
                                                + "]["
                                                + repositoryStateId
                                                + "]",
                                            failure
                                        );
                                        delegate.onFailure(failure);
                                    }
                                }
                            }
                        );
                    }
                }
            );
        }));
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static void submitUnbatchedTask(
        ClusterService clusterService,
        @SuppressWarnings("SameParameterValue") String source,
        ClusterStateUpdateTask task
    ) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}
