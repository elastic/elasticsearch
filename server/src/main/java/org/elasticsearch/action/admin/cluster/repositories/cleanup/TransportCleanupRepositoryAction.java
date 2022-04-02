/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
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

    private static final Logger logger = LogManager.getLogger(TransportCleanupRepositoryAction.class);

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportCleanupRepositoryAction(
        TransportService transportService,
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            CleanupRepositoryAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CleanupRepositoryRequest::new,
            indexNameExpressionResolver,
            CleanupRepositoryResponse::new,
            ThreadPool.Names.SAME
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
                final RepositoryCleanupInProgress repositoryCleanupInProgress = event.state()
                    .custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY);
                if (repositoryCleanupInProgress.hasCleanupInProgress() == false) {
                    return;
                }
                clusterService.submitStateUpdateTask(
                    "clean up repository cleanup task after master failover",
                    new ClusterStateUpdateTask() {
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
                    },
                    newExecutor()
                );
            }
        });
    }

    private static ClusterState removeInProgressCleanup(final ClusterState currentState) {
        return currentState.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY).hasCleanupInProgress()
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
        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        repository.getRepositoryData(repositoryDataListener);
        repositoryDataListener.whenComplete(repositoryData -> {
            final long repositoryStateId = repositoryData.getGenId();
            logger.info("Running cleanup operations on repository [{}][{}]", repositoryName, repositoryStateId);
            clusterService.submitStateUpdateTask(
                "cleanup repository [" + repositoryName + "][" + repositoryStateId + ']',
                new ClusterStateUpdateTask() {

                    private boolean startedCleanup = false;

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        SnapshotsService.ensureRepositoryExists(repositoryName, currentState);
                        final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(
                            RepositoryCleanupInProgress.TYPE,
                            RepositoryCleanupInProgress.EMPTY
                        );
                        if (repositoryCleanupInProgress.hasCleanupInProgress()) {
                            throw new IllegalStateException(
                                "Cannot cleanup ["
                                    + repositoryName
                                    + "] - a repository cleanup is already in-progress in ["
                                    + repositoryCleanupInProgress
                                    + "]"
                            );
                        }
                        final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                            SnapshotDeletionsInProgress.TYPE,
                            SnapshotDeletionsInProgress.EMPTY
                        );
                        if (deletionsInProgress.hasDeletionsInProgress()) {
                            throw new IllegalStateException(
                                "Cannot cleanup ["
                                    + repositoryName
                                    + "] - a snapshot is currently being deleted in ["
                                    + deletionsInProgress
                                    + "]"
                            );
                        }
                        SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
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
                        threadPool.executor(ThreadPool.Names.SNAPSHOT)
                            .execute(
                                ActionRunnable.wrap(
                                    listener,
                                    l -> blobStoreRepository.cleanup(
                                        repositoryStateId,
                                        SnapshotsService.minCompatibleVersion(newState.nodes().getMinNodeVersion(), repositoryData, null),
                                        ActionListener.wrap(result -> after(null, result), e -> after(e, null))
                                    )
                                )
                            );
                    }

                    private void after(@Nullable Exception failure, @Nullable RepositoryCleanupResult result) {
                        if (failure == null) {
                            logger.debug("Finished repository cleanup operations on [{}][{}]", repositoryName, repositoryStateId);
                        } else {
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "Failed to finish repository cleanup operations on [{}][{}]",
                                    repositoryName,
                                    repositoryStateId
                                ),
                                failure
                            );
                        }
                        assert failure != null || result != null;
                        if (startedCleanup == false) {
                            logger.debug("No cleanup task to remove from cluster state because we failed to start one", failure);
                            listener.onFailure(failure);
                            return;
                        }
                        clusterService.submitStateUpdateTask(
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
                                    logger.warn(
                                        () -> new ParameterizedMessage("[{}] failed to remove repository cleanup task", repositoryName),
                                        e
                                    );
                                    listener.onFailure(e);
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
                                        listener.onResponse(result);
                                    } else {
                                        logger.warn(
                                            () -> new ParameterizedMessage(
                                                "Failed to run repository cleanup operations on [{}][{}]",
                                                repositoryName,
                                                repositoryStateId
                                            ),
                                            failure
                                        );
                                        listener.onFailure(failure);
                                    }
                                }
                            },
                            newExecutor()
                        );
                    }
                },
                newExecutor()
            );
        }, listener::onFailure);
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }
}
