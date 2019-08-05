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
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public final class TransportCleanupRepositoryAction extends TransportMasterNodeAction<CleanupRepositoryRequest,
    CleanupRepositoryResponse> {

    private static final Logger logger = LogManager.getLogger(TransportCleanupRepositoryAction.class);

    private static final Version MIN_VERSION = Version.V_8_0_0;

    private final RepositoriesService repositoriesService;

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Inject
    public TransportCleanupRepositoryAction(TransportService transportService, ClusterService clusterService,
        RepositoriesService repositoriesService, ThreadPool threadPool, ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(CleanupRepositoryAction.NAME, transportService, clusterService, threadPool, actionFilters,
            CleanupRepositoryRequest::new, indexNameExpressionResolver);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected CleanupRepositoryResponse read(StreamInput in) throws IOException {
        return new CleanupRepositoryResponse(in);
    }

    @Override
    protected void masterOperation(Task task, CleanupRepositoryRequest request, ClusterState state,
        ActionListener<CleanupRepositoryResponse> listener) {
        if (state.nodes().getMinNodeVersion().onOrAfter(MIN_VERSION)) {
            cleanupRepo(request.repository(), ActionListener.map(listener, CleanupRepositoryResponse::new));
        } else {
            throw new IllegalArgumentException("Repository cleanup is only supported from version [" + MIN_VERSION
                + "] but the oldest node version in the cluster is [" + state.nodes().getMinNodeVersion() + ']');
        }
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
        final long repositoryStateId = repository.getRepositoryData().getGenId();
        clusterService.submitStateUpdateTask("cleanup repository", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.cleanupInProgress() == false) {
                    throw new IllegalStateException(
                        "Cannot cleanup [" + repositoryName + "] - a repository cleanup is already in-progress");
                }
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    throw new IllegalStateException("Cannot cleanup [" + repositoryName + "] - a snapshot is currently being deleted");
                }
                ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null && !snapshots.entries().isEmpty()) {
                    throw new IllegalStateException("Cannot cleanup [" + repositoryName + "] - a snapshot is currently running");
                }
                final RepositoryCleanupInProgress newCleanupInProgress =
                    new RepositoryCleanupInProgress(RepositoryCleanupInProgress.startedEntry(repositoryName, repositoryStateId));
                return clusterStateBuilder.putCustom(RepositoryCleanupInProgress.TYPE, newCleanupInProgress).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                after(e, null);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(listener) {
                    @Override
                    protected void doRun() {
                        Repository repository = repositoriesService.repository(repositoryName);
                        assert repository instanceof BlobStoreRepository;
                        ((BlobStoreRepository) repository)
                            .cleanup(repositoryStateId, ActionListener.wrap(result -> after(null, result), e -> after(e, null)));
                    }
                });
            }

            private void after(@Nullable Exception failure, @Nullable RepositoryCleanupResult result) {
                assert failure != null || result != null;
                clusterService.submitStateUpdateTask("Remove repository cleanup task", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        RepositoryCleanupInProgress cleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                        if (cleanupInProgress != null) {
                            boolean changed = false;
                            if (cleanupInProgress.cleanupInProgress() == false) {
                                cleanupInProgress = new RepositoryCleanupInProgress();
                                changed = true;
                            }
                            if (changed) {
                                return ClusterState.builder(currentState).putCustom(
                                    RepositoryCleanupInProgress.TYPE, cleanupInProgress).build();
                            }
                        }
                        return currentState;
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.warn(() -> new ParameterizedMessage("[{}] failed to remove repository cleanup task", repositoryName), e);
                        if (failure != null) {
                            e.addSuppressed(failure);
                        }
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        if (failure == null) {
                            listener.onResponse(result);
                        } else {
                            listener.onFailure(failure);
                        }
                    }
                });
            }
        });
    }
}
