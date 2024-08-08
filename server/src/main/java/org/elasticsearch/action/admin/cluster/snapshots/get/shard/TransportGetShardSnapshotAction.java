/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.IndexSnapshotsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public class TransportGetShardSnapshotAction extends TransportMasterNodeAction<GetShardSnapshotRequest, GetShardSnapshotResponse> {

    public static final ActionType<GetShardSnapshotResponse> TYPE = new ActionType<>("internal:admin/snapshot/get_shard");

    private final IndexSnapshotsService indexSnapshotsService;

    @Inject
    public TransportGetShardSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetShardSnapshotRequest::new,
            indexNameExpressionResolver,
            GetShardSnapshotResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexSnapshotsService = new IndexSnapshotsService(repositoriesService);
    }

    @Override
    protected void masterOperation(
        Task task,
        GetShardSnapshotRequest request,
        ClusterState state,
        ActionListener<GetShardSnapshotResponse> listener
    ) throws Exception {
        final Iterator<String> repositories = getRequestedRepositories(request, state);
        if (repositories.hasNext()) {
            new AsyncOperation(request, repositories, listener).processNextRepository();
        } else {
            listener.onResponse(GetShardSnapshotResponse.EMPTY);
        }
    }

    private class AsyncOperation {

        private static final Comparator<ShardSnapshotInfo> LATEST_SNAPSHOT_COMPARATOR = Comparator
            // prefer latest-starting snapshot
            .comparing(ShardSnapshotInfo::getStartedAt)
            // break ties by snapshot ID
            .thenComparing(snapshotInfo -> snapshotInfo.getSnapshot().getSnapshotId());

        private final GetShardSnapshotRequest request;
        private final Iterator<String> repositories;
        private final ActionListener<GetShardSnapshotResponse> responseListener;

        @Nullable
        ShardSnapshotInfo latestShardSnapshot;
        Map<String, RepositoryException> repositoryFailures = new HashMap<>();

        AsyncOperation(GetShardSnapshotRequest request, Iterator<String> repositories, ActionListener<GetShardSnapshotResponse> listener) {
            this.request = request;
            this.repositories = repositories;
            this.responseListener = listener;
        }

        void processNextRepository() {
            assert repositories.hasNext();
            indexSnapshotsService.getLatestSuccessfulSnapshotForShard(repositories.next(), request.getShardId(), new ActionListener<>() {
                @Override
                public void onResponse(Optional<ShardSnapshotInfo> optionalShardSnapshotInfo) {
                    optionalShardSnapshotInfo.ifPresent(shardSnapshotInfo -> {
                        if (latestShardSnapshot == null || LATEST_SNAPSHOT_COMPARATOR.compare(latestShardSnapshot, shardSnapshotInfo) < 0) {
                            latestShardSnapshot = shardSnapshotInfo;
                        }
                    });
                    next();
                }

                @Override
                public void onFailure(Exception e) {
                    if (request.isSingleRepositoryRequest() == false && e instanceof RepositoryException repositoryException) {
                        repositoryFailures.put(repositoryException.repository(), repositoryException);
                        next();
                    } else {
                        responseListener.onFailure(e);
                    }
                }

                private void next() {
                    if (repositories.hasNext()) {
                        // always forks, so no risk of stack overflow here
                        processNextRepository();
                    } else {
                        responseListener.onResponse(new GetShardSnapshotResponse(latestShardSnapshot, repositoryFailures));
                    }
                }
            });
        }
    }

    private static Iterator<String> getRequestedRepositories(GetShardSnapshotRequest request, ClusterState state) {
        if (request.getFromAllRepositories()) {
            return Iterators.map(RepositoriesMetadata.get(state).repositories().iterator(), RepositoryMetadata::name);
        } else {
            return request.getRepositories().iterator();
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetShardSnapshotRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
