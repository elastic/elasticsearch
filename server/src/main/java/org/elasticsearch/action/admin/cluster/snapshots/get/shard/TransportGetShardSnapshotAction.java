/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexSnapshotsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransportGetShardSnapshotAction extends TransportMasterNodeAction<GetShardSnapshotRequest, GetShardSnapshotResponse> {

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
            GetShardSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetShardSnapshotRequest::new,
            indexNameExpressionResolver,
            GetShardSnapshotResponse::new,
            ThreadPool.Names.SAME
        );
        this.indexSnapshotsService = new IndexSnapshotsService(repositoriesService);
    }

    @Override
    protected void masterOperation(GetShardSnapshotRequest request, ClusterState state, ActionListener<GetShardSnapshotResponse> listener)
        throws Exception {
        final Set<String> repositories = getRequestedRepositories(request, state);
        final ShardId shardId = request.getShardId();

        if (repositories.isEmpty()) {
            listener.onResponse(GetShardSnapshotResponse.EMPTY);
            return;
        }

        GroupedActionListener<Tuple<Optional<ShardSnapshotInfo>, RepositoryException>> groupedActionListener = new GroupedActionListener<>(
            listener.map(this::transformToResponse),
            repositories.size()
        );

        BlockingQueue<String> repositoriesQueue = new LinkedBlockingQueue<>(repositories);
        getShardSnapshots(repositoriesQueue, shardId, new ActionListener<Optional<ShardSnapshotInfo>>() {
            @Override
            public void onResponse(Optional<ShardSnapshotInfo> shardSnapshotInfo) {
                groupedActionListener.onResponse(Tuple.tuple(shardSnapshotInfo, null));
            }

            @Override
            public void onFailure(Exception err) {
                if (request.isSingleRepositoryRequest() == false && err instanceof RepositoryException) {
                    groupedActionListener.onResponse(Tuple.tuple(Optional.empty(), (RepositoryException) err));
                } else {
                    groupedActionListener.onFailure(err);
                }
            }
        });
    }

    private void getShardSnapshots(
        BlockingQueue<String> repositories,
        ShardId shardId,
        ActionListener<Optional<ShardSnapshotInfo>> listener
    ) {
        final String repository = repositories.poll();
        if (repository == null) {
            return;
        }

        indexSnapshotsService.getLatestSuccessfulSnapshotForShard(
            repository,
            shardId,
            ActionListener.runAfter(listener, () -> getShardSnapshots(repositories, shardId, listener))
        );
    }

    private GetShardSnapshotResponse transformToResponse(
        Collection<Tuple<Optional<ShardSnapshotInfo>, RepositoryException>> shardSnapshots
    ) {
        final Map<String, ShardSnapshotInfo> repositoryShardSnapshot = shardSnapshots.stream()
            .map(Tuple::v1)
            .filter(Objects::nonNull)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toMap(ShardSnapshotInfo::getRepository, Function.identity()));

        final Map<String, RepositoryException> failures = shardSnapshots.stream()
            .map(Tuple::v2)
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(RepositoryException::repository, Function.identity()));

        return new GetShardSnapshotResponse(repositoryShardSnapshot, failures);
    }

    private Set<String> getRequestedRepositories(GetShardSnapshotRequest request, ClusterState state) {
        RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        if (request.getFromAllRepositories()) {
            return repositories.repositories().stream().map(RepositoryMetadata::name).collect(Collectors.toSet());
        }

        return request.getRepositories().stream().filter(Objects::nonNull).collect(Collectors.toSet());
    }

    @Override
    protected ClusterBlockException checkBlock(GetShardSnapshotRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
