/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Tells index nodes the current generation of the shard-level metadata that a repository holds of their shards. The master has the
 * {@link RepositoryData} of the repository, which other nodes must not read from the repository, so index nodes ask the master instead,
 * the same way as peer recovery asks it for the latest snapshot of a shard (see
 * {@link org.elasticsearch.action.admin.cluster.snapshots.get.shard.TransportGetShardSnapshotAction}).
 */
public class TransportGetShardGenerationsAction extends TransportMasterNodeAction<GetShardGenerationsRequest, GetShardGenerationsResponse> {

    public static final String NAME = "internal:admin/stateless/snapshot/get_shard_generations";
    public static final ActionType<GetShardGenerationsResponse> TYPE = new ActionType<>(NAME);

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportGetShardGenerationsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        RepositoriesService repositoriesService
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetShardGenerationsRequest::new,
            GetShardGenerationsResponse::new,
            // loading the repository data may read the repository, which the transport threads must not do
            threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetShardGenerationsRequest request,
        ClusterState state,
        ActionListener<GetShardGenerationsResponse> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT_META);
        final var projectRepo = request.getProjectRepo();
        // throws if the repository does not exist, which is passed on to the node that asked
        final var repository = repositoriesService.repository(projectRepo.projectId(), projectRepo.name());
        repository.getRepositoryData(
            threadPool.executor(ThreadPool.Names.SNAPSHOT_META),
            listener.map(repositoryData -> getResponse(repositoryData, request.getShardIds()))
        );
    }

    static GetShardGenerationsResponse getResponse(RepositoryData repositoryData, Collection<ShardId> shardIds) {
        final Map<ShardId, RepositoryShardGeneration> shardGenerations = new HashMap<>();
        for (ShardId shardId : shardIds) {
            shardGenerations.put(shardId, getShardGeneration(repositoryData, shardId));
        }
        return new GetShardGenerationsResponse(repositoryData.getGenId(), shardGenerations);
    }

    /**
     * Known limitation: the repository is asked for the index by its name, as a shard id of a running node says which index it is of by
     * name and uuid but the repository tells indices apart by an id of its own. If an index was deleted and created again with the same
     * name, until its first snapshot completes this finds the shard generation of the old index, so what the repository is said to hold
     * of the new shards is what it holds of the old ones. Files are compared by name and length, so the backlog can be understated by the
     * files that happen to be the same in both, until the first snapshot of the new index has completed.
     *
     * @return the current generation of the shard-level metadata of the shard, or {@code null} if the repository has none: it does not
     *         know the index or the shard, or the shard is new (its first snapshot has not completed) or deleted
     */
    private static RepositoryShardGeneration getShardGeneration(RepositoryData repositoryData, ShardId shardId) {
        final IndexId indexId = repositoryData.getIndices().get(shardId.getIndexName());
        if (indexId == null) {
            return null;
        }
        final ShardGeneration generation = repositoryData.shardGenerations().getShardGen(indexId, shardId.id());
        if (generation == null
            || generation.equals(ShardGenerations.NEW_SHARD_GEN)
            || generation.equals(ShardGenerations.DELETED_SHARD_GEN)) {
            return null;
        }
        return new RepositoryShardGeneration(indexId, generation);
    }

    @Override
    protected ClusterBlockException checkBlock(GetShardGenerationsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
