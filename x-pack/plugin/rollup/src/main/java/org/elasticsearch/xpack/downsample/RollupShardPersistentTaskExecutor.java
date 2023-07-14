/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.rollup.action.RollupShardIndexerStatus;
import org.elasticsearch.xpack.core.rollup.action.RollupShardPersistentTaskState;
import org.elasticsearch.xpack.core.rollup.action.RollupShardTask;
import org.elasticsearch.xpack.rollup.Rollup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class RollupShardPersistentTaskExecutor extends PersistentTasksExecutor<RollupShardTaskParams> {
    private final Client client;
    private final IndicesService indicesService;

    public RollupShardPersistentTaskExecutor(
        final Client client,
        final IndicesService indicesService,
        final String taskName,
        final String executorName
    ) {
        super(taskName, executorName);
        this.client = Objects.requireNonNull(client);
        this.indicesService = Objects.requireNonNull(indicesService);
    }

    @Override
    protected void nodeOperation(final AllocatedPersistentTask task, final RollupShardTaskParams params, final PersistentTaskState state) {
        // NOTE: query the downsampling target index so that we can start the downsampling task from the latest indexed tsid.
        SearchHit[] lastRollupTsidHits;
        try {
            lastRollupTsidHits = client.prepareSearch(params.rollupIndex())
                .addSort(TimeSeriesIdFieldMapper.NAME, SortOrder.DESC)
                .setSize(1)
                .setQuery(new MatchAllQueryBuilder())
                .get()
                .getHits()
                .getHits();
        } catch (SearchPhaseExecutionException e) {
            // NOTE: searching the target downsample index for the latest tsid failed. We just start the task from scratch.
            lastRollupTsidHits = new SearchHit[0];
        }
        final RollupShardPersistentTaskState initialState = lastRollupTsidHits.length == 0
            ? new RollupShardPersistentTaskState(RollupShardIndexerStatus.INITIALIZED, null)
            : new RollupShardPersistentTaskState(
                RollupShardIndexerStatus.STARTED,
                Arrays.stream(lastRollupTsidHits).findFirst().get().field("_tsid").getValue()
            );
        final RollupShardIndexer rollupShardIndexer = new RollupShardIndexer(
            (RollupShardTask) task,
            client,
            getIndexService(indicesService, params),
            params.shardId(),
            params.rollupIndex(),
            params.downsampleConfig(),
            params.metrics(),
            params.labels(),
            initialState
        );
        try {
            rollupShardIndexer.execute();
        } catch (IOException e) {
            task.markAsFailed(e);
            throw new ElasticsearchException("Unable to run downsampling shard task [" + task.getPersistentTaskId() + "]", e);
        }
    }

    private static IndexService getIndexService(final IndicesService indicesService, final RollupShardTaskParams params) {
        return indicesService.indexService(params.shardId().getIndex());
    }

    @Override
    protected AllocatedPersistentTask createTask(
        long id,
        final String type,
        final String action,
        final TaskId parentTaskId,
        final PersistentTasksCustomMetadata.PersistentTask<RollupShardTaskParams> taskInProgress,
        final Map<String, String> headers
    ) {
        final RollupShardTaskParams params = taskInProgress.getParams();
        return new RollupShardTask(
            id,
            type,
            action,
            parentTaskId,
            params.rollupIndex(),
            params.indexStartTimeMillis(),
            params.indexEndTimeMillis(),
            params.downsampleConfig(),
            headers,
            params.shardId(),
            new RollupShardPersistentTaskState(RollupShardIndexerStatus.INITIALIZED, null)
        );
    }

    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        final RollupShardTaskParams params,
        final Collection<DiscoveryNode> candidateNodes,
        final ClusterState clusterState
    ) {
        // NOTE: downsampling works by running a task per each shard of the source index.
        // Here we make sure we assign the task to the actual node holding the shard identified by
        // the downsampling task shard id.
        final ShardId shardId = params.shardId();
        final ShardRouting shardRouting = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        if (shardRouting.started() == false) {
            return NO_NODE_FOUND;
        }

        return candidateNodes.stream()
            .filter(candidateNode -> candidateNode.getId().equals(shardRouting.currentNodeId()))
            .findAny()
            .map(
                node -> new PersistentTasksCustomMetadata.Assignment(
                    node.getId(),
                    "downsampling using node holding shard [" + shardId + "]"
                )
            )
            .orElse(NO_NODE_FOUND);
    }

    @Override
    public String getExecutor() {
        return Rollup.DOWSAMPLE_TASK_THREAD_POOL_NAME;
    }
}
