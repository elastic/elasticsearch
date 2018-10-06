/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.elasticsearch.xpack.ccr.CcrLicenseChecker.wrapClient;

public class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    private final Client client;
    private final ThreadPool threadPool;

    public ShardFollowTasksExecutor(Settings settings, Client client, ThreadPool threadPool) {
        super(settings, ShardFollowTask.NAME, Ccr.CCR_THREAD_POOL_NAME);
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    public void validate(ShardFollowTask params, ClusterState clusterState) {
        if (params.getLeaderClusterAlias() == null) {
            // We can only validate IndexRoutingTable in local cluster,
            // for remote cluster we would need to make a remote call and we cannot do this here.
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(params.getLeaderShardId().getIndex());
            if (routingTable.shard(params.getLeaderShardId().id()).primaryShard().started() == false) {
                throw new IllegalArgumentException("Not all copies of leader shard are started");
            }
        }

        IndexRoutingTable routingTable = clusterState.getRoutingTable().index(params.getFollowShardId().getIndex());
        if (routingTable.shard(params.getFollowShardId().id()).primaryShard().started() == false) {
            throw new IllegalArgumentException("Not all copies of follow shard are started");
        }
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<ShardFollowTask> taskInProgress,
                                                 Map<String, String> headers) {
        ShardFollowTask params = taskInProgress.getParams();
        final Client leaderClient;
        if (params.getLeaderClusterAlias() != null) {
            leaderClient = wrapClient(client.getRemoteClusterClient(params.getLeaderClusterAlias()), params.getHeaders());
        } else {
            leaderClient = wrapClient(client, params.getHeaders());
        }
        Client followerClient = wrapClient(client, params.getHeaders());
        BiConsumer<TimeValue, Runnable> scheduler = (delay, command) -> {
            try {
                threadPool.schedule(delay, Ccr.CCR_THREAD_POOL_NAME, command);
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug("couldn't schedule command, executor is shutting down", e);
                } else {
                    throw e;
                }
            }
        };
        return new ShardFollowNodeTask(
                id, type, action, getDescription(taskInProgress), parentTaskId, headers, params, scheduler, System::nanoTime) {

            @Override
            protected void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler) {
                Index leaderIndex = params.getLeaderShardId().getIndex();
                Index followIndex = params.getFollowShardId().getIndex();

                ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
                clusterStateRequest.clear();
                clusterStateRequest.metaData(true);
                clusterStateRequest.indices(leaderIndex.getName());

                leaderClient.admin().cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
                    IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().getIndexSafe(leaderIndex);
                    if (indexMetaData.getMappings().isEmpty()) {
                        assert indexMetaData.getMappingVersion() == 1;
                        handler.accept(indexMetaData.getMappingVersion());
                        return;
                    }

                    assert indexMetaData.getMappings().size() == 1 : "expected exactly one mapping, but got [" +
                        indexMetaData.getMappings().size() + "]";
                    MappingMetaData mappingMetaData = indexMetaData.getMappings().iterator().next().value;

                    PutMappingRequest putMappingRequest = new PutMappingRequest(followIndex.getName());
                    putMappingRequest.type(mappingMetaData.type());
                    putMappingRequest.source(mappingMetaData.source().string(), XContentType.JSON);
                    followerClient.admin().indices().putMapping(putMappingRequest, ActionListener.wrap(
                        putMappingResponse -> handler.accept(indexMetaData.getMappingVersion()),
                        errorHandler));
                }, errorHandler));
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(
                    final List<Translog.Operation> operations,
                    final long maxSeqNoOfUpdatesOrDeletes,
                    final Consumer<BulkShardOperationsResponse> handler,
                    final Consumer<Exception> errorHandler) {
                final BulkShardOperationsRequest request = new BulkShardOperationsRequest(
                    params.getFollowShardId(), operations, maxSeqNoOfUpdatesOrDeletes);
                followerClient.execute(BulkShardOperationsAction.INSTANCE, request,
                    ActionListener.wrap(response -> handler.accept(response), errorHandler));
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                ShardChangesAction.Request request =
                    new ShardChangesAction.Request(params.getLeaderShardId(), params.getRecordedLeaderIndexHistoryUUID());
                request.setFromSeqNo(from);
                request.setMaxOperationCount(maxOperationCount);
                request.setMaxOperationSizeInBytes(params.getMaxBatchSizeInBytes());
                request.setPollTimeout(params.getPollTimeout());
                leaderClient.execute(ShardChangesAction.INSTANCE, request, ActionListener.wrap(handler::accept, errorHandler));
            }
        };
    }

    interface BiLongConsumer {
        void accept(long x, long y);
    }

    @Override
    protected void nodeOperation(final AllocatedPersistentTask task, final ShardFollowTask params, final PersistentTaskState state) {
        Client followerClient = wrapClient(client, params.getHeaders());
        ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
        logger.info("{} Started to track leader shard {}", params.getFollowShardId(), params.getLeaderShardId());
        fetchGlobalCheckpoint(followerClient, params.getFollowShardId(),
                (followerGCP, maxSeqNo) -> shardFollowNodeTask.start(followerGCP, maxSeqNo, followerGCP, maxSeqNo), task::markAsFailed);
    }

    private void fetchGlobalCheckpoint(
            final Client client,
            final ShardId shardId,
            final BiLongConsumer handler,
            final Consumer<Exception> errorHandler) {
        client.admin().indices().stats(new IndicesStatsRequest().indices(shardId.getIndexName()), ActionListener.wrap(r -> {
            IndexStats indexStats = r.getIndex(shardId.getIndexName());
            Optional<ShardStats> filteredShardStats = Arrays.stream(indexStats.getShards())
                    .filter(shardStats -> shardStats.getShardRouting().shardId().equals(shardId))
                    .filter(shardStats -> shardStats.getShardRouting().primary())
                    .findAny();
            if (filteredShardStats.isPresent()) {
                final SeqNoStats seqNoStats = filteredShardStats.get().getSeqNoStats();
                final long globalCheckpoint = seqNoStats.getGlobalCheckpoint();
                final long maxSeqNo = seqNoStats.getMaxSeqNo();
                handler.accept(globalCheckpoint, maxSeqNo);
            } else {
                errorHandler.accept(new IllegalArgumentException("Cannot find shard stats for shard " + shardId));
            }
        }, errorHandler));
    }

}
