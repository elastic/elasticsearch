/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
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
import static org.elasticsearch.xpack.ccr.action.TransportResumeFollowAction.extractLeaderShardHistoryUUIDs;

public class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public ShardFollowTasksExecutor(Settings settings, Client client, ThreadPool threadPool, ClusterService clusterService) {
        super(settings, ShardFollowTask.NAME, Ccr.CCR_THREAD_POOL_NAME);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
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

        final String recordedLeaderShardHistoryUUID = getLeaderShardHistoryUUID(params);
        return new ShardFollowNodeTask(id, type, action, getDescription(taskInProgress), parentTaskId, headers, params,
            scheduler, System::nanoTime) {

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
                final String followerHistoryUUID,
                final List<Translog.Operation> operations,
                final long maxSeqNoOfUpdatesOrDeletes,
                final Consumer<BulkShardOperationsResponse> handler,
                final Consumer<Exception> errorHandler) {

                final BulkShardOperationsRequest request = new BulkShardOperationsRequest(params.getFollowShardId(),
                    followerHistoryUUID, operations, maxSeqNoOfUpdatesOrDeletes);
                followerClient.execute(BulkShardOperationsAction.INSTANCE, request,
                    ActionListener.wrap(response -> handler.accept(response), errorHandler));
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                ShardChangesAction.Request request =
                    new ShardChangesAction.Request(params.getLeaderShardId(), recordedLeaderShardHistoryUUID);
                request.setFromSeqNo(from);
                request.setMaxOperationCount(maxOperationCount);
                request.setMaxBatchSize(params.getMaxBatchSize());
                request.setPollTimeout(params.getPollTimeout());
                leaderClient.execute(ShardChangesAction.INSTANCE, request, ActionListener.wrap(handler::accept, errorHandler));
            }
        };
    }

    private String getLeaderShardHistoryUUID(ShardFollowTask params) {
        IndexMetaData followIndexMetaData = clusterService.state().metaData().index(params.getFollowShardId().getIndex());
        Map<String, String> ccrIndexMetadata = followIndexMetaData.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
        String[] recordedLeaderShardHistoryUUIDs = extractLeaderShardHistoryUUIDs(ccrIndexMetadata);
        return recordedLeaderShardHistoryUUIDs[params.getLeaderShardId().id()];
    }

    interface FollowerStatsInfoHandler {
        void accept(String followerHistoryUUID, long globalCheckpoint, long maxSeqNo);
    }

    @Override
    protected void nodeOperation(final AllocatedPersistentTask task, final ShardFollowTask params, final PersistentTaskState state) {
        Client followerClient = wrapClient(client, params.getHeaders());
        ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
        logger.info("{} Starting to track leader shard {}", params.getFollowShardId(), params.getLeaderShardId());

        FollowerStatsInfoHandler handler = (followerHistoryUUID, followerGCP, maxSeqNo) -> {
            shardFollowNodeTask.start(followerHistoryUUID, followerGCP, maxSeqNo, followerGCP, maxSeqNo);
        };
        Consumer<Exception> errorHandler = e -> {
            if (shardFollowNodeTask.isStopped()) {
                return;
            }

            if (ShardFollowNodeTask.shouldRetry(e)) {
                logger.debug(new ParameterizedMessage("failed to fetch follow shard global {} checkpoint and max sequence number",
                    shardFollowNodeTask), e);
                threadPool.schedule(params.getMaxRetryDelay(), Ccr.CCR_THREAD_POOL_NAME, () -> nodeOperation(task, params, state));
            } else {
                shardFollowNodeTask.markAsFailed(e);
            }
        };

        fetchFollowerShardInfo(followerClient, params.getFollowShardId(), handler, errorHandler);
    }

    private void fetchFollowerShardInfo(
            final Client client,
            final ShardId shardId,
            final FollowerStatsInfoHandler handler,
            final Consumer<Exception> errorHandler) {
        client.admin().indices().stats(new IndicesStatsRequest().indices(shardId.getIndexName()), ActionListener.wrap(r -> {
            IndexStats indexStats = r.getIndex(shardId.getIndexName());
            if (indexStats == null) {
                errorHandler.accept(new IndexNotFoundException(shardId.getIndex()));
                return;
            }

            Optional<ShardStats> filteredShardStats = Arrays.stream(indexStats.getShards())
                    .filter(shardStats -> shardStats.getShardRouting().shardId().equals(shardId))
                    .filter(shardStats -> shardStats.getShardRouting().primary())
                    .findAny();
            if (filteredShardStats.isPresent()) {
                final ShardStats shardStats = filteredShardStats.get();
                final CommitStats commitStats = shardStats.getCommitStats();
                final String historyUUID = commitStats.getUserData().get(Engine.HISTORY_UUID_KEY);

                final SeqNoStats seqNoStats = shardStats.getSeqNoStats();
                final long globalCheckpoint = seqNoStats.getGlobalCheckpoint();
                final long maxSeqNo = seqNoStats.getMaxSeqNo();
                handler.accept(historyUUID, globalCheckpoint, maxSeqNo);
            } else {
                errorHandler.accept(new ShardNotFoundException(shardId));
            }
        }, errorHandler));
    }

}
