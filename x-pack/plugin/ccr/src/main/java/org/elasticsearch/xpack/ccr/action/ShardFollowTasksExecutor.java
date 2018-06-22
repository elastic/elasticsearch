/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.Ccr;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
        logger.info("{} Creating node task to track leader shard {}, params [{}]",
            params.getFollowShardId(), params.getLeaderShardId(), params);

        final Client leaderClient;
        if (params.getLeaderClusterAlias() != null) {
            leaderClient = wrapClient(client.getRemoteClusterClient(params.getLeaderClusterAlias()), params);
        } else {
            leaderClient = wrapClient(client, params);
        }
        Client followerClient = wrapClient(client, params);
        BiConsumer<TimeValue, Runnable> scheduler =
            (delay, command) -> threadPool.schedule(delay, Ccr.CCR_THREAD_POOL_NAME, command);
        return new ShardFollowNodeTask(id, type, action, getDescription(taskInProgress), parentTaskId, headers,
            leaderClient, followerClient, params, scheduler);
    }

    @Override
    protected void nodeOperation(final AllocatedPersistentTask task, final ShardFollowTask params, final PersistentTaskState state) {
        ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
        logger.info("{} Started to track leader shard {}", params.getFollowShardId(), params.getLeaderShardId());
        fetchGlobalCheckpoint(shardFollowNodeTask.leaderClient, params.getLeaderShardId(), leaderGlobalCheckPoint -> {
            fetchGlobalCheckpoint(shardFollowNodeTask.followerClient, params.getFollowShardId(), followGlobalCheckPoint -> {
                shardFollowNodeTask.start(leaderGlobalCheckPoint, followGlobalCheckPoint);
            }, task::markAsFailed);
        }, task::markAsFailed);
    }

    private void fetchGlobalCheckpoint(Client client, ShardId shardId, LongConsumer handler, Consumer<Exception> errorHandler) {
        client.admin().indices().stats(new IndicesStatsRequest().indices(shardId.getIndexName()), ActionListener.wrap(r -> {
            IndexStats indexStats = r.getIndex(shardId.getIndexName());
            Optional<ShardStats> filteredShardStats = Arrays.stream(indexStats.getShards())
                    .filter(shardStats -> shardStats.getShardRouting().shardId().equals(shardId))
                    .filter(shardStats -> shardStats.getShardRouting().primary())
                    .findAny();

            if (filteredShardStats.isPresent()) {
                final long globalCheckPoint = filteredShardStats.get().getSeqNoStats().getGlobalCheckpoint();
                handler.accept(globalCheckPoint);
            } else {
                errorHandler.accept(new IllegalArgumentException("Cannot find shard stats for shard " + shardId));
            }
        }, errorHandler));
    }

    private static Client wrapClient(Client client, ShardFollowTask shardFollowTask) {
        if (shardFollowTask.getHeaders().isEmpty()) {
            return client;
        } else {
            final ThreadContext threadContext = client.threadPool().getThreadContext();
            Map<String, String> filteredHeaders = shardFollowTask.getHeaders().entrySet().stream()
                .filter(e -> ShardFollowTask.HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return new FilterClient(client) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse>
                void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                    final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
                    try (ThreadContext.StoredContext ignore = stashWithHeaders(threadContext, filteredHeaders)) {
                        super.doExecute(action, request, new ContextPreservingActionListener<>(supplier, listener));
                    }
                }
            };
        }
    }

    private static ThreadContext.StoredContext stashWithHeaders(ThreadContext threadContext, Map<String, String> headers) {
        final ThreadContext.StoredContext storedContext = threadContext.stashContext();
        threadContext.copyHeaders(headers.entrySet());
        return storedContext;
    }

}
