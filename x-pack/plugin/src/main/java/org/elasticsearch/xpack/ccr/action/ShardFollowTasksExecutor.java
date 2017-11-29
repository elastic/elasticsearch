/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.persistent.AllocatedPersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksExecutor;

import java.util.Arrays;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    // TODO: turn into cluster wide settings:
    private static final long BATCH_SIZE = 256;
    private static final TimeValue RETRY_TIMEOUT = TimeValue.timeValueMillis(500);

    private final Client client;
    private final ThreadPool threadPool;

    public ShardFollowTasksExecutor(Settings settings, Client client, ThreadPool threadPool) {
        super(settings, ShardFollowTask.NAME, Ccr.CCR_THREAD_POOL_NAME);
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    public void validate(ShardFollowTask params, ClusterState clusterState) {
        IndexRoutingTable routingTable = clusterState.getRoutingTable().index(params.getLeaderShardId().getIndex());
        if (routingTable.shard(params.getLeaderShardId().id()).primaryShard().started() == false) {
            throw new IllegalArgumentException("Not all copies of leader shard are started");
        }

        routingTable = clusterState.getRoutingTable().index(params.getFollowShardId().getIndex());
        if (routingTable.shard(params.getFollowShardId().id()).primaryShard().started() == false) {
            throw new IllegalArgumentException("Not all copies of follow shard are started");
        }
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, ShardFollowTask params, Task.Status status) {
        final ShardFollowTask.Status shardFollowStatus = (ShardFollowTask.Status) status;
        final long followGlobalCheckPoint;
        if (shardFollowStatus != null) {
            followGlobalCheckPoint = shardFollowStatus.getProcessedGlobalCheckpoint();
        } else {
            followGlobalCheckPoint = SequenceNumbers.NO_OPS_PERFORMED;
        }
        prepare(task, params.getLeaderShardId(), params.getFollowShardId(), followGlobalCheckPoint);
    }

    void prepare(AllocatedPersistentTask task, ShardId leaderShard, ShardId followerShard, long followGlobalCheckPoint) {
        if (task.getState() != AllocatedPersistentTask.State.STARTED) {
            // TODO: need better cancellation control
            return;
        }

        client.admin().indices().stats(new IndicesStatsRequest().indices(leaderShard.getIndexName()), ActionListener.wrap(r -> {
            Optional<ShardStats> leaderShardStats = Arrays.stream(r.getIndex(leaderShard.getIndexName()).getShards())
                    .filter(shardStats -> shardStats.getShardRouting().shardId().equals(leaderShard))
                    .filter(shardStats -> shardStats.getShardRouting().primary())
                    .findAny();

            if (leaderShardStats.isPresent()) {
                final long leaderGlobalCheckPoint = leaderShardStats.get().getSeqNoStats().getGlobalCheckpoint();
                // TODO: check if both indices have the same history uuid
                if (leaderGlobalCheckPoint == followGlobalCheckPoint) {
                    retry(task, leaderShard, followerShard, followGlobalCheckPoint);
                } else {
                    assert followGlobalCheckPoint < leaderGlobalCheckPoint : "followGlobalCheckPoint [" + leaderGlobalCheckPoint +
                            "] is not below leaderGlobalCheckPoint [" + followGlobalCheckPoint + "]";
                    ChunksCoordinator coordinator = new ChunksCoordinator(leaderShard, followerShard, e -> {
                        if (e == null) {
                            ShardFollowTask.Status newStatus = new ShardFollowTask.Status();
                            newStatus.setProcessedGlobalCheckpoint(leaderGlobalCheckPoint);
                            task.updatePersistentStatus(newStatus, ActionListener.wrap(
                                    persistentTask -> prepare(task, leaderShard, followerShard, leaderGlobalCheckPoint), task::markAsFailed)
                            );
                        } else {
                            task.markAsFailed(e);
                        }
                    });
                    coordinator.createChucks(followGlobalCheckPoint, leaderGlobalCheckPoint);
                    coordinator.processChuck();
                }
            } else {
                task.markAsFailed(new IllegalArgumentException("Cannot find shard stats for primary leader shard"));
            }
        }, task::markAsFailed));
    }

    private void retry(AllocatedPersistentTask task, ShardId leaderShard, ShardId followerShard, long followGlobalCheckPoint) {
        threadPool.schedule(RETRY_TIMEOUT, Ccr.CCR_THREAD_POOL_NAME, new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                task.markAsFailed(e);
            }

            @Override
            protected void doRun() throws Exception {
                prepare(task, leaderShard, followerShard, followGlobalCheckPoint);
            }
        });
    }

    class ChunksCoordinator {

        private final ShardId leaderShard;
        private final ShardId followerShard;
        private final Consumer<Exception> handler;
        private final Queue<long[]> chunks = new ConcurrentLinkedQueue<>();

        ChunksCoordinator(ShardId leaderShard, ShardId followerShard, Consumer<Exception> handler) {
            this.leaderShard = leaderShard;
            this.followerShard = followerShard;
            this.handler = handler;
        }

        void createChucks(long from, long to) {
            for (long i = from; i <= to; i += BATCH_SIZE) {
                long v2 = i + BATCH_SIZE < to ? i + BATCH_SIZE : to;
                chunks.add(new long[]{i, v2});
            }
        }

        void processChuck() {
            long[] chunk = chunks.poll();
            if (chunk == null) {
                handler.accept(null);
                return;
            }
            ChunkProcessor processor = new ChunkProcessor(leaderShard, followerShard, e -> {
                if (e == null) {
                    processChuck();
                } else {
                    handler.accept(e);
                }
            });
            processor.start(chunk[0], chunk[1]);
        }

    }

    class ChunkProcessor {

        private final ShardId leaderShard;
        private final ShardId followerShard;
        private final Consumer<Exception> handler;

        ChunkProcessor(ShardId leaderShard, ShardId followerShard, Consumer<Exception> handler) {
            this.leaderShard = leaderShard;
            this.followerShard = followerShard;
            this.handler = handler;
        }

        void start(long from, long to) {
            ShardChangesAction.Request request = new ShardChangesAction.Request(leaderShard);
            request.setMinSeqNo(from);
            request.setMaxSeqNo(to);
            client.execute(ShardChangesAction.INSTANCE, request, new ActionListener<ShardChangesAction.Response>() {
                @Override
                public void onResponse(ShardChangesAction.Response response) {
                    handleResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    assert e != null;
                    handler.accept(e);
                }
            });
        }

        void handleResponse(ShardChangesAction.Response response) {
            threadPool.executor(Ccr.CCR_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    assert e != null;
                    handler.accept(e);
                }

                @Override
                protected void doRun() throws Exception {
                    final BulkShardOperationsRequest request = new BulkShardOperationsRequest(followerShard, response.getOperations());
                    client.execute(BulkShardOperationsAction.INSTANCE, request, new ActionListener<BulkShardOperationsResponse>() {
                        @Override
                        public void onResponse(final BulkShardOperationsResponse bulkShardOperationsResponse) {
                            handler.accept(null);
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            assert e != null;
                            handler.accept(e);
                        }
                    });
                }
            });
        }

    }

}
