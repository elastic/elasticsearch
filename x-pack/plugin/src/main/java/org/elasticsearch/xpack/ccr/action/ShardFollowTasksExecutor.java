/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.persistent.AllocatedPersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.Arrays;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    // TODO: turn into cluster wide settings:
    private static final long BATCH_SIZE = 256;
    private static final TimeValue RETRY_TIMEOUT = TimeValue.timeValueMillis(500);

    private final InternalClient client;
    private final ThreadPool threadPool;

    public ShardFollowTasksExecutor(Settings settings, InternalClient client, ThreadPool threadPool) {
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
        final ShardId shardId = params.getLeaderShardId();
        final ShardFollowTask.Status shardFollowStatus = (ShardFollowTask.Status) status;
        final long followGlobalCheckPoint;
        if (shardFollowStatus != null) {
            followGlobalCheckPoint = shardFollowStatus.getProcessedGlobalCheckpoint();
        } else {
            followGlobalCheckPoint = SequenceNumbers.NO_OPS_PERFORMED;
        }
        prepare(task, shardId, followGlobalCheckPoint);
    }

    private void prepare(AllocatedPersistentTask task, ShardId leaderShard, long followGlobalCheckPoint) {
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
                    retry(task, leaderShard, followGlobalCheckPoint);
                } else {
                    assert followGlobalCheckPoint < leaderGlobalCheckPoint : "followGlobalCheckPoint [" + leaderGlobalCheckPoint +
                            "] is not below leaderGlobalCheckPoint [" + followGlobalCheckPoint + "]";
                    ChunksCoordinator coordinator = new ChunksCoordinator(leaderShard, e -> {
                        if (e == null) {
                            ShardFollowTask.Status newStatus = new ShardFollowTask.Status();
                            newStatus.setProcessedGlobalCheckpoint(leaderGlobalCheckPoint);
                            task.updatePersistentStatus(newStatus, ActionListener.wrap(
                                    persistentTask -> prepare(task, leaderShard, leaderGlobalCheckPoint), task::markAsFailed)
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

    private void retry(AllocatedPersistentTask task, ShardId leaderShard, long followGlobalCheckPoint) {
        threadPool.schedule(RETRY_TIMEOUT, Ccr.CCR_THREAD_POOL_NAME, new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                task.markAsFailed(e);
            }

            @Override
            protected void doRun() throws Exception {
                prepare(task, leaderShard, followGlobalCheckPoint);
            }
        });
    }

    class ChunksCoordinator {

        private final ShardId leaderShard;
        private final Consumer<Exception> handler;
        private final Queue<long[]> chunks = new ConcurrentLinkedQueue<>();

        ChunksCoordinator(ShardId leaderShard, Consumer<Exception> handler) {
            this.leaderShard = leaderShard;
            this.handler = handler;
        }

        void createChucks(long from, long to) {
            for (long i = from; i <= to; i += BATCH_SIZE) {
                long v2 = i + BATCH_SIZE < to ? i + BATCH_SIZE : to;
                chunks.add(new long[]{i, v2});
            }
        }

        void processChuck() {
            long[] chuck = chunks.poll();
            if (chuck == null) {
                handler.accept(null);
                return;
            }
            ChunkProcessor processor = new ChunkProcessor(leaderShard, (success, e) -> {
                if (success) {
                    processChuck();
                } else {
                    handler.accept(e);
                }
            });
            processor.start(chuck[0], chuck[1]);
        }

    }

    class ChunkProcessor {

        private final ShardId shardId;
        private final BiConsumer<Boolean, Exception> handler;

        ChunkProcessor(ShardId shardId, BiConsumer<Boolean, Exception> handler) {
            this.shardId = shardId;
            this.handler = handler;
        }

        void start(long from, long to) {
            ShardChangesAction.Request request = new ShardChangesAction.Request(shardId);
            request.setMinSeqNo(from);
            request.setMaxSeqNo(to);
            client.execute(ShardChangesAction.INSTANCE, request, new ActionListener<ShardChangesAction.Response>() {
                @Override
                public void onResponse(ShardChangesAction.Response response) {
                    handleResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    handler.accept(false, e);
                }
            });
        }

        void handleResponse(ShardChangesAction.Response response) {
            threadPool.executor(Ccr.CCR_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    handler.accept(false, e);
                }

                @Override
                protected void doRun() throws Exception {
                    // TODO: Wait for api to be available that accepts raw translog operations and log translog operations for now:
                    for (Translog.Operation operation : response.getOperations()) {
                        if (operation instanceof Translog.Index) {
                            Translog.Index indexOp = (Translog.Index) operation;
                            logger.debug("index op [{}], [{}]", indexOp.seqNo(), indexOp.id());
                        } else if (operation instanceof Translog.Delete) {
                            Engine.Delete deleteOp = (Engine.Delete) operation;
                            logger.debug("delete op [{}], [{}]", deleteOp.seqNo(), deleteOp.id());
                        } else if (operation instanceof Translog.NoOp) {
                            Engine.NoOp noOp = (Engine.NoOp) operation;
                            logger.debug("no op [{}], [{}]", noOp.seqNo(), noOp.reason());
                        }
                    }
                    handler.accept(true, null);
                }
            });
        }


    }
}
