/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    static final long DEFAULT_BATCH_SIZE = 1024;
    static final int PROCESSOR_RETRY_LIMIT = 16;
    static final int DEFAULT_CONCURRENT_PROCESSORS = 1;
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
        logger.info("Starting shard following [{}]", params);
        prepare(task, params, followGlobalCheckPoint);
    }

    void prepare(AllocatedPersistentTask task, ShardFollowTask params, long followGlobalCheckPoint) {
        if (task.getState() != AllocatedPersistentTask.State.STARTED) {
            // TODO: need better cancellation control
            return;
        }

        final ShardId leaderShard = params.getLeaderShardId();
        final ShardId followerShard = params.getFollowShardId();
        client.admin().indices().stats(new IndicesStatsRequest().indices(leaderShard.getIndexName()), ActionListener.wrap(r -> {
            Optional<ShardStats> leaderShardStats = Arrays.stream(r.getIndex(leaderShard.getIndexName()).getShards())
                    .filter(shardStats -> shardStats.getShardRouting().shardId().equals(leaderShard))
                    .filter(shardStats -> shardStats.getShardRouting().primary())
                    .findAny();

            if (leaderShardStats.isPresent()) {
                final long leaderGlobalCheckPoint = leaderShardStats.get().getSeqNoStats().getGlobalCheckpoint();
                // TODO: check if both indices have the same history uuid
                if (leaderGlobalCheckPoint == followGlobalCheckPoint) {
                    retry(task, params, followGlobalCheckPoint);
                } else {
                    assert followGlobalCheckPoint < leaderGlobalCheckPoint : "followGlobalCheckPoint [" + followGlobalCheckPoint +
                            "] is not below leaderGlobalCheckPoint [" + leaderGlobalCheckPoint + "]";
                    Executor ccrExecutor = threadPool.executor(Ccr.CCR_THREAD_POOL_NAME);
                    Consumer<Exception> handler = e -> {
                        if (e == null) {
                            ShardFollowTask.Status newStatus = new ShardFollowTask.Status();
                            newStatus.setProcessedGlobalCheckpoint(leaderGlobalCheckPoint);
                            task.updatePersistentStatus(newStatus, ActionListener.wrap(
                                    persistentTask -> logger.debug("[{}] Successfully updated global checkpoint to {}",
                                            leaderShard, leaderGlobalCheckPoint),
                                    updateStatusException -> {
                                        logger.error(new ParameterizedMessage("[{}] Failed to update global checkpoint to {}",
                                                leaderShard, leaderGlobalCheckPoint), updateStatusException);
                                        task.markAsFailed(updateStatusException);
                                    }
                            ));
                            prepare(task, params, leaderGlobalCheckPoint);
                        } else {
                            task.markAsFailed(e);
                        }
                    };
                    ChunksCoordinator coordinator = new ChunksCoordinator(client, ccrExecutor, params.getMaxChunkSize(),
                            params.getNumConcurrentChunks(), leaderShard, followerShard, handler);
                    coordinator.createChucks(followGlobalCheckPoint, leaderGlobalCheckPoint);
                    coordinator.start();
                }
            } else {
                task.markAsFailed(new IllegalArgumentException("Cannot find shard stats for primary leader shard"));
            }
        }, task::markAsFailed));
    }

    private void retry(AllocatedPersistentTask task, ShardFollowTask params, long followGlobalCheckPoint) {
        threadPool.schedule(RETRY_TIMEOUT, Ccr.CCR_THREAD_POOL_NAME, new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                task.markAsFailed(e);
            }

            @Override
            protected void doRun() throws Exception {
                prepare(task, params, followGlobalCheckPoint);
            }
        });
    }

    static class ChunksCoordinator {

        private static final Logger LOGGER = Loggers.getLogger(ChunksCoordinator.class);

        private final Client client;
        private final Executor ccrExecutor;

        private final long batchSize;
        private final int concurrentProcessors;
        private final ShardId leaderShard;
        private final ShardId followerShard;
        private final Consumer<Exception> handler;

        private final CountDown countDown;
        private final Queue<long[]> chunks = new ConcurrentLinkedQueue<>();
        private final AtomicReference<Exception> failureHolder = new AtomicReference<>();

        ChunksCoordinator(Client client, Executor ccrExecutor, long batchSize, int concurrentProcessors,
                          ShardId leaderShard, ShardId followerShard, Consumer<Exception> handler) {
            this.client = client;
            this.ccrExecutor = ccrExecutor;
            this.batchSize = batchSize;
            this.concurrentProcessors = concurrentProcessors;
            this.leaderShard = leaderShard;
            this.followerShard = followerShard;
            this.handler = handler;
            this.countDown = new CountDown(concurrentProcessors);
        }

        void createChucks(long from, long to) {
            LOGGER.debug("{} Creating chunks for operation range [{}] to [{}]", leaderShard, from, to);
            for (long i = from; i < to; i += batchSize) {
                long v2 = i + batchSize < to ? i + batchSize : to;
                chunks.add(new long[]{i == from ? i : i + 1, v2});
            }
        }

        void start() {
            LOGGER.debug("{} Start coordination of [{}] chunks with [{}] concurrent processors",
                    leaderShard, chunks.size(), concurrentProcessors);
            for (int i = 0; i < concurrentProcessors; i++) {
                ccrExecutor.execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        assert e != null;
                        LOGGER.error(() -> new ParameterizedMessage("{} Failure starting processor", leaderShard), e);
                        postProcessChuck(e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        processNextChunk();
                    }
                });
            }
        }

        void processNextChunk() {
            long[] chunk = chunks.poll();
            if (chunk == null) {
                postProcessChuck(null);
                return;
            }
            LOGGER.debug("{} Processing chunk [{}/{}]", leaderShard, chunk[0], chunk[1]);
            ChunkProcessor processor = new ChunkProcessor(client, ccrExecutor, leaderShard, followerShard, e -> {
                if (e == null) {
                    LOGGER.debug("{} Successfully processed chunk [{}/{}]", leaderShard, chunk[0], chunk[1]);
                    processNextChunk();
                } else {
                    LOGGER.error(() -> new ParameterizedMessage("{} Failure processing chunk [{}/{}]",
                            leaderShard, chunk[0], chunk[1]), e);
                    postProcessChuck(e);
                }
            });
            processor.start(chunk[0], chunk[1]);
        }

        void postProcessChuck(Exception e) {
            if (failureHolder.compareAndSet(null, e) == false) {
                Exception firstFailure = failureHolder.get();
                firstFailure.addSuppressed(e);
            }
            if (countDown.countDown()) {
                handler.accept(failureHolder.get());
            }
        }

        Queue<long[]> getChunks() {
            return chunks;
        }

    }

    static class ChunkProcessor {

        private final Client client;
        private final Executor ccrExecutor;

        private final ShardId leaderShard;
        private final ShardId followerShard;
        private final Consumer<Exception> handler;
        final AtomicInteger retryCounter = new AtomicInteger(0);

        ChunkProcessor(Client client, Executor ccrExecutor, ShardId leaderShard, ShardId followerShard, Consumer<Exception> handler) {
            this.client = client;
            this.ccrExecutor = ccrExecutor;
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
                    if (shouldRetry(e)) {
                        if (retryCounter.incrementAndGet() <= PROCESSOR_RETRY_LIMIT) {
                            start(from, to);
                        } else {
                            handler.accept(new ElasticsearchException("retrying failed [" + retryCounter.get() +
                                    "] times, aborting...", e));
                        }
                    } else {
                        handler.accept(e);
                    }
                }
            });
        }

        void handleResponse(ShardChangesAction.Response response) {
            ccrExecutor.execute(new AbstractRunnable() {
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
                            // No retry mechanism here, because if a failure is being redirected to this place it is considered
                            // non recoverable.
                            assert e != null;
                            handler.accept(e);
                        }
                    });
                }
            });
        }

        boolean shouldRetry(Exception e) {
            // TODO: What other exceptions should be retried?
            return NetworkExceptionHelper.isConnectException(e) ||
                    NetworkExceptionHelper.isCloseConnectionException(e);
        }

    }

}
