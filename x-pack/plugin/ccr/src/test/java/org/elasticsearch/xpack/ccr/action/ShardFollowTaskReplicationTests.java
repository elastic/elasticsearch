/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public class ShardFollowTaskReplicationTests extends ESIndexLevelReplicationTestCase {

    public void testSimpleCcrReplication() throws Exception {
        try (ReplicationGroup leaderGroup = createGroup(randomInt(2))) {
            leaderGroup.startAll();
            int docCount = leaderGroup.appendDocs(randomInt(64));
            leaderGroup.assertAllEqual(docCount);
            try (ReplicationGroup followerGroup = createFollowGroup(randomInt(2))) {
                followerGroup.startAll();
                ShardFollowNodeTask shardFollowTask = createShardFollowTask(leaderGroup, followerGroup);
                shardFollowTask.start(followerGroup.getPrimary().getGlobalCheckpoint());
                docCount += leaderGroup.appendDocs(randomInt(128));

                leaderGroup.assertAllEqual(docCount);
                int expectedCount = docCount;
                assertBusy(() -> followerGroup.assertAllEqual(expectedCount));
                shardFollowTask.markAsCompleted();
            }
        }
    }

    public void testFailLeaderReplicaShard() throws Exception {
        try (ReplicationGroup leaderGroup = createGroup(1 + randomInt(1))) {
            leaderGroup.startAll();
            try (ReplicationGroup followerGroup = createFollowGroup(randomInt(2))) {
                followerGroup.startAll();
                ShardFollowNodeTask shardFollowTask = createShardFollowTask(leaderGroup, followerGroup);
                shardFollowTask.start(followerGroup.getPrimary().getGlobalCheckpoint());
                int docCount = 256;
                leaderGroup.appendDocs(1);
                Runnable task = () -> {
                    try {
                        leaderGroup.appendDocs(docCount - 1);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                };
                Thread thread = new Thread(task);
                thread.start();

                // Remove and add a new replica
                IndexShard luckyReplica = randomFrom(leaderGroup.getReplicas());
                leaderGroup.removeReplica(luckyReplica);
                luckyReplica.close("stop replica", false);
                luckyReplica.store().close();
                leaderGroup.addReplica();
                leaderGroup.startReplicas(1);
                thread.join();

                leaderGroup.assertAllEqual(docCount);
                assertBusy(() -> {
                    followerGroup.assertAllEqual(docCount);
                });
                shardFollowTask.markAsCompleted();
            }
        }
    }

    @Override
    protected ReplicationGroup createGroup(int replicas, Settings settings) throws IOException {
        if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(settings)) {
            IndexMetaData metaData = buildIndexMetaData(replicas, settings, indexMapping);
            return new ReplicationGroup(metaData) {

                @Override
                protected EngineFactory getEngineFactory(ShardRouting routing) {
                    return new FollowingEngineFactory();
                }
            };
        } else {
            return super.createGroup(replicas, settings);
        }
    }

    private ReplicationGroup createFollowGroup(int replicas) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        return createGroup(replicas, settingsBuilder.build());
    }

    private ShardFollowNodeTask createShardFollowTask(ReplicationGroup leaderGroup, ReplicationGroup followerGroup) {
        ShardFollowTask params = new ShardFollowTask(null, new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0), 1024, 1, Long.MAX_VALUE, 1, 10240,
            TimeValue.timeValueMillis(10), TimeValue.timeValueMillis(10), Collections.emptyMap());

        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> {
            try {
                Thread.sleep(delay.millis());
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            task.run();
        };
        AtomicBoolean stopped = new AtomicBoolean(false);
        return new ShardFollowNodeTask(1L, "type", ShardFollowTask.NAME, "description", null, Collections.emptyMap(), params, scheduler) {
            @Override
            protected void updateMapping(LongConsumer handler) {
                // noop, as mapping updates are not tested
                handler.accept(1L);
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(List<Translog.Operation> operations, LongConsumer handler,
                                                               Consumer<Exception> errorHandler) {
                Runnable task = () -> {
                    try {
                        IndexShard primaryShard = followerGroup.getPrimary();
                        TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> result =
                            TransportBulkShardOperationsAction.shardOperationOnPrimary(primaryShard.shardId(), operations, primaryShard, logger);
                        List<Translog.Operation> replicaOps = result.replicaRequest().getOperations();
                        for (IndexShard replicaShard : followerGroup.getReplicas()) {
                            TransportBulkShardOperationsAction.applyTranslogOperations(replicaOps, replicaShard, Origin.REPLICA);
                        }
                        handler.accept(primaryShard.getGlobalCheckpoint());
                    } catch (Exception e) {
                        errorHandler.accept(e);
                    }
                };
                Thread thread = new Thread(task);
                thread.start();
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                Runnable task = () -> {
                    IndexShard indexShard = getIndexShard(leaderGroup);
                    long globalCheckpoint = indexShard.getGlobalCheckpoint();
                    try {
                        Translog.Operation[] ops = ShardChangesAction.getOperations(indexShard, globalCheckpoint, from, maxOperationCount,
                            params.getMaxBatchSizeInBytes());
                        // Hard code index metadata version, this is ok, as mapping updates are not tested here.
                        handler.accept(new ShardChangesAction.Response(1L, globalCheckpoint, ops));
                    } catch (Exception e) {
                        errorHandler.accept(e);
                    }
                };
                Thread thread = new Thread(task);
                thread.start();
            }

            @Override
            protected boolean isStopped() {
                return stopped.get();
            }

            @Override
            public void markAsCompleted() {
                stopped.set(true);
            }

            @Override
            public void markAsFailed(Exception e) {
                stopped.set(true);
            }

            private IndexShard getIndexShard(ReplicationGroup replicationGroup) {
                List<IndexShard> indexShards = new ArrayList<>(replicationGroup.getReplicas());
                indexShards.add(replicationGroup.getPrimary());
                return randomFrom(indexShards);
            }
        };
    }

}
