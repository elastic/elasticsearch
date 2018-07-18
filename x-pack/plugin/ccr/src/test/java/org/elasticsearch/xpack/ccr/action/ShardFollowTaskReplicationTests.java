/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
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
        try (ReplicationGroup leaderGroup = createGroup(randomInt(2));
             ReplicationGroup followerGroup = createFollowGroup(randomInt(2))) {
            leaderGroup.startAll();
            int docCount = leaderGroup.appendDocs(randomInt(64));
            leaderGroup.assertAllEqual(docCount);
            followerGroup.startAll();
            ShardFollowNodeTask shardFollowTask = createShardFollowTask(leaderGroup, followerGroup);
            shardFollowTask.start(followerGroup.getPrimary().getGlobalCheckpoint());
            docCount += leaderGroup.appendDocs(randomInt(128));
            leaderGroup.syncGlobalCheckpoint();

            leaderGroup.assertAllEqual(docCount);
            int expectedCount = docCount;
            assertBusy(() -> followerGroup.assertAllEqual(expectedCount));
            shardFollowTask.markAsCompleted();
        }
    }

    public void testFailLeaderReplicaShard() throws Exception {
        try (ReplicationGroup leaderGroup = createGroup(1 + randomInt(1));
             ReplicationGroup followerGroup = createFollowGroup(randomInt(2))) {
            leaderGroup.startAll();
            followerGroup.startAll();
            ShardFollowNodeTask shardFollowTask = createShardFollowTask(leaderGroup, followerGroup);
            shardFollowTask.start(followerGroup.getPrimary().getGlobalCheckpoint());
            int docCount = 256;
            leaderGroup.appendDocs(1);
            Runnable task = () -> {
                try {
                    leaderGroup.appendDocs(docCount - 1);
                    leaderGroup.syncGlobalCheckpoint();
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
            assertBusy(() -> followerGroup.assertAllEqual(docCount));
            shardFollowTask.markAsCompleted();
        }
    }

    @Override
    protected ReplicationGroup createGroup(int replicas, Settings settings) throws IOException {
        Settings newSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, replicas)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 10000)
            .put(settings)
            .build();
        if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(newSettings)) {
            IndexMetaData metaData = buildIndexMetaData(replicas, newSettings, indexMapping);
            return new ReplicationGroup(metaData) {

                @Override
                protected EngineFactory getEngineFactory(ShardRouting routing) {
                    return new FollowingEngineFactory();
                }
            };
        } else {
            return super.createGroup(replicas, newSettings);
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

        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> threadPool.schedule(delay, ThreadPool.Names.GENERIC, task);
        AtomicBoolean stopped = new AtomicBoolean(false);
        return new ShardFollowNodeTask(1L, "type", ShardFollowTask.NAME, "description", null, Collections.emptyMap(), params, scheduler) {
            @Override
            protected void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler) {
                // noop, as mapping updates are not tested
                handler.accept(1L);
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(List<Translog.Operation> operations, LongConsumer handler,
                                                               Consumer<Exception> errorHandler) {
                Runnable task = () -> {
                    BulkShardOperationsRequest request = new BulkShardOperationsRequest(params.getFollowShardId(), operations);
                    ActionListener<BulkShardOperationsResponse> listener =
                        ActionListener.wrap(r -> handler.accept(r.getGlobalCheckpoint()), errorHandler);
                    new CCRAction(request, listener, followerGroup).execute();
                };
                threadPool.executor(ThreadPool.Names.GENERIC).execute(task);
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                Runnable task = () -> {
                    List<IndexShard> indexShards = new ArrayList<>(leaderGroup.getReplicas());
                    indexShards.add(leaderGroup.getPrimary());
                    Collections.shuffle(indexShards, random());

                    Exception exception = null;
                    for (IndexShard indexShard : indexShards) {
                        long globalCheckpoint = indexShard.getGlobalCheckpoint();
                        try {
                            Translog.Operation[] ops = ShardChangesAction.getOperations(indexShard, globalCheckpoint, from,
                                maxOperationCount, params.getMaxBatchSizeInBytes());
                            // Hard code index metadata version, this is ok, as mapping updates are not tested here.
                            handler.accept(new ShardChangesAction.Response(1L, globalCheckpoint, ops));
                            return;
                        } catch (Exception e) {
                            exception = e;
                        }
                    }
                    assert exception != null;
                    errorHandler.accept(exception);
                };
                threadPool.executor(ThreadPool.Names.GENERIC).execute(task);
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

        };
    }

    class CCRAction extends ReplicationAction<BulkShardOperationsRequest, BulkShardOperationsRequest, BulkShardOperationsResponse> {

        CCRAction(BulkShardOperationsRequest request, ActionListener<BulkShardOperationsResponse> listener, ReplicationGroup group) {
            super(request, listener, group, "ccr");
        }

        @Override
        protected PrimaryResult performOnPrimary(IndexShard primary, BulkShardOperationsRequest request) throws Exception {
            TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> result =
                TransportBulkShardOperationsAction.shardOperationOnPrimary(primary.shardId(), request.getOperations(),
                    primary, logger);
            return new PrimaryResult(result.replicaRequest(), result.finalResponseIfSuccessful);
        }

        @Override
        protected void performOnReplica(BulkShardOperationsRequest request, IndexShard replica) throws Exception {
            TransportBulkShardOperationsAction.applyTranslogOperations(request.getOperations(), replica, Origin.REPLICA);
        }
    }

}
