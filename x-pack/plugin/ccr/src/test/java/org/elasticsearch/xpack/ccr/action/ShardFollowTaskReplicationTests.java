/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngine;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ShardFollowTaskReplicationTests extends ESIndexLevelReplicationTestCase {

    public void testSimpleCcrReplication() throws Exception {
        try (ReplicationGroup leaderGroup = createGroup(randomInt(2));
             ReplicationGroup followerGroup = createFollowGroup(randomInt(2))) {
            leaderGroup.startAll();
            int docCount = leaderGroup.appendDocs(randomInt(64));
            leaderGroup.assertAllEqual(docCount);
            followerGroup.startAll();
            ShardFollowNodeTask shardFollowTask = createShardFollowTask(leaderGroup, followerGroup);
            final SeqNoStats leaderSeqNoStats = leaderGroup.getPrimary().seqNoStats();
            final SeqNoStats followerSeqNoStats = followerGroup.getPrimary().seqNoStats();
            shardFollowTask.start(
                    leaderSeqNoStats.getGlobalCheckpoint(),
                    leaderSeqNoStats.getMaxSeqNo(),
                    followerSeqNoStats.getGlobalCheckpoint(),
                    followerSeqNoStats.getMaxSeqNo());
            docCount += leaderGroup.appendDocs(randomInt(128));
            leaderGroup.syncGlobalCheckpoint();
            leaderGroup.assertAllEqual(docCount);
            Set<String> indexedDocIds = getShardDocUIDs(leaderGroup.getPrimary());
            assertBusy(() -> {
                assertThat(followerGroup.getPrimary().getGlobalCheckpoint(), equalTo(leaderGroup.getPrimary().getGlobalCheckpoint()));
                followerGroup.assertAllEqual(indexedDocIds.size());
            });
            for (IndexShard shard : followerGroup) {
                assertThat(((FollowingEngine) (getEngine(shard))).getNumberOfOptimizedIndexing(), equalTo((long) docCount));
            }
            // Deletes should be replicated to the follower
            List<String> deleteDocIds = randomSubsetOf(indexedDocIds);
            for (String deleteId : deleteDocIds) {
                BulkItemResponse resp = leaderGroup.delete(new DeleteRequest(index.getName(), "type", deleteId));
                assertThat(resp.getResponse().getResult(), equalTo(DocWriteResponse.Result.DELETED));
            }
            leaderGroup.syncGlobalCheckpoint();
            assertBusy(() -> {
                assertThat(followerGroup.getPrimary().getGlobalCheckpoint(), equalTo(leaderGroup.getPrimary().getGlobalCheckpoint()));
                followerGroup.assertAllEqual(indexedDocIds.size() - deleteDocIds.size());
            });
            shardFollowTask.markAsCompleted();
            assertConsistentHistoryBetweenLeaderAndFollower(leaderGroup, followerGroup);
        }
    }

    public void testFailLeaderReplicaShard() throws Exception {
        try (ReplicationGroup leaderGroup = createGroup(1 + randomInt(1));
             ReplicationGroup followerGroup = createFollowGroup(randomInt(2))) {
            leaderGroup.startAll();
            followerGroup.startAll();
            ShardFollowNodeTask shardFollowTask = createShardFollowTask(leaderGroup, followerGroup);
            final SeqNoStats leaderSeqNoStats = leaderGroup.getPrimary().seqNoStats();
            final SeqNoStats followerSeqNoStats = followerGroup.getPrimary().seqNoStats();
            shardFollowTask.start(
                    leaderSeqNoStats.getGlobalCheckpoint(),
                    leaderSeqNoStats.getMaxSeqNo(),
                    followerSeqNoStats.getGlobalCheckpoint(),
                    followerSeqNoStats.getMaxSeqNo());
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
            assertThat(shardFollowTask.getFailure(), nullValue());
            assertBusy(() -> followerGroup.assertAllEqual(docCount));
            shardFollowTask.markAsCompleted();
            assertConsistentHistoryBetweenLeaderAndFollower(leaderGroup, followerGroup);
        }
    }

    public void testChangeHistoryUUID() throws Exception {
        try (ReplicationGroup leaderGroup = createGroup(0);
             ReplicationGroup followerGroup = createFollowGroup(0)) {
            leaderGroup.startAll();
            int docCount = leaderGroup.appendDocs(randomInt(64));
            leaderGroup.assertAllEqual(docCount);
            followerGroup.startAll();
            ShardFollowNodeTask shardFollowTask = createShardFollowTask(leaderGroup, followerGroup);
            final SeqNoStats leaderSeqNoStats = leaderGroup.getPrimary().seqNoStats();
            final SeqNoStats followerSeqNoStats = followerGroup.getPrimary().seqNoStats();
            shardFollowTask.start(
                leaderSeqNoStats.getGlobalCheckpoint(),
                leaderSeqNoStats.getMaxSeqNo(),
                followerSeqNoStats.getGlobalCheckpoint(),
                followerSeqNoStats.getMaxSeqNo());
            leaderGroup.syncGlobalCheckpoint();
            leaderGroup.assertAllEqual(docCount);
            Set<String> indexedDocIds = getShardDocUIDs(leaderGroup.getPrimary());
            assertBusy(() -> {
                assertThat(followerGroup.getPrimary().getGlobalCheckpoint(), equalTo(leaderGroup.getPrimary().getGlobalCheckpoint()));
                followerGroup.assertAllEqual(indexedDocIds.size());
            });

            String oldHistoryUUID = leaderGroup.getPrimary().getHistoryUUID();
            leaderGroup.reinitPrimaryShard();
            leaderGroup.getPrimary().store().bootstrapNewHistory();
            recoverShardFromStore(leaderGroup.getPrimary());
            String newHistoryUUID = leaderGroup.getPrimary().getHistoryUUID();

            // force the global checkpoint on the leader to advance
            leaderGroup.appendDocs(64);

            assertBusy(() -> {
                assertThat(shardFollowTask.isStopped(), is(true));
                assertThat(shardFollowTask.getFailure().getMessage(), equalTo("unexpected history uuid, expected [" + oldHistoryUUID +
                    "], actual [" + newHistoryUUID + "]"));
            });
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
        ShardFollowTask params = new ShardFollowTask(
            null,
            new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0),
            between(1, 64),
            between(1, 8),
            new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
            between(1, 4), 10240,
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueMillis(10),
            leaderGroup.getPrimary().getHistoryUUID(),
            Collections.emptyMap()
        );

        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> threadPool.schedule(delay, ThreadPool.Names.GENERIC, task);
        AtomicBoolean stopped = new AtomicBoolean(false);
        AtomicReference<Exception> failureHolder = new AtomicReference<>();
        LongSet fetchOperations = new LongHashSet();
        return new ShardFollowNodeTask(
                1L, "type", ShardFollowTask.NAME, "description", null, Collections.emptyMap(), params, scheduler, System::nanoTime) {
            @Override
            protected synchronized void onOperationsFetched(Translog.Operation[] operations) {
                super.onOperationsFetched(operations);
                for (Translog.Operation operation : operations) {
                    if (fetchOperations.add(operation.seqNo()) == false) {
                        throw new AssertionError("Operation [" + operation + " ] was fetched already");
                    }
                }
            }

            @Override
            protected void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler) {
                // noop, as mapping updates are not tested
                handler.accept(1L);
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(
                    final List<Translog.Operation> operations,
                    final long maxSeqNoOfUpdates,
                    final Consumer<BulkShardOperationsResponse> handler,
                    final Consumer<Exception> errorHandler) {
                Runnable task = () -> {
                    BulkShardOperationsRequest request = new BulkShardOperationsRequest(
                        params.getFollowShardId(), operations, maxSeqNoOfUpdates);
                    ActionListener<BulkShardOperationsResponse> listener = ActionListener.wrap(handler::accept, errorHandler);
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
                        try {
                            final SeqNoStats seqNoStats = indexShard.seqNoStats();
                            final long maxSeqNoOfUpdatesOrDeletes = indexShard.getMaxSeqNoOfUpdatesOrDeletes();
                            if (from > seqNoStats.getGlobalCheckpoint()) {
                                handler.accept(ShardChangesAction.getResponse(1L, seqNoStats,
                                    maxSeqNoOfUpdatesOrDeletes, ShardChangesAction.EMPTY_OPERATIONS_ARRAY));
                                return;
                            }
                            Translog.Operation[] ops = ShardChangesAction.getOperations(indexShard, seqNoStats.getGlobalCheckpoint(), from,
                                maxOperationCount, params.getRecordedLeaderIndexHistoryUUID(), params.getMaxBatchSize());
                            // hard code mapping version; this is ok, as mapping updates are not tested here
                            final ShardChangesAction.Response response = new ShardChangesAction.Response(
                                1L,
                                seqNoStats.getGlobalCheckpoint(),
                                seqNoStats.getMaxSeqNo(),
                                maxSeqNoOfUpdatesOrDeletes,
                                ops
                            );
                            handler.accept(response);
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
                failureHolder.set(e);
                stopped.set(true);
            }

            @Override
            public Exception getFailure() {
                return failureHolder.get();
            }
        };
    }

    private void assertConsistentHistoryBetweenLeaderAndFollower(ReplicationGroup leader, ReplicationGroup follower) throws IOException {
        int totalOps = leader.getPrimary().estimateNumberOfHistoryOperations("test", 0);
        for (IndexShard followingShard : follower) {
            assertThat(followingShard.estimateNumberOfHistoryOperations("test", 0), equalTo(totalOps));
        }
        for (IndexShard followingShard : follower) {
            assertThat(followingShard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(leader.getPrimary().getMaxSeqNoOfUpdatesOrDeletes()));
        }
    }

    class CCRAction extends ReplicationAction<BulkShardOperationsRequest, BulkShardOperationsRequest, BulkShardOperationsResponse> {

        CCRAction(BulkShardOperationsRequest request, ActionListener<BulkShardOperationsResponse> listener, ReplicationGroup group) {
            super(request, listener, group, "ccr");
        }

        @Override
        protected PrimaryResult performOnPrimary(IndexShard primary, BulkShardOperationsRequest request) throws Exception {
            TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> result =
                TransportBulkShardOperationsAction.shardOperationOnPrimary(primary.shardId(), request.getOperations(),
                    request.getMaxSeqNoOfUpdatesOrDeletes(), primary, logger);
            return new PrimaryResult(result.replicaRequest(), result.finalResponseIfSuccessful);
        }

        @Override
        protected void performOnReplica(BulkShardOperationsRequest request, IndexShard replica) throws Exception {
            TransportBulkShardOperationsAction.applyTranslogOperations(request.getOperations(), replica, Origin.REPLICA);
        }
    }

}
