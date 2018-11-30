/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngine;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

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
                    followerGroup.getPrimary().getHistoryUUID(),
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
                    followerGroup.getPrimary().getHistoryUUID(),
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

    public void testChangeLeaderHistoryUUID() throws Exception {
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
                followerGroup.getPrimary().getHistoryUUID(),
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
                ElasticsearchException failure = shardFollowTask.getStatus().getFatalException();
                assertThat(failure.getRootCause().getMessage(), equalTo("unexpected history uuid, expected [" + oldHistoryUUID +
                    "], actual [" + newHistoryUUID + "]"));
            });
        }
    }

    public void testChangeFollowerHistoryUUID() throws Exception {
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
                followerGroup.getPrimary().getHistoryUUID(),
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

            String oldHistoryUUID = followerGroup.getPrimary().getHistoryUUID();
            followerGroup.reinitPrimaryShard();
            followerGroup.getPrimary().store().bootstrapNewHistory();
            recoverShardFromStore(followerGroup.getPrimary());
            String newHistoryUUID = followerGroup.getPrimary().getHistoryUUID();

            // force the global checkpoint on the leader to advance
            leaderGroup.appendDocs(64);

            assertBusy(() -> {
                assertThat(shardFollowTask.isStopped(), is(true));
                ElasticsearchException failure = shardFollowTask.getStatus().getFatalException();
                assertThat(failure.getRootCause().getMessage(), equalTo("unexpected history uuid, expected [" + oldHistoryUUID +
                    "], actual [" + newHistoryUUID + "], shard is likely restored from snapshot or force allocated"));
            });
        }
    }

    public void testRetryBulkShardOperations() throws Exception {
        try (ReplicationGroup leaderGroup = createGroup(between(0, 1));
             ReplicationGroup followerGroup = createFollowGroup(between(1, 3))) {
            leaderGroup.startAll();
            followerGroup.startAll();
            leaderGroup.appendDocs(between(10, 100));
            leaderGroup.refresh("test");
            for (int numNoOps = between(1, 10), i = 0; i < numNoOps; i++) {
                long seqNo = leaderGroup.getPrimary().seqNoStats().getMaxSeqNo() + 1;
                Engine.NoOp noOp = new Engine.NoOp(seqNo, leaderGroup.getPrimary().getOperationPrimaryTerm(),
                    Engine.Operation.Origin.REPLICA, threadPool.relativeTimeInMillis(), "test-" + i);
                for (IndexShard shard : leaderGroup) {
                    getEngine(shard).noOp(noOp);
                }
            }
            for (String deleteId : randomSubsetOf(IndexShardTestCase.getShardDocUIDs(leaderGroup.getPrimary()))) {
                BulkItemResponse resp = leaderGroup.delete(new DeleteRequest("test", "type", deleteId));
                assertThat(resp.getFailure(), nullValue());
            }
            leaderGroup.syncGlobalCheckpoint();
            IndexShard leadingPrimary = leaderGroup.getPrimary();
            // Simulates some bulk requests are completed on the primary and replicated to some (but all) replicas of the follower
            // but the primary of the follower crashed before these requests completed.
            for (int numBulks = between(1, 5), i = 0; i < numBulks; i++) {
                long fromSeqNo = randomLongBetween(0, leadingPrimary.getGlobalCheckpoint());
                long toSeqNo = randomLongBetween(fromSeqNo, leadingPrimary.getGlobalCheckpoint());
                int numOps = Math.toIntExact(toSeqNo + 1 - fromSeqNo);
                Translog.Operation[] ops = ShardChangesAction.getOperations(leadingPrimary, leadingPrimary.getGlobalCheckpoint(),
                    fromSeqNo, numOps, leadingPrimary.getHistoryUUID(), new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES));

                IndexShard followingPrimary = followerGroup.getPrimary();
                TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> primaryResult =
                    TransportBulkShardOperationsAction.shardOperationOnPrimary(followingPrimary.shardId(),
                        followingPrimary.getHistoryUUID(), Arrays.asList(ops), leadingPrimary.getMaxSeqNoOfUpdatesOrDeletes(),
                        followingPrimary, logger);
                for (IndexShard replica : randomSubsetOf(followerGroup.getReplicas())) {
                    final PlainActionFuture<Releasable> permitFuture = new PlainActionFuture<>();
                    replica.acquireReplicaOperationPermit(followingPrimary.getOperationPrimaryTerm(),
                        followingPrimary.getGlobalCheckpoint(), followingPrimary.getMaxSeqNoOfUpdatesOrDeletes(),
                        permitFuture, ThreadPool.Names.SAME, primaryResult);
                    try (Releasable ignored = permitFuture.get()) {
                        TransportBulkShardOperationsAction.shardOperationOnReplica(primaryResult.replicaRequest(), replica, logger);
                    }
                }
            }
            // A follow-task retries these requests while the primary-replica resync is happening on the follower.
            followerGroup.promoteReplicaToPrimary(randomFrom(followerGroup.getReplicas()));
            ShardFollowNodeTask shardFollowTask = createShardFollowTask(leaderGroup, followerGroup);
            SeqNoStats followerSeqNoStats = followerGroup.getPrimary().seqNoStats();
            shardFollowTask.start(followerGroup.getPrimary().getHistoryUUID(), leadingPrimary.getGlobalCheckpoint(),
                leadingPrimary.getMaxSeqNoOfUpdatesOrDeletes(), followerSeqNoStats.getGlobalCheckpoint(), followerSeqNoStats.getMaxSeqNo());
            try {
                assertBusy(() -> {
                    assertThat(followerGroup.getPrimary().getGlobalCheckpoint(), equalTo(leadingPrimary.getGlobalCheckpoint()));
                    assertConsistentHistoryBetweenLeaderAndFollower(leaderGroup, followerGroup);
                });
            } finally {
                shardFollowTask.markAsCompleted();
            }
        }
    }

    public void testAddNewFollowingReplica() throws Exception {
        final byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
        final int numDocs = between(1, 100);
        final List<Translog.Operation> operations = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            operations.add(new Translog.Index("type", Integer.toString(i), i, primaryTerm, 0, source, null, -1));
        }
        Future<Void> recoveryFuture = null;
        try (ReplicationGroup group = createFollowGroup(between(0, 1))) {
            group.startAll();
            while (operations.isEmpty() == false) {
                List<Translog.Operation> bulkOps = randomSubsetOf(between(1, operations.size()), operations);
                operations.removeAll(bulkOps);
                BulkShardOperationsRequest bulkRequest = new BulkShardOperationsRequest(group.getPrimary().shardId(),
                    group.getPrimary().getHistoryUUID(), bulkOps, -1);
                new CcrAction(bulkRequest, new PlainActionFuture<>(), group).execute();
                if (randomInt(100) < 10) {
                    group.getPrimary().flush(new FlushRequest());
                }
                if (recoveryFuture == null && (randomInt(100) < 10 || operations.isEmpty())) {
                    group.getPrimary().sync();
                    IndexShard newReplica = group.addReplica();
                    // We need to recover the replica async to release the main thread for the following task to fill missing
                    // operations between the local checkpoint and max_seq_no which the recovering replica is waiting for.
                    recoveryFuture = group.asyncRecoverReplica(newReplica,
                        (shard, sourceNode) -> new RecoveryTarget(shard, sourceNode, recoveryListener, l -> {}) {});
                }
            }
            if (recoveryFuture != null) {
                recoveryFuture.get();
            }
            group.assertAllEqual(numDocs);
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
        settingsBuilder.put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(between(1, 1000), ByteSizeUnit.KB));
        return createGroup(replicas, settingsBuilder.build());
    }

    private ShardFollowNodeTask createShardFollowTask(ReplicationGroup leaderGroup, ReplicationGroup followerGroup) {
        ShardFollowTask params = new ShardFollowTask(
            null,
            new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0),
            between(1, 64),
            new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
            between(1, 8),
            between(1, 64),
            new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
            between(1, 4),
            10240,
            new ByteSizeValue(512, ByteSizeUnit.MB),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueMillis(10),
            Collections.emptyMap()
        );
        final String recordedLeaderIndexHistoryUUID = leaderGroup.getPrimary().getHistoryUUID();

        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> threadPool.schedule(delay, ThreadPool.Names.GENERIC, task);
        AtomicBoolean stopped = new AtomicBoolean(false);
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
            protected void innerUpdateSettings(LongConsumer handler, Consumer<Exception> errorHandler) {
                // no-op as settings updates are not tested here
                handler.accept(1L);
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(
                final String followerHistoryUUID,
                final List<Translog.Operation> operations,
                final long maxSeqNoOfUpdates,
                final Consumer<BulkShardOperationsResponse> handler,
                final Consumer<Exception> errorHandler) {
                Runnable task = () -> {
                    BulkShardOperationsRequest request = new BulkShardOperationsRequest(params.getFollowShardId(),
                        followerHistoryUUID, operations, maxSeqNoOfUpdates);
                    ActionListener<BulkShardOperationsResponse> listener = ActionListener.wrap(handler::accept, errorHandler);
                    new CcrAction(request, listener, followerGroup).execute();
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
                                handler.accept(ShardChangesAction.getResponse(1L, 1L, seqNoStats,
                                    maxSeqNoOfUpdatesOrDeletes, ShardChangesAction.EMPTY_OPERATIONS_ARRAY, 1L));
                                return;
                            }
                            Translog.Operation[] ops = ShardChangesAction.getOperations(indexShard, seqNoStats.getGlobalCheckpoint(), from,
                                maxOperationCount, recordedLeaderIndexHistoryUUID, params.getMaxReadRequestSize());
                            // hard code mapping version; this is ok, as mapping updates are not tested here
                            final ShardChangesAction.Response response = new ShardChangesAction.Response(
                                1L,
                                1L,
                                seqNoStats.getGlobalCheckpoint(),
                                seqNoStats.getMaxSeqNo(),
                                maxSeqNoOfUpdatesOrDeletes,
                                ops,
                                1L
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
                return super.isStopped() || stopped.get();
            }

            @Override
            public void markAsCompleted() {
                stopped.set(true);
            }

        };
    }

    private void assertConsistentHistoryBetweenLeaderAndFollower(ReplicationGroup leader, ReplicationGroup follower) throws Exception {
        final List<Tuple<String, Long>> docAndSeqNosOnLeader = getDocIdAndSeqNos(leader.getPrimary()).stream()
            .map(d -> Tuple.tuple(d.getId(), d.getSeqNo())).collect(Collectors.toList());
        final Set<Tuple<Long, Translog.Operation.Type>> operationsOnLeader = new HashSet<>();
        try (Translog.Snapshot snapshot = leader.getPrimary().getHistoryOperations("test", 0)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operationsOnLeader.add(Tuple.tuple(op.seqNo(), op.opType()));
            }
        }
        for (IndexShard followingShard : follower) {
            assertThat(followingShard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(leader.getPrimary().getMaxSeqNoOfUpdatesOrDeletes()));
            List<Tuple<String, Long>> docAndSeqNosOnFollower = getDocIdAndSeqNos(followingShard).stream()
                .map(d -> Tuple.tuple(d.getId(), d.getSeqNo())).collect(Collectors.toList());
            assertThat(docAndSeqNosOnFollower, equalTo(docAndSeqNosOnLeader));
            final Set<Tuple<Long, Translog.Operation.Type>> operationsOnFollower = new HashSet<>();
            try (Translog.Snapshot snapshot = followingShard.getHistoryOperations("test", 0)) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    operationsOnFollower.add(Tuple.tuple(op.seqNo(), op.opType()));
                }
            }
            assertThat(operationsOnFollower, equalTo(operationsOnLeader));
        }
    }

    class CcrAction extends ReplicationAction<BulkShardOperationsRequest, BulkShardOperationsRequest, BulkShardOperationsResponse> {

        CcrAction(BulkShardOperationsRequest request, ActionListener<BulkShardOperationsResponse> listener, ReplicationGroup group) {
            super(request, listener, group, "ccr");
        }

        @Override
        protected PrimaryResult performOnPrimary(IndexShard primary, BulkShardOperationsRequest request) throws Exception {
            final PlainActionFuture<Releasable> permitFuture = new PlainActionFuture<>();
            primary.acquirePrimaryOperationPermit(permitFuture, ThreadPool.Names.SAME, request);
            final TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> ccrResult;
            try (Releasable ignored = permitFuture.get()) {
                ccrResult = TransportBulkShardOperationsAction.shardOperationOnPrimary(primary.shardId(), request.getHistoryUUID(),
                    request.getOperations(), request.getMaxSeqNoOfUpdatesOrDeletes(), primary, logger);
            }
            return new PrimaryResult(ccrResult.replicaRequest(), ccrResult.finalResponseIfSuccessful) {
                @Override
                public void respond(ActionListener<BulkShardOperationsResponse> listener) {
                    ccrResult.respond(listener);
                }
            };
        }

        @Override
        protected void performOnReplica(BulkShardOperationsRequest request, IndexShard replica) throws Exception {
            TransportBulkShardOperationsAction.shardOperationOnReplica(request, replica, logger);
        }
    }

}
