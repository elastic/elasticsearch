/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.support.replication.TransportWriteActionTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.RestoreOnlyRepository;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrRetentionLeases;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ShardFollowTaskReplicationTests extends ESIndexLevelReplicationTestCase {

    public void testSimpleCcrReplication() throws Exception {
        try (ReplicationGroup leaderGroup = createLeaderGroup(randomInt(2))) {
            leaderGroup.startAll();
            try (ReplicationGroup followerGroup = createFollowGroup(leaderGroup, randomInt(2))) {
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
                    assertThat(followerGroup.getPrimary().getLastKnownGlobalCheckpoint(),
                        equalTo(leaderGroup.getPrimary().getLastKnownGlobalCheckpoint()));
                    followerGroup.assertAllEqual(indexedDocIds.size());
                });
                for (IndexShard shard : followerGroup) {
                    assertThat(EngineTestCase.getNumVersionLookups(getEngine(shard)), equalTo(0L));
                }
                // Deletes should be replicated to the follower
                List<String> deleteDocIds = randomSubsetOf(indexedDocIds);
                for (String deleteId : deleteDocIds) {
                    BulkItemResponse resp = leaderGroup.delete(new DeleteRequest(index.getName(), "type", deleteId));
                    assertThat(resp.getResponse().getResult(), equalTo(DocWriteResponse.Result.DELETED));
                }
                leaderGroup.syncGlobalCheckpoint();
                assertBusy(() -> {
                    assertThat(followerGroup.getPrimary().getLastKnownGlobalCheckpoint(),
                        equalTo(leaderGroup.getPrimary().getLastKnownGlobalCheckpoint()));
                    followerGroup.assertAllEqual(indexedDocIds.size() - deleteDocIds.size());
                });
                assertNull(shardFollowTask.getStatus().getFatalException());
                shardFollowTask.markAsCompleted();
                assertConsistentHistoryBetweenLeaderAndFollower(leaderGroup, followerGroup, true);
            }
        }
    }

    public void testAddRemoveShardOnLeader() throws Exception {
        try (ReplicationGroup leaderGroup = createLeaderGroup(1 + randomInt(1))) {
            leaderGroup.startAll();
            try (ReplicationGroup followerGroup = createFollowGroup(leaderGroup, randomInt(2))) {
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
                int batches = between(0, 10);
                int docCount = 0;
                boolean hasPromotion = false;
                for (int i = 0; i < batches; i++) {
                    docCount += leaderGroup.indexDocs(between(1, 5));
                    if (leaderGroup.getReplicas().isEmpty() == false && randomInt(100) < 5) {
                        IndexShard closingReplica = randomFrom(leaderGroup.getReplicas());
                        leaderGroup.removeReplica(closingReplica);
                        closingReplica.close("test", false);
                        closingReplica.store().close();
                    } else if (leaderGroup.getReplicas().isEmpty() == false && rarely()) {
                        IndexShard newPrimary = randomFrom(leaderGroup.getReplicas());
                        leaderGroup.promoteReplicaToPrimary(newPrimary).get();
                        hasPromotion = true;
                    } else if (randomInt(100) < 5) {
                        leaderGroup.addReplica();
                        leaderGroup.startReplicas(1);
                    }
                    leaderGroup.syncGlobalCheckpoint();
                }
                leaderGroup.assertAllEqual(docCount);
                assertThat(shardFollowTask.getFailure(), nullValue());
                int expectedDoc = docCount;
                assertBusy(() -> followerGroup.assertAllEqual(expectedDoc));
                assertNull(shardFollowTask.getStatus().getFatalException());
                shardFollowTask.markAsCompleted();
                assertConsistentHistoryBetweenLeaderAndFollower(leaderGroup, followerGroup, hasPromotion == false);
            }
        }
    }

    public void testChangeLeaderHistoryUUID() throws Exception {
        try (ReplicationGroup leaderGroup = createLeaderGroup(0)) {
            try (ReplicationGroup followerGroup = createFollowGroup(leaderGroup, 0)) {
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
                    assertThat(followerGroup.getPrimary().getLastKnownGlobalCheckpoint(),
                        equalTo(leaderGroup.getPrimary().getLastKnownGlobalCheckpoint()));
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
    }

    public void testChangeFollowerHistoryUUID() throws Exception {
        try (ReplicationGroup leaderGroup = createLeaderGroup(0)) {
            leaderGroup.startAll();
            try(ReplicationGroup followerGroup = createFollowGroup(leaderGroup, 0)) {
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
                    assertThat(followerGroup.getPrimary().getLastKnownGlobalCheckpoint(),
                        equalTo(leaderGroup.getPrimary().getLastKnownGlobalCheckpoint()));
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
    }

    public void testRetryBulkShardOperations() throws Exception {
        try (ReplicationGroup leaderGroup = createLeaderGroup(between(0, 1))) {
            leaderGroup.startAll();
            try(ReplicationGroup followerGroup = createFollowGroup(leaderGroup, between(1, 3))) {
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
                    long fromSeqNo = randomLongBetween(0, leadingPrimary.getLastKnownGlobalCheckpoint());
                    long toSeqNo = randomLongBetween(fromSeqNo, leadingPrimary.getLastKnownGlobalCheckpoint());
                    int numOps = Math.toIntExact(toSeqNo + 1 - fromSeqNo);
                    Translog.Operation[] ops = ShardChangesAction.getOperations(leadingPrimary,
                        leadingPrimary.getLastKnownGlobalCheckpoint(), fromSeqNo, numOps, leadingPrimary.getHistoryUUID(),
                        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES));

                    IndexShard followingPrimary = followerGroup.getPrimary();
                    TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> primaryResult =
                        TransportBulkShardOperationsAction.shardOperationOnPrimary(followingPrimary.shardId(),
                            followingPrimary.getHistoryUUID(), Arrays.asList(ops), leadingPrimary.getMaxSeqNoOfUpdatesOrDeletes(),
                            followingPrimary, logger);
                    for (IndexShard replica : randomSubsetOf(followerGroup.getReplicas())) {
                        final PlainActionFuture<Releasable> permitFuture = new PlainActionFuture<>();
                        replica.acquireReplicaOperationPermit(followingPrimary.getOperationPrimaryTerm(),
                            followingPrimary.getLastKnownGlobalCheckpoint(), followingPrimary.getMaxSeqNoOfUpdatesOrDeletes(),
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
                shardFollowTask.start(followerGroup.getPrimary().getHistoryUUID(),
                    leadingPrimary.getLastKnownGlobalCheckpoint(),
                    leadingPrimary.getMaxSeqNoOfUpdatesOrDeletes(),
                    followerSeqNoStats.getGlobalCheckpoint(),
                    followerSeqNoStats.getMaxSeqNo());
                try {
                    assertBusy(() -> {
                        assertThat(followerGroup.getPrimary().getLastKnownGlobalCheckpoint(),
                            equalTo(leadingPrimary.getLastKnownGlobalCheckpoint()));
                        assertConsistentHistoryBetweenLeaderAndFollower(leaderGroup, followerGroup, true);
                    });
                    assertNull(shardFollowTask.getStatus().getFatalException());
                } finally {
                    shardFollowTask.markAsCompleted();
                }
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
        Settings settings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(between(1, 1000), ByteSizeUnit.KB))
            .build();
        IndexMetadata indexMetadata = buildIndexMetadata(between(0, 1), settings, indexMapping);
        try (ReplicationGroup group = new ReplicationGroup(indexMetadata) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                return new FollowingEngineFactory();
            }
        }) {
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
                        (shard, sourceNode) -> new RecoveryTarget(shard, sourceNode, null, recoveryListener) {});
                }
            }
            if (recoveryFuture != null) {
                recoveryFuture.get();
            }
            group.assertAllEqual(numDocs);
        }
    }

    public void testSimpleRemoteRecovery() throws Exception {
        try (ReplicationGroup leader = createLeaderGroup(between(0, 1))) {
            leader.startAll();
            leader.appendDocs(between(0, 100));
            leader.flush();
            leader.syncGlobalCheckpoint();
            try (ReplicationGroup follower = createFollowGroup(leader, 0)) {
                follower.startAll();
                ShardFollowNodeTask followTask = createShardFollowTask(leader, follower);
                followTask.start(
                    follower.getPrimary().getHistoryUUID(),
                    leader.getPrimary().getLastKnownGlobalCheckpoint(),
                    leader.getPrimary().seqNoStats().getMaxSeqNo(),
                    follower.getPrimary().getLastKnownGlobalCheckpoint(),
                    follower.getPrimary().seqNoStats().getMaxSeqNo()
                );
                leader.appendDocs(between(0, 100));
                if (randomBoolean()) {
                    follower.recoverReplica(follower.addReplica());
                }
                assertBusy(() -> assertConsistentHistoryBetweenLeaderAndFollower(leader, follower, false));
                assertNull(followTask.getStatus().getFatalException());
                followTask.markAsCompleted();
            }
        }
    }

    public void testRetentionLeaseManagement() throws Exception {
        try (ReplicationGroup leader = createLeaderGroup(0)) {
            leader.startAll();
            try (ReplicationGroup follower = createFollowGroup(leader, 0)) {
                follower.startAll();
                final ShardFollowNodeTask task = createShardFollowTask(leader, follower);
                task.start(
                        follower.getPrimary().getHistoryUUID(),
                        leader.getPrimary().getLastKnownGlobalCheckpoint(),
                        leader.getPrimary().seqNoStats().getMaxSeqNo(),
                        follower.getPrimary().getLastKnownGlobalCheckpoint(),
                        follower.getPrimary().seqNoStats().getMaxSeqNo());
                final Scheduler.Cancellable renewable = task.getRenewable();
                assertNotNull(renewable);
                assertFalse(renewable.isCancelled());
                task.onCancelled();
                assertTrue(renewable.isCancelled());
                assertNull(task.getRenewable());
            }
        }
    }

    private ReplicationGroup createLeaderGroup(int replicas) throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 10000)
            .build();
        return createGroup(replicas, settings);
    }

    private ReplicationGroup createFollowGroup(ReplicationGroup leaderGroup, int replicas) throws IOException {
        final Settings settings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(
                        IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                        new ByteSizeValue(between(1, 1000), ByteSizeUnit.KB))
                .build();
        IndexMetadata indexMetadata = buildIndexMetadata(replicas, settings, indexMapping);
        return new ReplicationGroup(indexMetadata) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                return new FollowingEngineFactory();
            }
            @Override
            protected synchronized void recoverPrimary(IndexShard primary) {
                DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
                Snapshot snapshot = new Snapshot("foo", new SnapshotId("bar", UUIDs.randomBase64UUID()));
                ShardRouting routing = ShardRoutingHelper.newWithRestoreSource(primary.routingEntry(),
                    new RecoverySource.SnapshotRecoverySource(UUIDs.randomBase64UUID(), snapshot, Version.CURRENT,
                        new IndexId("test", UUIDs.randomBase64UUID(random()))));
                primary.markAsRecovering("remote recovery from leader", new RecoveryState(routing, localNode, null));
                final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
                primary.restoreFromRepository(new RestoreOnlyRepository(index.getName()) {
                    @Override
                    public void restoreShard(Store store, SnapshotId snapshotId, IndexId indexId, ShardId snapshotShardId,
                                             RecoveryState recoveryState, ActionListener<Void> listener) {
                        ActionListener.completeWith(listener, () -> {
                            IndexShard leader = leaderGroup.getPrimary();
                            Lucene.cleanLuceneIndex(primary.store().directory());
                            try (Engine.IndexCommitRef sourceCommit = leader.acquireSafeIndexCommit()) {
                                Store.MetadataSnapshot sourceSnapshot = leader.store().getMetadata(sourceCommit.getIndexCommit());
                                for (StoreFileMetadata md : sourceSnapshot) {
                                    primary.store().directory().copyFrom(
                                        leader.store().directory(), md.name(), md.name(), IOContext.DEFAULT);
                                }
                            }
                            recoveryState.getIndex().setFileDetailsComplete();
                            return null;
                        });
                    }
                }, future);
                try {
                    future.actionGet();
                } catch (Exception ex) {
                    throw new AssertionError(ex);
                }
            }
        };
    }

    private ShardFollowNodeTask createShardFollowTask(ReplicationGroup leaderGroup, ReplicationGroup followerGroup) {
        ShardFollowTask params = new ShardFollowTask(
            null,
            new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0),
            between(1, 64),
            between(1, 64),
            between(1, 8),
            between(1, 4),
            new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
            new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
            10240,
            new ByteSizeValue(512, ByteSizeUnit.MB),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueMillis(10),
            Collections.emptyMap()
        );
        final String recordedLeaderIndexHistoryUUID = leaderGroup.getPrimary().getHistoryUUID();

        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> threadPool.schedule(task, delay, ThreadPool.Names.GENERIC);
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
            protected void innerUpdateMapping(long minRequiredMappingVersion, LongConsumer handler, Consumer<Exception> errorHandler) {
                // noop, as mapping updates are not tested
                handler.accept(1L);
            }

            @Override
            protected void innerUpdateSettings(LongConsumer handler, Consumer<Exception> errorHandler) {
                // no-op as settings updates are not tested here
                handler.accept(1L);
            }

            @Override
            protected void innerUpdateAliases(LongConsumer handler, Consumer<Exception> errorHandler) {
                // no-op as alias updates are not tested here
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
                                handler.accept(ShardChangesAction.getResponse(
                                        1L,
                                        1L,
                                        1L,
                                        seqNoStats,
                                        maxSeqNoOfUpdatesOrDeletes,
                                        ShardChangesAction.EMPTY_OPERATIONS_ARRAY,
                                        1L));
                                return;
                            }
                            Translog.Operation[] ops = ShardChangesAction.getOperations(indexShard, seqNoStats.getGlobalCheckpoint(), from,
                                maxOperationCount, recordedLeaderIndexHistoryUUID, params.getMaxReadRequestSize());
                            // hard code mapping version; this is ok, as mapping updates are not tested here
                            final ShardChangesAction.Response response = new ShardChangesAction.Response(
                                1L,
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
            protected Scheduler.Cancellable scheduleBackgroundRetentionLeaseRenewal(final LongSupplier followerGlobalCheckpoint) {
                final String retentionLeaseId = CcrRetentionLeases.retentionLeaseId(
                        "follower",
                        followerGroup.getPrimary().routingEntry().index(),
                        "remote",
                        leaderGroup.getPrimary().routingEntry().index());
                final PlainActionFuture<ReplicationResponse> response = new PlainActionFuture<>();
                leaderGroup.addRetentionLease(
                        retentionLeaseId,
                        followerGlobalCheckpoint.getAsLong(),
                        "ccr",
                        ActionListener.wrap(response::onResponse, e -> fail(e.toString())));
                response.actionGet();
                return threadPool.scheduleWithFixedDelay(
                        () -> leaderGroup.renewRetentionLease(retentionLeaseId, followerGlobalCheckpoint.getAsLong(), "ccr"),
                        CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(
                                followerGroup.getPrimary().indexSettings().getSettings()),
                        ThreadPool.Names.GENERIC);
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

    private void assertConsistentHistoryBetweenLeaderAndFollower(ReplicationGroup leader, ReplicationGroup follower,
                                                                 boolean assertMaxSeqNoOfUpdatesOrDeletes) throws Exception {
        final List<Tuple<String, Long>> docAndSeqNosOnLeader = getDocIdAndSeqNos(leader.getPrimary()).stream()
            .map(d -> Tuple.tuple(d.getId(), d.getSeqNo())).collect(Collectors.toList());
        final Map<Long, Translog.Operation> operationsOnLeader = new HashMap<>();
        try (Translog.Snapshot snapshot = leader.getPrimary().newChangesSnapshot("test", 0, Long.MAX_VALUE, false)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operationsOnLeader.put(op.seqNo(), op);
            }
        }
        for (IndexShard followingShard : follower) {
            if (assertMaxSeqNoOfUpdatesOrDeletes) {
                assertThat(followingShard.getMaxSeqNoOfUpdatesOrDeletes(),
                    greaterThanOrEqualTo(leader.getPrimary().getMaxSeqNoOfUpdatesOrDeletes()));
            }
            List<Tuple<String, Long>> docAndSeqNosOnFollower = getDocIdAndSeqNos(followingShard).stream()
                .map(d -> Tuple.tuple(d.getId(), d.getSeqNo())).collect(Collectors.toList());
            assertThat(docAndSeqNosOnFollower, equalTo(docAndSeqNosOnLeader));
            try (Translog.Snapshot snapshot = followingShard.newChangesSnapshot("test", 0, Long.MAX_VALUE, false)) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    Translog.Operation leaderOp = operationsOnLeader.get(op.seqNo());
                    assertThat(TransportBulkShardOperationsAction.rewriteOperationWithPrimaryTerm(op, leaderOp.primaryTerm()),
                        equalTo(leaderOp));
                }
            }
        }
    }

    class CcrAction extends ReplicationAction<BulkShardOperationsRequest, BulkShardOperationsRequest, BulkShardOperationsResponse> {

        CcrAction(BulkShardOperationsRequest request, ActionListener<BulkShardOperationsResponse> listener, ReplicationGroup group) {
            super(request, listener, group, "ccr");
        }

        @Override
        protected void performOnPrimary(IndexShard primary, BulkShardOperationsRequest request, ActionListener<PrimaryResult> listener) {
            final PlainActionFuture<Releasable> permitFuture = new PlainActionFuture<>();
            primary.acquirePrimaryOperationPermit(permitFuture, ThreadPool.Names.SAME, request);
            final TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> ccrResult;
            try (Releasable ignored = permitFuture.get()) {
                ccrResult = TransportBulkShardOperationsAction.shardOperationOnPrimary(primary.shardId(), request.getHistoryUUID(),
                    request.getOperations(), request.getMaxSeqNoOfUpdatesOrDeletes(), primary, logger);
                TransportWriteActionTestHelper.performPostWriteActions(primary, request, ccrResult.location, logger);
            } catch (InterruptedException | ExecutionException | IOException e) {
                throw new RuntimeException(e);
            }
            listener.onResponse(new PrimaryResult(ccrResult.replicaRequest(), ccrResult.finalResponseIfSuccessful));
        }

        @Override
        protected void adaptResponse(BulkShardOperationsResponse response, IndexShard indexShard) {
            TransportBulkShardOperationsAction.adaptBulkShardOperationsResponse(response, indexShard);
        }

        @Override
        protected void performOnReplica(BulkShardOperationsRequest request, IndexShard replica) throws Exception {
            try (Releasable ignored = PlainActionFuture.get(f -> replica.acquireReplicaOperationPermit(
                getPrimaryShard().getPendingPrimaryTerm(), getPrimaryShard().getLastKnownGlobalCheckpoint(),
                getPrimaryShard().getMaxSeqNoOfUpdatesOrDeletes(), f, ThreadPool.Names.SAME, request))) {
                Translog.Location location = TransportBulkShardOperationsAction.shardOperationOnReplica(request, replica, logger).location;
                TransportWriteActionTestHelper.performPostWriteActions(replica, request, location, logger);
            }
        }
    }

}
