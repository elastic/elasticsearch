/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.replication;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTests;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.SnapshotMatchers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.translog.SnapshotMatchers.containsOperationsInAnyOrder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class IndexLevelReplicationTests extends ESIndexLevelReplicationTestCase {

    public void testSimpleReplication() throws Exception {
        try (ReplicationGroup shards = createGroup(randomInt(2))) {
            shards.startAll();
            final int docCount = randomInt(50);
            shards.indexDocs(docCount);
            shards.assertAllEqual(docCount);
            for (IndexShard replica : shards.getReplicas()) {
                assertThat(EngineTestCase.getNumVersionLookups(getEngine(replica)), equalTo(0L));
            }
        }
    }

    public void testSimpleAppendOnlyReplication() throws Exception {
        try (ReplicationGroup shards = createGroup(randomInt(2))) {
            shards.startAll();
            final int docCount = randomInt(50);
            shards.appendDocs(docCount);
            shards.assertAllEqual(docCount);
            for (IndexShard replica : shards.getReplicas()) {
                assertThat(EngineTestCase.getNumVersionLookups(getEngine(replica)), equalTo(0L));
            }
        }
    }

    public void testAppendWhileRecovering() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            CountDownLatch latch = new CountDownLatch(2);
            int numDocs = randomIntBetween(100, 200);
            shards.appendDocs(1);// just append one to the translog so we can assert below
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.countDown();
                        latch.await();
                        shards.appendDocs(numDocs - 1);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            };
            thread.start();
            IndexShard replica = shards.addReplica();
            Future<Void> future = shards.asyncRecoverReplica(replica,
                (indexShard, node) -> new RecoveryTarget(indexShard, node, recoveryListener) {
                    @Override
                    public void cleanFiles(int totalTranslogOps, long globalCheckpoint,
                                           Store.MetadataSnapshot sourceMetadata, ActionListener<Void> listener) {
                        super.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, ActionListener.runAfter(listener, () -> {
                            latch.countDown();
                            try {
                                latch.await();
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                        }));
                    }
                });
            future.get();
            thread.join();
            shards.assertAllEqual(numDocs);
            Engine engine = IndexShardTests.getEngineFromShard(shards.getPrimary());
            assertEquals(0, InternalEngineTests.getNumIndexVersionsLookups((InternalEngine) engine));
            assertEquals(0, InternalEngineTests.getNumVersionLookups((InternalEngine) engine));
        }
    }

    public void testRetryAppendOnlyAfterRecovering() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            final IndexRequest originalRequest = new IndexRequest(index.getName()).source("{}", XContentType.JSON);
            originalRequest.process(Version.CURRENT, null, index.getName());
            final IndexRequest retryRequest = copyIndexRequest(originalRequest);
            retryRequest.onRetry();
            shards.index(retryRequest);
            IndexShard replica = shards.addReplica();
            shards.recoverReplica(replica);
            shards.assertAllEqual(1);
            shards.index(originalRequest); // original append-only arrives after recovery completed
            shards.assertAllEqual(1);
            assertThat(replica.getMaxSeenAutoIdTimestamp(), equalTo(originalRequest.getAutoGeneratedTimestamp()));
        }
    }

    public void testAppendOnlyRecoveryThenReplication() throws Exception {
        CountDownLatch indexedOnPrimary = new CountDownLatch(1);
        CountDownLatch recoveryDone = new CountDownLatch(1);
        try (ReplicationGroup shards = new ReplicationGroup(buildIndexMetadata(1)) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                return config -> new InternalEngine(config) {
                    @Override
                    public IndexResult index(Index op) throws IOException {
                        IndexResult result = super.index(op);
                        if (op.origin() == Operation.Origin.PRIMARY) {
                            indexedOnPrimary.countDown();
                            // prevent the indexing on the primary from returning (it was added to Lucene and translog already)
                            // to make sure that this operation is replicated to the replica via recovery, then via replication.
                            try {
                                recoveryDone.await();
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                        }
                        return result;
                    }
                };
            }
        }) {
            shards.startAll();
            Thread thread = new Thread(() -> {
                IndexRequest indexRequest = new IndexRequest(index.getName()).source("{}", XContentType.JSON);
                try {
                    shards.index(indexRequest);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
            IndexShard replica = shards.addReplica();
            Future<Void> fut = shards.asyncRecoverReplica(replica,
                (shard, node) -> new RecoveryTarget(shard, node, recoveryListener) {
                    @Override
                    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
                        try {
                            indexedOnPrimary.await();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        super.prepareForTranslogOperations(totalTranslogOps, listener);
                    }
                });
            fut.get();
            recoveryDone.countDown();
            thread.join();
            shards.assertAllEqual(1);
        }
    }

    public void testInheritMaxValidAutoIDTimestampOnRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            final IndexRequest indexRequest = new IndexRequest(index.getName()).source("{}", XContentType.JSON);
            indexRequest.onRetry(); // force an update of the timestamp
            final BulkItemResponse response = shards.index(indexRequest);
            assertEquals(DocWriteResponse.Result.CREATED, response.getResponse().getResult());
            if (randomBoolean()) { // lets check if that also happens if no translog record is replicated
                shards.flush();
            }
            IndexShard replica = shards.addReplica();
            shards.recoverReplica(replica);

            SegmentsStats segmentsStats = replica.segmentStats(false, false);
            SegmentsStats primarySegmentStats = shards.getPrimary().segmentStats(false, false);
            assertNotEquals(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, primarySegmentStats.getMaxUnsafeAutoIdTimestamp());
            assertEquals(primarySegmentStats.getMaxUnsafeAutoIdTimestamp(), segmentsStats.getMaxUnsafeAutoIdTimestamp());
            assertNotEquals(Long.MAX_VALUE, segmentsStats.getMaxUnsafeAutoIdTimestamp());
        }
    }

    public void testCheckpointsAdvance() throws Exception {
        try (ReplicationGroup shards = createGroup(randomInt(3))) {
            shards.startPrimary();
            int numDocs = 0;
            int startedShards;
            do {
                numDocs += shards.indexDocs(randomInt(20));
                startedShards = shards.startReplicas(randomIntBetween(1, 2));
            } while (startedShards > 0);

            for (IndexShard shard : shards) {
                final SeqNoStats shardStats = shard.seqNoStats();
                final ShardRouting shardRouting = shard.routingEntry();
                assertThat(shardRouting + " local checkpoint mismatch", shardStats.getLocalCheckpoint(), equalTo(numDocs - 1L));
                /*
                 * After the last indexing operation completes, the primary will advance its global checkpoint. Without another indexing
                 * operation, or a background sync, the primary will not have broadcast this global checkpoint to its replicas. However, a
                 * shard could have recovered from the primary in which case its global checkpoint will be in-sync with the primary.
                 * Therefore, we can only assert that the global checkpoint is number of docs minus one (matching the primary, in case of a
                 * recovery), or number of docs minus two (received indexing operations but has not received a global checkpoint sync after
                 * the last operation completed).
                 */
                final Matcher<Long> globalCheckpointMatcher;
                if (shardRouting.primary()) {
                    globalCheckpointMatcher = numDocs == 0 ? equalTo(SequenceNumbers.NO_OPS_PERFORMED) : equalTo(numDocs - 1L);
                } else {
                    globalCheckpointMatcher = numDocs == 0 ? equalTo(SequenceNumbers.NO_OPS_PERFORMED)
                        : anyOf(equalTo(numDocs - 1L), equalTo(numDocs - 2L));
                }
                assertThat(shardRouting + " global checkpoint mismatch", shardStats.getGlobalCheckpoint(), globalCheckpointMatcher);
                assertThat(shardRouting + " max seq no mismatch", shardStats.getMaxSeqNo(), equalTo(numDocs - 1L));
            }

            // simulate a background global checkpoint sync at which point we expect the global checkpoint to advance on the replicas
            shards.syncGlobalCheckpoint();

            final long noOpsPerformed = SequenceNumbers.NO_OPS_PERFORMED;
            for (IndexShard shard : shards) {
                final SeqNoStats shardStats = shard.seqNoStats();
                final ShardRouting shardRouting = shard.routingEntry();
                assertThat(shardRouting + " local checkpoint mismatch", shardStats.getLocalCheckpoint(), equalTo(numDocs - 1L));
                assertThat(
                        shardRouting + " global checkpoint mismatch",
                        shardStats.getGlobalCheckpoint(),
                        numDocs == 0 ? equalTo(noOpsPerformed) : equalTo(numDocs - 1L));
                assertThat(shardRouting + " max seq no mismatch", shardStats.getMaxSeqNo(), equalTo(numDocs - 1L));
            }
        }
    }

    public void testConflictingOpsOnReplica() throws Exception {
        String mappings = "{ \"_doc\": { \"properties\": { \"f\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = new ReplicationGroup(buildIndexMetadata(2, mappings))) {
            shards.startAll();
            List<IndexShard> replicas = shards.getReplicas();
            IndexShard replica1 = replicas.get(0);
            IndexRequest indexRequest = new IndexRequest(index.getName()).id("1").source("{ \"f\": \"1\"}", XContentType.JSON);
            logger.info("--> isolated replica " + replica1.routingEntry());
            BulkShardRequest replicationRequest = indexOnPrimary(indexRequest, shards.getPrimary());
            for (int i = 1; i < replicas.size(); i++) {
                indexOnReplica(replicationRequest, shards, replicas.get(i));
            }

            logger.info("--> promoting replica to primary " + replica1.routingEntry());
            shards.promoteReplicaToPrimary(replica1).get();
            indexRequest = new IndexRequest(index.getName()).id("1").source("{ \"f\": \"2\"}", XContentType.JSON);
            shards.index(indexRequest);
            shards.refresh("test");
            for (IndexShard shard : shards) {
                try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                    TopDocs search = searcher.search(new TermQuery(new Term("f", "2")), 10);
                    assertEquals("shard " + shard.routingEntry() + " misses new version", 1, search.totalHits.value);
                }
            }
        }
    }

    public void testReplicaTermIncrementWithConcurrentPrimaryPromotion() throws Exception {
        String mappings = "{ \"_doc\": { \"properties\": { \"f\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = new ReplicationGroup(buildIndexMetadata(2, mappings))) {
            shards.startAll();
            long primaryPrimaryTerm = shards.getPrimary().getPendingPrimaryTerm();
            List<IndexShard> replicas = shards.getReplicas();
            IndexShard replica1 = replicas.get(0);
            IndexShard replica2 = replicas.get(1);

            shards.promoteReplicaToPrimary(replica1, (shard, listener) -> {});
            long newReplica1Term = replica1.getPendingPrimaryTerm();
            assertEquals(primaryPrimaryTerm + 1, newReplica1Term);

            assertEquals(primaryPrimaryTerm, replica2.getPendingPrimaryTerm());

            IndexRequest indexRequest = new IndexRequest(index.getName()).id("1").source("{ \"f\": \"1\"}", XContentType.JSON);
            BulkShardRequest replicationRequest = indexOnPrimary(indexRequest, replica1);

            CyclicBarrier barrier = new CyclicBarrier(2);
            Thread t1 = new Thread(() -> {
                try {
                    barrier.await();
                    indexOnReplica(replicationRequest, shards, replica2, newReplica1Term);
                } catch (IllegalStateException ise) {
                    assertThat(ise.getMessage(), either(containsString("is too old"))
                        .or(containsString("cannot be a replication target")).or(containsString("engine is closed")));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread t2 = new Thread(() -> {
                try {
                    barrier.await();
                    shards.promoteReplicaToPrimary(replica2).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t2.start();
            t1.start();
            t1.join();
            t2.join();

            assertEquals(newReplica1Term + 1, replica2.getPendingPrimaryTerm());
        }
    }

    public void testReplicaOperationWithConcurrentPrimaryPromotion() throws Exception {
        String mappings = "{ \"_doc\": { \"properties\": { \"f\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = new ReplicationGroup(buildIndexMetadata(1, mappings))) {
            shards.startAll();
            long primaryPrimaryTerm = shards.getPrimary().getPendingPrimaryTerm();
            IndexRequest indexRequest = new IndexRequest(index.getName()).id("1").source("{ \"f\": \"1\"}", XContentType.JSON);
            BulkShardRequest replicationRequest = indexOnPrimary(indexRequest, shards.getPrimary());

            List<IndexShard> replicas = shards.getReplicas();
            IndexShard replica = replicas.get(0);

            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicBoolean successFullyIndexed = new AtomicBoolean();
            Thread t1 = new Thread(() -> {
                try {
                    barrier.await();
                    indexOnReplica(replicationRequest, shards, replica, primaryPrimaryTerm);
                    successFullyIndexed.set(true);
                } catch (IllegalStateException ise) {
                    assertThat(ise.getMessage(), either(containsString("is too old"))
                        .or(containsString("cannot be a replication target")).or(containsString("engine is closed")));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread t2 = new Thread(() -> {
                try {
                    barrier.await();
                    shards.promoteReplicaToPrimary(replica).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t2.start();
            t1.start();
            t1.join();
            t2.join();

            assertEquals(primaryPrimaryTerm + 1, replica.getPendingPrimaryTerm());
            if (successFullyIndexed.get()) {
                try(Translog.Snapshot snapshot = getTranslog(replica).newSnapshot()) {
                    assertThat(snapshot.totalOperations(), equalTo(1));
                    Translog.Operation op = snapshot.next();
                    assertThat(op.primaryTerm(), equalTo(primaryPrimaryTerm));
                }
            }
        }
    }

    /**
     * test document failures (failures after seq_no generation) are added as noop operation to the translog
     * for primary and replica shards
     */
    public void testDocumentFailureReplication() throws Exception {
        final IOException indexException = new IOException("simulated indexing failure");
        final EngineFactory engineFactory = config -> InternalEngineTests.createInternalEngine((dir, iwc) ->
            new IndexWriter(dir, iwc) {
                @Override
                public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
                    boolean isTombstone = false;
                    for (IndexableField field : doc) {
                        if (SeqNoFieldMapper.TOMBSTONE_NAME.equals(field.name())) {
                            isTombstone = true;
                        }
                    }
                    if (isTombstone) {
                        return super.addDocument(doc); // allow to add Noop
                    } else {
                        throw indexException;
                    }
                }
            }, null, null, config);
        try (ReplicationGroup shards = new ReplicationGroup(buildIndexMetadata(0)) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) { return engineFactory; }}) {

            // start with the primary only so two first failures are replicated to replicas via recovery from the translog of the primary.
            shards.startPrimary();
            long primaryTerm = shards.getPrimary().getPendingPrimaryTerm();
            List<Translog.Operation> expectedTranslogOps = new ArrayList<>();
            BulkItemResponse indexResp = shards.index(new IndexRequest(index.getName()).id("1").source("{}", XContentType.JSON));
            assertThat(indexResp.isFailed(), equalTo(true));
            assertThat(indexResp.getFailure().getCause(), equalTo(indexException));
            expectedTranslogOps.add(new Translog.NoOp(0, primaryTerm, indexException.toString()));
            try (Translog.Snapshot snapshot = getTranslog(shards.getPrimary()).newSnapshot()) {
                assertThat(snapshot, SnapshotMatchers.containsOperationsInAnyOrder(expectedTranslogOps));
            }
            shards.assertAllEqual(0);

            int nReplica = randomIntBetween(1, 3);
            for (int i = 0; i < nReplica; i++) {
                shards.addReplica();
            }
            shards.startReplicas(nReplica);
            for (IndexShard shard : shards) {
                try (Translog.Snapshot snapshot = getTranslog(shard).newSnapshot()) {
                    // we flush at the end of peer recovery
                    if (shard.routingEntry().primary() || shard.indexSettings().isSoftDeleteEnabled() == false) {
                        assertThat(snapshot, SnapshotMatchers.containsOperationsInAnyOrder(expectedTranslogOps));
                    } else {
                        assertThat(snapshot.totalOperations(), equalTo(0));
                    }
                }
                try (Translog.Snapshot snapshot = shard.newChangesSnapshot("test", 0, Long.MAX_VALUE, false, randomBoolean())) {
                    assertThat(snapshot, SnapshotMatchers.containsOperationsInAnyOrder(expectedTranslogOps));
                }
            }
            // the failure replicated directly from the replication channel.
            indexResp = shards.index(new IndexRequest(index.getName()).id("any").source("{}", XContentType.JSON));
            assertThat(indexResp.getFailure().getCause(), equalTo(indexException));
            Translog.NoOp noop2 = new Translog.NoOp(1, primaryTerm, indexException.toString());
            expectedTranslogOps.add(noop2);

            for (IndexShard shard : shards) {
                try (Translog.Snapshot snapshot = getTranslog(shard).newSnapshot()) {
                    if (shard.routingEntry().primary() || shard.indexSettings().isSoftDeleteEnabled() == false) {
                        assertThat(snapshot, SnapshotMatchers.containsOperationsInAnyOrder(expectedTranslogOps));
                    } else {
                        assertThat(snapshot, SnapshotMatchers.containsOperationsInAnyOrder(Collections.singletonList(noop2)));
                    }
                }
                try (Translog.Snapshot snapshot = shard.newChangesSnapshot("test", 0, Long.MAX_VALUE, false, randomBoolean())) {
                    assertThat(snapshot, SnapshotMatchers.containsOperationsInAnyOrder(expectedTranslogOps));
                }
            }
            shards.assertAllEqual(0);
        }
    }

    /**
     * test request failures (failures before seq_no generation) are not added as a noop to translog
     */
    public void testRequestFailureReplication() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            BulkItemResponse response = shards.index(
                    new IndexRequest(index.getName()).id("1")
                            .source("{}", XContentType.JSON)
                            .version(2)
            );
            assertTrue(response.isFailed());
            assertThat(response.getFailure().getCause(), instanceOf(VersionConflictEngineException.class));
            shards.assertAllEqual(0);
            for (IndexShard indexShard : shards) {
                assertThat(indexShard.routingEntry() + " has the wrong number of ops in the translog",
                    indexShard.translogStats().estimatedNumberOfOperations(), equalTo(0));
            }

            // add some replicas
            int nReplica = randomIntBetween(1, 3);
            for (int i = 0; i < nReplica; i++) {
                shards.addReplica();
            }
            shards.startReplicas(nReplica);
            response = shards.index(
                    new IndexRequest(index.getName()).id("1")
                            .source("{}", XContentType.JSON)
                            .version(2)
            );
            assertTrue(response.isFailed());
            assertThat(response.getFailure().getCause(), instanceOf(VersionConflictEngineException.class));
            shards.assertAllEqual(0);
            for (IndexShard indexShard : shards) {
                assertThat(indexShard.routingEntry() + " has the wrong number of ops in the translog",
                    indexShard.translogStats().estimatedNumberOfOperations(), equalTo(0));
            }
        }
    }

    public void testSeqNoCollision() throws Exception {
        try (ReplicationGroup shards = createGroup(2)) {
            shards.startAll();
            int initDocs = shards.indexDocs(randomInt(10));
            List<IndexShard> replicas = shards.getReplicas();
            IndexShard replica1 = replicas.get(0);
            IndexShard replica2 = replicas.get(1);
            shards.syncGlobalCheckpoint();

            logger.info("--> Isolate replica1");
            IndexRequest indexDoc1 = new IndexRequest(index.getName()).id("d1").source("{}", XContentType.JSON);
            BulkShardRequest replicationRequest = indexOnPrimary(indexDoc1, shards.getPrimary());
            indexOnReplica(replicationRequest, shards, replica2);

            final Translog.Operation op1;
            final List<Translog.Operation> initOperations = new ArrayList<>(initDocs);
            try (Translog.Snapshot snapshot = getTranslog(replica2).newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(initDocs + 1));
                for (int i = 0; i < initDocs; i++) {
                    Translog.Operation op = snapshot.next();
                    assertThat(op, is(notNullValue()));
                    initOperations.add(op);
                }
                op1 = snapshot.next();
                assertThat(op1, notNullValue());
                assertThat(snapshot.next(), nullValue());
                assertThat(snapshot.skippedOperations(), equalTo(0));
            }
            logger.info("--> Promote replica1 as the primary");
            shards.promoteReplicaToPrimary(replica1).get(); // wait until resync completed.
            shards.index(new IndexRequest(index.getName()).id("d2").source("{}", XContentType.JSON));
            final Translog.Operation op2;
            try (Translog.Snapshot snapshot = getTranslog(replica2).newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(1));
                op2 = snapshot.next();
                assertThat(op2.seqNo(), equalTo(op1.seqNo()));
                assertThat(op2.primaryTerm(), greaterThan(op1.primaryTerm()));
                assertNull(snapshot.next());
                assertThat(snapshot.skippedOperations(), equalTo(0));
            }

            // Make sure that peer-recovery transfers all but non-overridden operations.
            IndexShard replica3 = shards.addReplica();
            logger.info("--> Promote replica2 as the primary");
            shards.promoteReplicaToPrimary(replica2).get();
            logger.info("--> Recover replica3 from replica2");
            recoverReplica(replica3, replica2, true);
            try (Translog.Snapshot snapshot = replica3.newChangesSnapshot("test", 0, Long.MAX_VALUE, false, randomBoolean())) {
                assertThat(snapshot.totalOperations(), equalTo(initDocs + 1));
                final List<Translog.Operation> expectedOps = new ArrayList<>(initOperations);
                expectedOps.add(op2);
                assertThat(snapshot, containsOperationsInAnyOrder(expectedOps));
                assertThat("Peer-recovery should not send overridden operations", snapshot.skippedOperations(), equalTo(0));
            }
            shards.assertAllEqual(initDocs + 1);
        }
    }

    /**
     * This test ensures the consistency between primary and replica with late and out of order delivery on the replica.
     * An index operation on the primary is followed by a delete operation. The delete operation is delivered first
     * and processed on the replica but the index is delayed with an interval that is even longer the gc deletes cycle.
     * This makes sure that that replica still remembers the delete operation and correctly ignores the stale index operation.
     */
    public void testLateDeliveryAfterGCTriggeredOnReplica() throws Exception {
        ThreadPool.terminate(this.threadPool, 10, TimeUnit.SECONDS);
        this.threadPool = new TestThreadPool(getClass().getName(),
            Settings.builder().put(threadPoolSettings()).put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0).build());

        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final TimeValue gcInterval = TimeValue.timeValueMillis(between(1, 10));
            // I think we can just set this to something very small (10ms?) and also set ThreadPool#ESTIMATED_TIME_INTERVAL_SETTING to 0?

            updateGCDeleteCycle(replica, gcInterval);
            final BulkShardRequest indexRequest = indexOnPrimary(
                new IndexRequest(index.getName()).id("d1").source("{}", XContentType.JSON), primary);
            final BulkShardRequest deleteRequest = deleteOnPrimary(new DeleteRequest(index.getName()).id("d1"), primary);
            deleteOnReplica(deleteRequest, shards, replica); // delete arrives on replica first.
            final long deleteTimestamp = threadPool.relativeTimeInMillis();
            replica.refresh("test");
            assertBusy(() ->
                assertThat(threadPool.relativeTimeInMillis() - deleteTimestamp, greaterThan(gcInterval.millis()))
            );
            getEngine(replica).maybePruneDeletes();
            indexOnReplica(indexRequest, shards, replica);  // index arrives on replica lately.
            shards.assertAllEqual(0);
        }
    }

    private void updateGCDeleteCycle(IndexShard shard, TimeValue interval) {
        IndexMetadata.Builder builder = IndexMetadata.builder(shard.indexSettings().getIndexMetadata());
        builder.settings(Settings.builder()
            .put(shard.indexSettings().getSettings())
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), interval.getStringRep())
        );
        shard.indexSettings().updateIndexMetadata(builder.build());
        shard.onSettingsChanged();
    }

    /**
     * This test ensures the consistency between primary and replica when non-append-only (eg. index request with id or delete) operation
     * of the same document is processed before the original append-only request on replicas. The append-only document can be exposed and
     * deleted on the primary before it is added to replica. Replicas should treat a late append-only request as a regular index request.
     */
    public void testOutOfOrderDeliveryForAppendOnlyOperations() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            // Append-only request - without id
            final BulkShardRequest indexRequest = indexOnPrimary(
                new IndexRequest(index.getName()).source("{}", XContentType.JSON), primary);
            final String docId = Iterables.get(getShardDocUIDs(primary), 0);
            final BulkShardRequest deleteRequest = deleteOnPrimary(new DeleteRequest(index.getName()).id(docId), primary);
            deleteOnReplica(deleteRequest, shards, replica);
            indexOnReplica(indexRequest, shards, replica);
            shards.assertAllEqual(0);
        }
    }

    public void testIndexingOptimizationUsingSequenceNumbers() throws Exception {
        final Set<String> liveDocs = new HashSet<>();
        try (ReplicationGroup group = createGroup(2)) {
            group.startAll();
            int numDocs = randomIntBetween(1, 100);
            long versionLookups = 0;
            for (int i = 0; i < numDocs; i++) {
                String id = Integer.toString(randomIntBetween(1, 100));
                if (randomBoolean()) {
                    group.index(new IndexRequest(index.getName()).id(id).source("{}", XContentType.JSON));
                    if (liveDocs.add(id) == false) {
                        versionLookups++;
                    }
                } else {
                    group.delete(new DeleteRequest(index.getName()).id(id));
                    liveDocs.remove(id);
                    versionLookups++;
                }
            }
            for (IndexShard replica : group.getReplicas()) {
                assertThat(EngineTestCase.getNumVersionLookups(getEngine(replica)), equalTo(versionLookups));
            }
        }
    }
}
