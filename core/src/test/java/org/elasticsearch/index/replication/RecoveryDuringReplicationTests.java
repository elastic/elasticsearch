/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.replication;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class RecoveryDuringReplicationTests extends ESIndexLevelReplicationTestCase {

    public void testIndexingDuringFileRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(randomInt(1))) {
            shards.startAll();
            int docs = shards.indexDocs(randomInt(50));
            shards.flush();
            IndexShard replica = shards.addReplica();
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);
            final RecoveryState.Stage blockOnStage = randomFrom(BlockingTarget.SUPPORTED_STAGES);
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(replica, (indexShard, node) ->
                new BlockingTarget(blockOnStage, recoveryBlocked, releaseRecovery, indexShard, node, recoveryListener, logger));

            recoveryBlocked.await();
            docs += shards.indexDocs(randomInt(20));
            releaseRecovery.countDown();
            recoveryFuture.get();

            shards.assertAllEqual(docs);
        }
    }

    public void testRecoveryOfDisconnectedReplica() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            int docs = shards.indexDocs(randomInt(50));
            shards.flush();
            final IndexShard originalReplica = shards.getReplicas().get(0);
            long replicaCommittedLocalCheckpoint = docs - 1;
            boolean replicaHasDocsSinceLastFlushedCheckpoint = false;
            for (int i = 0; i < randomInt(2); i++) {
                final int indexedDocs = shards.indexDocs(randomInt(5));
                docs += indexedDocs;
                if (indexedDocs > 0) {
                    replicaHasDocsSinceLastFlushedCheckpoint = true;
                }

                final boolean flush = randomBoolean();
                if (flush) {
                    originalReplica.flush(new FlushRequest());
                    replicaHasDocsSinceLastFlushedCheckpoint = false;
                    replicaCommittedLocalCheckpoint = docs - 1;
                }
            }

            // simulate a background global checkpoint sync at which point we expect the global checkpoint to advance on the replicas
            shards.syncGlobalCheckpoint();

            shards.removeReplica(originalReplica);

            final int missingOnReplica = shards.indexDocs(randomInt(5));
            docs += missingOnReplica;
            replicaHasDocsSinceLastFlushedCheckpoint |= missingOnReplica > 0;

            final boolean translogTrimmed;
            if (randomBoolean()) {
                shards.flush();
                translogTrimmed = randomBoolean();
                if (translogTrimmed) {
                    final Translog translog = shards.getPrimary().getTranslog();
                    translog.getDeletionPolicy().setRetentionAgeInMillis(0);
                    translog.trimUnreferencedReaders();
                }
            } else {
                translogTrimmed = false;
            }
            originalReplica.close("disconnected", false);
            IOUtils.close(originalReplica.store());
            final IndexShard recoveredReplica =
                shards.addReplicaWithExistingPath(originalReplica.shardPath(), originalReplica.routingEntry().currentNodeId());
            shards.recoverReplica(recoveredReplica);
            if (translogTrimmed && replicaHasDocsSinceLastFlushedCheckpoint) {
                // replica has something to catch up with, but since we trimmed the primary translog, we should fall back to full recovery
                assertThat(recoveredReplica.recoveryState().getIndex().fileDetails(), not(empty()));
            } else {
                assertThat(recoveredReplica.recoveryState().getIndex().fileDetails(), empty());
                assertThat(
                    recoveredReplica.recoveryState().getTranslog().recoveredOperations(),
                    equalTo(Math.toIntExact(docs - (replicaCommittedLocalCheckpoint + 1))));
            }

            docs += shards.indexDocs(randomInt(5));

            shards.assertAllEqual(docs);
        }
    }

    /*
     * Simulate a scenario with two replicas where one of the replicas receives an extra document, the other replica is promoted on primary
     * failure, the receiving replica misses the primary/replica re-sync and then recovers from the primary. We expect that a
     * sequence-number based recovery is performed and the extra document does not remain after recovery.
     */
    public void testRecoveryToReplicaThatReceivedExtraDocument() throws Exception {
        try (ReplicationGroup shards = createGroup(2)) {
            shards.startAll();
            final int docs = randomIntBetween(0, 16);
            for (int i = 0; i < docs; i++) {
                shards.index(
                        new IndexRequest("index", "type", Integer.toString(i)).source("{}", XContentType.JSON));
            }

            shards.flush();
            shards.syncGlobalCheckpoint();

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard promotedReplica = shards.getReplicas().get(0);
            final IndexShard remainingReplica = shards.getReplicas().get(1);
            // slip the extra document into the replica
            remainingReplica.applyIndexOperationOnReplica(
                    remainingReplica.getLocalCheckpoint() + 1,
                    1,
                    VersionType.EXTERNAL,
                    randomNonNegativeLong(),
                    false,
                    SourceToParse.source("index", "type", "replica", new BytesArray("{}"), XContentType.JSON),
                    mapping -> {});
            shards.promoteReplicaToPrimary(promotedReplica);
            oldPrimary.close("demoted", randomBoolean());
            oldPrimary.store().close();
            shards.removeReplica(remainingReplica);
            remainingReplica.close("disconnected", false);
            remainingReplica.store().close();
            // randomly introduce a conflicting document
            final boolean extra = randomBoolean();
            if (extra) {
                promotedReplica.applyIndexOperationOnPrimary(
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        SourceToParse.source("index", "type", "primary", new BytesArray("{}"), XContentType.JSON),
                        randomNonNegativeLong(),
                        false,
                        mapping -> {
                        });
            }
            final IndexShard recoveredReplica =
                    shards.addReplicaWithExistingPath(remainingReplica.shardPath(), remainingReplica.routingEntry().currentNodeId());
            shards.recoverReplica(recoveredReplica);

            assertThat(recoveredReplica.recoveryState().getIndex().fileDetails(), empty());
            assertThat(recoveredReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(extra ? 1 : 0));

            shards.assertAllEqual(docs + (extra ? 1 : 0));
        }
    }

    @TestLogging("org.elasticsearch.index.shard:TRACE,org.elasticsearch.indices.recovery:TRACE")
    public void testRecoveryAfterPrimaryPromotion() throws Exception {
        try (ReplicationGroup shards = createGroup(2)) {
            shards.startAll();
            int totalDocs = shards.indexDocs(randomInt(10));
            int committedDocs = 0;
            if (randomBoolean()) {
                shards.flush();
                committedDocs = totalDocs;
            }

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard newPrimary = shards.getReplicas().get(0);
            final IndexShard replica = shards.getReplicas().get(1);
            boolean expectSeqNoRecovery = true;
            if (randomBoolean()) {
                // simulate docs that were inflight when primary failed, these will be rolled back
                final int rollbackDocs = randomIntBetween(1, 5);
                logger.info("--> indexing {} rollback docs", rollbackDocs);
                for (int i = 0; i < rollbackDocs; i++) {
                    final IndexRequest indexRequest = new IndexRequest(index.getName(), "type", "rollback_" + i)
                            .source("{}", XContentType.JSON);
                    final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                    indexOnReplica(bulkShardRequest, replica);
                }
                if (randomBoolean()) {
                    oldPrimary.flush(new FlushRequest(index.getName()));
                    expectSeqNoRecovery = false;
                }
            }

            shards.promoteReplicaToPrimary(newPrimary);

            // check that local checkpoint of new primary is properly tracked after primary promotion
            assertThat(newPrimary.getLocalCheckpoint(), equalTo(totalDocs - 1L));
            assertThat(IndexShardTestCase.getEngine(newPrimary).seqNoService()
                .getTrackedLocalCheckpointForShard(newPrimary.routingEntry().allocationId().getId()), equalTo(totalDocs - 1L));

            // index some more
            totalDocs += shards.indexDocs(randomIntBetween(0, 5));

            if (randomBoolean()) {
                newPrimary.flush(new FlushRequest());
            }

            oldPrimary.close("demoted", false);
            oldPrimary.store().close();

            IndexShard newReplica = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            if (expectSeqNoRecovery) {
                assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
                assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(totalDocs - committedDocs));
            } else {
                assertThat(newReplica.recoveryState().getIndex().fileDetails(), not(empty()));
                assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(totalDocs));
            }

            // roll back the extra ops in the replica
            shards.removeReplica(replica);
            replica.close("resync", false);
            replica.store().close();
            newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            shards.assertAllEqual(totalDocs);
        }
    }

    @TestLogging("org.elasticsearch.index.shard:TRACE,org.elasticsearch.action.resync:TRACE")
    public void testResyncAfterPrimaryPromotion() throws Exception {
        // TODO: check translog trimming functionality once it's implemented
        try (ReplicationGroup shards = createGroup(2)) {
            shards.startAll();
            int initialDocs = shards.indexDocs(randomInt(10));
            boolean syncedGlobalCheckPoint = randomBoolean();
            if (syncedGlobalCheckPoint) {
                shards.syncGlobalCheckpoint();
            }

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard newPrimary = shards.getReplicas().get(0);

            // simulate docs that were inflight when primary failed
            final int extraDocs = randomIntBetween(0, 5);
            logger.info("--> indexing {} extra docs", extraDocs);
            for (int i = 0; i < extraDocs; i++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type", "extra_" + i)
                    .source("{}", XContentType.JSON);
                final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                indexOnReplica(bulkShardRequest, newPrimary);
            }
            logger.info("--> resyncing replicas");
            PrimaryReplicaSyncer.ResyncTask task = shards.promoteReplicaToPrimary(newPrimary).get();
            if (syncedGlobalCheckPoint) {
                assertEquals(extraDocs, task.getResyncedOperations());
            } else {
                assertThat(task.getResyncedOperations(), greaterThanOrEqualTo(extraDocs));
            }
            shards.assertAllEqual(initialDocs + extraDocs);
        }
    }

    @TestLogging(
            "_root:DEBUG,"
                    + "org.elasticsearch.action.bulk:TRACE,"
                    + "org.elasticsearch.action.get:TRACE,"
                    + "org.elasticsearch.cluster.service:TRACE,"
                    + "org.elasticsearch.discovery:TRACE,"
                    + "org.elasticsearch.indices.cluster:TRACE,"
                    + "org.elasticsearch.indices.recovery:TRACE,"
                    + "org.elasticsearch.index.seqno:TRACE,"
                    + "org.elasticsearch.index.shard:TRACE")
    public void testWaitForPendingSeqNo() throws Exception {
        IndexMetaData metaData = buildIndexMetaData(1);

        final int pendingDocs = randomIntBetween(1, 5);
        final BlockingEngineFactory primaryEngineFactory = new BlockingEngineFactory();

        try (ReplicationGroup shards = new ReplicationGroup(metaData) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                if (routing.primary()) {
                    return primaryEngineFactory;
                } else {
                    return null;
                }
            }
        }) {
            shards.startAll();
            int docs = shards.indexDocs(randomIntBetween(1, 10));
            // simulate a background global checkpoint sync at which point we expect the global checkpoint to advance on the replicas
            shards.syncGlobalCheckpoint();
            IndexShard replica = shards.getReplicas().get(0);
            shards.removeReplica(replica);
            closeShards(replica);

            docs += pendingDocs;
            primaryEngineFactory.latchIndexers(pendingDocs);
            CountDownLatch pendingDocsDone = new CountDownLatch(pendingDocs);
            for (int i = 0; i < pendingDocs; i++) {
                final String id = "pending_" + i;
                threadPool.generic().submit(() -> {
                    try {
                        shards.index(new IndexRequest(index.getName(), "type", id).source("{}", XContentType.JSON));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    } finally {
                        pendingDocsDone.countDown();
                    }
                });
            }

            // wait for the pending ops to "hang"
            primaryEngineFactory.awaitIndexersLatch();

            primaryEngineFactory.allowIndexing();
            // index some more
            docs += shards.indexDocs(randomInt(5));

            IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());

            CountDownLatch recoveryStart = new CountDownLatch(1);
            AtomicBoolean opsSent = new AtomicBoolean(false);
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(newReplica, (indexShard, node) -> {
                recoveryStart.countDown();
                return new RecoveryTarget(indexShard, node, recoveryListener, l -> {
                }) {
                    @Override
                    public long indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) throws IOException {
                        opsSent.set(true);
                        return super.indexTranslogOperations(operations, totalTranslogOps);
                    }
                };
            });

            recoveryStart.await();

            // index some more
            final int indexedDuringRecovery = shards.indexDocs(randomInt(5));
            docs += indexedDuringRecovery;

            assertFalse("recovery should wait on pending docs", opsSent.get());

            primaryEngineFactory.releaseLatchedIndexers();
            pendingDocsDone.await();

            // now recovery can finish
            recoveryFuture.get();

            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
            assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(),
                // we don't know which of the inflight operations made it into the translog range we re-play
                both(greaterThanOrEqualTo(docs-indexedDuringRecovery)).and(lessThanOrEqualTo(docs)));

            shards.assertAllEqual(docs);
        } finally {
            primaryEngineFactory.close();
        }
    }

    @TestLogging(
            "_root:DEBUG,"
                    + "org.elasticsearch.action.bulk:TRACE,"
                    + "org.elasticsearch.action.get:TRACE,"
                    + "org.elasticsearch.cluster.service:TRACE,"
                    + "org.elasticsearch.discovery:TRACE,"
                    + "org.elasticsearch.indices.cluster:TRACE,"
                    + "org.elasticsearch.indices.recovery:TRACE,"
                    + "org.elasticsearch.index.seqno:TRACE,"
                    + "org.elasticsearch.index.shard:TRACE")
    public void testCheckpointsAndMarkingInSync() throws Exception {
        final IndexMetaData metaData = buildIndexMetaData(0);
        final BlockingEngineFactory replicaEngineFactory = new BlockingEngineFactory();
        try (
                ReplicationGroup shards = new ReplicationGroup(metaData) {
                    @Override
                    protected EngineFactory getEngineFactory(final ShardRouting routing) {
                        if (routing.primary()) {
                            return null;
                        } else {
                            return replicaEngineFactory;
                        }
                    }
                };
                AutoCloseable ignored = replicaEngineFactory // make sure we release indexers before closing
        ) {
            shards.startPrimary();
            final int docs = shards.indexDocs(randomIntBetween(1, 10));
            logger.info("indexed [{}] docs", docs);
            final CountDownLatch pendingDocDone = new CountDownLatch(1);
            final CountDownLatch pendingDocActiveWithExtraDocIndexed = new CountDownLatch(1);
            final CountDownLatch phaseTwoStartLatch = new CountDownLatch(1);
            final IndexShard replica = shards.addReplica();
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(
                    replica,
                    (indexShard, node) -> new RecoveryTarget(indexShard, node, recoveryListener, l -> {}) {
                        @Override
                        public long indexTranslogOperations(final List<Translog.Operation> operations, final int totalTranslogOps)
                             throws IOException {
                            // index a doc which is not part of the snapshot, but also does not complete on replica
                            replicaEngineFactory.latchIndexers(1);
                            threadPool.generic().submit(() -> {
                                try {
                                    shards.index(new IndexRequest(index.getName(), "type", "pending").source("{}", XContentType.JSON));
                                } catch (final Exception e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    pendingDocDone.countDown();
                                }
                            });
                            try {
                                // the pending doc is latched in the engine
                                replicaEngineFactory.awaitIndexersLatch();
                                // unblock indexing for the next doc
                                replicaEngineFactory.allowIndexing();
                                shards.index(new IndexRequest(index.getName(), "type", "completed").source("{}", XContentType.JSON));
                                pendingDocActiveWithExtraDocIndexed.countDown();
                            } catch (final Exception e) {
                                throw new AssertionError(e);
                            }
                            try {
                                phaseTwoStartLatch.await();
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                            return super.indexTranslogOperations(operations, totalTranslogOps);
                        }
                    });
            pendingDocActiveWithExtraDocIndexed.await();
            assertThat(pendingDocDone.getCount(), equalTo(1L));
            {
                final long expectedDocs = docs + 2L;
                assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                // recovery has not completed, therefore the global checkpoint can have advanced on the primary
                assertThat(shards.getPrimary().getGlobalCheckpoint(), equalTo(expectedDocs - 1));
                // the pending document is not done, the checkpoints can not have advanced on the replica
                assertThat(replica.getLocalCheckpoint(), lessThan(expectedDocs - 1));
                assertThat(replica.getGlobalCheckpoint(), lessThan(expectedDocs - 1));
            }

            // wait for recovery to enter the translog phase
            phaseTwoStartLatch.countDown();

            // wait for the translog phase to complete and the recovery to block global checkpoint advancement
            awaitBusy(() -> shards.getPrimary().pendingInSync());
            {
                shards.index(new IndexRequest(index.getName(), "type", "last").source("{}", XContentType.JSON));
                final long expectedDocs = docs + 3L;
                assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                // recovery is now in the process of being completed, therefore the global checkpoint can not have advanced on the primary
                assertThat(shards.getPrimary().getGlobalCheckpoint(), equalTo(expectedDocs - 2));
                assertThat(replica.getLocalCheckpoint(), lessThan(expectedDocs - 2));
                assertThat(replica.getGlobalCheckpoint(), lessThan(expectedDocs - 2));
            }

            replicaEngineFactory.releaseLatchedIndexers();
            pendingDocDone.await();
            recoveryFuture.get();
            {
                final long expectedDocs = docs + 3L;
                assertBusy(() -> {
                    assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                    assertThat(shards.getPrimary().getGlobalCheckpoint(), equalTo(expectedDocs - 1));
                    assertThat(replica.getLocalCheckpoint(), equalTo(expectedDocs - 1));
                    // the global checkpoint advances can only advance here if a background global checkpoint sync fires
                    assertThat(replica.getGlobalCheckpoint(), anyOf(equalTo(expectedDocs - 1), equalTo(expectedDocs - 2)));
                });
            }
        }
    }

    public static class BlockingTarget extends RecoveryTarget {

        private final CountDownLatch recoveryBlocked;
        private final CountDownLatch releaseRecovery;
        private final RecoveryState.Stage stageToBlock;
        static final EnumSet<RecoveryState.Stage> SUPPORTED_STAGES =
            EnumSet.of(RecoveryState.Stage.INDEX, RecoveryState.Stage.TRANSLOG, RecoveryState.Stage.FINALIZE);
        private final Logger logger;

        public BlockingTarget(RecoveryState.Stage stageToBlock, CountDownLatch recoveryBlocked, CountDownLatch releaseRecovery,
                              IndexShard shard, DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener,
                              Logger logger) {
            super(shard, sourceNode, listener, version -> {});
            this.recoveryBlocked = recoveryBlocked;
            this.releaseRecovery = releaseRecovery;
            this.stageToBlock = stageToBlock;
            this.logger = logger;
            if (SUPPORTED_STAGES.contains(stageToBlock) == false) {
                throw new UnsupportedOperationException(stageToBlock + " is not supported");
            }
        }

        private boolean hasBlocked() {
            return recoveryBlocked.getCount() == 0;
        }

        private void blockIfNeeded(RecoveryState.Stage currentStage) {
            if (currentStage == stageToBlock) {
                logger.info("--> blocking recovery on stage [{}]", currentStage);
                recoveryBlocked.countDown();
                try {
                    releaseRecovery.await();
                    logger.info("--> recovery continues from stage [{}]", currentStage);
                } catch (InterruptedException e) {
                    throw new RuntimeException("blockage released");
                }
            }
        }

        @Override
        public long indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) throws IOException {
            if (hasBlocked() == false) {
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            return super.indexTranslogOperations(operations, totalTranslogOps);
        }

        @Override
        public void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException {
            blockIfNeeded(RecoveryState.Stage.INDEX);
            super.cleanFiles(totalTranslogOps, sourceMetaData);
        }

        @Override
        public void finalizeRecovery(long globalCheckpoint) {
            if (hasBlocked() == false) {
                // it maybe that not ops have been transferred, block now
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            blockIfNeeded(RecoveryState.Stage.FINALIZE);
            super.finalizeRecovery(globalCheckpoint);
        }

    }

    static class BlockingEngineFactory implements EngineFactory, AutoCloseable {

        private final List<CountDownLatch> blocks = new ArrayList<>();

        private final AtomicReference<CountDownLatch> blockReference = new AtomicReference<>();
        private final AtomicReference<CountDownLatch> blockedIndexers = new AtomicReference<>();

        public synchronized void latchIndexers(int count) {
            final CountDownLatch block = new CountDownLatch(1);
            blocks.add(block);
            blockedIndexers.set(new CountDownLatch(count));
            assert blockReference.compareAndSet(null, block);
        }

        public void awaitIndexersLatch() throws InterruptedException {
            blockedIndexers.get().await();
        }

        public synchronized void allowIndexing() {
            final CountDownLatch previous = blockReference.getAndSet(null);
            assert previous == null || blocks.contains(previous);
        }

        public synchronized void releaseLatchedIndexers() {
            allowIndexing();
            blocks.forEach(CountDownLatch::countDown);
            blocks.clear();
        }

        @Override
        public Engine newReadWriteEngine(final EngineConfig config) {
            return InternalEngineTests.createInternalEngine(
                    (directory, writerConfig) ->
                            new IndexWriter(directory, writerConfig) {
                                @Override
                                public long addDocument(final Iterable<? extends IndexableField> doc) throws IOException {
                                    final CountDownLatch block = blockReference.get();
                                    if (block != null) {
                                        final CountDownLatch latch = blockedIndexers.get();
                                        if (latch != null) {
                                            latch.countDown();
                                        }
                                        try {
                                            block.await();
                                        } catch (InterruptedException e) {
                                            throw new AssertionError(e);
                                        }
                                    }
                                    return super.addDocument(doc);
                                }
                            },
                    null,
                    null,
                    config);
        }

        @Override
        public void close() throws Exception {
            releaseLatchedIndexers();
        }

    }

}
