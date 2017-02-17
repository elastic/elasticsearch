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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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
            shards.getPrimary().updateGlobalCheckpointOnPrimary();
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

                final boolean sync = randomBoolean();
                if (sync) {
                    shards.getPrimary().updateGlobalCheckpointOnPrimary();
                }
            }

            shards.removeReplica(originalReplica);

            final int missingOnReplica = shards.indexDocs(randomInt(5));
            docs += missingOnReplica;
            replicaHasDocsSinceLastFlushedCheckpoint |= missingOnReplica > 0;

            if (randomBoolean()) {
                shards.getPrimary().updateGlobalCheckpointOnPrimary();
            }

            final boolean flushPrimary = randomBoolean();
            if (flushPrimary) {
                shards.flush();
            }

            originalReplica.close("disconnected", false);
            IOUtils.close(originalReplica.store());
            final IndexShard recoveredReplica =
                shards.addReplicaWithExistingPath(originalReplica.shardPath(), originalReplica.routingEntry().currentNodeId());
            shards.recoverReplica(recoveredReplica);
            if (flushPrimary && replicaHasDocsSinceLastFlushedCheckpoint) {
                // replica has something to catch up with, but since we flushed the primary, we should fall back to full recovery
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
            // we need some indexing to happen to transfer local checkpoint information to the primary
            // so it can update the global checkpoint and communicate to replicas
            boolean expectSeqNoRecovery = totalDocs > 0;


            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard newPrimary = shards.getReplicas().get(0);
            final IndexShard replica = shards.getReplicas().get(1);
            if (randomBoolean()) {
                // simulate docs that were inflight when primary failed, these will be rolled back
                final int rollbackDocs = randomIntBetween(1, 5);
                logger.info("--> indexing {} rollback docs", rollbackDocs);
                for (int i = 0; i < rollbackDocs; i++) {
                    final IndexRequest indexRequest = new IndexRequest(index.getName(), "type", "rollback_" + i)
                            .source("{}", XContentType.JSON);
                    final IndexResponse primaryResponse = indexOnPrimary(indexRequest, oldPrimary);
                    indexOnReplica(primaryResponse, indexRequest, replica);
                }
                if (randomBoolean()) {
                    oldPrimary.flush(new FlushRequest(index.getName()));
                    expectSeqNoRecovery = false;
                }
            }

            shards.promoteReplicaToPrimary(newPrimary);
            // index some more
            totalDocs += shards.indexDocs(randomIntBetween(0, 5));

            oldPrimary.close("demoted", false);
            oldPrimary.store().close();

            IndexShard newReplica = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            if (expectSeqNoRecovery) {
                assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
                assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(totalDocs - committedDocs));
            } else {
                assertThat(newReplica.recoveryState().getIndex().fileDetails(), not(empty()));
                assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(totalDocs - committedDocs));
            }

            shards.removeReplica(replica);
            replica.close("resync", false);
            replica.store().close();
            newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            shards.assertAllEqual(totalDocs);
        }
    }

    @TestLogging("_root:DEBUG,org.elasticsearch.action.bulk:TRACE,org.elasticsearch.action.get:TRACE," +
        "org.elasticsearch.discovery:TRACE," +
        "org.elasticsearch.cluster.service:TRACE,org.elasticsearch.indices.recovery:TRACE," +
        "org.elasticsearch.indices.cluster:TRACE,org.elasticsearch.index.shard:TRACE," +
        "org.elasticsearch.index.seqno:TRACE"
    )
    public void testWaitForPendingSeqNo() throws Exception {
        IndexMetaData metaData = buildIndexMetaData(1);

        final int pendingDocs = randomIntBetween(1, 5);
        final AtomicReference<Semaphore> blockIndexingOnPrimary = new AtomicReference<>();
        final CountDownLatch blockedIndexers = new CountDownLatch(pendingDocs);

        try (ReplicationGroup shards = new ReplicationGroup(metaData) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                if (routing.primary()) {
                    return new EngineFactory() {
                        @Override
                        public Engine newReadWriteEngine(EngineConfig config) {
                            return InternalEngineTests.createInternalEngine((directory, writerConfig) ->
                                new IndexWriter(directory, writerConfig) {
                                    @Override
                                    public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
                                        Semaphore block = blockIndexingOnPrimary.get();
                                        if (block != null) {
                                            blockedIndexers.countDown();
                                            try {
                                                block.acquire();
                                            } catch (InterruptedException e) {
                                                throw new AssertionError("unexpectedly interrupted", e);
                                            }
                                        }
                                        return super.addDocument(doc);
                                    }

                                }, null, config);
                        }

                        @Override
                        public Engine newReadOnlyEngine(EngineConfig config) {
                            throw new UnsupportedOperationException();
                        }
                    };
                } else {
                    return null;
                }
            }
        }) {
            shards.startAll();
            int docs = shards.indexDocs(randomIntBetween(1,10));
            IndexShard replica = shards.getReplicas().get(0);
            shards.removeReplica(replica);
            closeShards(replica);

            docs += pendingDocs;
            final Semaphore pendingDocsSemaphore = new Semaphore(pendingDocs);
            blockIndexingOnPrimary.set(pendingDocsSemaphore);
            blockIndexingOnPrimary.get().acquire(pendingDocs);
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
            blockedIndexers.await();

            blockIndexingOnPrimary.set(null);
            // index some more
            docs += shards.indexDocs(randomInt(5));

            IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());

            CountDownLatch recoveryStart = new CountDownLatch(1);
            AtomicBoolean preparedForTranslog = new AtomicBoolean(false);
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(newReplica, (indexShard, node) -> {
                recoveryStart.countDown();
                return new RecoveryTarget(indexShard, node, recoveryListener, l -> {
                }) {
                    @Override
                    public void prepareForTranslogOperations(int totalTranslogOps, long maxUnsafeAutoIdTimestamp) throws IOException {
                        preparedForTranslog.set(true);
                        super.prepareForTranslogOperations(totalTranslogOps, maxUnsafeAutoIdTimestamp);
                    }
                };
            });

            recoveryStart.await();

            for (int i = 0; i < pendingDocs; i++) {
                assertFalse((pendingDocs - i) + " pending operations, recovery should wait", preparedForTranslog.get());
                pendingDocsSemaphore.release();
            }

            pendingDocsDone.await();

            // now recovery can finish
            recoveryFuture.get();

            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
            assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(docs));

            shards.assertAllEqual(docs);
        }
    }

    private static class BlockingTarget extends RecoveryTarget {

        private final CountDownLatch recoveryBlocked;
        private final CountDownLatch releaseRecovery;
        private final RecoveryState.Stage stageToBlock;
        static final EnumSet<RecoveryState.Stage> SUPPORTED_STAGES =
            EnumSet.of(RecoveryState.Stage.INDEX, RecoveryState.Stage.TRANSLOG, RecoveryState.Stage.FINALIZE);
        private final Logger logger;

        BlockingTarget(RecoveryState.Stage stageToBlock, CountDownLatch recoveryBlocked, CountDownLatch releaseRecovery, IndexShard shard,
                       DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener, Logger logger) {
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
        public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) {
            if (hasBlocked() == false) {
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            super.indexTranslogOperations(operations, totalTranslogOps);
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

}
