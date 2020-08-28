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
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocIdSeqNoAndSource;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
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
                        new IndexRequest("index").id(Integer.toString(i)).source("{}", XContentType.JSON));
            }

            shards.flush();
            shards.syncGlobalCheckpoint();

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard promotedReplica = shards.getReplicas().get(0);
            final IndexShard remainingReplica = shards.getReplicas().get(1);
            // slip the extra document into the replica
            remainingReplica.applyIndexOperationOnReplica(
                    remainingReplica.getLocalCheckpoint() + 1,
                    remainingReplica.getOperationPrimaryTerm(),
                    1,
                    randomNonNegativeLong(),
                    false,
                    new SourceToParse("index", "replica", new BytesArray("{}"), XContentType.JSON));
            shards.promoteReplicaToPrimary(promotedReplica).get();
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
                        new SourceToParse("index", "primary", new BytesArray("{}"), XContentType.JSON),
                        SequenceNumbers.UNASSIGNED_SEQ_NO, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                        false);
            }
            final IndexShard recoveredReplica =
                    shards.addReplicaWithExistingPath(remainingReplica.shardPath(), remainingReplica.routingEntry().currentNodeId());
            shards.recoverReplica(recoveredReplica);

            assertThat(recoveredReplica.recoveryState().getIndex().fileDetails(), empty());
            assertThat(recoveredReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(extra ? 1 : 0));

            shards.assertAllEqual(docs + (extra ? 1 : 0));
        }
    }

    public void testRecoveryAfterPrimaryPromotion() throws Exception {
        try (ReplicationGroup shards = createGroup(2)) {
            shards.startAll();
            int totalDocs = shards.indexDocs(randomInt(10));
            shards.syncGlobalCheckpoint();
            if (randomBoolean()) {
                shards.flush();
            }

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard newPrimary = shards.getReplicas().get(0);
            final IndexShard replica = shards.getReplicas().get(1);
            if (randomBoolean()) {
                // simulate docs that were inflight when primary failed, these will be rolled back
                final int rollbackDocs = randomIntBetween(1, 5);
                logger.info("--> indexing {} rollback docs", rollbackDocs);
                for (int i = 0; i < rollbackDocs; i++) {
                    final IndexRequest indexRequest = new IndexRequest(index.getName()).id("rollback_" + i)
                            .source("{}", XContentType.JSON);
                    final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                    indexOnReplica(bulkShardRequest, shards, replica);
                }
                if (randomBoolean()) {
                    oldPrimary.flush(new FlushRequest(index.getName()));
                }
            }
            long globalCheckpointOnOldPrimary = oldPrimary.getLastSyncedGlobalCheckpoint();
            Optional<SequenceNumbers.CommitInfo> safeCommitOnOldPrimary =
                oldPrimary.store().findSafeIndexCommit(globalCheckpointOnOldPrimary);
            assertTrue(safeCommitOnOldPrimary.isPresent());
            shards.promoteReplicaToPrimary(newPrimary).get();

            // check that local checkpoint of new primary is properly tracked after primary promotion
            assertThat(newPrimary.getLocalCheckpoint(), equalTo(totalDocs - 1L));
            assertThat(IndexShardTestCase.getReplicationTracker(newPrimary)
                .getTrackedLocalCheckpointForShard(newPrimary.routingEntry().allocationId().getId()).getLocalCheckpoint(),
                equalTo(totalDocs - 1L));

            // index some more
            int moreDocs = shards.indexDocs(randomIntBetween(0, 5));
            totalDocs += moreDocs;

            // As a replica keeps a safe commit, the file-based recovery only happens if the required translog
            // for the sequence based recovery are not fully retained and extra documents were added to the primary.
            boolean expectSeqNoRecovery = (moreDocs == 0 || randomBoolean());
            int uncommittedOpsOnPrimary = 0;
            if (expectSeqNoRecovery == false) {
                IndexMetadata.Builder builder = IndexMetadata.builder(newPrimary.indexSettings().getIndexMetadata());
                builder.settings(Settings.builder().put(newPrimary.indexSettings().getSettings())
                    .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0));
                newPrimary.indexSettings().updateIndexMetadata(builder.build());
                newPrimary.onSettingsChanged();
                // Make sure the global checkpoint on the new primary is persisted properly,
                // otherwise the deletion policy won't trim translog
                assertBusy(() -> {
                    shards.syncGlobalCheckpoint();
                    assertThat(newPrimary.getLastSyncedGlobalCheckpoint(), equalTo(newPrimary.seqNoStats().getMaxSeqNo()));
                });
                newPrimary.flush(new FlushRequest().force(true));
                if (replica.indexSettings().isSoftDeleteEnabled()) {
                    // We need an extra flush to advance the min_retained_seqno on the new primary so ops-based won't happen.
                    // The min_retained_seqno only advances when a merge asks for the retention query.
                    newPrimary.flush(new FlushRequest().force(true));

                    // We also need to make sure that there is no retention lease holding on to any history. The lease for the old primary
                    // expires since there are no unassigned shards in this replication group).
                    assertBusy(() -> {
                        newPrimary.syncRetentionLeases();
                        //noinspection OptionalGetWithoutIsPresent since there must be at least one lease
                        assertThat(newPrimary.getRetentionLeases().leases().stream().mapToLong(RetentionLease::retainingSequenceNumber)
                            .min().getAsLong(), greaterThan(newPrimary.seqNoStats().getMaxSeqNo()));
                    });
                }
                uncommittedOpsOnPrimary = shards.indexDocs(randomIntBetween(0, 10));
                totalDocs += uncommittedOpsOnPrimary;
            }

            if (randomBoolean()) {
                uncommittedOpsOnPrimary = 0;
                shards.syncGlobalCheckpoint();
                newPrimary.flush(new FlushRequest());
            }

            oldPrimary.close("demoted", false);
            oldPrimary.store().close();

            IndexShard newReplica = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            if (expectSeqNoRecovery) {
                assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
                assertThat(newReplica.recoveryState().getTranslog().totalLocal(),
                    equalTo(Math.toIntExact(globalCheckpointOnOldPrimary - safeCommitOnOldPrimary.get().localCheckpoint)));
                assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(),
                    equalTo(Math.toIntExact(totalDocs - 1 - safeCommitOnOldPrimary.get().localCheckpoint)));
            } else {
                assertThat(newReplica.recoveryState().getIndex().fileDetails(), not(empty()));
                assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(uncommittedOpsOnPrimary));
            }
            // Make sure that flushing on a recovering shard is ok.
            shards.flush();
            shards.assertAllEqual(totalDocs);
        }
    }

    public void testReplicaRollbackStaleDocumentsInPeerRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(2)) {
            shards.startAll();
            IndexShard oldPrimary = shards.getPrimary();
            IndexShard newPrimary = shards.getReplicas().get(0);
            IndexShard replica = shards.getReplicas().get(1);
            int goodDocs = shards.indexDocs(scaledRandomIntBetween(1, 20));
            shards.flush();
            // simulate docs that were inflight when primary failed, these will be rolled back
            int staleDocs = scaledRandomIntBetween(1, 10);
            logger.info("--> indexing {} stale docs", staleDocs);
            for (int i = 0; i < staleDocs; i++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName()).id("stale_" + i)
                    .source("{}", XContentType.JSON);
                final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                indexOnReplica(bulkShardRequest, shards, replica);
            }
            shards.flush();
            shards.promoteReplicaToPrimary(newPrimary).get();
            // Recover a replica should rollback the stale documents
            shards.removeReplica(replica);
            replica.close("recover replica - first time", false);
            replica.store().close();
            replica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(replica);
            shards.assertAllEqual(goodDocs);
            // Index more docs - move the global checkpoint >= seqno of the stale operations.
            goodDocs += shards.indexDocs(scaledRandomIntBetween(staleDocs, staleDocs * 5));
            shards.syncGlobalCheckpoint();
            assertThat(replica.getLastSyncedGlobalCheckpoint(), equalTo(replica.seqNoStats().getMaxSeqNo()));
            // Recover a replica again should also rollback the stale documents.
            shards.removeReplica(replica);
            replica.close("recover replica - second time", false);
            replica.store().close();
            IndexShard anotherReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(anotherReplica);
            shards.assertAllEqual(goodDocs);
            shards.flush();
            shards.assertAllEqual(goodDocs);
        }
    }

    public void testResyncAfterPrimaryPromotion() throws Exception {
        String mappings = "{ \"_doc\": { \"properties\": { \"f\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = new ReplicationGroup(buildIndexMetadata(2, mappings))) {
            shards.startAll();
            int initialDocs = randomInt(10);

            for (int i = 0; i < initialDocs; i++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName()).id("initial_doc_" + i)
                    .source("{ \"f\": \"normal\"}", XContentType.JSON);
                shards.index(indexRequest);
            }

            boolean syncedGlobalCheckPoint = randomBoolean();
            if (syncedGlobalCheckPoint) {
                shards.syncGlobalCheckpoint();
            }

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard newPrimary = shards.getReplicas().get(0);
            final IndexShard justReplica = shards.getReplicas().get(1);

            // simulate docs that were inflight when primary failed
            final int extraDocs = randomInt(5);
            logger.info("--> indexing {} extra docs", extraDocs);
            for (int i = 0; i < extraDocs; i++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName()).id("extra_doc_" + i)
                    .source("{ \"f\": \"normal\"}", XContentType.JSON);
                final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                indexOnReplica(bulkShardRequest, shards, newPrimary);
            }

            final int extraDocsToBeTrimmed = randomIntBetween(0, 10);
            logger.info("--> indexing {} extra docs to be trimmed", extraDocsToBeTrimmed);
            for (int i = 0; i < extraDocsToBeTrimmed; i++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName()).id("extra_trimmed_" + i)
                    .source("{ \"f\": \"trimmed\"}", XContentType.JSON);
                final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                // have to replicate to another replica != newPrimary one - the subject to trim
                indexOnReplica(bulkShardRequest, shards, justReplica);
            }

            logger.info("--> resyncing replicas seqno_stats primary {} replica {}", oldPrimary.seqNoStats(), newPrimary.seqNoStats());
            PrimaryReplicaSyncer.ResyncTask task = shards.promoteReplicaToPrimary(newPrimary).get();
            if (syncedGlobalCheckPoint) {
                assertEquals(extraDocs, task.getResyncedOperations());
            } else {
                assertThat(task.getResyncedOperations(), greaterThanOrEqualTo(extraDocs));
            }
            shards.assertAllEqual(initialDocs + extraDocs);
            for (IndexShard replica : shards.getReplicas()) {
                assertThat(replica.getMaxSeqNoOfUpdatesOrDeletes(),
                    greaterThanOrEqualTo(shards.getPrimary().getMaxSeqNoOfUpdatesOrDeletes()));
            }

            // check translog on replica is trimmed
            int translogOperations = 0;
            try(Translog.Snapshot snapshot = getTranslog(justReplica).newSnapshot()) {
                Translog.Operation next;
                while ((next = snapshot.next()) != null) {
                    translogOperations++;
                    assertThat("unexpected op: " + next, (int)next.seqNo(), lessThan(initialDocs + extraDocs));
                    assertThat("unexpected primaryTerm: " + next.primaryTerm(), next.primaryTerm(),
                        is(oldPrimary.getPendingPrimaryTerm()));
                    final Translog.Source source = next.getSource();
                    assertThat(source.source.utf8ToString(), is("{ \"f\": \"normal\"}"));
                }
            }
            assertThat(translogOperations, either(equalTo(initialDocs + extraDocs)).or(equalTo(task.getResyncedOperations())));
        }
    }

    public void testDoNotWaitForPendingSeqNo() throws Exception {
        IndexMetadata metadata = buildIndexMetadata(1);

        final int pendingDocs = randomIntBetween(1, 5);
        final BlockingEngineFactory primaryEngineFactory = new BlockingEngineFactory();

        try (ReplicationGroup shards = new ReplicationGroup(metadata) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                if (routing.primary()) {
                    return primaryEngineFactory;
                } else {
                    return new InternalEngineFactory();
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
                        shards.index(new IndexRequest(index.getName()).id(id).source("{}", XContentType.JSON));
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
            AtomicBoolean recoveryDone = new AtomicBoolean(false);
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(newReplica, (indexShard, node) -> {
                recoveryStart.countDown();
                return new RecoveryTarget(indexShard, node, recoveryListener) {
                    @Override
                    public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
                        recoveryDone.set(true);
                        super.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, listener);
                    }
                };
            });

            recoveryStart.await();

            // index some more
            final int indexedDuringRecovery = shards.indexDocs(randomInt(5));
            docs += indexedDuringRecovery;

            assertBusy(() -> assertTrue("recovery should not wait for on pending docs", recoveryDone.get()));

            primaryEngineFactory.releaseLatchedIndexers();
            pendingDocsDone.await();

            // now recovery can finish
            recoveryFuture.get();

            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
            shards.assertAllEqual(docs);
        } finally {
            primaryEngineFactory.close();
        }
    }

    public void testCheckpointsAndMarkingInSync() throws Exception {
        final IndexMetadata metadata = buildIndexMetadata(0);
        final BlockingEngineFactory replicaEngineFactory = new BlockingEngineFactory();
        try (
                ReplicationGroup shards = new ReplicationGroup(metadata) {
                    @Override
                    protected EngineFactory getEngineFactory(final ShardRouting routing) {
                        if (routing.primary()) {
                            return new InternalEngineFactory();
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
                    (indexShard, node) -> new RecoveryTarget(indexShard, node, recoveryListener) {
                        @Override
                        public void indexTranslogOperations(
                                final List<Translog.Operation> operations,
                                final int totalTranslogOps,
                                final long maxAutoIdTimestamp,
                                final long maxSeqNoOfUpdates,
                                final RetentionLeases retentionLeases,
                                final long mappingVersion,
                                final ActionListener<Long> listener) {
                            // index a doc which is not part of the snapshot, but also does not complete on replica
                            replicaEngineFactory.latchIndexers(1);
                            threadPool.generic().submit(() -> {
                                try {
                                    shards.index(new IndexRequest(index.getName()).id("pending").source("{}", XContentType.JSON));
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
                                shards.index(new IndexRequest(index.getName()).id("completed").source("{}", XContentType.JSON));
                                pendingDocActiveWithExtraDocIndexed.countDown();
                            } catch (final Exception e) {
                                throw new AssertionError(e);
                            }
                            try {
                                phaseTwoStartLatch.await();
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                            super.indexTranslogOperations(
                                    operations,
                                    totalTranslogOps,
                                    maxAutoIdTimestamp,
                                    maxSeqNoOfUpdates,
                                    retentionLeases,
                                    mappingVersion,
                                    listener);
                        }
                    });
            pendingDocActiveWithExtraDocIndexed.await();
            assertThat(pendingDocDone.getCount(), equalTo(1L));
            {
                final long expectedDocs = docs + 2L;
                assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                // recovery has not completed, therefore the global checkpoint can have advanced on the primary
                assertThat(shards.getPrimary().getLastKnownGlobalCheckpoint(), equalTo(expectedDocs - 1));
                // the pending document is not done, the checkpoints can not have advanced on the replica
                assertThat(replica.getLocalCheckpoint(), lessThan(expectedDocs - 1));
                assertThat(replica.getLastKnownGlobalCheckpoint(), lessThan(expectedDocs - 1));
            }

            // wait for recovery to enter the translog phase
            phaseTwoStartLatch.countDown();

            // wait for the translog phase to complete and the recovery to block global checkpoint advancement
            assertBusy(() -> assertTrue(shards.getPrimary().pendingInSync()));
            {
                shards.index(new IndexRequest(index.getName()).id("last").source("{}", XContentType.JSON));
                final long expectedDocs = docs + 3L;
                assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                // recovery is now in the process of being completed, therefore the global checkpoint can not have advanced on the primary
                assertThat(shards.getPrimary().getLastKnownGlobalCheckpoint(), equalTo(expectedDocs - 2));
                assertThat(replica.getLocalCheckpoint(), lessThan(expectedDocs - 2));
                assertThat(replica.getLastKnownGlobalCheckpoint(), lessThan(expectedDocs - 2));
            }

            replicaEngineFactory.releaseLatchedIndexers();
            pendingDocDone.await();
            recoveryFuture.get();
            {
                final long expectedDocs = docs + 3L;
                assertBusy(() -> {
                    assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                    assertThat(shards.getPrimary().getLastKnownGlobalCheckpoint(), equalTo(expectedDocs - 1));
                    assertThat(replica.getLocalCheckpoint(), equalTo(expectedDocs - 1));
                    // the global checkpoint advances can only advance here if a background global checkpoint sync fires
                    assertThat(replica.getLastKnownGlobalCheckpoint(), anyOf(equalTo(expectedDocs - 1), equalTo(expectedDocs - 2)));
                });
            }
        }
    }

    public void testTransferMaxSeenAutoIdTimestampOnResync() throws Exception {
        try (ReplicationGroup shards = createGroup(2)) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            IndexShard replica1 = shards.getReplicas().get(0);
            IndexShard replica2 = shards.getReplicas().get(1);
            long maxTimestampOnReplica1 = -1;
            long maxTimestampOnReplica2 = -1;
            List<IndexRequest> replicationRequests = new ArrayList<>();
            for (int numDocs = between(1, 10), i = 0; i < numDocs; i++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName()).source("{}", XContentType.JSON);
                indexRequest.process(Version.CURRENT, null, index.getName());
                final IndexRequest copyRequest;
                if (randomBoolean()) {
                    copyRequest = copyIndexRequest(indexRequest);
                    indexRequest.onRetry();
                } else {
                    copyRequest = copyIndexRequest(indexRequest);
                    copyRequest.onRetry();
                }
                replicationRequests.add(copyRequest);
                final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, primary);
                if (randomBoolean()) {
                    indexOnReplica(bulkShardRequest, shards, replica1);
                    maxTimestampOnReplica1 = Math.max(maxTimestampOnReplica1, indexRequest.getAutoGeneratedTimestamp());
                } else {
                    indexOnReplica(bulkShardRequest, shards, replica2);
                    maxTimestampOnReplica2 = Math.max(maxTimestampOnReplica2, indexRequest.getAutoGeneratedTimestamp());
                }
            }
            assertThat(replica1.getMaxSeenAutoIdTimestamp(), equalTo(maxTimestampOnReplica1));
            assertThat(replica2.getMaxSeenAutoIdTimestamp(), equalTo(maxTimestampOnReplica2));
            shards.promoteReplicaToPrimary(replica1).get();
            assertThat(replica2.getMaxSeenAutoIdTimestamp(), equalTo(maxTimestampOnReplica1));
            for (IndexRequest request : replicationRequests) {
                shards.index(request); // deliver via normal replication
            }
            for (IndexShard shard : shards) {
                assertThat(shard.getMaxSeenAutoIdTimestamp(), equalTo(Math.max(maxTimestampOnReplica1, maxTimestampOnReplica2)));
            }
        }
    }

    public void testAddNewReplicas() throws Exception {
        AtomicBoolean stopped = new AtomicBoolean();
        List<Thread> threads = new ArrayList<>();
        Runnable stopIndexing = () -> {
            try {
                stopped.set(true);
                for (Thread thread : threads) {
                    thread.join();
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };
        try (ReplicationGroup shards = createGroup(between(0, 1));
             Releasable ignored = stopIndexing::run) {
            shards.startAll();
            boolean appendOnly = randomBoolean();
            AtomicInteger docId = new AtomicInteger();
            int numThreads = between(1, 3);
            for (int i = 0; i < numThreads; i++) {
                Thread thread = new Thread(() -> {
                    while (stopped.get() == false) {
                        try {
                            int nextId = docId.incrementAndGet();
                            if (appendOnly) {
                                String id = randomBoolean() ? Integer.toString(nextId) : null;
                                shards.index(new IndexRequest(index.getName()).id(id).source("{}", XContentType.JSON));
                            } else if (frequently()) {
                                String id = Integer.toString(frequently() ? nextId : between(0, nextId));
                                shards.index(new IndexRequest(index.getName()).id(id).source("{}", XContentType.JSON));
                            } else {
                                String id = Integer.toString(between(0, nextId));
                                shards.delete(new DeleteRequest(index.getName()).id(id));
                            }
                            if (randomInt(100) < 10) {
                                shards.getPrimary().flush(new FlushRequest());
                            }
                            if (randomInt(100) < 5) {
                                shards.getPrimary().forceMerge(new ForceMergeRequest().flush(randomBoolean()).maxNumSegments(1));
                            }
                        } catch (Exception ex) {
                            throw new AssertionError(ex);
                        }
                    }
                });
                threads.add(thread);
                thread.start();
            }
            assertBusy(() -> assertThat(docId.get(), greaterThanOrEqualTo(50)), 60, TimeUnit.SECONDS); // we flush quite often
            shards.getPrimary().sync();
            IndexShard newReplica = shards.addReplica();
            shards.recoverReplica(newReplica);
            assertBusy(() -> assertThat(docId.get(), greaterThanOrEqualTo(100)), 60, TimeUnit.SECONDS); // we flush quite often
            stopIndexing.run();
            assertBusy(() -> assertThat(getDocIdAndSeqNos(newReplica), equalTo(getDocIdAndSeqNos(shards.getPrimary()))));
        }
    }

    public void testRollbackOnPromotion() throws Exception {
        try (ReplicationGroup shards = createGroup(between(2, 3))) {
            shards.startAll();
            IndexShard newPrimary = randomFrom(shards.getReplicas());
            int initDocs = shards.indexDocs(randomInt(100));
            int inFlightOpsOnNewPrimary = 0;
            int inFlightOps = scaledRandomIntBetween(10, 200);
            for (int i = 0; i < inFlightOps; i++) {
                String id = "extra-" + i;
                IndexRequest primaryRequest = new IndexRequest(index.getName()).id(id).source("{}", XContentType.JSON);
                BulkShardRequest replicationRequest = indexOnPrimary(primaryRequest, shards.getPrimary());
                for (IndexShard replica : shards.getReplicas()) {
                    if (randomBoolean()) {
                        indexOnReplica(replicationRequest, shards, replica);
                        if (replica == newPrimary) {
                            inFlightOpsOnNewPrimary++;
                        }
                    }
                }
                if (randomBoolean()) {
                    shards.syncGlobalCheckpoint();
                }
                if (rarely()) {
                    shards.flush();
                }
            }
            shards.refresh("test");
            List<DocIdSeqNoAndSource> docsBelowGlobalCheckpoint = EngineTestCase.getDocIds(getEngine(newPrimary), randomBoolean())
                .stream().filter(doc -> doc.getSeqNo() <= newPrimary.getLastKnownGlobalCheckpoint()).collect(Collectors.toList());
            CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean done = new AtomicBoolean();
            Thread thread = new Thread(() -> {
                List<IndexShard> replicas = new ArrayList<>(shards.getReplicas());
                replicas.remove(newPrimary);
                latch.countDown();
                while (done.get() == false) {
                    try {
                        List<DocIdSeqNoAndSource> exposedDocs = EngineTestCase.getDocIds(getEngine(randomFrom(replicas)), randomBoolean());
                        assertThat(docsBelowGlobalCheckpoint, everyItem(is(in(exposedDocs))));
                        assertThat(randomFrom(replicas).getLocalCheckpoint(), greaterThanOrEqualTo(initDocs - 1L));
                    } catch (AlreadyClosedException ignored) {
                        // replica swaps engine during rollback
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            });
            thread.start();
            latch.await();
            shards.promoteReplicaToPrimary(newPrimary).get();
            shards.assertAllEqual(initDocs + inFlightOpsOnNewPrimary);
            int moreDocsAfterRollback = shards.indexDocs(scaledRandomIntBetween(1, 20));
            shards.assertAllEqual(initDocs + inFlightOpsOnNewPrimary + moreDocsAfterRollback);
            done.set(true);
            thread.join();
            shards.syncGlobalCheckpoint();
            for (IndexShard shard : shards) {
                shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
                assertThat(shard.translogStats().getUncommittedOperations(), equalTo(0));
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
            super(shard, sourceNode, listener);
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
        public void indexTranslogOperations(
                final List<Translog.Operation> operations,
                final int totalTranslogOps,
                final long maxAutoIdTimestamp,
                final long maxSeqNoOfUpdates,
                final RetentionLeases retentionLeases,
                final long mappingVersion,
                final ActionListener<Long> listener) {
            if (hasBlocked() == false) {
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            super.indexTranslogOperations(
                operations, totalTranslogOps, maxAutoIdTimestamp, maxSeqNoOfUpdates, retentionLeases, mappingVersion, listener);
        }

        @Override
        public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata,
                               ActionListener<Void> listener) {
            blockIfNeeded(RecoveryState.Stage.INDEX);
            super.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, listener);
        }

        @Override
        public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
            if (hasBlocked() == false) {
                // it maybe that not ops have been transferred, block now
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            blockIfNeeded(RecoveryState.Stage.FINALIZE);
            super.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, listener);
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
