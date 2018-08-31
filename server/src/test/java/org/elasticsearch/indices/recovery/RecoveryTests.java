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

package org.elasticsearch.indices.recovery;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.replication.RecoveryDuringReplicationTests;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.SnapshotMatchers;
import org.elasticsearch.index.translog.Translog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class RecoveryTests extends ESIndexLevelReplicationTestCase {

    public void testTranslogHistoryTransferred() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            int docs = shards.indexDocs(10);
            getTranslog(shards.getPrimary()).rollGeneration();
            shards.flush();
            if (randomBoolean()) {
                docs += shards.indexDocs(10);
            }
            shards.addReplica();
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            assertThat(replica.estimateTranslogOperationsFromMinSeq(0), equalTo(docs));
        }
    }

    public void testRetentionPolicyChangeDuringRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            shards.indexDocs(10);
            getTranslog(shards.getPrimary()).rollGeneration();
            shards.flush();
            shards.indexDocs(10);
            final IndexShard replica = shards.addReplica();
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);
            Future<Void> future = shards.asyncRecoverReplica(replica,
                (indexShard, node) -> new RecoveryDuringReplicationTests.BlockingTarget(RecoveryState.Stage.TRANSLOG,
                    recoveryBlocked, releaseRecovery, indexShard, node, recoveryListener, logger));
            recoveryBlocked.await();
            IndexMetaData.Builder builder = IndexMetaData.builder(replica.indexSettings().getIndexMetaData());
            builder.settings(Settings.builder().put(replica.indexSettings().getSettings())
                .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1")
                .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1")
                // force a roll and flush
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "100b")
            );
            replica.indexSettings().updateIndexMetaData(builder.build());
            replica.onSettingsChanged();
            releaseRecovery.countDown();
            future.get();
            // rolling/flushing is async
            assertBusy(() -> {
                assertThat(replica.getLastSyncedGlobalCheckpoint(), equalTo(19L));
                assertThat(replica.estimateTranslogOperationsFromMinSeq(0), equalTo(0));
            });
        }
    }

    public void testRecoveryWithOutOfOrderDelete() throws Exception {
        /*
         * The flow of this test:
         * - delete #1
         * - roll generation (to create gen 2)
         * - index #0
         * - index #3
         * - flush (commit point has max_seqno 3, and local checkpoint 1 -> points at gen 2, previous commit point is maintained)
         * - index #2
         * - index #5
         * - If flush and the translog retention disabled, delete #1 will be removed while index #0 is still retained and replayed.
         */
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            // create out of order delete and index op on replica
            final IndexShard orgReplica = shards.getReplicas().get(0);
            final String indexName = orgReplica.shardId().getIndexName();

            // delete #1
            orgReplica.applyDeleteOperationOnReplica(1, 2, "type", "id");
            getTranslog(orgReplica).rollGeneration(); // isolate the delete in it's own generation
            // index #0
            orgReplica.applyIndexOperationOnReplica(0, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false,
                SourceToParse.source(indexName, "type", "id", new BytesArray("{}"), XContentType.JSON));
            // index #3
            orgReplica.applyIndexOperationOnReplica(3, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false,
                SourceToParse.source(indexName, "type", "id-3", new BytesArray("{}"), XContentType.JSON));
            // Flushing a new commit with local checkpoint=1 allows to delete the translog gen #1.
            orgReplica.flush(new FlushRequest().force(true).waitIfOngoing(true));
            // index #2
            orgReplica.applyIndexOperationOnReplica(2, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false,
                SourceToParse.source(indexName, "type", "id-2", new BytesArray("{}"), XContentType.JSON));
            orgReplica.updateGlobalCheckpointOnReplica(3L, "test");
            // index #5 -> force NoOp #4.
            orgReplica.applyIndexOperationOnReplica(5, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false,
                SourceToParse.source(indexName, "type", "id-5", new BytesArray("{}"), XContentType.JSON));

            final int translogOps;
            if (randomBoolean()) {
                if (randomBoolean()) {
                    logger.info("--> flushing shard (translog will be trimmed)");
                    IndexMetaData.Builder builder = IndexMetaData.builder(orgReplica.indexSettings().getIndexMetaData());
                    builder.settings(Settings.builder().put(orgReplica.indexSettings().getSettings())
                        .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1")
                        .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1"));
                    orgReplica.indexSettings().updateIndexMetaData(builder.build());
                    orgReplica.onSettingsChanged();
                    translogOps = 5; // 4 ops + seqno gaps (delete #1 is removed but index #0 will be replayed).
                } else {
                    logger.info("--> flushing shard (translog will be retained)");
                    translogOps = 6; // 5 ops + seqno gaps
                }
                flushShard(orgReplica);
            } else {
                translogOps = 6; // 5 ops + seqno gaps
            }

            final IndexShard orgPrimary = shards.getPrimary();
            shards.promoteReplicaToPrimary(orgReplica).get(); // wait for primary/replica sync to make sure seq# gap is closed.

            IndexShard newReplica = shards.addReplicaWithExistingPath(orgPrimary.shardPath(), orgPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);
            shards.assertAllEqual(3);

            assertThat(newReplica.estimateTranslogOperationsFromMinSeq(0), equalTo(translogOps));
        }
    }

    public void testDifferentHistoryUUIDDisablesOPsRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            // index some shared docs
            final int flushedDocs = 10;
            final int nonFlushedDocs = randomIntBetween(0, 10);
            final int numDocs = flushedDocs + nonFlushedDocs;
            shards.indexDocs(flushedDocs);
            shards.flush();
            shards.indexDocs(nonFlushedDocs);

            IndexShard replica = shards.getReplicas().get(0);
            final String historyUUID = replica.getHistoryUUID();
            Translog.TranslogGeneration translogGeneration = getTranslog(replica).getGeneration();
            shards.removeReplica(replica);
            replica.close("test", false);
            IndexWriterConfig iwc = new IndexWriterConfig(null)
                .setCommitOnClose(false)
                // we don't want merges to happen here - we call maybe merge on the engine
                // later once we stared it up otherwise we would need to wait for it here
                // we also don't specify a codec here and merges should use the engines for this index
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            Map<String, String> userData = new HashMap<>(replica.store().readLastCommittedSegmentsInfo().getUserData());
            final String translogUUIDtoUse;
            final long translogGenToUse;
            final String historyUUIDtoUse = UUIDs.randomBase64UUID(random());
            if (randomBoolean()) {
                // create a new translog
                translogUUIDtoUse = Translog.createEmptyTranslog(replica.shardPath().resolveTranslog(), flushedDocs,
                    replica.shardId(), replica.getPendingPrimaryTerm());
                translogGenToUse = 1;
            } else {
                translogUUIDtoUse = translogGeneration.translogUUID;
                translogGenToUse = translogGeneration.translogFileGeneration;
            }
            try (IndexWriter writer = new IndexWriter(replica.store().directory(), iwc)) {
                userData.put(Engine.HISTORY_UUID_KEY, historyUUIDtoUse);
                userData.put(Translog.TRANSLOG_UUID_KEY, translogUUIDtoUse);
                userData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(translogGenToUse));
                writer.setLiveCommitData(userData.entrySet());
                writer.commit();
            }
            replica.store().close();
            IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);
            // file based recovery should be made
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), not(empty()));
            assertThat(newReplica.estimateTranslogOperationsFromMinSeq(0), equalTo(numDocs));

            // history uuid was restored
            assertThat(newReplica.getHistoryUUID(), equalTo(historyUUID));
            assertThat(newReplica.commitStats().getUserData().get(Engine.HISTORY_UUID_KEY), equalTo(historyUUID));

            shards.assertAllEqual(numDocs);
        }
    }

    public void testPeerRecoveryPersistGlobalCheckpoint() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            final long numDocs = shards.indexDocs(between(1, 100));
            if (randomBoolean()) {
                shards.flush();
            }
            final IndexShard replica = shards.addReplica();
            shards.recoverReplica(replica);
            assertThat(replica.getLastSyncedGlobalCheckpoint(), equalTo(numDocs - 1));
        }
    }

    public void testPeerRecoverySendSafeCommitInFileBased() throws Exception {
        IndexShard primaryShard = newStartedShard(true);
        int numDocs = between(1, 100);
        long globalCheckpoint = 0;
        for (int i = 0; i < numDocs; i++) {
            Engine.IndexResult result = primaryShard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
                SourceToParse.source(primaryShard.shardId().getIndexName(), "_doc", Integer.toString(i), new BytesArray("{}"),
                    XContentType.JSON),
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            if (randomBoolean()) {
                globalCheckpoint = randomLongBetween(globalCheckpoint, i);
                primaryShard.updateLocalCheckpointForShard(primaryShard.routingEntry().allocationId().getId(), globalCheckpoint);
                primaryShard.updateGlobalCheckpointForShard(primaryShard.routingEntry().allocationId().getId(), globalCheckpoint);
                primaryShard.flush(new FlushRequest());
            }
        }
        IndexShard replicaShard = newShard(primaryShard.shardId(), false);
        updateMappings(replicaShard, primaryShard.indexSettings().getIndexMetaData());
        recoverReplica(replicaShard, primaryShard, true);
        List<IndexCommit> commits = DirectoryReader.listCommits(replicaShard.store().directory());
        long maxSeqNo = Long.parseLong(commits.get(0).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
        assertThat(maxSeqNo, lessThanOrEqualTo(globalCheckpoint));
        closeShards(primaryShard, replicaShard);
    }

    public void testSequenceBasedRecoveryKeepsTranslog() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            final int initDocs = scaledRandomIntBetween(0, 20);
            int uncommittedDocs = 0;
            for (int i = 0; i < initDocs; i++) {
                shards.indexDocs(1);
                uncommittedDocs++;
                if (randomBoolean()) {
                    shards.syncGlobalCheckpoint();
                    shards.flush();
                    uncommittedDocs = 0;
                }
            }
            shards.removeReplica(replica);
            final int moreDocs = shards.indexDocs(scaledRandomIntBetween(0, 20));
            if (randomBoolean()) {
                shards.flush();
            }
            replica.close("test", randomBoolean());
            replica.store().close();
            final IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            try (Translog.Snapshot snapshot = getTranslog(newReplica).newSnapshot()) {
                assertThat("Sequence based recovery should keep existing translog", snapshot, SnapshotMatchers.size(initDocs + moreDocs));
            }
            assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(uncommittedDocs + moreDocs));
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
        }
    }

    /**
     * This test makes sure that there is no infinite loop of flushing (the condition `shouldPeriodicallyFlush` eventually is false)
     * in peer-recovery if a primary sends a fully-baked index commit.
     */
    public void testShouldFlushAfterPeerRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            int numDocs = shards.indexDocs(between(10, 100));
            final long translogSizeOnPrimary = shards.getPrimary().translogStats().getUncommittedSizeInBytes();
            shards.flush();

            final IndexShard replica = shards.addReplica();
            IndexMetaData.Builder builder = IndexMetaData.builder(replica.indexSettings().getIndexMetaData());
            long flushThreshold = RandomNumbers.randomLongBetween(random(), 100, translogSizeOnPrimary);
            builder.settings(Settings.builder().put(replica.indexSettings().getSettings())
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), flushThreshold + "b")
            );
            replica.indexSettings().updateIndexMetaData(builder.build());
            replica.onSettingsChanged();
            shards.recoverReplica(replica);
            // Make sure the flushing will eventually be completed (eg. `shouldPeriodicallyFlush` is false)
            assertBusy(() -> assertThat(getEngine(replica).shouldPeriodicallyFlush(), equalTo(false)));
            assertThat(replica.estimateTranslogOperationsFromMinSeq(0), equalTo(numDocs));
            shards.assertAllEqual(numDocs);
        }
    }
}
