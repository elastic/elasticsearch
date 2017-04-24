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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTests;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryTarget;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IndexLevelReplicationTests extends ESIndexLevelReplicationTestCase {

    public void testSimpleReplication() throws Exception {
        try (ReplicationGroup shards = createGroup(randomInt(2))) {
            shards.startAll();
            final int docCount = randomInt(50);
            shards.indexDocs(docCount);
            shards.assertAllEqual(docCount);
        }
    }

    public void testSimpleAppendOnlyReplication() throws Exception {
        try (ReplicationGroup shards = createGroup(randomInt(2))) {
            shards.startAll();
            final int docCount = randomInt(50);
            shards.appendDocs(docCount);
            shards.assertAllEqual(docCount);
        }
    }

    public void testAppendWhileRecovering() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            IndexShard replica = shards.addReplica();
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
            Future<Void> future = shards.asyncRecoverReplica(replica, (indexShard, node)
                    -> new RecoveryTarget(indexShard, node, recoveryListener, version -> {
            }) {
                @Override
                public void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException {
                    super.cleanFiles(totalTranslogOps, sourceMetaData);
                    latch.countDown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
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

    public void testInheritMaxValidAutoIDTimestampOnRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            final IndexRequest indexRequest = new IndexRequest(index.getName(), "type").source("{}", XContentType.JSON);
            indexRequest.onRetry(); // force an update of the timestamp
            final BulkItemResponse response = shards.index(indexRequest);
            assertEquals(DocWriteResponse.Result.CREATED, response.getResponse().getResult());
            if (randomBoolean()) { // lets check if that also happens if no translog record is replicated
                shards.flush();
            }
            IndexShard replica = shards.addReplica();
            shards.recoverReplica(replica);

            SegmentsStats segmentsStats = replica.segmentStats(false);
            SegmentsStats primarySegmentStats = shards.getPrimary().segmentStats(false);
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

            if (numDocs == 0 || randomBoolean()) {
                // in the case we have no indexing, we simulate the background global checkpoint sync
                shards.getPrimary().updateGlobalCheckpointOnPrimary();
            }
            for (IndexShard shard : shards) {
                final SeqNoStats shardStats = shard.seqNoStats();
                final ShardRouting shardRouting = shard.routingEntry();
                logger.debug("seq_no stats for {}: {}", shardRouting, XContentHelper.toString(shardStats,
                        new ToXContent.MapParams(Collections.singletonMap("pretty", "false"))));
                assertThat(shardRouting + " local checkpoint mismatch", shardStats.getLocalCheckpoint(), equalTo(numDocs - 1L));

                assertThat(shardRouting + " global checkpoint mismatch", shardStats.getGlobalCheckpoint(), equalTo(numDocs - 1L));
                assertThat(shardRouting + " max seq no mismatch", shardStats.getMaxSeqNo(), equalTo(numDocs - 1L));
            }
        }
    }

    public void testConflictingOpsOnReplica() throws Exception {
        Map<String, String> mappings =
                Collections.singletonMap("type", "{ \"type\": { \"properties\": { \"f\": { \"type\": \"keyword\"} }}}");
        try (ReplicationGroup shards = new ReplicationGroup(buildIndexMetaData(2, mappings))) {
            shards.startAll();
            IndexShard replica1 = shards.getReplicas().get(0);
            logger.info("--> isolated replica " + replica1.routingEntry());
            shards.removeReplica(replica1);
            IndexRequest indexRequest = new IndexRequest(index.getName(), "type", "1").source("{ \"f\": \"1\"}", XContentType.JSON);
            shards.index(indexRequest);
            shards.addReplica(replica1);
            logger.info("--> promoting replica to primary " + replica1.routingEntry());
            shards.promoteReplicaToPrimary(replica1);
            indexRequest = new IndexRequest(index.getName(), "type", "1").source("{ \"f\": \"2\"}", XContentType.JSON);
            shards.index(indexRequest);
            shards.refresh("test");
            for (IndexShard shard : shards) {
                try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                    TopDocs search = searcher.searcher().search(new TermQuery(new Term("f", "2")), 10);
                    assertEquals("shard " + shard.routingEntry() + " misses new version", 1, search.totalHits);
                }
            }
        }
    }

    /**
     * test document failures (failures after seq_no generation) are added as noop operation to the translog
     * for primary and replica shards
     */
    public void testDocumentFailureReplication() throws Exception {
        final String failureMessage = "simulated document failure";
        final ThrowingDocumentFailureEngineFactory throwingDocumentFailureEngineFactory =
                new ThrowingDocumentFailureEngineFactory(failureMessage);
        try (ReplicationGroup shards = new ReplicationGroup(buildIndexMetaData(0)) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                return throwingDocumentFailureEngineFactory;
            }}) {

            // test only primary
            shards.startPrimary();
            BulkItemResponse response = shards.index(
                    new IndexRequest(index.getName(), "testDocumentFailureReplication", "1")
                            .source("{}", XContentType.JSON)
            );
            assertTrue(response.isFailed());
            assertNoOpTranslogOperationForDocumentFailure(shards, 1, failureMessage);
            shards.assertAllEqual(0);

            // add some replicas
            int nReplica = randomIntBetween(1, 3);
            for (int i = 0; i < nReplica; i++) {
                shards.addReplica();
            }
            shards.startReplicas(nReplica);
            response = shards.index(
                    new IndexRequest(index.getName(), "testDocumentFailureReplication", "1")
                            .source("{}", XContentType.JSON)
            );
            assertTrue(response.isFailed());
            assertNoOpTranslogOperationForDocumentFailure(shards, 2, failureMessage);
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
                    new IndexRequest(index.getName(), "testRequestFailureException", "1")
                            .source("{}", XContentType.JSON)
                            .version(2)
            );
            assertTrue(response.isFailed());
            assertThat(response.getFailure().getCause(), instanceOf(VersionConflictEngineException.class));
            shards.assertAllEqual(0);
            for (IndexShard indexShard : shards) {
                try(Translog.View view = indexShard.acquireTranslogView()) {
                    assertThat(view.totalOperations(), equalTo(0));
                }
            }

            // add some replicas
            int nReplica = randomIntBetween(1, 3);
            for (int i = 0; i < nReplica; i++) {
                shards.addReplica();
            }
            shards.startReplicas(nReplica);
            response = shards.index(
                    new IndexRequest(index.getName(), "testRequestFailureException", "1")
                            .source("{}", XContentType.JSON)
                            .version(2)
            );
            assertTrue(response.isFailed());
            assertThat(response.getFailure().getCause(), instanceOf(VersionConflictEngineException.class));
            shards.assertAllEqual(0);
            for (IndexShard indexShard : shards) {
                try(Translog.View view = indexShard.acquireTranslogView()) {
                    assertThat(view.totalOperations(), equalTo(0));
                }
            }
        }
    }

    /** Throws <code>documentFailure</code> on every indexing operation */
    static class ThrowingDocumentFailureEngineFactory implements EngineFactory {
        final String documentFailureMessage;

        ThrowingDocumentFailureEngineFactory(String documentFailureMessage) {
            this.documentFailureMessage = documentFailureMessage;
        }

        @Override
        public Engine newReadWriteEngine(EngineConfig config) {
            return InternalEngineTests.createInternalEngine((directory, writerConfig) ->
                    new IndexWriter(directory, writerConfig) {
                        @Override
                        public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
                            assert documentFailureMessage != null;
                            throw new IOException(documentFailureMessage);
                        }
                    }, null, config);
        }
    }

    private static void assertNoOpTranslogOperationForDocumentFailure(
            Iterable<IndexShard> replicationGroup,
            int expectedOperation,
            String failureMessage) throws IOException {
        for (IndexShard indexShard : replicationGroup) {
            try(Translog.View view = indexShard.acquireTranslogView()) {
                assertThat(view.totalOperations(), equalTo(expectedOperation));
                final Translog.Snapshot snapshot = view.snapshot();
                long expectedSeqNo = 0L;
                Translog.Operation op = snapshot.next();
                do {
                    assertThat(op.opType(), equalTo(Translog.Operation.Type.NO_OP));
                    assertThat(op.seqNo(), equalTo(expectedSeqNo));
                    assertThat(((Translog.NoOp) op).reason(), containsString(failureMessage));
                    op = snapshot.next();
                    expectedSeqNo++;
                } while (op != null);
            }
        }
    }
}
