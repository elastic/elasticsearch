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

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTests;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryTarget;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

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
                        shards.appendDocs(numDocs-1);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            };
            thread.start();
            Future<Void> future = shards.asyncRecoverReplica(replica, (indexShard, node)
                -> new RecoveryTarget(indexShard, node, recoveryListener, version -> {}) {
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
            Engine engine = IndexShardTests.getEngineFromShard(replica);
            assertEquals("expected at no version lookups ", InternalEngineTests.getNumVersionLookups((InternalEngine) engine), 0);
            for (IndexShard shard : shards) {
                engine = IndexShardTests.getEngineFromShard(shard);
                assertEquals(0, InternalEngineTests.getNumIndexVersionsLookups((InternalEngine) engine));
                assertEquals(0, InternalEngineTests.getNumVersionLookups((InternalEngine) engine));
            }
        }
    }

    public void testInheritMaxValidAutoIDTimestampOnRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            final IndexRequest indexRequest = new IndexRequest(index.getName(), "type").source("{}");
            indexRequest.onRetry(); // force an update of the timestamp
            final IndexResponse response = shards.index(indexRequest);
            assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
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

}
