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
package org.elasticsearch.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveriesCollection;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryTarget;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class RecoveriesCollectionTests extends ESIndexLevelReplicationTestCase {
    static final PeerRecoveryTargetService.RecoveryListener listener = new PeerRecoveryTargetService.RecoveryListener() {
        @Override
        public void onRecoveryDone(RecoveryState state) {

        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {

        }
    };

    public void testLastAccessTimeUpdate() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            final RecoveriesCollection collection = new RecoveriesCollection(logger, threadPool);
            final long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shards.addReplica());
            try (RecoveriesCollection.RecoveryRef status = collection.getRecovery(recoveryId)) {
                final long lastSeenTime = status.target().lastAccessTime();
                assertBusy(() -> {
                    try (RecoveriesCollection.RecoveryRef currentStatus = collection.getRecovery(recoveryId)) {
                        assertThat("access time failed to update", lastSeenTime, lessThan(currentStatus.target().lastAccessTime()));
                    }
                });
            } finally {
                collection.cancelRecovery(recoveryId, "life");
            }
        }
    }

    public void testRecoveryTimeout() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            final RecoveriesCollection collection = new RecoveriesCollection(logger, threadPool);
            final AtomicBoolean failed = new AtomicBoolean();
            final CountDownLatch latch = new CountDownLatch(1);
            final long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shards.addReplica(),
                new PeerRecoveryTargetService.RecoveryListener() {
                    @Override
                    public void onRecoveryDone(RecoveryState state) {
                        latch.countDown();
                    }

                    @Override
                    public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
                        failed.set(true);
                        latch.countDown();
                    }
                }, TimeValue.timeValueMillis(100));
            try {
                latch.await(30, TimeUnit.SECONDS);
                assertTrue("recovery failed to timeout", failed.get());
            } finally {
                collection.cancelRecovery(recoveryId, "meh");
            }
        }

    }

    public void testRecoveryCancellation() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            final RecoveriesCollection collection = new RecoveriesCollection(logger, threadPool);
            final long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shards.addReplica());
            final long recoveryId2 = startRecovery(collection, shards.getPrimaryNode(), shards.addReplica());
            try (RecoveriesCollection.RecoveryRef recoveryRef = collection.getRecovery(recoveryId)) {
                ShardId shardId = recoveryRef.target().shardId();
                assertTrue("failed to cancel recoveries", collection.cancelRecoveriesForShard(shardId, "test"));
                assertThat("all recoveries should be cancelled", collection.size(), equalTo(0));
            } finally {
                collection.cancelRecovery(recoveryId, "meh");
                collection.cancelRecovery(recoveryId2, "meh");
            }
        }
    }

    public void testResetRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            int numDocs = randomIntBetween(1, 15);
            shards.indexDocs(numDocs);
            final RecoveriesCollection collection = new RecoveriesCollection(logger, threadPool);
            IndexShard shard = shards.addReplica();
            final long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shard);
            RecoveryTarget recoveryTarget = collection.getRecoveryTarget(recoveryId);
            final int currentAsTarget = shard.recoveryStats().currentAsTarget();
            final int referencesToStore = recoveryTarget.store().refCount();
            IndexShard indexShard = recoveryTarget.indexShard();
            Store store = recoveryTarget.store();
            String tempFileName = recoveryTarget.getTempNameForFile("foobar");
            RecoveryTarget resetRecovery = collection.resetRecovery(recoveryId, TimeValue.timeValueMinutes(60));
            final long resetRecoveryId = resetRecovery.recoveryId();
            assertNotSame(recoveryTarget, resetRecovery);
            assertNotSame(recoveryTarget.cancellableThreads(), resetRecovery.cancellableThreads());
            assertSame(indexShard, resetRecovery.indexShard());
            assertSame(store, resetRecovery.store());
            assertEquals(referencesToStore, resetRecovery.store().refCount());
            assertEquals(currentAsTarget, shard.recoveryStats().currentAsTarget());
            assertEquals(recoveryTarget.refCount(), 0);
            expectThrows(ElasticsearchException.class, () -> recoveryTarget.store());
            expectThrows(ElasticsearchException.class, () -> recoveryTarget.indexShard());
            String resetTempFileName = resetRecovery.getTempNameForFile("foobar");
            assertNotEquals(tempFileName, resetTempFileName);
            assertEquals(currentAsTarget, shard.recoveryStats().currentAsTarget());
            try (RecoveriesCollection.RecoveryRef newRecoveryRef = collection.getRecovery(resetRecoveryId)) {
                shards.recoverReplica(shard, (s, n) -> {
                    assertSame(s, newRecoveryRef.target().indexShard());
                    return newRecoveryRef.target();
                }, false);
            }
            shards.assertAllEqual(numDocs);
            assertNull("recovery is done", collection.getRecovery(recoveryId));
        }
    }

    long startRecovery(RecoveriesCollection collection, DiscoveryNode sourceNode, IndexShard shard) {
        return startRecovery(collection,sourceNode, shard, listener, TimeValue.timeValueMinutes(60));
    }

    long startRecovery(RecoveriesCollection collection, DiscoveryNode sourceNode, IndexShard indexShard,
                       PeerRecoveryTargetService.RecoveryListener listener, TimeValue timeValue) {
        final DiscoveryNode rNode = getDiscoveryNode(indexShard.routingEntry().currentNodeId());
        indexShard.markAsRecovering("remote", new RecoveryState(indexShard.routingEntry(), sourceNode, rNode));
        indexShard.prepareForIndexRecovery();
        return collection.startRecovery(indexShard, sourceNode, listener, timeValue);
    }
}
