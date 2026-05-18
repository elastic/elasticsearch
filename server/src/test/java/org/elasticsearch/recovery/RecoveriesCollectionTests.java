/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveriesCollection;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;

import static org.hamcrest.Matchers.equalTo;

public class RecoveriesCollectionTests extends ESIndexLevelReplicationTestCase {
    static final PeerRecoveryTargetService.RecoveryListener listener = new PeerRecoveryTargetService.RecoveryListener() {
        @Override
        public void onRecoveryDone(
            RecoveryState state,
            ShardLongFieldRange timestampMillisFieldRange,
            ShardLongFieldRange eventIngestedMillisFieldRange
        ) {

        }

        @Override
        public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {

        }
    };

    public void testRecoveryCancellation() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            final RecoveriesCollection collection = new RecoveriesCollection(logger);
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
            final RecoveriesCollection collection = new RecoveriesCollection(logger);
            IndexShard shard = shards.addReplica();
            final long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shard);
            RecoveryTarget recoveryTarget = collection.getRecoveryTarget(recoveryId);
            final int currentAsTarget = shard.recoveryStats().currentAsTarget();
            final int referencesToStore = recoveryTarget.store().refCount();
            IndexShard indexShard = recoveryTarget.indexShard();
            Store store = recoveryTarget.store();
            String tempFileName = recoveryTarget.getTempNameForFile("foobar");
            RecoveryTarget resetRecovery = collection.resetRecovery(recoveryId);
            final long resetRecoveryId = resetRecovery.recoveryId();
            assertNotSame(recoveryTarget, resetRecovery);
            assertNotSame(recoveryTarget.cancellableThreads(), resetRecovery.cancellableThreads());
            assertSame(indexShard, resetRecovery.indexShard());
            assertSame(store, resetRecovery.store());
            assertEquals(referencesToStore, resetRecovery.store().refCount());
            assertEquals(currentAsTarget, shard.recoveryStats().currentAsTarget());
            assertEquals(recoveryTarget.refCount(), 0);
            if (Assertions.ENABLED) {
                expectThrows(AssertionError.class, recoveryTarget::store);
                expectThrows(AssertionError.class, recoveryTarget::indexShard);
            }
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

    static long startRecovery(RecoveriesCollection collection, DiscoveryNode sourceNode, IndexShard shard) {
        final DiscoveryNode rNode = getDiscoveryNode(shard.routingEntry().currentNodeId());
        shard.markAsRecovering("remote", new RecoveryState(shard.routingEntry(), sourceNode, rNode));
        shard.prepareForIndexRecovery();
        return collection.startRecovery(shard, sourceNode, 0L, null, listener, null);
    }

}
