/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.MockLog;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class RecoveriesCollectionTests extends ESIndexLevelReplicationTestCase {
    static final RecoveryListener listener = RecoveryListener.NOOP;

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

    /// Regression test. There was previously a race between {@link RecoveriesCollection#cancelRecoveriesForShard} and {@link
    /// RecoveriesCollection#failRecovery} / {@link RecoveriesCollection#markRecoveryAsDone} / {@link RecoveriesCollection#cancelRecovery}
    /// that this test reproduce.
    public void testRaceFailRecoveryWithCancelRecoveriesForShard() throws Exception {
        ContenderTuple cancelRecoveriesForShard = new ContenderTuple(
            (collection, shardId, recoveryId) -> () -> collection.cancelRecoveriesForShard(shardId, "cancel for shard"),
            "cancel for shard"
        );

        ContenderTuple raceContender = switch (randomInt(2)) {
            case 0 -> new ContenderTuple(
                (collection, shardId, recoveryId) -> () -> collection.markRecoveryAsDone(recoveryId),
                " marking recovery from"
            );
            case 1 -> new ContenderTuple(
                (collection, shardId, recoveryId) -> () -> collection.cancelRecovery(recoveryId, "cancel by recovery id"),
                "cancel by recovery id"
            );
            case 2 -> new ContenderTuple(
                (collection, shardId, recoveryId) -> () -> collection.failRecovery(
                    recoveryId,
                    new RecoveryFailedException(fakeRecoveryState(), "failed", new RuntimeException("cause")),
                    false
                ),
                " failing recovery from"
            );
            default -> throw new IllegalStateException("Unexpected value: " + randomInt(2));
        };

        raceAndAssertExactlyOneLogMessage(raceContender, cancelRecoveriesForShard);
    }

    /// Race the two contenders against each other and assert that the log contains exactly one message
    /// that contains any of the two expected messages.
    private void raceAndAssertExactlyOneLogMessage(ContenderTuple firstContender, ContenderTuple secondContender) throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            final RecoveriesCollection collection = new RecoveriesCollection(logger);
            IndexShard shard = shards.addReplica();
            ShardId shardId = shard.shardId();
            long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shard);

            Level saved = logger.getLevel();
            Loggers.setLevel(logger, Level.TRACE);
            try (MockLog mocklog = MockLog.capture(getClass())) {
                mocklog.addExpectation(
                    new ExactlyOneOfExpectation(getClass().getName(), firstContender.expectedLogMessage, secondContender.expectedLogMessage)
                );
                startInParallel(
                    firstContender.factory.build(collection, shardId, recoveryId),
                    secondContender.factory.build(collection, shardId, recoveryId)
                );
                mocklog.assertAllExpectationsMatched();
            } finally {
                Loggers.setLevel(logger, saved);
            }
        }
    }

    static long startRecovery(RecoveriesCollection collection, DiscoveryNode sourceNode, IndexShard shard) {
        final DiscoveryNode rNode = getDiscoveryNode(shard.routingEntry().currentNodeId());
        shard.markAsRecovering("remote", new RecoveryState(shard.routingEntry(), sourceNode, rNode));
        shard.prepareForIndexRecovery();
        return collection.startRecovery(shard, sourceNode, 0L, null, listener, null);
    }

    private static RecoveryState fakeRecoveryState() {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("index", 1, "node", true, ShardRoutingState.INITIALIZING);
        return new RecoveryState(shardRouting, DiscoveryNodeUtils.create("source"), DiscoveryNodeUtils.create("target"));
    }

    @FunctionalInterface
    private interface ContenderFactory {
        Runnable build(RecoveriesCollection collection, ShardId shardId, long recoveryId);
    }

    private record ContenderTuple(ContenderFactory factory, String expectedLogMessage) {}

    /// Count the number of messages that contain first or second and assert that we counted exactly one.
    private static final class ExactlyOneOfExpectation implements MockLog.LoggingExpectation {
        private final String loggerName;
        private final String first;
        private final String second;
        private final AtomicInteger firstCount = new AtomicInteger();
        private final AtomicInteger secondCount = new AtomicInteger();

        ExactlyOneOfExpectation(String loggerName, String first, String second) {
            this.loggerName = loggerName;
            this.first = first;
            this.second = second;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel() != Level.TRACE || loggerName.equals(event.getLoggerName()) == false) {
                return;
            }
            String message = event.getMessage().getFormattedMessage();
            if (message.contains(first)) {
                firstCount.incrementAndGet();
            } else if (message.contains(second)) {
                secondCount.incrementAndGet();
            }
        }

        @Override
        public void assertMatched() {
            int failCount = firstCount.get();
            int cancelCount = secondCount.get();
            assertThat("expected exactly one recovery outcome trace", failCount + cancelCount, equalTo(1));
        }
    }
}
