/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.recovery;

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
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveriesCollection;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.test.MockLog;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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

    public void testRaceMarkRecoveryAsDoneWithCancelRecoveriesForShard() throws Exception {
        Function<ShardId, String> firstSupplier = (id) -> id + " marking recovery from";
        Function<ShardId, String> secondSupplier = (id) -> id + " canceled recovery from";
        ContenderFactory firstContenderFactory = (collection, shardId, recoveryId) -> () -> collection.markRecoveryAsDone(recoveryId);
        ContenderFactory secondContenderFactory = (collection, shardId, recoveryId) -> () -> collection.cancelRecoveriesForShard(
            shardId,
            "test"
        );

        raceAndAssertExactlyOneLogMessage(firstSupplier, secondSupplier, firstContenderFactory, secondContenderFactory);
    }

    public void testRaceFailRecoveryWithCancelRecoveriesForShard() throws Exception {
        Function<ShardId, String> firstSupplier = (id) -> id + " failing recovery from";
        Function<ShardId, String> secondSupplier = (id) -> id + " canceled recovery from";
        ContenderFactory firstContenderFactory = (collection, shardId, recoveryId) -> () -> collection.failRecovery(
            recoveryId,
            new RecoveryFailedException(fakeRecoveryState(), "failed", new RuntimeException("cause")),
            false
        );
        ContenderFactory secondContenderFactory = (collection, shardId, recoveryId) -> () -> collection.cancelRecoveriesForShard(
            shardId,
            "test"
        );

        raceAndAssertExactlyOneLogMessage(firstSupplier, secondSupplier, firstContenderFactory, secondContenderFactory);
    }

    public void testRaceCancelRecoveryWithCancelRecoveriesForShard() throws Exception {
        Function<ShardId, String> firstSupplier = (id) -> "first reason";
        Function<ShardId, String> secondSupplier = (id) -> "second reason";
        ContenderFactory firstContenderFactory = (collection, shardId, recoveryId) -> () -> collection.cancelRecovery(
            recoveryId,
            "first reason"
        );
        ContenderFactory secondContenderFactory = (collection, shardId, recoveryId) -> () -> collection.cancelRecoveriesForShard(
            shardId,
            "second reason"
        );

        raceAndAssertExactlyOneLogMessage(firstSupplier, secondSupplier, firstContenderFactory, secondContenderFactory);
    }

    /// Race the two contenders against each other and assert that the log contains exactly one message
    /// that contains any of the two expected messages.
    private void raceAndAssertExactlyOneLogMessage(
        Function<ShardId, String> firstExpectedMessage,
        Function<ShardId, String> secondExpectedMessage,
        ContenderFactory firstContender,
        ContenderFactory secondContender
    ) throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            final RecoveriesCollection collection = new RecoveriesCollection(logger);
            IndexShard shard = shards.addReplica();
            ShardId shardId = shard.shardId();
            long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shard);

            Level saved = logger.getLevel();
            Loggers.setLevel(logger, Level.TRACE);
            try (MockLog mocklog = MockLog.capture(getClass())) {
                mocklog.addExpectation(
                    new ExactlyOneOfExpectation(
                        getClass().getName(),
                        firstExpectedMessage.apply(shardId),
                        secondExpectedMessage.apply(shardId)
                    )
                );
                startInParallel(
                    firstContender.build(collection, shardId, recoveryId),
                    secondContender.build(collection, shardId, recoveryId)
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
            assertTrue("fail and cancel must not both log", failCount == 0 || cancelCount == 0);
        }
    }
}
