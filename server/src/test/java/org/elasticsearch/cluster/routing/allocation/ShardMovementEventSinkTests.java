/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.routing.MoveReason;
import org.elasticsearch.cluster.routing.MoveType;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.sameInstance;

public class ShardMovementEventSinkTests extends ESTestCase {

    private static final ShardId SHARD_ID = new ShardId("test-index", "_na_", 0);

    /**
     * Records each event as a list of the method name and its arguments
     */
    static class RecordingSink implements ShardMovementEventSink {
        final List<List<Object>> events = new ArrayList<>();

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            events.add(List.of("shardInitialized", unassignedShard, initializedShard));
        }

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            events.add(List.of("shardStarted", initializingShard, startedShard));
        }

        @Override
        public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, MoveReason reason) {
            events.add(List.of("relocationStarted", startedShard, targetRelocatingShard, reason));
        }

        @Override
        public void relocationCompleted(ShardRouting removedRelocationSource) {
            events.add(List.of("relocationCompleted", removedRelocationSource));
        }

        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            events.add(List.of("shardFailed", failedShard, unassignedInfo));
        }
    }

    public void testCompositeDeliversToAllSinksAndIsolatesFailures() {
        final var received1 = new RecordingSink();
        final var received2 = new RecordingSink();
        final var failing = new ShardMovementEventSink() {
            @Override
            public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
                throw new RuntimeException("boom");
            }
        };
        final var composite = ShardMovementEventSink.composite(List.of(received1, failing, received2));

        final var shard = shardRoutingBuilder(SHARD_ID, "node-1", true, ShardRoutingState.STARTED).build();
        final var unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, null);
        composite.shardFailed(shard, unassignedInfo);
        composite.shardFailed(shard, unassignedInfo);
        final var event = List.of("shardFailed", shard, unassignedInfo);
        assertThat(received1.events, contains(event, event));
        assertThat(received2.events, contains(event, event));
    }

    public void testCompositeOfNoneIsNoop() {
        assertThat(ShardMovementEventSink.composite(List.of()), sameInstance(ShardMovementEventSink.NOOP));
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.LoggingShardMovementEventSink:DEBUG",
        reason = "verifies debug level logging"
    )
    public void testLoggingSink() {
        final var sink = new LoggingShardMovementEventSink();
        final var startedShard = shardRoutingBuilder(SHARD_ID, "node-1", true, ShardRoutingState.STARTED).build();
        final var targetShard = shardRoutingBuilder(SHARD_ID, "node-2", true, ShardRoutingState.INITIALIZING).build();

        MockLog.assertThatLogger(
            () -> sink.relocationStarted(startedShard, targetShard, new MoveReason(MoveType.CANNOT_REMAIN, "filter")),
            LoggingShardMovementEventSink.class,
            new MockLog.SeenEventExpectation(
                "relocation started",
                LoggingShardMovementEventSink.class.getCanonicalName(),
                Level.DEBUG,
                "[test-index][0][P] relocating from [node-1] to [node-2]: move(cannot-remain)"
            )
        );
        MockLog.assertThatLogger(
            () -> sink.shardFailed(startedShard, new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, null)),
            LoggingShardMovementEventSink.class,
            new MockLog.SeenEventExpectation(
                "shard failed",
                LoggingShardMovementEventSink.class.getCanonicalName(),
                Level.DEBUG,
                "[test-index][0][P] failed on [node-1]: NODE_LEFT"
            )
        );
    }
}
