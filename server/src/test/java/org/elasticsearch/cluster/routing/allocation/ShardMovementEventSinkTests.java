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

    public void testCompositeDeliversToAllSinksAndIsolatesFailures() {
        final var received1 = new ArrayList<ShardMovementEvent>();
        final var received2 = new ArrayList<ShardMovementEvent>();
        final ShardMovementEventSink failing = event -> { throw new RuntimeException("boom"); };
        final var composite = ShardMovementEventSink.composite(List.of(received1::add, failing, received2::add));

        final var event = new ShardMovementEvent.ShardFailed(
            shardRoutingBuilder(SHARD_ID, "node-1", true, ShardRoutingState.STARTED).build(),
            new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, null)
        );
        composite.accept(event);
        composite.accept(event);
        assertThat(received1, contains(event, event));
        assertThat(received2, contains(event, event));
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
            () -> sink.accept(
                new ShardMovementEvent.RelocationStarted(startedShard, targetShard, new MoveReason(MoveType.CANNOT_REMAIN, "filter"))
            ),
            LoggingShardMovementEventSink.class,
            new MockLog.SeenEventExpectation(
                "relocation started",
                LoggingShardMovementEventSink.class.getCanonicalName(),
                Level.DEBUG,
                "[test-index][0][P] relocating from [node-1] to [node-2]: move(cannot-remain)"
            )
        );
        MockLog.assertThatLogger(
            () -> sink.accept(new ShardMovementEvent.ShardFailed(startedShard, new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, null))),
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
