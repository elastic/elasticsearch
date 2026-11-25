/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Counters for what the sender of data node requests does when every known copy of a shard has failed as unavailable. They show
 * whether queries wait for newer routing at all, whether the wait is long enough, and whether the resolution attempts run out.
 * <p>
 * The counters are registered once per node, when the instance is built, and shared by every sender.
 */
final class ShardResolutionMetrics {

    static final String ROUTING_WAITS = "es.esql.shard_resolution.routing_waits.total";
    static final String ROUTING_WAIT_TIMEOUTS = "es.esql.shard_resolution.routing_wait_timeouts.total";
    static final String ATTEMPTS_EXHAUSTED = "es.esql.shard_resolution.attempts_exhausted.total";

    static final ShardResolutionMetrics NOOP = new ShardResolutionMetrics(MeterRegistry.NOOP);

    private final LongCounter routingWaits;
    private final LongCounter routingWaitTimeouts;
    private final LongCounter attemptsExhausted;

    ShardResolutionMetrics(MeterRegistry meterRegistry) {
        routingWaits = meterRegistry.registerLongCounter(ROUTING_WAITS, "ESQL shard routing waits", "unit");
        routingWaitTimeouts = meterRegistry.registerLongCounter(ROUTING_WAIT_TIMEOUTS, "ESQL shard routing wait timeouts", "unit");
        attemptsExhausted = meterRegistry.registerLongCounter(
            ATTEMPTS_EXHAUSTED,
            "ESQL data-node request senders that exhausted their shard resolution attempts",
            "unit"
        );
    }

    /**
     * A routing wait was armed. One wait covers every shard that stalled in the same pass of the sending loop, so this counts
     * stalls of a sender, not shards.
     */
    void routingWaitStarted() {
        routingWaits.increment();
    }

    /**
     * A routing wait ended because no newer routing arrived in time. Compared with {@link #routingWaitStarted()} it tells whether
     * {@link EsqlPlugin#SHARD_ROUTING_WAIT_TIMEOUT} is long enough for the relocations of the cluster.
     */
    void routingWaitTimedOut() {
        routingWaitTimeouts.increment();
    }

    /**
     * A sender failed a shard because its resolution attempts were spent. Counted once per sender, the first time it happens.
     * <p>
     * This counts senders and not queries. A query has one sender per executed plan and per cluster, and nothing is shared between
     * the senders of a query that could tell them that another one has counted already. The number is an upper bound of the number
     * of queries, and answers the question it is there for: whether the attempts run out at all.
     */
    void attemptsExhausted() {
        attemptsExhausted.increment();
    }
}
