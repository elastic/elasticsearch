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
 * Metrics for retrying shard-backed ES|QL work after routing or node availability changes.
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

    void routingWaitStarted() {
        routingWaits.increment();
    }

    void routingWaitTimedOut() {
        routingWaitTimeouts.increment();
    }

    void attemptsExhausted() {
        attemptsExhausted.increment();
    }
}
