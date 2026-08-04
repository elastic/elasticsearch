/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Locale;
import java.util.Map;

/// Metrics for SIGTERM graceful shutdown.
public class SigtermShutdownMetrics {

    public static final String SHUTDOWN_DURATION_HISTOGRAM = "es.shutdown.sigterm.duration.histogram";
    public static final String SHARD_MIGRATION_DURATION_HISTOGRAM = "es.shutdown.sigterm.migration.duration.histogram";

    public static final String ATTRIBUTE_NAME_STATUS = "status";
    public static final String ATTRIBUTE_NAME_TIMED_OUT = "timed_out";

    public static final SigtermShutdownMetrics NOOP = new SigtermShutdownMetrics(MeterRegistry.NOOP);

    private final LongHistogram shutdownDurationMillis;
    private final LongHistogram shardMigrationDurationMillis;

    public SigtermShutdownMetrics(MeterRegistry meterRegistry) {
        this.shutdownDurationMillis = meterRegistry.registerLongHistogram(
            SHUTDOWN_DURATION_HISTOGRAM,
            "Time spent in SIGTERM handleTermination waiting for graceful shutdown to complete",
            "ms"
        );
        this.shardMigrationDurationMillis = meterRegistry.registerLongHistogram(
            SHARD_MIGRATION_DURATION_HISTOGRAM,
            "Time spent waiting for shard migration to complete during SIGTERM shutdown",
            "ms"
        );
    }

    /// Records the overall duration of [`SigtermTerminationHandler#handleTermination`].
    ///
    /// @param durationMillis elapsed time in milliseconds
    /// @param status overall shutdown outcome (for example `complete`, `failed`, `in_progress`)
    /// @param timedOut true when the SIGTERM timeout expired before overall shutdown completed
    public void recordShutdownTime(long durationMillis, String status, boolean timedOut) {
        shutdownDurationMillis.record(durationMillis, attributes(status, timedOut));
    }

    /// Records the shard-migration wait, from put-shutdown until migration is `COMPLETE`, or until SIGTERM timeout.
    ///
    /// @param durationMillis elapsed time in milliseconds
    /// @param completed false when migration had not completed before the SIGTERM timeout
    public void recordMigrationTime(long durationMillis, boolean completed) {
        shardMigrationDurationMillis.record(durationMillis, Map.of(ATTRIBUTE_NAME_TIMED_OUT, completed == false));
    }

    private static Map<String, Object> attributes(String status, boolean timedOut) {
        return Map.of(ATTRIBUTE_NAME_STATUS, status.toLowerCase(Locale.ROOT), ATTRIBUTE_NAME_TIMED_OUT, timedOut);
    }
}
