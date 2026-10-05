/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Locale;
import java.util.Map;

/// Metrics for SIGTERM graceful shutdown.
public class SigtermShutdownMetrics {

    public static final String SHUTDOWN_DURATION_HISTOGRAM = "es.shutdown.sigterm.duration.histogram";
    public static final String SHARD_MIGRATION_DURATION_HISTOGRAM = "es.shutdown.sigterm.migration.duration.histogram";

    public static final String ATTRIBUTE_NAME_STATUS = "es_shutdown_status";
    public static final String ATTRIBUTE_NAME_TIMED_OUT = "es_shutdown_timed_out";
    public static final String ATTRIBUTE_NAME_MIGRATION_COMPLETED = "es_shutdown_migration_completed";

    public static final SigtermShutdownMetrics NOOP = new SigtermShutdownMetrics(MeterRegistry.NOOP);

    // Seconds rather than milliseconds: default histogram buckets top out at 131072 (see modules/apm/METERING.md).
    // In milliseconds, that's ~2.2 minutes but SIGTERM default timeout is 1 hour (StatelessSigtermPlugin#TIMEOUT_SETTING).
    private final DoubleHistogram shutdownDurationSeconds;
    private final DoubleHistogram shardMigrationDurationSeconds;

    public SigtermShutdownMetrics(MeterRegistry meterRegistry) {
        this.shutdownDurationSeconds = meterRegistry.registerDoubleHistogram(
            SHUTDOWN_DURATION_HISTOGRAM,
            "Time spent in SIGTERM handleTermination waiting for graceful shutdown to complete",
            "s"
        );
        this.shardMigrationDurationSeconds = meterRegistry.registerDoubleHistogram(
            SHARD_MIGRATION_DURATION_HISTOGRAM,
            "Time spent waiting for shard migration to complete during SIGTERM shutdown",
            "s"
        );
    }

    /// Records the overall duration of [`SigtermTerminationHandler#handleTermination`].
    ///
    /// @param durationMillis elapsed time in milliseconds
    /// @param status overall shutdown outcome (for example `complete`, `failed`, `in_progress`)
    /// @param timedOut true when the SIGTERM timeout expired before overall shutdown completed
    public void recordShutdownTime(long durationMillis, SigtermTerminationHandler.ShutdownStatus status, boolean timedOut) {
        shutdownDurationSeconds.record(toSeconds(durationMillis), attributes(status, timedOut));
    }

    /// Records the shard-migration wait, from put-shutdown ack until migration is `COMPLETE`, or until the wait ends.
    ///
    /// @param durationMillis elapsed time in milliseconds
    /// @param completed true when migration reached `COMPLETE` before the wait ended
    public void recordMigrationTime(long durationMillis, boolean completed) {
        shardMigrationDurationSeconds.record(toSeconds(durationMillis), Map.of(ATTRIBUTE_NAME_MIGRATION_COMPLETED, completed));
    }

    private static double toSeconds(long durationMillis) {
        return durationMillis / 1000.0;
    }

    private static Map<String, Object> attributes(SigtermTerminationHandler.ShutdownStatus status, boolean timedOut) {
        return Map.of(ATTRIBUTE_NAME_STATUS, status.name().toLowerCase(Locale.ROOT), ATTRIBUTE_NAME_TIMED_OUT, timedOut);
    }
}
