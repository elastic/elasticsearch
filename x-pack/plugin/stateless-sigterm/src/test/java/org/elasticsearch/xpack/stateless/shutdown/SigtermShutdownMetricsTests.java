/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SigtermShutdownMetricsTests extends ESTestCase {

    private RecordingMeterRegistry registry;
    private SigtermShutdownMetrics metrics;

    @Before
    public void createMetrics() {
        registry = new RecordingMeterRegistry();
        metrics = new SigtermShutdownMetrics(registry);
    }

    public void testRecordShutdownTime() {
        final long durationMs = randomLongBetween(0, TimeUnit.HOURS.toMillis(2));
        final var status = randomFrom(SigtermTerminationHandler.ShutdownStatus.values());
        metrics.recordShutdownTime(durationMs, status, false);

        final List<Measurement> measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHUTDOWN_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getDouble(), equalTo(durationMs / 1000.0));
        assertThat(
            measurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_STATUS),
            equalTo(status.name().toLowerCase(Locale.ROOT))
        );
        assertThat(measurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_TIMED_OUT), equalTo(false));
    }

    public void testRecordMigrationTime() {
        final long durationMs = randomLongBetween(0, TimeUnit.HOURS.toMillis(2));
        metrics.recordMigrationTime(durationMs, true);

        List<Measurement> measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHARD_MIGRATION_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getDouble(), equalTo(durationMs / 1000.0));
        assertThat(measurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_MIGRATION_COMPLETED), equalTo(true));

        metrics.recordMigrationTime(durationMs, false);
        measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHARD_MIGRATION_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(2));
        assertThat(measurements.get(1).attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_MIGRATION_COMPLETED), equalTo(false));
    }
}
