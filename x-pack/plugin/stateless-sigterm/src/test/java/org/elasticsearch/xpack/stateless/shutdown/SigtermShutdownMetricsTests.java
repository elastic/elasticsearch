/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.common.Strings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownMetrics.ATTRIBUTE_NAME_STATUS;
import static org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownMetrics.ATTRIBUTE_NAME_TIMED_OUT;
import static org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownMetrics.SHARD_MIGRATION_DURATION_HISTOGRAM;
import static org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownMetrics.SHUTDOWN_DURATION_HISTOGRAM;
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
        final long durationMs = randomNonNegativeLong();
        final var status = randomBoolean() ? "COMPLETE" : "FAILED";
        metrics.recordShutdownTime(durationMs, status, false);

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, SHUTDOWN_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getLong(), equalTo(durationMs));
        assertThat(measurements.getFirst().attributes().get(ATTRIBUTE_NAME_STATUS), equalTo(Strings.toLowercaseAscii(status)));
        assertThat(measurements.getFirst().attributes().get(ATTRIBUTE_NAME_TIMED_OUT), equalTo(false));
    }

    public void testRecordMigrationTime() {
        final long durationMs = randomNonNegativeLong();
        metrics.recordMigrationTime(durationMs, true);

        List<Measurement> measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, SHARD_MIGRATION_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getLong(), equalTo(durationMs));
        assertThat(measurements.getFirst().attributes().get(ATTRIBUTE_NAME_TIMED_OUT), equalTo(false));

        metrics.recordMigrationTime(durationMs, false);
        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, SHARD_MIGRATION_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(2));
        assertThat(measurements.get(1).attributes().get(ATTRIBUTE_NAME_TIMED_OUT), equalTo(true));
    }
}
