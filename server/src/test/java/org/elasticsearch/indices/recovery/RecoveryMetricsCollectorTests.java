/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.indices.recovery.RecoveryMetricsCollector.RECOVERY_GATE_BLOCKED_CURRENT_DURATION_METRIC;
import static org.elasticsearch.indices.recovery.RecoveryMetricsCollector.RECOVERY_GATE_BLOCKED_CURRENT_METRIC;
import static org.elasticsearch.indices.recovery.RecoveryMetricsCollector.RECOVERY_GATE_BLOCKED_DURATION_METRIC;
import static org.elasticsearch.indices.recovery.RecoveryMetricsCollector.RECOVERY_GATE_BLOCKED_TOTAL_METRIC;
import static org.elasticsearch.indices.recovery.RecoveryMetricsCollector.RECOVERY_GATE_NAME_ATTRIBUTE_KEY;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RecoveryMetricsCollectorTests extends ESTestCase {

    public void testRecordsRecoveryGateMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final RecoveryMetricsCollector collector = new RecoveryMetricsCollector(
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY),
            () -> 0L
        );
        final String gateName = randomIdentifier();
        final String secondGateName = randomValueOtherThan(gateName, ESTestCase::randomIdentifier);
        final long blockedTimeMillis = randomLongBetween(0, 60_000);
        final long secondBlockedTimeMillis = randomLongBetween(0, 60_000);

        assertThat(telemetryPlugin.getLongCounterMeasurement(RECOVERY_GATE_BLOCKED_TOTAL_METRIC), empty());
        assertThat(telemetryPlugin.getLongHistogramMeasurement(RECOVERY_GATE_BLOCKED_DURATION_METRIC), empty());
        assertBlockedCurrentMetric(telemetryPlugin, 0L);

        collector.onRecoveriesBlocked(gateName);
        assertBlockedCurrentMetric(telemetryPlugin, 1L);
        collector.onRecoveriesUnblocked(blockedTimeMillis);
        assertBlockedCurrentMetric(telemetryPlugin, 0L);
        collector.onRecoveriesBlocked(secondGateName);
        assertBlockedCurrentMetric(telemetryPlugin, 1L);
        collector.onRecoveriesUnblocked(secondBlockedTimeMillis);
        assertBlockedCurrentMetric(telemetryPlugin, 0L);

        final var blockedMeasurements = telemetryPlugin.getLongCounterMeasurement(RECOVERY_GATE_BLOCKED_TOTAL_METRIC);
        assertThat(blockedMeasurements, hasSize(2));
        assertThat(blockedMeasurements.stream().mapToLong(measurement -> measurement.getLong()).sum(), equalTo(2L));
        assertThat(blockedMeasurements.getFirst().attributes(), equalTo(Map.of(RECOVERY_GATE_NAME_ATTRIBUTE_KEY, gateName)));
        assertThat(blockedMeasurements.getLast().attributes(), equalTo(Map.of(RECOVERY_GATE_NAME_ATTRIBUTE_KEY, secondGateName)));
        final var blockedDurationMeasurements = telemetryPlugin.getLongHistogramMeasurement(RECOVERY_GATE_BLOCKED_DURATION_METRIC);
        assertThat(blockedDurationMeasurements, hasSize(2));
        assertThat(blockedDurationMeasurements.getFirst().getLong(), equalTo(blockedTimeMillis));
        assertThat(blockedDurationMeasurements.getLast().getLong(), equalTo(secondBlockedTimeMillis));

        collector.close();
        assertFalse(telemetryPlugin.getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE).contains(RECOVERY_GATE_BLOCKED_CURRENT_METRIC));
        assertFalse(
            telemetryPlugin.getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE).contains(RECOVERY_GATE_BLOCKED_CURRENT_DURATION_METRIC)
        );
    }

    public void testCurrentBlockedDurationMetric() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final var relativeTimeMillis = new AtomicLong(randomFrom(0L, randomLongBetween(-60_000, -1), randomLongBetween(1, 60_000)));
        try (var collector = new RecoveryMetricsCollector(telemetryPlugin.getTelemetryProvider(Settings.EMPTY), relativeTimeMillis::get)) {
            assertCurrentBlockedDurationMetric(telemetryPlugin, 0L);
            int blocks = randomInt(10);
            for (int i = 0; i < blocks; i++) {
                collector.onRecoveriesBlocked(randomIdentifier());
                assertCurrentBlockedDurationMetric(telemetryPlugin, 0L);

                final long elapsed = randomLongBetween(1, 60_000);
                relativeTimeMillis.addAndGet(elapsed);
                assertCurrentBlockedDurationMetric(telemetryPlugin, elapsed);
                relativeTimeMillis.addAndGet(elapsed);
                assertCurrentBlockedDurationMetric(telemetryPlugin, 2 * elapsed);

                collector.onRecoveriesUnblocked(2 * elapsed);
                relativeTimeMillis.addAndGet(randomLongBetween(1, 60_000));
                // Do not collect between blocks: the next observation must still reflect only the new block.
            }
            assertCurrentBlockedDurationMetric(telemetryPlugin, 0L);
        }
    }

    private static void assertBlockedCurrentMetric(TestTelemetryPlugin telemetryPlugin, long expected) {
        telemetryPlugin.collect();
        assertThat(telemetryPlugin.getLongGaugeMeasurement(RECOVERY_GATE_BLOCKED_CURRENT_METRIC).getLast().getLong(), equalTo(expected));
    }

    private static void assertCurrentBlockedDurationMetric(TestTelemetryPlugin telemetryPlugin, long expected) {
        telemetryPlugin.collect();
        assertThat(
            telemetryPlugin.getLongGaugeMeasurement(RECOVERY_GATE_BLOCKED_CURRENT_DURATION_METRIC).getLast().getLong(),
            equalTo(expected)
        );
    }
}
