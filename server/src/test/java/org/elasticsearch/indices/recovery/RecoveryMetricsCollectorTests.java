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
import org.elasticsearch.indices.recovery.ThrottlingRecoveryService.BlockedState;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
            () -> null,
            () -> 0L
        );
        final String gateName = randomIdentifier();
        final String secondGateName = randomValueOtherThan(gateName, ESTestCase::randomIdentifier);
        final long blockedTimeMillis = randomLongBetween(0, 60_000);
        final long secondBlockedTimeMillis = randomLongBetween(0, 60_000);

        assertThat(telemetryPlugin.getLongCounterMeasurement(RECOVERY_GATE_BLOCKED_TOTAL_METRIC), empty());
        assertThat(telemetryPlugin.getLongHistogramMeasurement(RECOVERY_GATE_BLOCKED_DURATION_METRIC), empty());

        collector.onRecoveriesBlocked(gateName);
        collector.onRecoveriesUnblocked(blockedTimeMillis);
        collector.onRecoveriesBlocked(secondGateName);
        collector.onRecoveriesUnblocked(secondBlockedTimeMillis);

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

    public void testCurrentRecoveryGateMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final var blockedState = new AtomicReference<BlockedState>();
        final var relativeTimeMillis = new AtomicLong(randomFrom(0L, randomLongBetween(-60_000, -1), randomLongBetween(1, 60_000)));
        try (
            var ignored = new RecoveryMetricsCollector(
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY),
                blockedState::get,
                relativeTimeMillis::get
            )
        ) {
            int blocks = between(2, 10);
            for (int i = 0; i < blocks; i++) {
                assertCurrentGateMetrics(telemetryPlugin, 0L, 0L);
                // Gauges observe the authoritative state without needing scheduling callbacks.
                blockedState.set(new BlockedState(randomIdentifier(), relativeTimeMillis.get()));
                assertCurrentGateMetrics(telemetryPlugin, 1L, 0L);

                final long elapsed = randomLongBetween(1, 60_000);
                relativeTimeMillis.addAndGet(elapsed);
                assertCurrentGateMetrics(telemetryPlugin, 1L, elapsed);
                relativeTimeMillis.addAndGet(elapsed);
                assertCurrentGateMetrics(telemetryPlugin, 1L, 2 * elapsed);

                blockedState.set(null);
                relativeTimeMillis.addAndGet(randomLongBetween(1, 60_000));
                // Do not collect between blocks: the next observation must still reflect only the new block.
            }
            assertCurrentGateMetrics(telemetryPlugin, 0L, 0L);
        }
    }

    private static void assertCurrentGateMetrics(TestTelemetryPlugin telemetryPlugin, long expectedBlocked, long expectedDuration) {
        telemetryPlugin.collect();
        assertThat(
            telemetryPlugin.getLongGaugeMeasurement(RECOVERY_GATE_BLOCKED_CURRENT_METRIC).getLast().getLong(),
            equalTo(expectedBlocked)
        );
        assertThat(
            telemetryPlugin.getLongGaugeMeasurement(RECOVERY_GATE_BLOCKED_CURRENT_DURATION_METRIC).getLast().getLong(),
            equalTo(expectedDuration)
        );
    }
}
