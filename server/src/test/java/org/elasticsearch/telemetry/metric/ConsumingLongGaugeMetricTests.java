/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.OptionalLong;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class ConsumingLongGaugeMetricTests extends ESTestCase {

    private static final String GAUGE_NAME = "test.gauge";

    public void testGaugeDoesNotReportBeforeFirstSet() {
        final var registry = new RecordingMeterRegistry();
        ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), empty());
    }

    public void testGaugeReportsValueAfterSet() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE);
        metric.set(value);

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), RecordingMeterRegistry.measures(value));
    }

    public void testGaugeValueIsConsumedAfterPoll() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE);
        metric.set(value);

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), RecordingMeterRegistry.measures(value));

        registry.getRecorder().resetCalls();
        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), empty());
    }

    public void testGaugeReportsAgainAfterSecondSet() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long firstValue = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE);
        metric.set(firstValue);

        registry.getRecorder().collect();
        registry.getRecorder().resetCalls();

        final long secondValue = randomValueOtherThan(firstValue, () -> randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE));
        metric.set(secondValue);

        registry.getRecorder().collect();
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME),
            RecordingMeterRegistry.measures(secondValue)
        );
    }

    public void testGetIfPresentReturnsEmptyForUninitializedGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        assertThat(metric.getIfPresent(), equalTo(OptionalLong.empty()));
    }

    public void testGetIfPresentReturnsValueAfterSetOnGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE);
        metric.set(value);
        assertThat(metric.getIfPresent(), equalTo(OptionalLong.of(value)));
    }

    public void testGetIfPresentReturnsEmptyAfterPollOnGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        metric.set(randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE));
        registry.getRecorder().collect();
        assertThat(metric.getIfPresent(), equalTo(OptionalLong.empty()));
    }

    public void testGaugeWithCustomNoValue() {
        final var registry = new RecordingMeterRegistry();
        final long customNoValue = randomLong();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes", customNoValue);

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), empty());

        final long value = randomValueOtherThan(customNoValue, ESTestCase::randomLong);
        metric.set(value);

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), RecordingMeterRegistry.measures(value));

        registry.getRecorder().resetCalls();
        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), empty());
    }

    public void testSetGaugeToDefaultNoValueThrowsAssertionError() {
        assumeTrue("This test only makes sense if assertions are enabled", Assertions.ENABLED);
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        expectThrows(AssertionError.class, () -> metric.set(Long.MIN_VALUE));
    }

    public void testSetGaugeToCustomNoValueThrowsAssertionError() {
        assumeTrue("This test only makes sense if assertions are enabled", Assertions.ENABLED);
        final long customNoValue = randomLong();
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes", customNoValue);
        expectThrows(AssertionError.class, () -> metric.set(customNoValue));
    }
}
