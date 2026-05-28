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

public class LongGaugeMetricTests extends ESTestCase {

    private static final String GAUGE_NAME = "test.gauge";

    public void testNonConsumingGaugeAlwaysReportsCurrentValue() {
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLong();
        metric.set(value);

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), RecordingMeterRegistry.measures(value));

        registry.getRecorder().resetCalls();
        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), RecordingMeterRegistry.measures(value));
    }

    public void testConsumingGaugeThrowsAssertionErrorIfGetIsCalled() {
        assumeTrue("This test only makes sense if assertions are enabled", Assertions.ENABLED);
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");
        expectThrows(AssertionError.class, metric::get);
    }

    public void testConsumingGaugeDoesNotReportBeforeFirstSet() {
        final var registry = new RecordingMeterRegistry();
        LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), empty());
    }

    public void testConsumingGaugeReportsValueAfterSet() {
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE);
        metric.set(value);

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), RecordingMeterRegistry.measures(value));
    }

    public void testConsumingGaugeValueIsConsumedAfterPoll() {
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE);
        metric.set(value);

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), RecordingMeterRegistry.measures(value));

        registry.getRecorder().resetCalls();
        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), empty());
    }

    public void testConsumingGaugeReportsAgainAfterSecondSet() {
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");
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

    public void testGetIfPresentReturnsEmptyForUninitializedConsumingGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");
        assertThat(metric.getIfPresent(), equalTo(OptionalLong.empty()));
    }

    public void testGetIfPresentReturnsValueAfterSetOnConsumingGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE);
        metric.set(value);
        assertThat(metric.getIfPresent(), equalTo(OptionalLong.of(value)));
    }

    public void testGetIfPresentReturnsEmptyAfterPollOnConsumingGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");
        metric.set(randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE));
        registry.getRecorder().collect();
        assertThat(metric.getIfPresent(), equalTo(OptionalLong.empty()));
    }

    public void testGetIfPresentAlwaysReturnsPresentForNonConsumingGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        assertThat(metric.getIfPresent(), equalTo(OptionalLong.of(0L)));
        metric.set(randomLong());
        registry.getRecorder().collect();
        assertThat(metric.getIfPresent().isPresent(), equalTo(true));
    }

    public void testConsumingGaugeWithCustomNoValue() {
        final var registry = new RecordingMeterRegistry();
        final long customNoValue = randomLong();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes", customNoValue);

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

    public void testSetConsumingGaugeToDefaultNoValueThrowsAssertionError() {
        assumeTrue("This test only makes sense if assertions are enabled", Assertions.ENABLED);
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes");
        expectThrows(AssertionError.class, () -> metric.set(Long.MIN_VALUE));
    }

    public void testSetConsumingGaugeToCustomNoValueThrowsAssertionError() {
        assumeTrue("This test only makes sense if assertions are enabled", Assertions.ENABLED);
        final long customNoValue = randomLong();
        final var registry = new RecordingMeterRegistry();
        final var metric = LongGaugeMetric.createConsuming(registry, GAUGE_NAME, "desc", "bytes", customNoValue);
        expectThrows(AssertionError.class, () -> metric.set(customNoValue));
    }
}
