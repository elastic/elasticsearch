/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
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
        final long value = randomLong();
        metric.set(value);

        registry.getRecorder().collect();
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME), RecordingMeterRegistry.measures(value));
    }

    public void testGaugeValueIsConsumedAfterPoll() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLong();
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
        final long firstValue = randomLong();
        metric.set(firstValue);

        registry.getRecorder().collect();
        registry.getRecorder().resetCalls();

        final long secondValue = randomValueOtherThan(firstValue, ESTestCase::randomLong);
        metric.set(secondValue);

        registry.getRecorder().collect();
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME),
            RecordingMeterRegistry.measures(secondValue)
        );
    }

    public void testGetValueIfPresentReturnsEmptyForUninitializedGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        assertThat(metric.getValueIfPresent(), equalTo(OptionalLong.empty()));
    }

    public void testGetValueIfPresentReturnsValueAfterSetOnGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLong();
        metric.set(value);
        assertThat(metric.getValueIfPresent(), equalTo(OptionalLong.of(value)));
    }

    public void testGetValueIfPresentReturnsEmptyAfterPollOnGauge() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        metric.set(randomLong());
        registry.getRecorder().collect();
        assertThat(metric.getValueIfPresent(), equalTo(OptionalLong.empty()));
    }

    public void testGaugeReportsAttributesAfterSet() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLong();
        final var attributes = Map.<String, Object>of("ratio", 12.5d, "label", "x");
        metric.set(value, attributes);

        registry.getRecorder().collect();
        final var measurement = registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME).getFirst();
        assertThat(measurement.getLong(), equalTo(value));
        assertThat(measurement.attributes(), equalTo(attributes));
    }

    public void testSetWithNullAttributesThrows() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        expectThrows(NullPointerException.class, () -> metric.set(randomLong(), null));
    }

    public void testSetSnapshotsAttributes() {
        final var registry = new RecordingMeterRegistry();
        final var metric = ConsumingLongGaugeMetric.create(registry, GAUGE_NAME, "desc", "bytes");
        final long value = randomLong();
        final var attributes = new HashMap<String, Object>();
        attributes.put("label", "before");
        metric.set(value, attributes);
        attributes.put("label", "after");

        registry.getRecorder().collect();
        final var measurement = registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, GAUGE_NAME).getFirst();
        assertThat(measurement.getLong(), equalTo(value));
        assertThat(measurement.attributes(), equalTo(Map.of("label", "before")));
    }
}
