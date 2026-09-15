/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TestMeterUsages {

    private static final Logger logger = LogManager.getLogger(TestMeterUsages.class);

    public static final String CUSTOM_BOUNDARIES_LONG_HISTOGRAM_NAME = "es.test.long_hist_custom_bounds.histogram";
    public static final String CUSTOM_BOUNDARIES_DOUBLE_HISTOGRAM_NAME = "es.test.double_hist_custom_bounds.histogram";
    public static final List<Long> CUSTOM_BOUNDARIES = List.of(0L, 10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L);

    private final DoubleCounter doubleCounter;
    private final DoubleCounter longCounter;
    private final DoubleHistogram doubleHistogram;
    private final LongHistogram longHistogram;
    private final LongHistogram longHistogramCustomBoundaries;
    private final DoubleHistogram doubleHistogramCustomBoundaries;
    private final AtomicReference<DoubleWithAttributes> doubleWithAttributes = new AtomicReference<>();
    private final AtomicReference<LongWithAttributes> longWithAttributes = new AtomicReference<>();
    private final AtomicReference<DoubleWithAttributes> asyncDoubleWithAttributes = new AtomicReference<>();
    private final AtomicReference<LongWithAttributes> asyncLongWithAttributes = new AtomicReference<>();

    public TestMeterUsages(MeterRegistry meterRegistry) {
        this.doubleCounter = meterRegistry.registerDoubleCounter("es.test.long_counter.total", "test", "unit");
        this.longCounter = meterRegistry.registerDoubleCounter("es.test.double_counter.total", "test", "unit");
        this.doubleHistogram = meterRegistry.registerDoubleHistogram("es.test.double_histogram.histogram", "test", "unit");
        this.longHistogram = meterRegistry.registerLongHistogram("es.test.long_histogram.histogram", "test", "unit");
        this.longHistogramCustomBoundaries = meterRegistry.registerLongHistogram(
            CUSTOM_BOUNDARIES_LONG_HISTOGRAM_NAME,
            "test",
            "unit",
            CUSTOM_BOUNDARIES
        );
        this.doubleHistogramCustomBoundaries = meterRegistry.registerDoubleHistogram(
            CUSTOM_BOUNDARIES_DOUBLE_HISTOGRAM_NAME,
            "test",
            "unit",
            CUSTOM_BOUNDARIES.stream().map(Long::doubleValue).toList()
        );
        meterRegistry.registerDoubleAsyncGauge("es.test.double_gauge.current", "test", "unit", () -> {
            var value = doubleWithAttributes.get();
            logger.trace("[es.test.double_gauge.current] callback with value [{}]", value);
            return value;
        });
        meterRegistry.registerLongAsyncGauge("es.test.long_gauge.current", "test", "unit", () -> {
            var value = longWithAttributes.get();
            logger.trace("[es.test.long_gauge.current] callback with value [{}]", value);
            return value;
        });
        meterRegistry.registerLongAsyncCounter("es.test.async_long_counter.total", "test", "unit", () -> {
            var value = asyncLongWithAttributes.get();
            logger.trace("[es.test.async_long_counter.total] callback with value [{}]", value);
            return value;
        });
        meterRegistry.registerDoubleAsyncCounter("es.test.async_double_counter.total", "test", "unit", () -> {
            var value = asyncDoubleWithAttributes.get();
            logger.trace("[es.test.async_double_counter.total] callback with value [{}]", value);
            return value;
        });
    }

    public void recordMetric(String metricName, String metricValue) {
        if (CUSTOM_BOUNDARIES_LONG_HISTOGRAM_NAME.equals(metricName)) {
            longHistogramCustomBoundaries.record(Long.parseLong(metricValue));
        } else if (CUSTOM_BOUNDARIES_DOUBLE_HISTOGRAM_NAME.equals(metricName)) {
            doubleHistogramCustomBoundaries.record(Double.parseDouble(metricValue));
        } else {
            logger.warn("recordMetric: unknown metric [{}]", metricName);
        }
    }

    public void testUponRequest() {
        logger.info("setting counters");
        doubleCounter.increment();
        longCounter.increment();
        doubleHistogram.record(1.0);
        doubleHistogram.record(2.0);
        longHistogram.record(1);
        longHistogram.record(2);

        // triggers gauges and async counters
        logger.trace("setting async counters");
        doubleWithAttributes.set(new DoubleWithAttributes(1.0, Map.of()));
        longWithAttributes.set(new LongWithAttributes(1, Map.of()));
        asyncDoubleWithAttributes.updateAndGet(prev -> new DoubleWithAttributes(prev == null ? 1.0 : prev.value() + 1.0, Map.of()));
        asyncLongWithAttributes.updateAndGet(prev -> new LongWithAttributes(prev == null ? 1 : prev.value() + 1, Map.of()));
    }
}
