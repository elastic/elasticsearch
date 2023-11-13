/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TestMeterUsages {

    private final DoubleCounter doubleCounter;
    private final DoubleCounter longCounter;
    private final DoubleHistogram doubleHistogram;
    private final LongHistogram longHistogram;
    private final AtomicReference<DoubleWithAttributes> doubleWithAttributes = new AtomicReference<>();
    private final AtomicReference<LongWithAttributes> longWithAttributes = new AtomicReference<>();

    public TestMeterUsages(MeterRegistry meterRegistry) {
        this.doubleCounter = meterRegistry.registerDoubleCounter("testDoubleCounter", "test", "unit");
        this.longCounter = meterRegistry.registerDoubleCounter("testLongCounter", "test", "unit");
        this.doubleHistogram = meterRegistry.registerDoubleHistogram("testDoubleHistogram", "test", "unit");
        this.longHistogram = meterRegistry.registerLongHistogram("testLongHistogram", "test", "unit");
        meterRegistry.registerDoubleGauge("testDoubleGauge", "test", "unit", doubleWithAttributes::get);
        meterRegistry.registerLongGauge("testLongGauge", "test", "unit", longWithAttributes::get);
    }

    public void testUponRequest() {
        doubleCounter.increment();
        longCounter.increment();
        doubleHistogram.record(1.0);
        doubleHistogram.record(2.0);
        longHistogram.record(1);
        longHistogram.record(2);
        doubleWithAttributes.set(new DoubleWithAttributes(1.0, Map.of()));
        longWithAttributes.set(new LongWithAttributes(1, Map.of()));
    }
}
