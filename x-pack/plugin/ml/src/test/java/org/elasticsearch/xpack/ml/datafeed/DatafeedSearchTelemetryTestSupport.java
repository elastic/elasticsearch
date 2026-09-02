/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Wires a real {@link DatafeedSearchTelemetry} to mock counters so extractor tests can verify emissions.
 */
public final class DatafeedSearchTelemetryTestSupport {

    public final LongCounter responsesCounter;
    public final LongCounter fullPageCounter;
    public final DatafeedSearchTelemetry telemetry;

    public DatafeedSearchTelemetryTestSupport() {
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        responsesCounter = mock(LongCounter.class);
        fullPageCounter = mock(LongCounter.class);
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.RESPONSES_METRIC), anyString(), anyString())).thenReturn(
            responsesCounter
        );
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.FULL_PAGE_METRIC), anyString(), anyString())).thenReturn(
            fullPageCounter
        );
        telemetry = new DatafeedSearchTelemetry(meterRegistry);
    }
}
