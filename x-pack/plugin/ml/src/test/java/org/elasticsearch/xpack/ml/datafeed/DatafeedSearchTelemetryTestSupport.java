/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Wires a real {@link DatafeedSearchTelemetry} to mock histograms/counters so extractor tests can verify emissions.
 */
public final class DatafeedSearchTelemetryTestSupport {

    public final LongHistogram resultCountHistogram;
    public final LongHistogram pageSizeHistogram;
    public final LongCounter fullPageCounter;
    public final DatafeedSearchTelemetry telemetry;

    public DatafeedSearchTelemetryTestSupport() {
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        resultCountHistogram = mock(LongHistogram.class);
        pageSizeHistogram = mock(LongHistogram.class);
        fullPageCounter = mock(LongCounter.class);
        when(meterRegistry.registerLongHistogram(eq(DatafeedSearchTelemetry.RESULT_COUNT_METRIC), anyString(), anyString(), anyList()))
            .thenReturn(resultCountHistogram);
        when(meterRegistry.registerLongHistogram(eq(DatafeedSearchTelemetry.PAGE_SIZE_METRIC), anyString(), anyString(), anyList()))
            .thenReturn(pageSizeHistogram);
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.FULL_PAGE_METRIC), anyString(), anyString())).thenReturn(
            fullPageCounter
        );
        telemetry = new DatafeedSearchTelemetry(meterRegistry);
    }
}
