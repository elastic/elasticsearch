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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedSearchTelemetry.ExtractorType;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedSearchTelemetryTests extends ESTestCase {

    private static final Pattern ALLOWED_METRIC_SUFFIX = Pattern.compile(
        "\\.(total|current|ratio|status|usage|size|utilization|histogram|time)$"
    );

    public void testMetricNamesShouldUseAllowedApmSuffixes() {
        for (String metricName : List.of(
            DatafeedSearchTelemetry.RESULT_COUNT_METRIC,
            DatafeedSearchTelemetry.PAGE_SIZE_METRIC,
            DatafeedSearchTelemetry.FULL_PAGE_METRIC
        )) {
            assertThat(
                "Metric name [" + metricName + "] must end with an allowed MetricValidator suffix",
                ALLOWED_METRIC_SUFFIX.matcher(metricName).find(),
                is(true)
            );
        }
    }

    public void testRecordSearchResultsShouldRecordResultCountAndPageSizeHistograms() {
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        LongHistogram resultCountHistogram = mock(LongHistogram.class);
        LongHistogram pageSizeHistogram = mock(LongHistogram.class);
        LongCounter fullPageCounter = mock(LongCounter.class);
        when(meterRegistry.registerLongHistogram(eq(DatafeedSearchTelemetry.RESULT_COUNT_METRIC), anyString(), anyString(), anyList()))
            .thenReturn(resultCountHistogram);
        when(meterRegistry.registerLongHistogram(eq(DatafeedSearchTelemetry.PAGE_SIZE_METRIC), anyString(), anyString(), anyList()))
            .thenReturn(pageSizeHistogram);
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.FULL_PAGE_METRIC), anyString(), anyString())).thenReturn(
            fullPageCounter
        );

        DatafeedSearchTelemetry telemetry = new DatafeedSearchTelemetry(meterRegistry);
        telemetry.recordSearchResults(ExtractorType.SCROLL, 1000, 1000);

        verify(resultCountHistogram).record(
            1000,
            Map.of(DatafeedSearchTelemetry.EXTRACTOR_TYPE_ATTRIBUTE, ExtractorType.SCROLL.attributeValue())
        );
        verify(pageSizeHistogram).record(
            1000,
            Map.of(DatafeedSearchTelemetry.EXTRACTOR_TYPE_ATTRIBUTE, ExtractorType.SCROLL.attributeValue())
        );
    }

    public void testRecordSearchResultsGivenAggregationShouldOmitPageSizeHistogram() {
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        LongHistogram resultCountHistogram = mock(LongHistogram.class);
        LongHistogram pageSizeHistogram = mock(LongHistogram.class);
        LongCounter fullPageCounter = mock(LongCounter.class);
        when(meterRegistry.registerLongHistogram(eq(DatafeedSearchTelemetry.RESULT_COUNT_METRIC), anyString(), anyString(), anyList()))
            .thenReturn(resultCountHistogram);
        when(meterRegistry.registerLongHistogram(eq(DatafeedSearchTelemetry.PAGE_SIZE_METRIC), anyString(), anyString(), anyList()))
            .thenReturn(pageSizeHistogram);
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.FULL_PAGE_METRIC), anyString(), anyString())).thenReturn(
            fullPageCounter
        );

        DatafeedSearchTelemetry telemetry = new DatafeedSearchTelemetry(meterRegistry);
        telemetry.recordSearchResults(ExtractorType.AGGREGATION, 4, null);

        verify(resultCountHistogram).record(
            4,
            Map.of(DatafeedSearchTelemetry.EXTRACTOR_TYPE_ATTRIBUTE, ExtractorType.AGGREGATION.attributeValue())
        );
        verify(pageSizeHistogram, never()).record(anyLong(), anyMap());
    }

    public void testClassifyExtractorTypeShouldDistinguishScrollAggregationAndComposite() {
        DatafeedConfig scroll = mock(DatafeedConfig.class);
        when(scroll.hasAggregations()).thenReturn(false);
        assertThat(DatafeedSearchTelemetry.classifyExtractorType(scroll, NamedXContentRegistry.EMPTY), equalTo(ExtractorType.SCROLL));

        DatafeedConfig aggregation = mock(DatafeedConfig.class);
        when(aggregation.hasAggregations()).thenReturn(true);
        when(aggregation.hasCompositeAgg(any())).thenReturn(false);
        assertThat(
            DatafeedSearchTelemetry.classifyExtractorType(aggregation, NamedXContentRegistry.EMPTY),
            equalTo(ExtractorType.AGGREGATION)
        );

        DatafeedConfig composite = mock(DatafeedConfig.class);
        when(composite.hasAggregations()).thenReturn(true);
        when(composite.hasCompositeAgg(any())).thenReturn(true);
        assertThat(DatafeedSearchTelemetry.classifyExtractorType(composite, NamedXContentRegistry.EMPTY), equalTo(ExtractorType.COMPOSITE));
    }

    public void testRecordFullPageShouldIncrementFullPageCounter() {
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        LongHistogram resultCountHistogram = mock(LongHistogram.class);
        LongHistogram pageSizeHistogram = mock(LongHistogram.class);
        LongCounter fullPageCounter = mock(LongCounter.class);
        when(meterRegistry.registerLongHistogram(eq(DatafeedSearchTelemetry.RESULT_COUNT_METRIC), anyString(), anyString(), anyList()))
            .thenReturn(resultCountHistogram);
        when(meterRegistry.registerLongHistogram(eq(DatafeedSearchTelemetry.PAGE_SIZE_METRIC), anyString(), anyString(), anyList()))
            .thenReturn(pageSizeHistogram);
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.FULL_PAGE_METRIC), anyString(), anyString())).thenReturn(
            fullPageCounter
        );

        DatafeedSearchTelemetry telemetry = new DatafeedSearchTelemetry(meterRegistry);
        telemetry.recordFullPage(ExtractorType.COMPOSITE);

        verify(fullPageCounter).incrementBy(
            1,
            Map.of(DatafeedSearchTelemetry.EXTRACTOR_TYPE_ATTRIBUTE, ExtractorType.COMPOSITE.attributeValue())
        );
    }
}
