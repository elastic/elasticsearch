/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedSearchTelemetry.ExtractorType;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedSearchTelemetryTests extends ESTestCase {

    public void testResultsBucketShouldClassifyCountsAroundEsqlDefaultCap() {
        assertThat(DatafeedSearchTelemetry.resultsBucket(0), equalTo("0"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(1), equalTo("1_99"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(99), equalTo("1_99"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(100), equalTo("100_499"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(499), equalTo("100_499"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(500), equalTo("500_999"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(999), equalTo("500_999"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(1000), equalTo("1000"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(1001), equalTo("gt_1000"));
        assertThat(DatafeedSearchTelemetry.resultsBucket(Integer.MAX_VALUE), equalTo("gt_1000"));
    }

    public void testRecordSearchResultsShouldIncrementCounterWithAttributes() {
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        LongCounter searchResultsCounter = mock(LongCounter.class);
        LongCounter fullPageCounter = mock(LongCounter.class);
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.RESULTS_METRIC), anyString(), anyString())).thenReturn(
            searchResultsCounter
        );
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.FULL_PAGE_METRIC), anyString(), anyString())).thenReturn(
            fullPageCounter
        );

        DatafeedSearchTelemetry telemetry = new DatafeedSearchTelemetry(meterRegistry);
        telemetry.recordSearchResults(ExtractorType.SCROLL, 1000);

        verify(searchResultsCounter).incrementBy(
            1,
            Map.of(
                DatafeedSearchTelemetry.EXTRACTOR_TYPE_ATTRIBUTE,
                ExtractorType.SCROLL.attributeValue(),
                DatafeedSearchTelemetry.RESULTS_BUCKET_ATTRIBUTE,
                "1000"
            )
        );
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
        LongCounter searchResultsCounter = mock(LongCounter.class);
        LongCounter fullPageCounter = mock(LongCounter.class);
        when(meterRegistry.registerLongCounter(eq(DatafeedSearchTelemetry.RESULTS_METRIC), anyString(), anyString())).thenReturn(
            searchResultsCounter
        );
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
