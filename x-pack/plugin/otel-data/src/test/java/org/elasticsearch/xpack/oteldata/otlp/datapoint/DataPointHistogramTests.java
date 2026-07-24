/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.HashSet;
import java.util.List;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class DataPointHistogramTests extends ESTestCase {

    private final HashSet<String> validationErrors = new HashSet<>();

    public void testHistogram() {
        DataPoint.Histogram histo = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(histo.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(histo.getTemporality(), equalTo(AGGREGATION_TEMPORALITY_DELTA));
        assertThat(histo.isValid(validationErrors, MappingHints.DEFAULT_TDIGEST), equalTo(true));
        assertThat(validationErrors, empty());
    }

    public void testCumulativeHistogramAsExponentialHistogram() {
        DataPoint.Histogram histo = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_CUMULATIVE).build())
                .build()
        );
        assertThat(histo.getDynamicTemplate(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM), equalTo("exponential_histogram"));
        assertThat(histo.getTemporality(), equalTo(AGGREGATION_TEMPORALITY_CUMULATIVE));
        assertThat(histo.isValid(validationErrors, MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM), equalTo(true));
        assertThat(validationErrors, empty());
    }

    public void testCumulativeHistogramUnsupported() {
        DataPoint.Histogram histo = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_CUMULATIVE).build())
                .build()
        );
        assertThat(histo.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(histo.isValid(validationErrors, MappingHints.DEFAULT_TDIGEST), equalTo(false));
        assertThat(
            validationErrors,
            contains(containsString("cumulative histogram metrics are only supported when stored as exponential_histogram"))
        );
    }

    public void testHistogramSingleBucketWithoutBounds() {
        DataPoint.Histogram histo = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder().addBucketCounts(1).setCount(1).setSum(2.0).build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(histo.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(histo.isValid(validationErrors, MappingHints.DEFAULT_TDIGEST), equalTo(true));
        assertThat(validationErrors, empty());
    }

    public void testHistogramInvalidBucketCountsLength() {
        DataPoint.Histogram histo = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder().addAllBucketCounts(List.of(1L, 2L)).build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(histo.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(histo.isValid(validationErrors, MappingHints.DEFAULT_TDIGEST), equalTo(false));
        assertThat(validationErrors, contains(containsString("histogram bucket count must be one greater than explicit bounds count")));
    }

    public void testHistogramBucketBoundsNotSorted() {
        DataPoint.Histogram histo = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder()
                .addExplicitBounds(2.0)
                .addExplicitBounds(1.0)
                .addBucketCounts(1)
                .addBucketCounts(1)
                .addBucketCounts(1)
                .build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(histo.isValid(validationErrors, MappingHints.DEFAULT_TDIGEST), equalTo(false));
        assertThat(validationErrors, contains(containsString("histogram bounds are not sorted or not unique")));
    }

    public void testHistogramBucketBoundsNotUnique() {
        DataPoint.Histogram histo = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder()
                .addExplicitBounds(1.0)
                .addExplicitBounds(1.0)
                .addBucketCounts(1)
                .addBucketCounts(1)
                .addBucketCounts(1)
                .build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(histo.isValid(validationErrors, MappingHints.DEFAULT_TDIGEST), equalTo(false));
        assertThat(validationErrors, contains(containsString("histogram bounds are not sorted or not unique")));
    }
}
