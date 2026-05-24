/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.HashSet;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.mappingHints;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class DataPointExponentialHistogramTests extends ESTestCase {

    private final HashSet<String> validationErrors = new HashSet<>();

    public void testExponentialHistogramAsTDigest() {
        DataPoint.ExponentialHistogram histogram = new DataPoint.ExponentialHistogram(
            ExponentialHistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setExponentialHistogram(ExponentialHistogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(histogram.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(histogram.isValid(validationErrors), equalTo(true));
        assertThat(validationErrors, empty());
    }

    public void testExponentialHistogramAsExponentialHistogram() {
        DataPoint.ExponentialHistogram histogram = new DataPoint.ExponentialHistogram(
            ExponentialHistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setExponentialHistogram(ExponentialHistogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(histogram.getDynamicTemplate(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM), equalTo("exponential_histogram"));
        assertThat(histogram.isValid(validationErrors), equalTo(true));
        assertThat(validationErrors, empty());
    }

    public void testExponentialHistogramMappingHint() {
        DataPoint.ExponentialHistogram histogram = new DataPoint.ExponentialHistogram(
            ExponentialHistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setExponentialHistogram(ExponentialHistogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(
            histogram.getDynamicTemplate(
                MappingHints.DEFAULT_TDIGEST.withConfigFromAttributes(mappingHints(MappingHints.AGGREGATE_METRIC_DOUBLE))
            ),
            equalTo("summary")
        );
        assertThat(histogram.isValid(validationErrors), equalTo(true));
        assertThat(validationErrors, empty());
    }

    public void testExponentialHistogramUnsupportedTemporality() {
        DataPoint.ExponentialHistogram histogram = new DataPoint.ExponentialHistogram(
            ExponentialHistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setExponentialHistogram(
                    ExponentialHistogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_CUMULATIVE).build()
                )
                .build()
        );
        assertThat(histogram.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(histogram.isValid(validationErrors), equalTo(false));
        assertThat(validationErrors, contains(containsString("cumulative exponential histogram metrics are not supported")));
    }
}
