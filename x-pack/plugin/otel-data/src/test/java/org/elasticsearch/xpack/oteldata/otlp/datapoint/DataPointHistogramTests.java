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

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class DataPointHistogramTests extends ESTestCase {

    private final HashSet<String> validationErrors = new HashSet<>();

    public void testExponentialHistogram() {
        DataPoint.Histogram doubleGauge = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(doubleGauge.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(doubleGauge.isValid(validationErrors), equalTo(true));
        assertThat(validationErrors, empty());
    }

    public void testExponentialHistogramUnsupportedTemporality() {
        DataPoint.Histogram doubleGauge = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder().build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_CUMULATIVE).build())
                .build()
        );
        assertThat(doubleGauge.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(doubleGauge.isValid(validationErrors), equalTo(false));
        assertThat(validationErrors, contains(containsString("cumulative histogram metrics are not supported")));
    }

    public void testExponentialHistogramInvalidBucketCountWithoutBounds() {
        DataPoint.Histogram doubleGauge = new DataPoint.Histogram(
            HistogramDataPoint.newBuilder().addBucketCounts(1).build(),
            Metric.newBuilder()
                .setHistogram(Histogram.newBuilder().setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA).build())
                .build()
        );
        assertThat(doubleGauge.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("histogram"));
        assertThat(doubleGauge.isValid(validationErrors), equalTo(false));
        assertThat(validationErrors, contains(containsString("histogram with a single bucket and no explicit bounds is not supported")));
    }
}
