/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.List;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.hamcrest.Matchers.equalTo;

public class DataPointNumberTests extends ESTestCase {

    private final long nowUnixNanos = System.currentTimeMillis() * 1_000_000L;

    public void testGauge() {
        DataPoint.Number doubleGauge = new DataPoint.Number(
            createDoubleDataPoint(nowUnixNanos),
            createGaugeMetric("system.cpu.usage", "", List.of())
        );
        assertThat(doubleGauge.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("gauge_double"));
        DataPoint.Number longGauge = new DataPoint.Number(
            createLongDataPoint(nowUnixNanos),
            createGaugeMetric("system.cpu.usage", "", List.of())
        );
        assertThat(longGauge.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("gauge_long"));
    }

    public void testCounterTemporality() {
        DataPoint.Number doubleCumulative = new DataPoint.Number(
            createDoubleDataPoint(nowUnixNanos),
            createSumMetric("http.requests.count", "", List.of(), true, AGGREGATION_TEMPORALITY_CUMULATIVE)
        );
        assertThat(doubleCumulative.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("counter_double"));
        DataPoint.Number longCumulative = new DataPoint.Number(
            createLongDataPoint(nowUnixNanos),
            createSumMetric("http.requests.count", "", List.of(), true, AGGREGATION_TEMPORALITY_CUMULATIVE)
        );
        assertThat(longCumulative.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("counter_long"));
        DataPoint.Number doubleDelta = new DataPoint.Number(
            createDoubleDataPoint(nowUnixNanos),
            createSumMetric("http.requests.count", "", List.of(), true, AGGREGATION_TEMPORALITY_DELTA)
        );
        assertThat(doubleDelta.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("gauge_double"));
        DataPoint.Number longDelta = new DataPoint.Number(
            createLongDataPoint(nowUnixNanos),
            createSumMetric("http.requests.count", "", List.of(), true, AGGREGATION_TEMPORALITY_DELTA)
        );
        assertThat(longDelta.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("gauge_long"));
    }

    public void testCounterNonMonotonic() {
        DataPoint.Number doubleNonMonotonic = new DataPoint.Number(
            createDoubleDataPoint(nowUnixNanos),
            createSumMetric("http.requests.count", "", List.of(), false, AGGREGATION_TEMPORALITY_CUMULATIVE)
        );
        assertThat(doubleNonMonotonic.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("gauge_double"));
        DataPoint.Number longNonMonotonic = new DataPoint.Number(
            createLongDataPoint(nowUnixNanos),
            createSumMetric("http.requests.count", "", List.of(), false, AGGREGATION_TEMPORALITY_DELTA)
        );
        assertThat(longNonMonotonic.getDynamicTemplate(MappingHints.DEFAULT_TDIGEST), equalTo("gauge_long"));
    }

}
