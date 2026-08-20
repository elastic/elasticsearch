/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that histograms registered without explicit boundaries are configured with the APM default bucket ladder
 * (see {@link HistogramBuckets}) rather than the OTel SDK's own default boundaries.
 */
public class HistogramAdapterTests extends ESTestCase {

    private InMemoryMetricReader reader;
    private APMMeterRegistry registry;

    @Before
    public void init() {
        reader = InMemoryMetricReader.create();
        SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        registry = new APMMeterRegistry(provider.get("elasticsearch"));
    }

    public void testLongHistogramUsesCustomBoundaries() {
        List<Long> customBoundaries = List.of(10L, 100L, 1_000L, 10_000L, 100_000L);
        String name = "es.test.custom.long.histogram";
        registry.registerLongHistogram(name, "desc", "ms", customBoundaries).record(randomNonNegativeLong());

        List<Double> actualBoundaries = reader.collectAllMetrics()
            .stream()
            .filter(m -> m.getName().equals(name))
            .findFirst()
            .orElseThrow()
            .getHistogramData()
            .getPoints()
            .iterator()
            .next()
            .getBoundaries();

        assertThat(actualBoundaries, equalTo(customBoundaries.stream().map(Long::doubleValue).toList()));
    }

    public void testDoubleHistogramUsesCustomBoundaries() {
        List<Double> customBoundaries = List.of(0.01, 0.1, 1.0, 10.0, 100.0);
        String name = "es.test.custom.double.histogram";
        registry.registerDoubleHistogram(name, "desc", "ms", customBoundaries).record(randomDoubleBetween(0.0, 100.0, true));

        List<Double> actualBoundaries = reader.collectAllMetrics()
            .stream()
            .filter(m -> m.getName().equals(name))
            .findFirst()
            .orElseThrow()
            .getHistogramData()
            .getPoints()
            .iterator()
            .next()
            .getBoundaries();

        assertThat(actualBoundaries, equalTo(customBoundaries));
    }

    public void testHistogramsUseApmDefaultBoundaries() {
        String longName = "es.test.long.histogram";
        String doubleName = "es.test.double.histogram";
        registry.registerLongHistogram(longName, "desc", "ms").record(60000L);
        registry.registerDoubleHistogram(doubleName, "desc", "s").record(60.0);

        Map<String, List<Double>> boundariesByName = reader.collectAllMetrics()
            .stream()
            .collect(toMap(MetricData::getName, metric -> metric.getHistogramData().getPoints().iterator().next().getBoundaries()));

        assertThat(boundariesByName.get(longName), equalTo(HistogramBuckets.APM_DEFAULT_LONGS.stream().map(Long::doubleValue).toList()));
        assertThat(boundariesByName.get(doubleName), equalTo(HistogramBuckets.APM_DEFAULT));
    }
}
