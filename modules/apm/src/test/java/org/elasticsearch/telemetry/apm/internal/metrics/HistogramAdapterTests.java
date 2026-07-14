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

import org.elasticsearch.telemetry.apm.APMMeterRegistry;
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
