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
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.action.search.SearchResponseMetrics.STORE_BYTES_READ_BUCKETS;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.STORE_BYTES_READ_HISTOGRAM_NAME;
import static org.hamcrest.Matchers.equalTo;

public class SearchResponseMetricsHistogramTests extends ESTestCase {

    private InMemoryMetricReader reader;
    private SearchResponseMetrics metrics;

    @Before
    public void init() {
        reader = InMemoryMetricReader.create();
        SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        metrics = new SearchResponseMetrics(new APMMeterRegistry(provider.get("elasticsearch")));
    }

    public void testStoreBytesReadBuckets() {
        metrics.recordStoreBytesRead(0, Map.of());
        metrics.recordStoreBytesRead(ByteSizeValue.ofMb(5).getBytes(), Map.of());
        metrics.recordStoreBytesRead(ByteSizeValue.ofMb(6).getBytes(), Map.of());
        metrics.recordStoreBytesRead(ByteSizeValue.ofGb(3).getBytes(), Map.of());
        metrics.recordStoreBytesRead(ByteSizeValue.ofTb(5).getBytes(), Map.of());

        HistogramPointData point = reader.collectAllMetrics()
            .stream()
            .filter(m -> m.getName().equals(STORE_BYTES_READ_HISTOGRAM_NAME))
            .findFirst()
            .orElseThrow()
            .getHistogramData()
            .getPoints()
            .iterator()
            .next();

        List<Double> boundaries = point.getBoundaries();
        assertThat(boundaries, equalTo(STORE_BYTES_READ_BUCKETS.stream().map(Long::doubleValue).toList()));

        int zeroBucket = boundaries.indexOf(0.0);
        int overflowBucket = boundaries.size();
        List<Long> expectedCounts = new ArrayList<>(Collections.nCopies(boundaries.size() + 1, 0L));
        expectedCounts.set(zeroBucket, 1L);
        expectedCounts.set(boundaries.indexOf((double) ByteSizeValue.ofMb(8).getBytes()), 2L);
        expectedCounts.set(boundaries.indexOf((double) ByteSizeValue.ofGb(4).getBytes()), 1L);
        expectedCounts.set(overflowBucket, 1L);
        assertThat(point.getCounts(), equalTo(expectedCounts));
    }
}
