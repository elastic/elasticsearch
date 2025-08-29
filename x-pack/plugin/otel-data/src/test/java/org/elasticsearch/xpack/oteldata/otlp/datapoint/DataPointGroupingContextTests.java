/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createMetricsRequest;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createResourceMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createScopeMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;

public class DataPointGroupingContextTests extends ESTestCase {

    private final DataPointGroupingContext context = new DataPointGroupingContext(new BufferedByteStringAccessor());
    private final long nowUnixNanos = System.currentTimeMillis() * 1_000_000L;

    public void testGroupingSameGroup() throws Exception {
        // Group data points
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos, List.of()))),
                createGaugeMetric("system.memory.usage", "", List.of(createDoubleDataPoint(nowUnixNanos, List.of()))),
                createSumMetric(
                    "http.requests.count",
                    "",
                    List.of(createLongDataPoint(nowUnixNanos, List.of())),
                    true,
                    AGGREGATION_TEMPORALITY_CUMULATIVE
                )
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(3, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage());

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(1, groupCount.get());
    }

    public void testGroupingDifferentGroupUnit() throws Exception {
        // Group data points
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric("system.cpu.usage", "{percent}", List.of(createDoubleDataPoint(nowUnixNanos, List.of()))),
                createGaugeMetric("system.memory.usage", "By", List.of(createLongDataPoint(nowUnixNanos, List.of())))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage());

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(2, groupCount.get());
    }

    public void testGroupingDifferentResource() throws Exception {
        ResourceMetrics resource1 = createResourceMetrics(
            List.of(keyValue("service.name", "test-service_1")),
            List.of(
                createScopeMetrics(
                    "test",
                    "1.0.0",
                    List.of(createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos, List.of()))))
                )
            )
        );
        ResourceMetrics resource2 = createResourceMetrics(
            List.of(keyValue("service.name", "test-service_2")),
            List.of(
                createScopeMetrics(
                    "test",
                    "1.0.0",
                    List.of(createGaugeMetric("system.memory.usage", "", List.of(createLongDataPoint(nowUnixNanos, List.of()))))
                )
            )
        );

        context.groupDataPoints(ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(List.of(resource1, resource2)).build());
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage());

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(2, groupCount.get());
    }

    public void testGroupingDifferentScope() throws Exception {
        ResourceMetrics resource1 = createResourceMetrics(
            List.of(keyValue("service.name", "test-service")),
            List.of(
                createScopeMetrics(
                    "test_scope_1",
                    "1.0.0",
                    List.of(createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos, List.of()))))
                )
            )
        );
        ResourceMetrics resource2 = createResourceMetrics(
            List.of(keyValue("service.name", "test-service")),
            List.of(
                createScopeMetrics(
                    "test_scope_2",
                    "1.0.0",
                    List.of(createGaugeMetric("system.memory.usage", "", List.of(createLongDataPoint(nowUnixNanos, List.of()))))
                )
            )
        );

        context.groupDataPoints(ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(List.of(resource1, resource2)).build());
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage());

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(2, groupCount.get());
    }

    public void testGroupingDifferentGroupTimestamp() throws Exception {
        // Group data points
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos + 1, List.of()))),
                createGaugeMetric("system.memory.usage", "", List.of(createLongDataPoint(nowUnixNanos, List.of())))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage());

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(2, groupCount.get());
    }

    public void testGroupingDifferentGroupAttributes() throws Exception {
        // Group data points
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric(
                    "system.cpu.usage",
                    "",
                    List.of(createDoubleDataPoint(nowUnixNanos, List.of(keyValue("core", "cpu0"))))
                ),
                createGaugeMetric("system.memory.usage", "", List.of(createLongDataPoint(nowUnixNanos, List.of())))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage());

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(2, groupCount.get());
    }

}
