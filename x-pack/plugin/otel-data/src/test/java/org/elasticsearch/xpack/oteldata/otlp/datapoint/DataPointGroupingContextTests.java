/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createExponentialHistogramMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createHistogramMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createMetricsRequest;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createResourceMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createScopeMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSummaryDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSummaryMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class DataPointGroupingContextTests extends ESTestCase {

    private final DataPointGroupingContext context = new DataPointGroupingContext(new BufferedByteStringAccessor());
    private final long nowUnixNanos = System.currentTimeMillis() * 1_000_000L;

    public void testGroupingSameGroup() throws Exception {
        // Group data points
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos))),
                createGaugeMetric("system.memory.usage", "", List.of(createDoubleDataPoint(nowUnixNanos))),
                createSumMetric(
                    "http.requests.count",
                    "",
                    List.of(createLongDataPoint(nowUnixNanos)),
                    true,
                    AGGREGATION_TEMPORALITY_CUMULATIVE
                ),
                createExponentialHistogramMetric(
                    "http.request.duration",
                    "",
                    List.of(
                        ExponentialHistogramDataPoint.newBuilder().setTimeUnixNano(nowUnixNanos).setStartTimeUnixNano(nowUnixNanos).build()
                    ),
                    AGGREGATION_TEMPORALITY_DELTA
                ),
                createHistogramMetric(
                    "http.request.size",
                    "",
                    List.of(HistogramDataPoint.newBuilder().setTimeUnixNano(nowUnixNanos).setStartTimeUnixNano(nowUnixNanos).build()),
                    AGGREGATION_TEMPORALITY_DELTA
                ),
                createSummaryMetric("summary", "", List.of(createSummaryDataPoint(nowUnixNanos, List.of())))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(6, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(1, groupCount.get());
    }

    public void testGroupingDifferentTargetIndex() throws Exception {
        // Group data points
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric(
                    "system.cpu.usage",
                    "",
                    List.of(createDoubleDataPoint(nowUnixNanos, nowUnixNanos, List.of(keyValue("data_stream.dataset", "custom"))))
                ),
                createGaugeMetric("system.memory.usage", "", List.of(createDoubleDataPoint(nowUnixNanos)))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

        AtomicInteger groupCount = new AtomicInteger(0);
        List<String> targetIndexes = new ArrayList<>();
        context.consume(dataPointGroup -> {
            groupCount.incrementAndGet();
            targetIndexes.add(dataPointGroup.targetIndex().index());
        });
        assertEquals(2, groupCount.get());
        assertThat(targetIndexes, containsInAnyOrder("metrics-custom.otel-default", "metrics-generic.otel-default"));
    }

    public void testGroupingDifferentGroupUnit() throws Exception {
        // Group data points
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric("system.cpu.usage", "{percent}", List.of(createDoubleDataPoint(nowUnixNanos))),
                createGaugeMetric("system.memory.usage", "By", List.of(createLongDataPoint(nowUnixNanos)))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(2, groupCount.get());
    }

    public void testGroupingDuplicateNameSameTimeSeries() throws Exception {
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric("system.cpu.usage", "{percent}", List.of(createDoubleDataPoint(nowUnixNanos))),
                createGaugeMetric("system.cpu.usage", "{percent}", List.of(createLongDataPoint(nowUnixNanos)))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(1, context.getIgnoredDataPoints());
        assertThat(context.getIgnoredDataPointsMessage(10), containsString("Duplicate metric name 'system.cpu.usage' for timestamp"));

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(1, groupCount.get());
    }

    public void testGroupingDuplicateNameDifferentTimeSeries() throws Exception {
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos))),
                createGaugeMetric("system.cpu.usage", "{percent}", List.of(createLongDataPoint(nowUnixNanos)))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());

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
                    List.of(createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos))))
                )
            )
        );
        ResourceMetrics resource2 = createResourceMetrics(
            List.of(keyValue("service.name", "test-service_2")),
            List.of(
                createScopeMetrics(
                    "test",
                    "1.0.0",
                    List.of(createGaugeMetric("system.memory.usage", "", List.of(createLongDataPoint(nowUnixNanos))))
                )
            )
        );

        context.groupDataPoints(ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(List.of(resource1, resource2)).build());
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

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
                    List.of(createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos))))
                )
            )
        );
        ResourceMetrics resource2 = createResourceMetrics(
            List.of(keyValue("service.name", "test-service")),
            List.of(
                createScopeMetrics(
                    "test_scope_2",
                    "1.0.0",
                    List.of(createGaugeMetric("system.memory.usage", "", List.of(createLongDataPoint(nowUnixNanos))))
                )
            )
        );

        context.groupDataPoints(ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(List.of(resource1, resource2)).build());
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(2, groupCount.get());
    }

    public void testGroupingDifferentGroupTimestamp() throws Exception {
        // Group data points
        ExportMetricsServiceRequest metricsRequest = createMetricsRequest(
            List.of(
                createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos + 1))),
                createGaugeMetric("system.memory.usage", "", List.of(createLongDataPoint(nowUnixNanos)))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

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
                    List.of(createDoubleDataPoint(nowUnixNanos, nowUnixNanos, List.of(keyValue("core", "cpu0"))))
                ),
                createGaugeMetric("system.memory.usage", "", List.of(createLongDataPoint(nowUnixNanos)))
            )
        );
        context.groupDataPoints(metricsRequest);
        assertEquals(2, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

        AtomicInteger groupCount = new AtomicInteger(0);
        context.consume(dataPointGroup -> groupCount.incrementAndGet());
        assertEquals(2, groupCount.get());
    }

    public void testReceiverBasedRouting() throws Exception {
        String scopeName =
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/processscraper";
        ResourceMetrics resource = createResourceMetrics(
            List.of(keyValue("service.name", "test-service_1")),
            List.of(
                createScopeMetrics(
                    scopeName,
                    "1.0.0",
                    List.of(createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos))))
                )
            )
        );

        context.groupDataPoints(ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(List.of(resource)).build());
        assertEquals(1, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

        List<String> targetIndexes = new ArrayList<>();
        context.consume(dataPointGroup -> targetIndexes.add(dataPointGroup.targetIndex().index()));
        assertThat(targetIndexes, containsInAnyOrder("metrics-hostmetricsreceiver.otel-default"));
    }

    public void testReceiverBasedRoutingWithoutTrailingSlash() throws Exception {
        String scopeName = "/receiver/foo";
        ResourceMetrics resource = createResourceMetrics(
            List.of(keyValue("service.name", "test-service_1")),
            List.of(
                createScopeMetrics(
                    scopeName,
                    "1.0.0",
                    List.of(createGaugeMetric("system.cpu.usage", "", List.of(createDoubleDataPoint(nowUnixNanos))))
                )
            )
        );

        context.groupDataPoints(ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(List.of(resource)).build());
        assertEquals(1, context.totalDataPoints());
        assertEquals(0, context.getIgnoredDataPoints());
        assertEquals("", context.getIgnoredDataPointsMessage(10));

        List<String> targetIndexes = new ArrayList<>();
        context.consume(dataPointGroup -> targetIndexes.add(dataPointGroup.targetIndex().index()));
        assertThat(targetIndexes, containsInAnyOrder("metrics-foo.otel-default"));
    }

}
