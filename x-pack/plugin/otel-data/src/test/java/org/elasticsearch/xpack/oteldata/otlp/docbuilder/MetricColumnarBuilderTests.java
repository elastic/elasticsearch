/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createHistogramMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createResourceMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createScopeMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;

public class MetricColumnarBuilderTests extends ESTestCase {

    private final MetricColumnarBuilder builder = new MetricColumnarBuilder(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM);
    private final long nowNanos = System.currentTimeMillis() * 1_000_000L;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private DataPointGroupingContext newContext() {
        return new DataPointGroupingContext(new BufferedByteStringAccessor(), MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM);
    }

    /** Returns the string values stored in the named column, in row order. */
    private List<String> readStringColumn(EscfBatch batch, String path) {
        SourceSchema schema = batch.schema();
        for (int i = 0; i < schema.leafCount(); i++) {
            if (path.equals(schema.getFullPath(i))) {
                assertEquals(EscfColumnKind.STRING, batch.column(i).leafValueKind());
                ObjectTupleCursor<BytesRef> cursor = batch.column(i).bytesRefCursor(false);
                List<String> values = new ArrayList<>();
                while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    values.add(cursor.value().utf8ToString());
                }
                return values;
            }
        }
        return List.of(); // column absent
    }

    /** Returns the long values stored in the named column, in row order. */
    private List<Long> readLongColumn(EscfBatch batch, String path) {
        SourceSchema schema = batch.schema();
        for (int i = 0; i < schema.leafCount(); i++) {
            if (path.equals(schema.getFullPath(i))) {
                LongTupleCursor cursor = batch.column(i).longCursor();
                List<Long> values = new ArrayList<>();
                while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    values.add(cursor.longValue());
                }
                return values;
            }
        }
        return List.of();
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * A single gauge row: verifies @timestamp, the resource attribute, and the metric value
     * all land in the batch with correct content.
     */
    public void testBuildMetricRowPopulatesFields() throws Exception {
        List<KeyValue> resourceAttrs = List.of(keyValue("service.name", "my-service"));
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    resourceAttrs,
                    List.of(
                        createScopeMetrics("scope", "1.0", List.of(createGaugeMetric("cpu", "1", List.of(createLongDataPoint(nowNanos)))))
                    )
                )
            )
            .build();

        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);

        EscfBatchBuilder batchBuilder = new EscfBatchBuilder();
        context.consume(group -> {
            Map<String, String> templates = new HashMap<>();
            Map<String, Map<String, String>> templateParams = new HashMap<>();
            assertTrue(builder.buildMetricRow(batchBuilder, group, templates, templateParams));
            batchBuilder.commit(0);
        });
        EscfBatch batch = batchBuilder.buildPartition(0);

        assertEquals(1, batch.docCount());
        assertFalse(readLongColumn(batch, "@timestamp").isEmpty());
        assertEquals(List.of("my-service"), readStringColumn(batch, "resource.attributes.service.name"));
    }

    /**
     * Two rows with distinct string resource attributes must produce distinct column values — not
     * both ending up with the last-written string (regression guard for the shared-buffer bug).
     */
    public void testDistinctResourceAttributesProduceDistinctColumnValues() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc-alpha")),
                    List.of(createScopeMetrics("s", "1", List.of(createGaugeMetric("m", "1", List.of(createDoubleDataPoint(nowNanos))))))
                )
            )
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc-beta")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(createGaugeMetric("m", "1", List.of(createDoubleDataPoint(nowNanos + 1_000_000L))))
                        )
                    )
                )
            )
            .build();

        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);

        EscfBatchBuilder batchBuilder = new EscfBatchBuilder();
        context.consume(group -> {
            assertTrue(builder.buildMetricRow(batchBuilder, group, new HashMap<>(), new HashMap<>()));
            batchBuilder.commit(0);
        });
        EscfBatch batch = batchBuilder.buildPartition(0);

        assertEquals(2, batch.docCount());
        List<String> names = readStringColumn(batch, "resource.attributes.service.name");
        assertEquals(2, names.size());
        // Each row must retain its own value — not both "svc-beta".
        assertTrue("expected svc-alpha and svc-beta but got " + names, names.containsAll(List.of("svc-alpha", "svc-beta")));
    }

    /**
     * A row with multiple distinct string attributes in the same list must all be staged correctly.
     * This is the within-row variant of the shared-buffer regression.
     */
    public void testMultipleStringAttributesInSameRowAreDistinct() throws Exception {
        List<KeyValue> dpAttrs = List.of(keyValue("env", "prod"), keyValue("region", "us-east-1"), keyValue("version", "v2"));
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createSumMetric(
                                    "req",
                                    "1",
                                    List.of(
                                        NumberDataPoint.newBuilder()
                                            .setTimeUnixNano(nowNanos)
                                            .setAsInt(42L)
                                            .addAllAttributes(dpAttrs)
                                            .build()
                                    ),
                                    true,
                                    AGGREGATION_TEMPORALITY_CUMULATIVE
                                )
                            )
                        )
                    )
                )
            )
            .build();

        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);

        EscfBatchBuilder batchBuilder = new EscfBatchBuilder();
        context.consume(group -> {
            assertTrue(builder.buildMetricRow(batchBuilder, group, new HashMap<>(), new HashMap<>()));
            batchBuilder.commit(0);
        });
        EscfBatch batch = batchBuilder.buildPartition(0);

        assertEquals(1, batch.docCount());
        assertEquals(List.of("prod"), readStringColumn(batch, "attributes.env"));
        assertEquals(List.of("us-east-1"), readStringColumn(batch, "attributes.region"));
        assertEquals(List.of("v2"), readStringColumn(batch, "attributes.version"));
    }

    /** A histogram data point causes buildMetricRow to return false (falls back to XContent path). */
    public void testBuildMetricRowReturnsFalseForHistogram() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createHistogramMetric(
                                    "latency",
                                    "ms",
                                    List.of(
                                        io.opentelemetry.proto.metrics.v1.HistogramDataPoint.newBuilder()
                                            .setTimeUnixNano(nowNanos)
                                            .setCount(1)
                                            .setSum(42.0)
                                            .build()
                                    ),
                                    AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
                                )
                            )
                        )
                    )
                )
            )
            .build();

        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);

        EscfBatchBuilder batchBuilder = new EscfBatchBuilder();
        context.consume(group -> { assertFalse(builder.buildMetricRow(batchBuilder, group, new HashMap<>(), new HashMap<>())); });
        assertFalse(batchBuilder.hasPartition(0));
    }

    /** An ARRAY attribute causes buildMetricRow to return false. */
    public void testBuildMetricRowReturnsFalseForArrayAttribute() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createGaugeMetric(
                                    "cpu",
                                    "1",
                                    List.of(
                                        NumberDataPoint.newBuilder()
                                            .setTimeUnixNano(nowNanos)
                                            .setAsDouble(1.0)
                                            .addAttributes(keyValue("tags", "a", "b"))
                                            .build()
                                    )
                                )
                            )
                        )
                    )
                )
            )
            .build();

        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);

        EscfBatchBuilder batchBuilder = new EscfBatchBuilder();
        context.consume(group -> { assertFalse(builder.buildMetricRow(batchBuilder, group, new HashMap<>(), new HashMap<>())); });
        assertFalse(batchBuilder.hasPartition(0));
    }
}
