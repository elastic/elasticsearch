/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.anyValue;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.anyValueArray;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.anyValueBytes;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createResourceMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createScopeMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValueArray;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValueBytes;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValueList;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class OTLPMetricBatchBuilderTests extends MetricDocumentTestCase {

    @Override
    protected boolean producesTsid() {
        return false;
    }

    @Override
    protected List<Document> buildDocuments(MappingHints defaultMappingHints, DataPointGroupingContext context) throws IOException {
        List<Map<String, String>> dynamicTemplates = new ArrayList<>();
        List<Map<String, Map<String, String>>> dynamicTemplateParams = new ArrayList<>();
        try (
            OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(BytesRefRecycler.NON_RECYCLING_INSTANCE, defaultMappingHints)
        ) {
            for (DataPointGroupingContext.ResourceGroup resourceGroup : context.resourceGroups()) {
                assertTrue(batchBuilder.buildMetricRows(resourceGroup, (group, t, p) -> {
                    dynamicTemplates.add(t);
                    dynamicTemplateParams.add(p);
                }));
            }
            try (EscfBatch batch = batchBuilder.build()) {
                List<Document> documents = new ArrayList<>();
                for (int row = 0; row < batch.docCount(); row++) {
                    XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                    SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
                    ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
                    documents.add(new Document(doc, dynamicTemplates.get(row), dynamicTemplateParams.get(row), null));
                }
                return documents;
            }
        }
    }

    /** Writes every resource group of {@code context} into {@code batchBuilder}, asserting each is eligible. */
    private static void buildAllRows(OTLPMetricBatchBuilder batchBuilder, DataPointGroupingContext context) {
        for (DataPointGroupingContext.ResourceGroup resourceGroup : context.resourceGroups()) {
            assertTrue(batchBuilder.buildMetricRows(resourceGroup, (group, t, p) -> {}));
        }
    }

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

        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        buildAllRows(batchBuilder, context);
        EscfBatch batch = batchBuilder.build();

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

        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        buildAllRows(batchBuilder, context);
        EscfBatch batch = batchBuilder.build();

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

        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        buildAllRows(batchBuilder, context);
        EscfBatch batch = batchBuilder.build();

        assertEquals(1, batch.docCount());
        assertEquals(List.of("prod"), readStringColumn(batch, "attributes.env"));
        assertEquals(List.of("us-east-1"), readStringColumn(batch, "attributes.region"));
        assertEquals(List.of("v2"), readStringColumn(batch, "attributes.version"));
    }

    /**
     * A key repeated within a resource, scope, or data-point attribute list keeps its first value and the
     * rest are dropped — even a repeat whose value alone would make the row ineligible.
     */
    public void testDuplicateAttributeKeysKeepFirstValue() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("host", "first"), keyValue("host", "second")),
                    List.of(
                        ScopeMetrics.newBuilder()
                            .setScope(
                                InstrumentationScope.newBuilder()
                                    .setName("s")
                                    .addAttributes(keyValue("lib", 1L))
                                    .addAttributes(keyValue("lib", 2L))
                            )
                            .addMetrics(
                                createGaugeMetric(
                                    "cpu",
                                    "1",
                                    List.of(
                                        NumberDataPoint.newBuilder()
                                            .setTimeUnixNano(nowNanos)
                                            .setAsDouble(1.0)
                                            .addAttributes(keyValue("env", "prod"))
                                            .addAttributes(keyValue("env", "staging"))
                                            .addAttributes(keyValue("env", "a", "b"))
                                            .build()
                                    )
                                )
                            )
                            .build()
                    )
                )
            )
            .build();

        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);

        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        buildAllRows(batchBuilder, context);
        EscfBatch batch = batchBuilder.build();

        assertEquals(1, batch.docCount());
        assertEquals(List.of("first"), readStringColumn(batch, "resource.attributes.host"));
        assertEquals(List.of(1L), readLongColumn(batch, "scope.attributes.lib"));
        assertEquals(List.of("prod"), readStringColumn(batch, "attributes.env"));
    }

    /** Dotted metric names (e.g. {@code gauge.0}) are written as a nested-path column, not a top-level key. */
    public void testDottedMetricNameWritesNestedColumn() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics("s", "1", List.of(createGaugeMetric("gauge.0", "1", List.of(createLongDataPoint(nowNanos)))))
                    )
                )
            )
            .build();

        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);

        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        buildAllRows(batchBuilder, context);
        EscfBatch batch = batchBuilder.build();

        assertEquals(1, batch.docCount());
        // The dotted metric name "gauge.0" must produce a column at path "metrics.gauge.0".
        assertFalse("metrics.gauge.0 column must be present", readLongColumn(batch, "metrics.gauge.0").isEmpty());
    }

    /** An ARRAY attribute becomes an array column, keeping element order. */
    public void testArrayAttributeWritesArrayColumn() throws Exception {
        EscfBatch batch = buildSingleRow(keyValue("tags", "a", "b"));

        assertEquals(1, batch.docCount());
        assertEquals(List.of("a", "b"), readStringColumn(batch, "attributes.tags"));
    }

    /**
     * A KVLIST attribute makes the row ineligible: the {@code complex_attributes} dynamic template maps an
     * attribute object to {@code flattened}, which per-child-key columns would not match.
     */
    public void testBuildMetricRowReturnsFalseForObjectAttribute() throws Exception {
        assertRowIneligible(keyValue("http", keyValueList(keyValue("method", "GET"))));
    }

    /** An empty KVLIST is rejected along with every other object. */
    public void testBuildMetricRowReturnsFalseForEmptyObjectAttribute() throws Exception {
        assertRowIneligible(keyValue("empty", keyValueList()));
    }

    /** An object inside an array is rejected too, however deep it sits. */
    public void testBuildMetricRowReturnsFalseForObjectInsideArray() throws Exception {
        assertRowIneligible(keyValueArray("items", anyValue(keyValueList(keyValue("a", 1L)))));
    }

    /** A bytes attribute makes the row ineligible*/
    public void testBuildMetricRowReturnsFalseForBytesAttribute() throws Exception {
        assertRowIneligible(keyValueBytes("payload", new byte[] { 1, 2, 3 }));
    }

    /** A bytes value nested inside an array is rejected the same way a top-level one is. */
    public void testBuildMetricRowReturnsFalseForBytesInsideArray() throws Exception {
        assertRowIneligible(keyValueArray("mixed", anyValue("a"), anyValueBytes(new byte[] { 1 })));
    }

    public void testBuildMetricRowReturnsFalseForNestedArray() throws Exception {
        assertRowIneligible(keyValueArray("nested", anyValueArray(anyValue(1L), anyValue(2L))));
    }

    /**
     * Resource fields are filled for every row of the resource group and scope fields for every row of their scope
     * group, while each row keeps its own data-point attributes.
     */
    public void testSharedResourceAndScopeFieldsAreFilledForEveryRow() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("host.id", 7L)),
                    List.of(
                        ScopeMetrics.newBuilder()
                            .setScope(InstrumentationScope.newBuilder().setName("s1").addAttributes(keyValue("lib", "a")))
                            .addMetrics(
                                createGaugeMetric(
                                    "cpu",
                                    "1",
                                    List.of(
                                        createDoubleDataPoint(nowNanos, 0, List.of(keyValue("core", "0"))),
                                        createDoubleDataPoint(nowNanos, 0, List.of(keyValue("core", "1")))
                                    )
                                )
                            )
                            .build(),
                        ScopeMetrics.newBuilder()
                            .setScope(InstrumentationScope.newBuilder().setName("s2").addAttributes(keyValue("lib", "b")))
                            .addMetrics(createGaugeMetric("mem", "1", List.of(createDoubleDataPoint(nowNanos))))
                            .build()
                    )
                )
            )
            .build();

        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);
        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        buildAllRows(batchBuilder, context);
        EscfBatch batch = batchBuilder.build();

        assertEquals(3, batch.docCount());
        assertEquals(List.of("svc", "svc", "svc"), readStringColumn(batch, "resource.attributes.service.name"));
        assertEquals(List.of(7L, 7L, 7L), readLongColumn(batch, "resource.attributes.host.id"));
        List<String> scopeNames = readStringColumn(batch, "scope.name");
        List<String> scopeLibs = readStringColumn(batch, "scope.attributes.lib");
        List<String> cores = new ArrayList<>();
        for (int row = 0; row < batch.docCount(); row++) {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
            String scopeName = doc.evaluate("scope.name");
            assertEquals(scopeNames.get(row), scopeName);
            assertEquals(scopeName.equals("s1") ? "a" : "b", scopeLibs.get(row));
            if (scopeName.equals("s1")) {
                cores.add(doc.evaluate("attributes.core"));
            } else {
                assertNull(doc.evaluate("attributes.core"));
            }
        }
        assertThat(scopeNames, containsInAnyOrder("s1", "s1", "s2"));
        assertThat(cores, containsInAnyOrder("0", "1"));
    }

    /** A scope without rows next to a populated one leaves no values in the rows of the populated scope. */
    public void testEmptyScopeWritesNoValues() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        ScopeMetrics.newBuilder()
                            .setScope(InstrumentationScope.newBuilder().setName("empty").addAttributes(keyValue("only_empty", "x")))
                            .build(),
                        createScopeMetrics("s", "1", List.of(createGaugeMetric("cpu", "1", List.of(createDoubleDataPoint(nowNanos)))))
                    )
                )
            )
            .build();
        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);
        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        buildAllRows(batchBuilder, context);
        EscfBatch batch = batchBuilder.build();

        assertEquals(1, batch.docCount());
        assertEquals(List.of("s"), readStringColumn(batch, "scope.name"));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        SourceRowToXContent.writeRow(batch.row(0), batch.schema(), builder);
        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        assertNull(doc.evaluate("scope.attributes.only_empty"));
    }

    /** A resource attribute that can't be expressed as a column is only found when filling, after the rows are committed. */
    public void testBuildMetricRowsReturnsFalseForBytesResourceAttribute() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValueBytes("payload", new byte[] { 1, 2, 3 })),
                    List.of(createScopeMetrics("s", "1", List.of(createGaugeMetric("cpu", "1", List.of(createDoubleDataPoint(nowNanos))))))
                )
            )
            .build();
        DataPointGroupingContext context = newContext();
        context.groupDataPoints(request);
        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        for (DataPointGroupingContext.ResourceGroup resourceGroup : context.resourceGroups()) {
            assertFalse(batchBuilder.buildMetricRows(resourceGroup, (group, t, p) -> {}));
        }
    }

    /** Builds a one-row batch from a single gauge data point carrying {@code dataPointAttributes}. */
    private EscfBatch buildSingleRow(KeyValue... dataPointAttributes) throws Exception {
        DataPointGroupingContext context = newContext();
        context.groupDataPoints(gaugeRequest(dataPointAttributes));
        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        buildAllRows(batchBuilder, context);
        return batchBuilder.build();
    }

    /** Asserts that a data point carrying {@code dataPointAttributes} cannot be written as ESCF columns. */
    private void assertRowIneligible(KeyValue... dataPointAttributes) throws Exception {
        DataPointGroupingContext context = newContext();
        context.groupDataPoints(gaugeRequest(dataPointAttributes));
        OTLPMetricBatchBuilder batchBuilder = new OTLPMetricBatchBuilder(
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        for (DataPointGroupingContext.ResourceGroup resourceGroup : context.resourceGroups()) {
            assertFalse(batchBuilder.buildMetricRows(resourceGroup, (group, t, p) -> fail("an ineligible row must not be committed")));
        }
        assertEquals(0, batchBuilder.docCount());
    }

    private ExportMetricsServiceRequest gaugeRequest(KeyValue... dataPointAttributes) {
        return ExportMetricsServiceRequest.newBuilder()
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
                                            .addAllAttributes(List.of(dataPointAttributes))
                                            .build()
                                    )
                                )
                            )
                        )
                    )
                )
            )
            .build();
    }
}
