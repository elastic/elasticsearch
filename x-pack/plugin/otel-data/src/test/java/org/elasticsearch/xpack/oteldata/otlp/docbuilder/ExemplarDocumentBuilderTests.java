/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.Exemplar;
import io.opentelemetry.proto.metrics.v1.Metric;

import com.google.protobuf.ByteString;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.OtlpUtils;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleExemplar;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongExemplar;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class ExemplarDocumentBuilderTests extends ESTestCase {

    private static final IndexVersion INDEX_VERSION = IndexVersion.current();
    private final BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
    private final ExemplarDocumentBuilder exemplarDocumentBuilder = new ExemplarDocumentBuilder(byteStringAccessor);

    public void testBuildDoubleExemplarDocument() throws Exception {
        String traceId = "00112233445566778899aabbccddeeff";
        String spanId = "0011223344556677";
        Exemplar exemplar = createDoubleExemplar(
            1_234_567_890L,
            0.42,
            List.of(keyValue("thread.id", 42L)),
            ByteString.copyFrom(HexFormat.of().parseHex(traceId)),
            ByteString.copyFrom(HexFormat.of().parseHex(spanId))
        );
        Metric metric = createGaugeMetric(
            "request.duration",
            "s",
            List.of(createDoubleDataPoint(2_000_000_000L, 1_000_000_000L, List.of(keyValue("route", "/checkout")), List.of(exemplar)))
        );
        DataPointGroupingContext.DataPointGroup group = group(metric);
        DataPoint dataPoint = group.dataPoints().getFirst();
        Map<String, String> dynamicTemplates = new HashMap<>();
        Map<String, Map<String, String>> dynamicTemplateParams = new HashMap<>();

        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            exemplarDocumentBuilder.buildExemplarDocument(
                builder,
                group,
                dataPoint,
                exemplar,
                group.targetIndex().exemplarsTarget(),
                dynamicTemplates,
                dynamicTemplateParams,
                INDEX_VERSION
            );
            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
            assertThat(doc.evaluate("@timestamp"), equalTo(1234));
            assertThat(doc.evaluate("data_stream.type"), equalTo("exemplars"));
            assertThat(doc.evaluate("data_stream.dataset"), equalTo("generic.otel"));
            assertThat(doc.evaluate("resource.attributes.service\\.name"), equalTo("test-service"));
            assertThat(doc.evaluate("scope.name"), equalTo("test"));
            assertThat(doc.evaluate("attributes.route"), equalTo("/checkout"));
            assertThat(doc.evaluate("unit"), equalTo("s"));
            assertThat(doc.evaluate("filtered_attributes.thread\\.id"), equalTo(42));
            assertThat(doc.evaluate("trace_id"), equalTo(traceId));
            assertThat(doc.evaluate("span_id"), equalTo(spanId));
            assertThat(doc.evaluate("metrics.request\\.duration"), equalTo(0.42));
        }
        assertThat(dynamicTemplates, hasEntry("metrics.request.duration", "exemplar_value_double"));
        assertThat(dynamicTemplateParams, hasEntry("metrics.request.duration", Map.of("unit", "s")));
    }

    public void testExemplarsUseMetricSpecificTsid() throws Exception {
        Exemplar doubleExemplar = createDoubleExemplar(1_000_000L, 0.42, List.of(), ByteString.EMPTY, ByteString.EMPTY);
        Exemplar longExemplar = createLongExemplar(1_000_000L, 42L);
        List<Metric> metrics = List.of(
            createGaugeMetric(
                "request.duration",
                "s",
                List.of(createDoubleDataPoint(3_000_000L, 0, List.of(keyValue("route", "/checkout")), List.of(doubleExemplar)))
            ),
            createGaugeMetric(
                "request.size",
                "s",
                List.of(createLongDataPoint(3_000_000L, 0, List.of(keyValue("route", "/checkout")), List.of(longExemplar)))
            )
        );
        DataPointGroupingContext.DataPointGroup group = group(metrics);
        MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder(byteStringAccessor, MappingHints.DEFAULT_TDIGEST);
        BytesRef metricTsid;
        String metricNamesHash;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            metricTsid = metricDocumentBuilder.buildMetricDocument(builder, group, new HashMap<>(), new HashMap<>(), INDEX_VERSION);
            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
            metricNamesHash = doc.evaluate("_metric_names_hash");
        }

        Set<BytesRef> exemplarTsids = new HashSet<>();
        Set<String> exemplarMetricNamesHashes = new HashSet<>();
        for (DataPoint dataPoint : group.dataPoints()) {
            Exemplar exemplar = dataPoint.getExemplars().getFirst();
            Map<String, String> dynamicTemplates = new HashMap<>();
            try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
                BytesRef exemplarTsid = exemplarDocumentBuilder.buildExemplarDocument(
                    builder,
                    group,
                    dataPoint,
                    exemplar,
                    group.targetIndex().exemplarsTarget(),
                    dynamicTemplates,
                    new HashMap<>(),
                    INDEX_VERSION
                );
                assertNotEquals(metricTsid, exemplarTsid);
                assertTrue(exemplarTsids.add(exemplarTsid));
                ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
                String exemplarMetricNamesHash = doc.evaluate("_metric_names_hash");
                assertNotEquals(metricNamesHash, exemplarMetricNamesHash);
                assertTrue(exemplarMetricNamesHashes.add(exemplarMetricNamesHash));
            }
            String expectedTemplate = exemplar.getValueCase() == Exemplar.ValueCase.AS_INT
                ? "exemplar_value_long"
                : "exemplar_value_double";
            assertThat(dynamicTemplates, hasEntry("metrics." + dataPoint.getMetricName(), expectedTemplate));
        }
        assertThat(exemplarTsids.size(), equalTo(2));
        assertThat(exemplarMetricNamesHashes.size(), equalTo(2));
    }

    private DataPointGroupingContext.DataPointGroup group(Metric metric) throws IOException {
        return group(List.of(metric));
    }

    private DataPointGroupingContext.DataPointGroup group(List<Metric> metrics) throws IOException {
        DataPointGroupingContext context = new DataPointGroupingContext(byteStringAccessor, MappingHints.DEFAULT_TDIGEST);
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "test-service")),
                    List.of(OtlpUtils.createScopeMetrics("test", "1.0.0", metrics))
                )
            )
            .build();
        context.groupDataPoints(request);
        AtomicReference<DataPointGroupingContext.DataPointGroup> group = new AtomicReference<>();
        context.consume(dataPointGroup -> assertTrue(group.compareAndSet(null, dataPointGroup)));
        return group.get();
    }
}
