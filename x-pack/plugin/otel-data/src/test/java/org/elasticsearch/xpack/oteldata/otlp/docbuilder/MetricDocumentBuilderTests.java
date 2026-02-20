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
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.OtlpUtils;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createExponentialHistogramMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createHistogramMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSummaryMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.mappingHints;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;

public class MetricDocumentBuilderTests extends ESTestCase {

    private final MetricDocumentBuilder documentBuilder = new MetricDocumentBuilder(
        new BufferedByteStringAccessor(),
        MappingHints.DEFAULT_TDIGEST
    );
    private final DataPointGroupingContext dataPointGroupingContext = new DataPointGroupingContext(new BufferedByteStringAccessor());
    private final long timestamp = randomLong();
    private final long startTimestamp = randomLong();

    public void testBuildMetricDocument() throws IOException {
        List<KeyValue> resourceAttributes = new ArrayList<>();
        resourceAttributes.add(keyValue("service.name", "test-service"));
        resourceAttributes.add(keyValue("host.name", "test-host"));
        Resource resource = Resource.newBuilder().addAllAttributes(resourceAttributes).setDroppedAttributesCount(1).build();
        InstrumentationScope scope = InstrumentationScope.newBuilder()
            .setName("test-scope")
            .setVersion("1.0.0")
            .setDroppedAttributesCount(2)
            .addAttributes(keyValue("scope_attr", "value"))
            .build();

        List<KeyValue> dataPointAttributes = List.of(keyValue("operation", "test"), (keyValue("environment", "production")));
        Metric gaugeMetric = createGaugeMetric(
            "system.cpu.usage",
            "{test}",
            List.of(createDoubleDataPoint(timestamp, startTimestamp, dataPointAttributes))
        );
        Metric sumMetric = createSumMetric(
            "system.network.packets",
            "{test}",
            List.of(createLongDataPoint(timestamp, startTimestamp, dataPointAttributes)),
            true,
            AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
        );
        dataPointGroupingContext.groupDataPoints(
            ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(
                    ResourceMetrics.newBuilder()
                        .setResource(resource)
                        .setSchemaUrl("https://opentelemetry.io/schemas/1.0.0")
                        .addScopeMetrics(
                            ScopeMetrics.newBuilder()
                                .setScope(scope)
                                .setSchemaUrl("https://opentelemetry.io/schemas/1.0.0")
                                .addMetrics(gaugeMetric)
                                .addMetrics(sumMetric)
                                .build()
                        )
                        .build()
                )
                .build()
        );

        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            Map<String, Map<String, String>> dynamicTemplateParams = new HashMap<>();
            BytesRef tsid = documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, dynamicTemplateParams);
            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.<Number>evaluate("@timestamp").longValue(), equalTo(TimeUnit.NANOSECONDS.toMillis(timestamp)));
            assertThat(doc.<Number>evaluate("start_timestamp").longValue(), equalTo(TimeUnit.NANOSECONDS.toMillis(startTimestamp)));
            assertThat(doc.evaluate("data_stream.type"), equalTo("metrics"));
            assertThat(doc.evaluate("data_stream.dataset"), equalTo("generic.otel"));
            assertThat(doc.evaluate("data_stream.namespace"), equalTo("default"));
            assertThat(doc.evaluate("resource.schema_url"), equalTo("https://opentelemetry.io/schemas/1.0.0"));
            assertThat(doc.evaluate("resource.dropped_attributes_count"), equalTo(1));
            assertThat(doc.evaluate("resource.attributes.service\\.name"), equalTo("test-service"));
            assertThat(doc.evaluate("resource.attributes.host\\.name"), equalTo("test-host"));
            assertThat(doc.evaluate("scope.name"), equalTo("test-scope"));
            assertThat(doc.evaluate("scope.version"), equalTo("1.0.0"));
            assertThat(doc.evaluate("scope.schema_url"), equalTo("https://opentelemetry.io/schemas/1.0.0"));
            assertThat(doc.evaluate("scope.dropped_attributes_count"), equalTo(2));
            assertThat(doc.evaluate("scope.attributes.scope_attr"), equalTo("value"));
            assertThat(doc.evaluate("_metric_names_hash"), isA(String.class));
            assertThat(doc.evaluate("attributes.operation"), equalTo("test"));
            assertThat(doc.evaluate("attributes.environment"), equalTo("production"));
            assertThat(doc.evaluate("unit"), equalTo("{test}"));
            assertThat(doc.evaluate("metrics.system\\.cpu\\.usage"), isA(Number.class));
            assertThat(doc.evaluate("metrics.system\\.network\\.packets"), isA(Number.class));
            assertThat(dynamicTemplates, hasEntry("metrics.system.cpu.usage", "gauge_double"));
            assertThat(dynamicTemplates, hasEntry("metrics.system.network.packets", "counter_long"));

            TsidBuilder expectedTsidBuilder = new TsidBuilder();
            expectedTsidBuilder.addStringDimension("resource.attributes.service.name", "test-service");
            expectedTsidBuilder.addStringDimension("resource.attributes.host.name", "test-host");
            expectedTsidBuilder.addStringDimension("scope.name", "test-scope");
            expectedTsidBuilder.addStringDimension("scope.attributes.scope_attr", "value");
            expectedTsidBuilder.addStringDimension("_metric_names_hash", doc.<String>evaluate("_metric_names_hash"));
            expectedTsidBuilder.addStringDimension("attributes.operation", "test");
            expectedTsidBuilder.addStringDimension("attributes.environment", "production");
            expectedTsidBuilder.addStringDimension("unit", "{test}");
            assertThat(tsid, equalTo(expectedTsidBuilder.buildTsid()));

            assertThat(dynamicTemplateParams, hasEntry("metrics.system.cpu.usage", Map.of("unit", "{test}")));
            assertThat(dynamicTemplateParams, hasEntry("metrics.system.network.packets", Map.of("unit", "{test}")));
        });
    }

    public void testAttributeTypes() throws IOException {
        List<KeyValue> resourceAttributes = new ArrayList<>();
        resourceAttributes.add(keyValue("string_attr", "string_value"));
        resourceAttributes.add(keyValue("bool_attr", true));
        resourceAttributes.add(keyValue("int_attr", 123L));
        resourceAttributes.add(keyValue("double_attr", 123.45));
        resourceAttributes.add(keyValue("array_attr", "value1", "value2"));
        ExportMetricsServiceRequest metricsRequest = OtlpUtils.createMetricsRequest(
            resourceAttributes,
            List.of(createGaugeMetric("test.metric", "", List.of(createDoubleDataPoint(timestamp, startTimestamp, List.of()))))
        );
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            documentBuilder.buildMetricDocument(builder, dataPointGroup, new HashMap<>(), new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("resource.attributes.string_attr"), equalTo("string_value"));
            assertThat(doc.evaluate("resource.attributes.bool_attr"), equalTo(true));
            assertThat(doc.evaluate("resource.attributes.int_attr"), equalTo(123));
            assertThat(doc.evaluate("resource.attributes.double_attr"), equalTo(123.45));

            assertThat(doc.evaluate("resource.attributes.array_attr.0"), equalTo("value1"));
            assertThat(doc.evaluate("resource.attributes.array_attr.1"), equalTo("value2"));
        });
    }

    public void testEmptyFields() throws IOException {
        Metric metric = createGaugeMetric("test.metric", "", List.of(createDoubleDataPoint(timestamp, startTimestamp, List.of())));
        ResourceMetrics resourceMetrics = OtlpUtils.createResourceMetrics(
            List.of(keyValue("service.name", "test-service")),
            List.of(ScopeMetrics.newBuilder().addMetrics(metric).build())
        );

        ExportMetricsServiceRequest metricsRequest = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            // Verify that empty fields are not included
            assertThat(doc.evaluate("resource.schema_url"), is(nullValue()));
            assertThat(doc.evaluate("resource.dropped_attributes_count"), is(nullValue()));
            assertThat(doc.evaluate("scope.name"), is(nullValue()));
            assertThat(doc.evaluate("scope.schema_url"), is(nullValue()));
            assertThat(doc.evaluate("scope.dropped_attributes_count"), is(nullValue()));
            assertThat(doc.evaluate("scope.version"), is(nullValue()));
            assertThat(doc.evaluate("unit"), is(nullValue()));
        });
    }

    public void testExponentialHistogramAsTDigest() throws Exception {
        ExponentialHistogramDataPoint dataPoint = ExponentialHistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setZeroCount(1)
            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
            .setNegative(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
            .build();

        ExportMetricsServiceRequest metricsRequest = OtlpUtils.createMetricsRequest(
            List.of(createExponentialHistogramMetric("exponential_histogram", "", List.of(dataPoint), AGGREGATION_TEMPORALITY_DELTA))
        );
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("metrics.exponential_histogram.values"), equalTo(List.of(-3.0, -1.5, 0.0, 1.5, 3.0)));
            assertThat(doc.evaluate("metrics.exponential_histogram.counts"), equalTo(List.of(1, 1, 1, 1, 1)));
            assertThat(dynamicTemplates, hasEntry("metrics.exponential_histogram", "histogram"));
        });
    }

    public void testExponentialHistogramAsAggregateMetricDouble() throws Exception {
        ExponentialHistogramDataPoint dataPoint = ExponentialHistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setSum(42)
            .setCount(1L)
            .addAllAttributes(mappingHints(MappingHints.AGGREGATE_METRIC_DOUBLE))
            .build();

        ExportMetricsServiceRequest metricsRequest = OtlpUtils.createMetricsRequest(
            List.of(createExponentialHistogramMetric("histogram", "", List.of(dataPoint), AGGREGATION_TEMPORALITY_DELTA))
        );
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("metrics.histogram.sum"), equalTo(42.0));
            assertThat(doc.evaluate("metrics.histogram.value_count"), equalTo(1));
            assertThat(dynamicTemplates, hasEntry("metrics.histogram", "summary"));
        });
    }

    public void testExponentialHistogramAsExponentialHistogram() throws Exception {
        MetricDocumentBuilder documentBuilder = new MetricDocumentBuilder(
            new BufferedByteStringAccessor(),
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );

        ExponentialHistogramDataPoint dataPoint = ExponentialHistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setZeroCount(1)
            .setZeroThreshold(0.42)
            .setScale(2)
            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 2L)))
            .setNegative(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(3).addAllBucketCounts(List.of(5L, 6L)))
            .setSum(42.42)
            .setMin(-7)
            .setMax(8)
            .build();

        ExportMetricsServiceRequest metricsRequest = OtlpUtils.createMetricsRequest(
            List.of(createExponentialHistogramMetric("exponential_histogram", "", List.of(dataPoint), AGGREGATION_TEMPORALITY_DELTA))
        );
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("metrics.exponential_histogram.scale"), equalTo(2));
            assertThat(doc.evaluate("metrics.exponential_histogram.zero.count"), equalTo(1));
            assertThat(doc.evaluate("metrics.exponential_histogram.zero.threshold"), equalTo(0.42));
            assertThat(doc.evaluate("metrics.exponential_histogram.positive.indices"), equalTo(List.of(0, 1)));
            assertThat(doc.evaluate("metrics.exponential_histogram.positive.counts"), equalTo(List.of(1, 2)));
            assertThat(doc.evaluate("metrics.exponential_histogram.negative.indices"), equalTo(List.of(3, 4)));
            assertThat(doc.evaluate("metrics.exponential_histogram.negative.counts"), equalTo(List.of(5, 6)));
            assertThat(doc.evaluate("metrics.exponential_histogram.sum"), equalTo(42.42));
            assertThat(doc.evaluate("metrics.exponential_histogram.min"), equalTo(-7.0));
            assertThat(doc.evaluate("metrics.exponential_histogram.max"), equalTo(8.0));
            assertThat(dynamicTemplates, hasEntry("metrics.exponential_histogram", "exponential_histogram"));
        });
    }

    public void testHistogram() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .addBucketCounts(10L)
            .addExplicitBounds(5.0)
            .build();

        ExportMetricsServiceRequest metricsRequest = OtlpUtils.createMetricsRequest(
            List.of(createHistogramMetric("histogram", "", List.of(dataPoint), AGGREGATION_TEMPORALITY_DELTA))
        );
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("metrics.histogram.values"), equalTo(List.of(2.5)));
            assertThat(doc.evaluate("metrics.histogram.counts"), equalTo(List.of(10)));
            assertThat(dynamicTemplates, hasEntry("metrics.histogram", "histogram"));
        });
    }

    public void testHistogramAsAggregateMetricDouble() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setSum(42)
            .setCount(1L)
            .addAllAttributes(mappingHints(MappingHints.AGGREGATE_METRIC_DOUBLE))
            .build();

        ExportMetricsServiceRequest metricsRequest = OtlpUtils.createMetricsRequest(
            List.of(createHistogramMetric("histogram", "", List.of(dataPoint), AGGREGATION_TEMPORALITY_DELTA))
        );
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("metrics.histogram.sum"), equalTo(42.0));
            assertThat(doc.evaluate("metrics.histogram.value_count"), equalTo(1));
            assertThat(dynamicTemplates, hasEntry("metrics.histogram", "summary"));
        });
    }

    public void testHistogramAsExponentialHistogram() throws Exception {
        MetricDocumentBuilder documentBuilder = new MetricDocumentBuilder(
            new BufferedByteStringAccessor(),
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );

        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .addAllBucketCounts(List.of(5L, 10L))
            .addExplicitBounds(5.0)
            .setSum(42)
            .setMin(1.0)
            .setMax(9.0)
            .build();

        ExportMetricsServiceRequest metricsRequest = OtlpUtils.createMetricsRequest(
            List.of(createHistogramMetric("exponential_histogram", "", List.of(dataPoint), AGGREGATION_TEMPORALITY_DELTA))
        );
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("metrics.exponential_histogram.scale"), equalTo(MAX_SCALE));
            assertThat(
                doc.evaluate("metrics.exponential_histogram.positive.indices"),
                equalTo(List.of(-1, 363368827853L, 638246734797L, 871342349565L))
            );
            assertThat(doc.evaluate("metrics.exponential_histogram.positive.counts"), equalTo(List.of(1, 4, 9, 1)));
            assertThat(doc.evaluate("metrics.exponential_histogram.sum"), equalTo(42.0));
            assertThat(doc.evaluate("metrics.exponential_histogram.min"), equalTo(1.0));
            assertThat(doc.evaluate("metrics.exponential_histogram.max"), equalTo(9.0));
            assertThat(dynamicTemplates, hasEntry("metrics.exponential_histogram", "exponential_histogram"));
        });
    }

    public void testSummary() throws Exception {
        SummaryDataPoint dataPoint = SummaryDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setCount(1)
            .setSum(42.0)
            .addAllAttributes(mappingHints(MappingHints.DOC_COUNT))
            .build();

        ExportMetricsServiceRequest metricsRequest = OtlpUtils.createMetricsRequest(
            List.of(createSummaryMetric("summary", "", List.of(dataPoint)))
        );
        dataPointGroupingContext.groupDataPoints(metricsRequest);
        assertThat(dataPointGroupingContext.totalDataPoints(), equalTo(1));
        dataPointGroupingContext.consume(dataPointGroup -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            HashMap<String, String> dynamicTemplates = new HashMap<>();
            documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, new HashMap<>());

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("metrics.summary.sum"), equalTo(42.0));
            assertThat(doc.evaluate("metrics.summary.value_count"), equalTo(1));
            assertThat(doc.evaluate("_doc_count"), equalTo(1));
            assertThat(dynamicTemplates, hasEntry("metrics.summary", "summary"));
        });
    }

}
