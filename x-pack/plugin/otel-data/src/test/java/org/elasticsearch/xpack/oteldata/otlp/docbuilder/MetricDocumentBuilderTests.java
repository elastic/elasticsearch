/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;

import com.google.protobuf.ByteString;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
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

    private final MetricDocumentBuilder documentBuilder = new MetricDocumentBuilder(new BufferedByteStringAccessor());
    private final long timestamp = randomLong();
    private final long startTimestamp = randomLong();

    public void testBuildMetricDocument() throws IOException {
        List<KeyValue> resourceAttributes = new ArrayList<>();
        resourceAttributes.add(keyValue("service.name", "test-service"));
        resourceAttributes.add(keyValue("host.name", "test-host"));
        Resource resource = Resource.newBuilder().addAllAttributes(resourceAttributes).setDroppedAttributesCount(1).build();
        ByteString resourceSchemaUrl = ByteString.copyFromUtf8("https://opentelemetry.io/schemas/1.0.0");

        InstrumentationScope scope = InstrumentationScope.newBuilder()
            .setName("test-scope")
            .setVersion("1.0.0")
            .setDroppedAttributesCount(2)
            .addAttributes(keyValue("scope_attr", "value"))
            .build();
        ByteString scopeSchemaUrl = ByteString.copyFromUtf8("https://opentelemetry.io/schemas/1.0.0");

        List<KeyValue> dataPointAttributes = List.of(keyValue("operation", "test"), (keyValue("environment", "production")));

        DataPointGroupingContext.DataPointGroup dataPointGroup = new DataPointGroupingContext.DataPointGroup(
            resource,
            resourceSchemaUrl,
            scope,
            scopeSchemaUrl,
            dataPointAttributes,
            "{test}",
            TargetIndex.defaultMetrics()
        );
        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.Number(
                createDoubleDataPoint(timestamp, startTimestamp, dataPointAttributes),
                createGaugeMetric("system.cpu.usage", "", List.of())
            )
        );
        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.Number(
                createLongDataPoint(timestamp, startTimestamp, dataPointAttributes),
                createSumMetric(
                    "system.network.packets",
                    "{test}",
                    List.of(),
                    true,
                    AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
                )
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        HashMap<String, String> dynamicTemplates = documentBuilder.buildMetricDocument(builder, dataPointGroup);
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
    }

    public void testAttributeTypes() throws IOException {
        List<KeyValue> resourceAttributes = new ArrayList<>();
        resourceAttributes.add(keyValue("string_attr", "string_value"));
        resourceAttributes.add(keyValue("bool_attr", true));
        resourceAttributes.add(keyValue("int_attr", 123L));
        resourceAttributes.add(keyValue("double_attr", 123.45));
        resourceAttributes.add(keyValue("array_attr", "value1", "value2"));

        Resource resource = Resource.newBuilder().addAllAttributes(resourceAttributes).build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().build();

        DataPointGroupingContext.DataPointGroup dataPointGroup = new DataPointGroupingContext.DataPointGroup(
            resource,
            null,
            scope,
            null,
            List.of(),
            "",
            TargetIndex.defaultMetrics()
        );
        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.Number(createDoubleDataPoint(timestamp), createGaugeMetric("test.metric", "", List.of()))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        documentBuilder.buildMetricDocument(builder, dataPointGroup);

        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

        assertThat(doc.evaluate("resource.attributes.string_attr"), equalTo("string_value"));
        assertThat(doc.evaluate("resource.attributes.bool_attr"), equalTo(true));
        assertThat(doc.evaluate("resource.attributes.int_attr"), equalTo(123));
        assertThat(doc.evaluate("resource.attributes.double_attr"), equalTo(123.45));

        assertThat(doc.evaluate("resource.attributes.array_attr.0"), equalTo("value1"));
        assertThat(doc.evaluate("resource.attributes.array_attr.1"), equalTo("value2"));
    }

    public void testEmptyFields() throws IOException {
        Resource resource = Resource.newBuilder().build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().build();

        DataPointGroupingContext.DataPointGroup dataPointGroup = new DataPointGroupingContext.DataPointGroup(
            resource,
            null,
            scope,
            null,
            List.of(),
            "",
            TargetIndex.defaultMetrics()
        );

        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.Number(createDoubleDataPoint(timestamp), createGaugeMetric("test.metric", "", List.of()))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        documentBuilder.buildMetricDocument(builder, dataPointGroup);

        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        // Verify that empty fields are not included
        assertThat(doc.evaluate("resource.schema_url"), is(nullValue()));
        assertThat(doc.evaluate("resource.dropped_attributes_count"), is(nullValue()));
        assertThat(doc.evaluate("scope.name"), is(nullValue()));
        assertThat(doc.evaluate("scope.schema_url"), is(nullValue()));
        assertThat(doc.evaluate("scope.dropped_attributes_count"), is(nullValue()));
        assertThat(doc.evaluate("scope.version"), is(nullValue()));
        assertThat(doc.evaluate("unit"), is(nullValue()));
    }

    public void testExponentialHistogram() throws Exception {
        Resource resource = Resource.newBuilder().build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().build();

        DataPointGroupingContext.DataPointGroup dataPointGroup = new DataPointGroupingContext.DataPointGroup(
            resource,
            null,
            scope,
            null,
            List.of(),
            "",
            TargetIndex.defaultMetrics()
        );
        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.ExponentialHistogram(
                ExponentialHistogramDataPoint.newBuilder()
                    .setTimeUnixNano(timestamp)
                    .setStartTimeUnixNano(startTimestamp)
                    .setZeroCount(1)
                    .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .build(),
                createExponentialHistogramMetric("exponential_histogram", "", List.of(), AGGREGATION_TEMPORALITY_DELTA)
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        HashMap<String, String> dynamicTemplates = documentBuilder.buildMetricDocument(builder, dataPointGroup);

        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        assertThat(doc.evaluate("metrics.exponential_histogram.values"), equalTo(List.of(-3.0, -1.5, 0.0, 1.5, 3.0)));
        assertThat(doc.evaluate("metrics.exponential_histogram.counts"), equalTo(List.of(1, 1, 1, 1, 1)));
        assertThat(dynamicTemplates, hasEntry("metrics.exponential_histogram", "histogram"));
    }

    public void testExponentialHistogramAsAggregateMetricDouble() throws Exception {
        Resource resource = Resource.newBuilder().build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().build();

        DataPointGroupingContext.DataPointGroup dataPointGroup = new DataPointGroupingContext.DataPointGroup(
            resource,
            null,
            scope,
            null,
            List.of(),
            "",
            TargetIndex.defaultMetrics()
        );
        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.ExponentialHistogram(
                ExponentialHistogramDataPoint.newBuilder()
                    .setTimeUnixNano(timestamp)
                    .setStartTimeUnixNano(startTimestamp)
                    .setSum(42)
                    .setCount(1L)
                    .addAllAttributes(mappingHints(MappingHints.AGGREGATE_METRIC_DOUBLE))
                    .build(),
                createExponentialHistogramMetric("histogram", "", List.of(), AGGREGATION_TEMPORALITY_DELTA)
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        HashMap<String, String> dynamicTemplates = documentBuilder.buildMetricDocument(builder, dataPointGroup);

        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        assertThat(doc.evaluate("metrics.histogram.sum"), equalTo(42.0));
        assertThat(doc.evaluate("metrics.histogram.value_count"), equalTo(1));
        assertThat(dynamicTemplates, hasEntry("metrics.histogram", "summary"));
    }

    public void testHistogram() throws Exception {
        Resource resource = Resource.newBuilder().build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().build();

        DataPointGroupingContext.DataPointGroup dataPointGroup = new DataPointGroupingContext.DataPointGroup(
            resource,
            null,
            scope,
            null,
            List.of(),
            "",
            TargetIndex.defaultMetrics()
        );
        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.Histogram(
                HistogramDataPoint.newBuilder()
                    .setTimeUnixNano(timestamp)
                    .setStartTimeUnixNano(startTimestamp)
                    .addBucketCounts(10L)
                    .addExplicitBounds(5.0)
                    .build(),
                createHistogramMetric("histogram", "", List.of(), AGGREGATION_TEMPORALITY_DELTA)
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        HashMap<String, String> dynamicTemplates = documentBuilder.buildMetricDocument(builder, dataPointGroup);

        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        assertThat(doc.evaluate("metrics.histogram.values"), equalTo(List.of(2.5)));
        assertThat(doc.evaluate("metrics.histogram.counts"), equalTo(List.of(10)));
        assertThat(dynamicTemplates, hasEntry("metrics.histogram", "histogram"));
    }

    public void testHistogramAsAggregateMetricDouble() throws Exception {
        Resource resource = Resource.newBuilder().build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().build();

        DataPointGroupingContext.DataPointGroup dataPointGroup = new DataPointGroupingContext.DataPointGroup(
            resource,
            null,
            scope,
            null,
            List.of(),
            "",
            TargetIndex.defaultMetrics()
        );
        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.Histogram(
                HistogramDataPoint.newBuilder()
                    .setTimeUnixNano(timestamp)
                    .setStartTimeUnixNano(startTimestamp)
                    .setSum(42)
                    .setCount(1L)
                    .addAllAttributes(mappingHints(MappingHints.AGGREGATE_METRIC_DOUBLE))
                    .build(),
                createHistogramMetric("histogram", "", List.of(), AGGREGATION_TEMPORALITY_DELTA)
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        HashMap<String, String> dynamicTemplates = documentBuilder.buildMetricDocument(builder, dataPointGroup);

        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        assertThat(doc.evaluate("metrics.histogram.sum"), equalTo(42.0));
        assertThat(doc.evaluate("metrics.histogram.value_count"), equalTo(1));
        assertThat(dynamicTemplates, hasEntry("metrics.histogram", "summary"));
    }

    public void testSummary() throws Exception {
        Resource resource = Resource.newBuilder().build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().build();

        DataPointGroupingContext.DataPointGroup dataPointGroup = new DataPointGroupingContext.DataPointGroup(
            resource,
            null,
            scope,
            null,
            List.of(),
            "",
            TargetIndex.defaultMetrics()
        );
        dataPointGroup.addDataPoint(
            Set.of(),
            new DataPoint.Summary(
                SummaryDataPoint.newBuilder()
                    .setTimeUnixNano(timestamp)
                    .setStartTimeUnixNano(startTimestamp)
                    .setCount(1)
                    .setSum(42.0)
                    .addAllAttributes(mappingHints(MappingHints.DOC_COUNT))
                    .build(),
                createSummaryMetric("summary", "", List.of())
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        HashMap<String, String> dynamicTemplates = documentBuilder.buildMetricDocument(builder, dataPointGroup);

        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        assertThat(doc.evaluate("metrics.summary.sum"), equalTo(42.0));
        assertThat(doc.evaluate("metrics.summary.value_count"), equalTo(1));
        assertThat(doc.evaluate("_doc_count"), equalTo(1));
        assertThat(dynamicTemplates, hasEntry("metrics.summary", "summary"));
    }
}
