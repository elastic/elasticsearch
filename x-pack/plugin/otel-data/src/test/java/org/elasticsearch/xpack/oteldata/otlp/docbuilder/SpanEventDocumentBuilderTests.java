/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;

import com.google.protobuf.ByteString;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.OtlpUtils;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class SpanEventDocumentBuilderTests extends ESTestCase {

    private static final ByteString RESOURCE_SCHEMA_URL = ByteString.copyFromUtf8("https://opentelemetry.io/schemas/1.0.0");
    private static final ByteString SCOPE_SCHEMA_URL = ByteString.copyFromUtf8("https://opentelemetry.io/schemas/1.0.0");

    private final SpanEventDocumentBuilder documentBuilder = new SpanEventDocumentBuilder(new BufferedByteStringAccessor());

    public void testBuildSpanEventDocument() throws IOException {
        ByteString traceId = ByteString.copyFrom(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });
        ByteString spanId = ByteString.copyFrom(new byte[] { 16, 17, 18, 19, 20, 21, 22, 23 });
        Resource resource = Resource.newBuilder()
            .setDroppedAttributesCount(1)
            .addAttributes(OtlpUtils.keyValue("service.name", "checkout-service"))
            .addAttributes(OtlpUtils.keyValue("resource.geo.location.lon", 9.9))
            .addAttributes(OtlpUtils.keyValue("resource.geo.location.lat", 10.1))
            .build();
        InstrumentationScope scope = InstrumentationScope.newBuilder()
            .setName("checkout-tracer")
            .setVersion("1.0.0")
            .addAttributes(OtlpUtils.keyValue("scope_attr", "value"))
            .addAttributes(OtlpUtils.keyValue("scope.geo.location.lon", 11.1))
            .addAttributes(OtlpUtils.keyValue("scope.geo.location.lat", 12.2))
            .build();
        Span span = Span.newBuilder().setTraceId(traceId).setSpanId(spanId).build();
        Span.Event event = Span.Event.newBuilder()
            .setName("exception")
            .setTimeUnixNano(2_000_000_000L)
            .setDroppedAttributesCount(3)
            .addAttributes(OtlpUtils.keyValue("event.attr.foo", "event.attr.bar"))
            .addAttributes(OtlpUtils.keyValue("client.geo.location.lon", 1.1))
            .addAttributes(OtlpUtils.keyValue("client.geo.location.lat", 2.2))
            .addAttributes(OtlpUtils.keyValue("event.name", "ignored-original-name"))
            .build();

        ObjectPath doc = buildDocument(resource, scope, span, event);

        assertThat(doc.<Number>evaluate("@timestamp").longValue(), equalTo(TimeUnit.NANOSECONDS.toMillis(2_000_000_000L)));
        assertThat(doc.evaluate("trace_id"), equalTo(MessageDigests.toHexString(traceId.toByteArray())));
        assertThat(doc.evaluate("span_id"), equalTo(MessageDigests.toHexString(spanId.toByteArray())));
        assertThat(doc.evaluate("event_name"), equalTo("exception"));
        assertThat(doc.evaluate("dropped_attributes_count"), equalTo(3));
        assertThat(doc.evaluate("data_stream.type"), equalTo("logs"));
        assertThat(doc.evaluate("data_stream.dataset"), equalTo("generic.otel"));
        assertThat(doc.evaluate("data_stream.namespace"), equalTo("default"));
        assertThat(doc.evaluate("attributes.event\\.attr\\.foo"), equalTo("event.attr.bar"));
        assertThat(doc.evaluate("attributes.client\\.geo\\.location"), equalTo(List.of(1.1, 2.2)));
        assertThat(doc.evaluate("attributes.event\\.name"), equalTo("exception"));
        assertThat(doc.evaluate("resource.schema_url"), equalTo("https://opentelemetry.io/schemas/1.0.0"));
        assertThat(doc.evaluate("resource.attributes.service\\.name"), equalTo("checkout-service"));
        assertThat(doc.evaluate("resource.attributes.resource\\.geo\\.location"), equalTo(List.of(9.9, 10.1)));
        assertThat(doc.evaluate("resource.dropped_attributes_count"), equalTo(1));
        assertThat(doc.evaluate("scope.schema_url"), equalTo("https://opentelemetry.io/schemas/1.0.0"));
        assertThat(doc.evaluate("scope.name"), equalTo("checkout-tracer"));
        assertThat(doc.evaluate("scope.version"), equalTo("1.0.0"));
        assertThat(doc.evaluate("scope.attributes"), equalTo(Map.of("scope_attr", "value", "scope.geo.location", List.of(11.1, 12.2))));
    }

    private ObjectPath buildDocument(Resource resource, InstrumentationScope scope, Span span, Span.Event event) throws IOException {
        TargetIndex targetIndex = TargetIndex.evaluate(
            "logs",
            event.getAttributesList(),
            null,
            scope.getAttributesList(),
            resource.getAttributesList()
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        documentBuilder.buildSpanEventDocument(builder, resource, RESOURCE_SCHEMA_URL, scope, SCOPE_SCHEMA_URL, targetIndex, span, event);
        return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
    }
}
