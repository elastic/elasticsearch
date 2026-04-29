/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;

import com.google.protobuf.ByteString;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SpanDocumentBuilderTests extends ESTestCase {

    private static final ByteString RESOURCE_SCHEMA_URL = ByteString.copyFromUtf8("https://opentelemetry.io/schemas/1.0.0");
    private static final ByteString SCOPE_SCHEMA_URL = ByteString.copyFromUtf8("https://opentelemetry.io/schemas/1.0.0");

    private final SpanDocumentBuilder documentBuilder = new SpanDocumentBuilder(new BufferedByteStringAccessor());

    public void testBuildSpanDocument() throws IOException {
        ByteString traceId = randomHexByteString(16);
        ByteString spanId = randomHexByteString(8);
        ByteString parentSpanId = randomHexByteString(8);
        byte[] linkPayload = randomByteArrayOfLength(8);

        Resource resource = Resource.newBuilder()
            .setDroppedAttributesCount(1)
            .addAttributes(keyValue("service.name", "checkout-service"))
            .addAttributes(keyValue("host.name", "test-host"))
            .build();
        InstrumentationScope scope = InstrumentationScope.newBuilder()
            .setName("checkout-tracer")
            .setVersion("1.0.0")
            .setDroppedAttributesCount(2)
            .addAttributes(keyValue("scope_attr", "value"))
            .build();
        Span span = Span.newBuilder()
            .setTraceId(traceId)
            .setSpanId(spanId)
            .setParentSpanId(parentSpanId)
            .setTraceState("vendor=value")
            .setName("GET /orders")
            .setKind(Span.SpanKind.SPAN_KIND_SERVER)
            .setStartTimeUnixNano(2_000_000_000L)
            .setEndTimeUnixNano(2_500_000_000L)
            .setDroppedAttributesCount(3)
            .setDroppedEventsCount(4)
            .setDroppedLinksCount(5)
            .addAttributes(keyValue("http.method", "GET"))
            .addAttributes(keyValue("http.status_code", 404L))
            .addLinks(
                Span.Link.newBuilder()
                    .setTraceId(randomHexByteString(16))
                    .setSpanId(randomHexByteString(8))
                    .setTraceState("linked=true")
                    .setDroppedAttributesCount(1)
                    .addAttributes(
                        keyValue(
                            "labels",
                            KeyValueList.newBuilder().addValues(keyValue("environment", "prod")).addValues(keyValue("zone", "a")).build()
                        )
                    )
                    .addAttributes(
                        KeyValue.newBuilder()
                            .setKey("payload")
                            .setValue(AnyValue.newBuilder().setBytesValue(ByteString.copyFrom(linkPayload)).build())
                            .build()
                    )
                    .build()
            )
            .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_ERROR).setMessage("not found").build())
            .build();

        ObjectPath doc = buildDocument(resource, scope, span);

        assertThat(doc.evaluate("@timestamp"), equalTo(2000));
        assertThat(doc.evaluate("trace_id"), equalTo(MessageDigests.toHexString(traceId.toByteArray())));
        assertThat(doc.evaluate("span_id"), equalTo(MessageDigests.toHexString(spanId.toByteArray())));
        assertThat(doc.evaluate("parent_span_id"), equalTo(MessageDigests.toHexString(parentSpanId.toByteArray())));
        assertThat(doc.evaluate("trace_state"), equalTo("vendor=value"));
        assertThat(doc.evaluate("name"), equalTo("GET /orders"));
        assertThat(doc.evaluate("kind"), equalTo("Server"));
        assertThat(doc.<Number>evaluate("duration").longValue(), equalTo(500_000_000L));
        assertThat(doc.evaluate("dropped_events_count"), equalTo(4));
        assertThat(doc.evaluate("dropped_links_count"), equalTo(5));
        assertThat(doc.evaluate("resource.schema_url"), equalTo("https://opentelemetry.io/schemas/1.0.0"));
        assertThat(doc.evaluate("resource.dropped_attributes_count"), equalTo(1));
        assertThat(doc.evaluate("resource.attributes.service\\.name"), equalTo("checkout-service"));
        assertThat(doc.evaluate("scope.name"), equalTo("checkout-tracer"));
        assertThat(doc.evaluate("scope.version"), equalTo("1.0.0"));
        assertThat(doc.evaluate("scope.schema_url"), equalTo("https://opentelemetry.io/schemas/1.0.0"));
        assertThat(doc.evaluate("scope.dropped_attributes_count"), equalTo(2));
        assertThat(doc.evaluate("scope.attributes.scope_attr"), equalTo("value"));
        assertThat(doc.evaluate("data_stream.type"), equalTo("traces"));
        assertThat(doc.evaluate("data_stream.dataset"), equalTo("generic.otel"));
        assertThat(doc.evaluate("data_stream.namespace"), equalTo("default"));
        assertThat(doc.evaluate("dropped_attributes_count"), equalTo(3));
        assertThat(doc.evaluate("attributes.http\\.method"), equalTo("GET"));
        assertThat(doc.evaluate("attributes.http\\.status_code"), equalTo(404));
        assertThat(doc.evaluate("links.0.trace_state"), equalTo("linked=true"));
        assertThat(doc.evaluate("links.0.dropped_attributes_count"), equalTo(1));
        assertThat(doc.evaluate("links.0.attributes.labels"), equalTo(Map.of("environment", "prod", "zone", "a")));
        assertThat(doc.evaluate("links.0.attributes.payload"), equalTo(Base64.getEncoder().encodeToString(linkPayload)));
        assertThat(doc.evaluate("status.code"), equalTo("Error"));
        assertThat(doc.evaluate("status.message"), equalTo("not found"));
    }

    public void testTimestampFallsBackToEndTime() throws IOException {
        Span span = Span.newBuilder().setEndTimeUnixNano(5_000_000_000L).build();

        ObjectPath doc = buildDocument(Resource.getDefaultInstance(), InstrumentationScope.getDefaultInstance(), span);

        assertThat(doc.evaluate("@timestamp"), equalTo(5000));
        assertThat(doc.evaluate("kind"), equalTo("Unspecified"));
        assertThat(doc.evaluate("duration"), nullValue());
        assertThat(doc.evaluate("status"), nullValue());
        assertThat(doc.evaluate("links"), nullValue());
    }

    public void testTimestampPreservesSubMillisecondPrecision() throws IOException {
        Span span = Span.newBuilder().setStartTimeUnixNano(1_721_314_113_467_654_123L).build();

        ObjectPath doc = buildDocument(Resource.getDefaultInstance(), InstrumentationScope.getDefaultInstance(), span);

        assertThat(doc.evaluate("@timestamp"), equalTo("1721314113467.654123"));
    }

    public void testStatusMessageWithoutCodeIsIgnored() throws IOException {
        Span span = Span.newBuilder()
            .setStartTimeUnixNano(2_000_000_000L)
            .setStatus(Status.newBuilder().setMessage("status message without code").build())
            .build();

        ObjectPath doc = buildDocument(Resource.getDefaultInstance(), InstrumentationScope.getDefaultInstance(), span);

        assertThat(doc.evaluate("status"), nullValue());
    }

    public void testSpanWithoutTimestampsUsesEpoch() throws IOException {
        Span span = Span.newBuilder().setName("span-without-timestamp").build();

        ObjectPath doc = buildDocument(Resource.getDefaultInstance(), InstrumentationScope.getDefaultInstance(), span);

        assertThat(doc.evaluate("@timestamp"), equalTo(0));
        assertThat(doc.evaluate("duration"), nullValue());
    }

    private ObjectPath buildDocument(Resource resource, InstrumentationScope scope, Span span) throws IOException {
        TargetIndex targetIndex = TargetIndex.evaluate(
            "traces",
            span.getAttributesList(),
            null,
            scope.getAttributesList(),
            resource.getAttributesList()
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        documentBuilder.buildSpanDocument(builder, resource, RESOURCE_SCHEMA_URL, scope, SCOPE_SCHEMA_URL, targetIndex, span);
        return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
    }

    private static ByteString randomHexByteString(int length) {
        return ByteString.copyFrom(randomByteArrayOfLength(length));
    }
}
