/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.resource.v1.Resource;

import com.google.protobuf.ByteString;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.DocumentMetadata;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LogDocumentBuilderTests extends ESTestCase {

    private final LogDocumentBuilder documentBuilder = new LogDocumentBuilder(new BufferedByteStringAccessor());

    public void testTextBody() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setObservedTimeUnixNano(2_000_000_000L)
            .setSeverityNumber(SeverityNumber.SEVERITY_NUMBER_INFO)
            .setSeverityText("INFO")
            .setBody(AnyValue.newBuilder().setStringValue("Hello world").build())
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("body.text"), equalTo("Hello world"));
        assertThat(doc.evaluate("body.structured"), nullValue());
        assertThat(doc.evaluate("@timestamp"), equalTo(1000));
        assertThat(doc.evaluate("observed_timestamp"), equalTo(2000));
        assertThat(doc.evaluate("severity_text"), equalTo("INFO"));
        assertThat(doc.evaluate("severity_number"), equalTo(SeverityNumber.SEVERITY_NUMBER_INFO.getNumber()));
        assertThat(doc.evaluate("attributes"), equalTo(Map.of()));
    }

    public void testKvlistBody() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(
                AnyValue.newBuilder()
                    .setKvlistValue(
                        KeyValueList.newBuilder()
                            .addValues(keyValue("key1", "value1"))
                            .addValues(keyValue("key2", 42L))
                            .addValues(keyValue("key3", true))
                    )
                    .build()
            )
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("body.text"), nullValue());
        assertThat(doc.evaluate("body.structured.key1"), equalTo("value1"));
        assertThat(doc.evaluate("body.structured.key2"), equalTo(42));
        assertThat(doc.evaluate("body.structured.key3"), equalTo(true));
    }

    public void testKvlistBodyNested() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(
                AnyValue.newBuilder()
                    .setKvlistValue(
                        KeyValueList.newBuilder()
                            .addValues(keyValue("outer", "value"))
                            .addValues(
                                KeyValue.newBuilder()
                                    .setKey("nested")
                                    .setValue(
                                        AnyValue.newBuilder()
                                            .setKvlistValue(KeyValueList.newBuilder().addValues(keyValue("inner_key", "inner_value")))
                                    )
                            )
                    )
                    .build()
            )
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("body.structured.outer"), equalTo("value"));
        assertThat(doc.evaluate("body.structured.nested"), equalTo(Map.of("inner_key", "inner_value")));
    }

    public void testArrayBodyAllMaps() throws IOException {
        // An array of kvlist values should produce body.structured (no wrapper)
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(
                AnyValue.newBuilder()
                    .setArrayValue(
                        ArrayValue.newBuilder()
                            .addValues(AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addValues(keyValue("a", "1"))))
                            .addValues(AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addValues(keyValue("b", "2"))))
                    )
                    .build()
            )
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("body.text"), nullValue());
        // structured directly contains the array (no wrapping "value" object)
        assertThat(doc.evaluate("body.structured.0"), equalTo(Map.of("a", "1")));
        assertThat(doc.evaluate("body.structured.1"), equalTo(Map.of("b", "2")));
    }

    public void testArrayBodyMixedValues() throws IOException {
        // An array with non-kvlist values should be wrapped in body.value.structured
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(
                AnyValue.newBuilder()
                    .setArrayValue(
                        ArrayValue.newBuilder()
                            .addValues(AnyValue.newBuilder().setStringValue("plain string"))
                            .addValues(AnyValue.newBuilder().setIntValue(99))
                    )
                    .build()
            )
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("body.text"), nullValue());
        assertThat(doc.evaluate("body.structured.value.0"), equalTo("plain string"));
        assertThat(doc.evaluate("body.structured.value.1"), equalTo(99));
    }

    public void testEmptyArrayBodyIsOmitted() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setArrayValue(ArrayValue.newBuilder()).build())
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("body"), nullValue());
    }

    public void testEventNameFromField() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("something happened").build())
            .setEventName("my.event")
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("event_name"), equalTo("my.event"));
    }

    public void testEventNameFromAttribute() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("something happened").build())
            .addAttributes(keyValue("event.name", "legacy.event"))
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("event_name"), equalTo("legacy.event"));
    }

    public void testEventNameFieldTakesPrecedenceOverAttribute() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("something happened").build())
            .setEventName("field.event")
            .addAttributes(keyValue("event.name", "attribute.event"))
            .build();

        ObjectPath doc = buildDocument(logRecord);

        // The eventName field takes precedence; the attribute fallback is not used
        assertThat(doc.evaluate("event_name"), equalTo("field.event"));
    }

    public void testKvlistAttribute() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .addAttributes(
                keyValue(
                    "my_kvlist",
                    KeyValueList.newBuilder()
                        .addValues(keyValue("nested_key1", "nested_value1"))
                        .addValues(keyValue("nested_key2", 42L))
                        .addValues(keyValue("nested_key3", true))
                        .build()
                )
            )
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("attributes.my_kvlist.nested_key1"), equalTo("nested_value1"));
        assertThat(doc.evaluate("attributes.my_kvlist.nested_key2"), equalTo(42));
        assertThat(doc.evaluate("attributes.my_kvlist.nested_key3"), equalTo(true));
    }

    public void testBytesAttribute() throws IOException {
        byte[] bytes = randomByteArrayOfLength(16);
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .addAttributes(
                KeyValue.newBuilder()
                    .setKey("my_bytes")
                    .setValue(AnyValue.newBuilder().setBytesValue(ByteString.copyFrom(bytes)).build())
                    .build()
            )
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("attributes.my_bytes"), equalTo(Base64.getEncoder().encodeToString(bytes)));
    }

    public void testSpanAndTraceIdsAreHexEncoded() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .setSpanId(ByteString.copyFrom(new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77 }))
            .setTraceId(
                ByteString.copyFrom(
                    new byte[] {
                        0x00,
                        0x11,
                        0x22,
                        0x33,
                        0x44,
                        0x55,
                        0x66,
                        0x77,
                        (byte) 0x88,
                        (byte) 0x99,
                        (byte) 0xAA,
                        (byte) 0xBB,
                        (byte) 0xCC,
                        (byte) 0xDD,
                        (byte) 0xEE,
                        (byte) 0xFF }
                )
            )
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("span_id"), equalTo("0011223344556677"));
        assertThat(doc.evaluate("trace_id"), equalTo("00112233445566778899aabbccddeeff"));
    }

    public void testEmptySpanAndTraceIdsAreOmitted() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .setSpanId(ByteString.EMPTY)
            .setTraceId(ByteString.EMPTY)
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("span_id"), nullValue());
        assertThat(doc.evaluate("trace_id"), nullValue());
    }

    public void testTimestampFallsBackToObservedTimestamp() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setObservedTimeUnixNano(5_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("@timestamp"), equalTo(5000));
        assertThat(doc.evaluate("observed_timestamp"), equalTo(5000));
    }

    public void testTimestampPreservesSubMillisecondPrecision() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_721_314_113_467_654_123L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("@timestamp"), equalTo("1721314113467.654123"));
        assertThat(doc.evaluate("observed_timestamp"), equalTo(0));
    }

    public void testTimestampPadsSubMillisecondRemainder() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_123L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("@timestamp"), equalTo("1000.000123"));
        assertThat(doc.evaluate("observed_timestamp"), equalTo(0));
    }

    public void testEmptyBodyIsOmitted() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder().setTimeUnixNano(1_000_000_000L).build();

        ObjectPath doc = buildDocument(logRecord);

        assertThat(doc.evaluate("body"), nullValue());
        assertThat(doc.evaluate("attributes"), equalTo(Map.of()));
    }

    public void testControlAttributesAreFiltered() throws IOException {
        Resource resource = Resource.newBuilder()
            .addAttributes(keyValue("service.name", "test-service"))
            .addAttributes(keyValue("elastic.mapping.mode", "otel"))
            .build();
        InstrumentationScope scope = InstrumentationScope.newBuilder()
            .setName("test-scope")
            .addAttributes(keyValue("scope.keep", "value"))
            .build();
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .addAttributes(keyValue("keep", "value"))
            .addAttributes(keyValue("data_stream.type", "logs"))
            .addAttributes(keyValue("elasticsearch.document_id", "doc-id"))
            .addAttributes(keyValue(DocumentMetadata.INGEST_PIPELINE_ATTRIBUTE, "logs-pipeline"))
            .build();

        ObjectPath doc = buildDocument(resource, scope, logRecord);

        assertThat(doc.evaluate("attributes.keep"), equalTo("value"));
        assertThat(doc.evaluate("attributes.data_stream\\.type"), nullValue());
        assertThat(doc.evaluate("attributes.elasticsearch\\.document_id"), nullValue());
        assertThat(doc.evaluate("attributes.elasticsearch\\.ingest_pipeline"), nullValue());
        assertThat(doc.evaluate("scope.attributes.scope\\.keep"), equalTo("value"));
        assertThat(doc.evaluate("resource.attributes.service\\.name"), equalTo("test-service"));
        assertThat(doc.evaluate("resource.attributes.elastic\\.mapping\\.mode"), nullValue());
    }

    public void testFilteredControlAttributesCreateEmptyAttributesObject() throws IOException {
        Resource resource = Resource.newBuilder().addAttributes(keyValue("elastic.mapping.mode", "otel")).build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().setName("test-scope").build();
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .addAttributes(keyValue("data_stream.type", "logs"))
            .addAttributes(keyValue("elasticsearch.document_id", "doc-id"))
            .addAttributes(keyValue(DocumentMetadata.INGEST_PIPELINE_ATTRIBUTE, "logs-pipeline"))
            .build();

        ObjectPath doc = buildDocument(resource, scope, logRecord);

        assertThat(doc.evaluate("attributes"), equalTo(Map.of()));
        assertThat(doc.evaluate("scope.attributes"), equalTo(Map.of()));
        assertThat(doc.evaluate("resource.attributes"), equalTo(Map.of()));
    }

    private ObjectPath buildDocument(LogRecord logRecord) throws IOException {
        Resource resource = Resource.newBuilder().addAttributes(keyValue("service.name", "test-service")).build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().setName("test-scope").setVersion("1.0.0").build();
        return buildDocument(resource, scope, logRecord);
    }

    private ObjectPath buildDocument(Resource resource, InstrumentationScope scope, LogRecord logRecord) throws IOException {
        TargetIndex targetIndex = TargetIndex.evaluate("logs", List.of(), null, List.of(), List.of());

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        documentBuilder.buildLogDocument(builder, resource, ByteString.EMPTY, scope, ByteString.EMPTY, targetIndex, logRecord);
        return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
    }
}
