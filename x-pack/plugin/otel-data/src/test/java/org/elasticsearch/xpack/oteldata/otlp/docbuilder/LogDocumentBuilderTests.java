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
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        assertThat(doc.<Number>evaluate("@timestamp").longValue(), equalTo(TimeUnit.NANOSECONDS.toMillis(1_000_000_000L)));
        assertThat(doc.<Number>evaluate("observed_timestamp").longValue(), equalTo(TimeUnit.NANOSECONDS.toMillis(2_000_000_000L)));
        assertThat(doc.evaluate("severity_text"), equalTo("INFO"));
        assertThat(doc.evaluate("severity_number"), equalTo(SeverityNumber.SEVERITY_NUMBER_INFO.getNumber()));
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
        assertThat(doc.evaluate("body.value.structured.0"), equalTo("plain string"));
        assertThat(doc.evaluate("body.value.structured.1"), equalTo(99));
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

    public void testTimestampFallsBackToObservedTimestamp() throws IOException {
        LogRecord logRecord = LogRecord.newBuilder()
            .setObservedTimeUnixNano(5_000_000_000L)
            .setBody(AnyValue.newBuilder().setStringValue("msg").build())
            .build();

        ObjectPath doc = buildDocument(logRecord);

        // When timeUnixNano is 0, @timestamp should fall back to observedTimeUnixNano
        assertThat(doc.<Number>evaluate("@timestamp").longValue(), equalTo(TimeUnit.NANOSECONDS.toMillis(5_000_000_000L)));
        assertThat(doc.<Number>evaluate("observed_timestamp").longValue(), equalTo(TimeUnit.NANOSECONDS.toMillis(5_000_000_000L)));
    }

    private ObjectPath buildDocument(LogRecord logRecord) throws IOException {
        Resource resource = Resource.newBuilder().addAttributes(keyValue("service.name", "test-service")).build();
        InstrumentationScope scope = InstrumentationScope.newBuilder().setName("test-scope").setVersion("1.0.0").build();
        TargetIndex targetIndex = TargetIndex.evaluate("logs", List.of(), null, List.of(), List.of());

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        documentBuilder.buildLogDocument(builder, resource, ByteString.EMPTY, scope, ByteString.EMPTY, targetIndex, logRecord);
        return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
    }
}
