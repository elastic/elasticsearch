/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.KeyValue;
import io.opentelemetry.api.common.Value;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class ElasticsearchOtelAppenderTests extends ESTestCase {

    private static InMemoryLogRecordExporter exporter;
    private static SdkLoggerProvider provider;
    private static ElasticsearchOtelAppender appender;

    @BeforeClass
    public static void start() {
        exporter = InMemoryLogRecordExporter.create();
        provider = SdkLoggerProvider.builder().addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter)).build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setLoggerProvider(provider).build();
        appender = new ElasticsearchOtelAppender("test", sdk, null);
        appender.start();
    }

    @AfterClass
    public static void stop() {
        appender.stop();
        provider.close();
    }

    @Before
    public void reset() {
        exporter.reset();
    }

    // --- helper methods ---

    private LogRecordData emitMapMessage(ESLogMessage msg) {
        exporter.reset();
        var level = randomFrom(Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.TRACE);
        appender.append(Log4jLogEvent.newBuilder().setLoggerName("test.logger").setLevel(level).setMessage(msg).build());
        List<LogRecordData> records = exporter.getFinishedLogRecordItems();
        assertThat(records, hasSize(1));
        assertThat(records.getFirst().getSeverity().toString(), equalTo(level.toString()));
        return records.getFirst();
    }

    private LogRecordData emitPlainMessage(String text) {
        var level = randomFrom(Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.TRACE);
        appender.append(
            Log4jLogEvent.newBuilder().setLoggerName("test.logger").setLevel(level).setMessage(new SimpleMessage(text)).build()
        );
        List<LogRecordData> records = exporter.getFinishedLogRecordItems();
        assertThat(records, hasSize(1));
        assertThat(records.getFirst().getSeverity().toString(), equalTo(level.toString()));
        return records.getFirst();
    }

    private <T, R> void assertSimpleMessage(AttributeKey<R> key, T attribute, R result) {
        ESLogMessage msg = new ESLogMessage().field(key.getKey(), attribute);
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(key), equalTo(result));
    }

    private <T> void assertSimpleMessage(AttributeKey<T> key, T attribute) {
        assertSimpleMessage(key, attribute, attribute);
    }

    // --- key naming ---

    public void testNoPrefixOnKeys() {
        ESLogMessage msg = new ESLogMessage().field("mykey", "myval");
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.stringKey("mykey")), equalTo("myval"));
        assertThat(record.getAttributes().get(AttributeKey.stringKey("log4j.map_message.mykey")), nullValue());
    }

    // --- primitive types ---

    public void testPrimitiveAttributes() {
        // string
        assertSimpleMessage(AttributeKey.stringKey("k"), "hello");
        // long
        assertSimpleMessage(AttributeKey.longKey("k"), 42L);
        // integer widened to long
        assertSimpleMessage(AttributeKey.longKey("k"), 7, 7L);
        // double
        assertSimpleMessage(AttributeKey.doubleKey("k"), 3.14, 3.14);
        // float widened to double
        assertSimpleMessage(AttributeKey.doubleKey("k"), 1.5f, 1.5);
        // boolean
        assertSimpleMessage(AttributeKey.booleanKey("k"), true);
        // unknown type falls back to toString
        assertSimpleMessage(AttributeKey.stringKey("k"), new Object() {
            @Override
            public String toString() {
                return "custom";
            }
        }, "custom");
    }

    // --- list types ---

    public void testListAttributes() {
        // string
        assertSimpleMessage(AttributeKey.stringArrayKey("k"), List.of("a", "b", "c"));
        // long
        assertSimpleMessage(AttributeKey.longArrayKey("k"), List.of(1L, 2L, 3L));
        // integer widened to long
        assertSimpleMessage(AttributeKey.longArrayKey("k"), List.of(1, 2, 3), List.of(1L, 2L, 3L));
        // boolean
        assertSimpleMessage(AttributeKey.booleanArrayKey("k"), List.of(true, false));
        // double
        assertSimpleMessage(AttributeKey.doubleArrayKey("k"), List.of(1.5, 2.5));
        // float widened to double
        assertSimpleMessage(AttributeKey.doubleArrayKey("k"), List.of(1.0f, 2.0f), List.of(1.0, 2.0));
        // unknown type falls back to toString
        assertSimpleMessage(AttributeKey.stringArrayKey("k"), List.of(new Object() {
            @Override
            public String toString() {
                return "obj1";
            }
        }, new Object() {
            @Override
            public String toString() {
                return "obj2";
            }
        }), List.of("obj1", "obj2"));
    }

    // --- array types ---

    public void testArrayAttributes() {
        // string
        assertSimpleMessage(AttributeKey.stringArrayKey("k"), new String[] { "a", "b" }, List.of("a", "b"));
        // long primitive
        assertSimpleMessage(AttributeKey.longArrayKey("k"), new long[] { 1L, 2L, 3L }, List.of(1L, 2L, 3L));
        // int primitive widened to long
        assertSimpleMessage(AttributeKey.longArrayKey("k"), new int[] { 4, 5 }, List.of(4L, 5L));
        // double primitive
        assertSimpleMessage(AttributeKey.doubleArrayKey("k"), new double[] { 1.0, 2.5 }, List.of(1.0, 2.5));
        // float primitive widened to double
        assertSimpleMessage(AttributeKey.doubleArrayKey("k"), new float[] { 1.0f, 2.0f }, List.of(1.0, 2.0));
        // boolean primitive
        assertSimpleMessage(AttributeKey.booleanArrayKey("k"), new boolean[] { true, false, true }, List.of(true, false, true));
        // Long boxed array
        assertSimpleMessage(AttributeKey.longArrayKey("k"), new Long[] { 10L, 20L }, List.of(10L, 20L));
    }

    // --- map type dispatch ---

    @SuppressWarnings("unchecked")
    public void testMapAttribute() {
        Map<String, Object> data = new HashMap<>();
        data.put("s", "hello");
        data.put("l", 42L);
        data.put("b", true);
        data.put("i", 5);
        data.put("d", 2.5);
        data.put("f", 1.5f);
        data.put("o", new Object() {
            @Override
            public String toString() {
                return "obj";
            }
        });
        data.put("n", null);
        data.put("L", List.of("a", "b", "c"));
        ESLogMessage msg = new ESLogMessage().field("k", data);
        LogRecordData record = emitMapMessage(msg);
        Value<?> v = record.getAttributes().get(AttributeKey.valueKey("k"));
        assertNotNull(v);
        // Value.of(Map) returns Value<List<KeyValue>>
        List<KeyValue> kvList = (List<KeyValue>) v.getValue();
        Map<String, Value<?>> map = new HashMap<>();
        kvList.forEach(kv -> map.put(kv.getKey(), kv.getValue()));
        assertThat(map.get("s"), equalTo(Value.of("hello")));
        assertThat(map.get("l"), equalTo(Value.of(42L)));
        assertThat(map.get("b"), equalTo(Value.of(true)));
        assertThat(map.get("i"), equalTo(Value.of(5L)));
        assertThat(map.get("d"), equalTo(Value.of(2.5)));
        assertThat(map.get("f"), equalTo(Value.of(1.5)));
        assertThat(map.get("o"), equalTo(Value.of("obj")));
        assertThat(map.get("L"), equalTo(Value.of(List.of(Value.of("a"), Value.of("b"), Value.of("c")))));
        assertFalse(map.containsKey("n"));
    }

    @SuppressWarnings("unchecked")
    public void testNestedMapAttribute() {
        Map<String, Object> inner = Map.of("a", 1L);
        ESLogMessage msg = new ESLogMessage().field("k", Map.of("nested", inner));
        LogRecordData record = emitMapMessage(msg);
        Value<?> v = record.getAttributes().get(AttributeKey.valueKey("k"));
        assertNotNull(v);
        List<KeyValue> outerKvList = (List<KeyValue>) v.getValue();
        Map<String, Value<?>> outer = new HashMap<>();
        outerKvList.forEach(kv -> outer.put(kv.getKey(), kv.getValue()));
        Value<?> nestedVal = outer.get("nested");
        assertNotNull(nestedVal);
        List<KeyValue> innerKvList = (List<KeyValue>) nestedVal.getValue();
        Map<String, Value<?>> inner2 = new HashMap<>();
        innerKvList.forEach(kv -> inner2.put(kv.getKey(), kv.getValue()));
        assertThat(inner2.get("a"), equalTo(Value.of(1L)));
    }

    // --- body handling ---

    public void testMessageKeyBecomesBody() {
        ESLogMessage msg = new ESLogMessage().field("message", "the body").field("other", "val");
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getBodyValue().asString(), equalTo("the body"));
        // "message" key must not appear as an attribute
        assertThat(record.getAttributes().get(AttributeKey.stringKey("message")), nullValue());
        // other attributes are still emitted
        assertThat(record.getAttributes().get(AttributeKey.stringKey("other")), equalTo("val"));
    }

    public void testNonMapMessageUsesFormattedBody() {
        LogRecordData record = emitPlainMessage("plain text");
        assertThat(record.getBodyValue().asString(), equalTo("plain text"));
        assertThat(record.getAttributes().size(), equalTo(0));
    }

    // --- null handling ---

    public void testNullValueSkipped() {
        ESLogMessage msg = new ESLogMessage().field("present", "yes").field("absent", null);
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.stringKey("present")), equalTo("yes"));
        assertThat(record.getAttributes().get(AttributeKey.stringKey("absent")), nullValue());
    }

    public void testEmptyListSkipped() {
        ESLogMessage msg = new ESLogMessage().field("k", List.of());
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().size(), equalTo(0));
    }

    public void testEmptyArraySkipped() {
        ESLogMessage msg = new ESLogMessage().field("k", new String[0]);
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().size(), equalTo(0));
    }

    public void testEmptyMapSkipped() {
        ESLogMessage msg = new ESLogMessage().field("k", Map.of());
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().size(), equalTo(0));
    }

    // --- trace context ---

    public void testTraceIdFromMapMessageSetsSpanContext() {
        String traceId = "0af7651916cd43dd8448eb211c80319c";
        ESLogMessage msg = new ESLogMessage().field("trace.id", traceId).field("message", "traced");
        LogRecordData record = emitMapMessage(msg);
        SpanContext spanCtx = record.getSpanContext();
        assertThat(spanCtx.isValid(), equalTo(true));
        assertThat(spanCtx.getTraceId(), equalTo(traceId));
        // "trace.id" key must not appear as an attribute
        assertThat(record.getAttributes().get(AttributeKey.stringKey("trace.id")), nullValue());
    }

    public void testMissingTraceIdLeavesInvalidSpanContext() {
        ESLogMessage msg = new ESLogMessage().field("message", "no trace");
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getSpanContext().isValid(), equalTo(false));
    }

    public void testEmptyTraceIdLeavesInvalidSpanContext() {
        ESLogMessage msg = new ESLogMessage().field("trace.id", "").field("message", "empty trace");
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getSpanContext().isValid(), equalTo(false));
    }

    // --- exception ---
    public void testThrowable() {
        ESLogMessage msg = new ESLogMessage().field("test", "test");
        appender.append(
            Log4jLogEvent.newBuilder()
                .setLoggerName("test.logger")
                .setLevel(Level.INFO)
                .setMessage(msg)
                .setThrown(new RuntimeException("test exception"))
                .build()
        );
        List<LogRecordData> records = exporter.getFinishedLogRecordItems();
        assertThat(records, hasSize(1));
        var record = records.getFirst();
        assertThat(record.getAttributes().get(AttributeKey.stringKey("test")), equalTo("test"));
        assertThat(record.getAttributes().get(AttributeKey.stringKey("exception.message")), equalTo("test exception"));
        assertThat(record.getAttributes().get(AttributeKey.stringKey("exception.type")), equalTo("java.lang.RuntimeException"));
    }
}
