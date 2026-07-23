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
import io.opentelemetry.api.logs.Severity;
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

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class ElasticsearchOtelAppenderTests extends ESTestCase {

    private InMemoryLogRecordExporter exporter;
    private SdkLoggerProvider provider;
    private OpenTelemetrySdk sdk;
    private ElasticsearchOtelAppender appender;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        exporter = InMemoryLogRecordExporter.create();
        provider = SdkLoggerProvider.builder().addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter)).build();
        sdk = OpenTelemetrySdk.builder().setLoggerProvider(provider).build();
        appender = new ElasticsearchOtelAppender("test", sdk);
        appender.start();
    }

    @Override
    public void tearDown() throws Exception {
        appender.stop();
        provider.close();
        super.tearDown();
    }

    // --- helper methods ---

    private LogRecordData emitMapMessage(ESLogMessage msg) {
        appender.append(Log4jLogEvent.newBuilder().setLoggerName("test.logger").setLevel(Level.INFO).setMessage(msg).build());
        List<LogRecordData> records = exporter.getFinishedLogRecordItems();
        assertThat(records, hasSize(1));
        return records.getFirst();
    }

    private LogRecordData emitPlainMessage(String text) {
        appender.append(
            Log4jLogEvent.newBuilder().setLoggerName("test.logger").setLevel(Level.INFO).setMessage(new SimpleMessage(text)).build()
        );
        List<LogRecordData> records = exporter.getFinishedLogRecordItems();
        assertThat(records, hasSize(1));
        return records.getFirst();
    }

    // --- key naming ---

    public void testNoPrefixOnKeys() {
        ESLogMessage msg = new ESLogMessage().field("mykey", "myval");
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.stringKey("mykey")), equalTo("myval"));
        assertThat(record.getAttributes().get(AttributeKey.stringKey("log4j.map_message.mykey")), nullValue());
    }

    // --- primitive type dispatch ---

    public void testStringAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", "hello");
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.stringKey("k")), equalTo("hello"));
    }

    public void testLongAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", 42L);
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.longKey("k")), equalTo(42L));
    }

    public void testIntegerWidenedToLong() {
        ESLogMessage msg = new ESLogMessage().field("k", 7);
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.longKey("k")), equalTo(7L));
    }

    public void testDoubleAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", 3.14);
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.doubleKey("k")), equalTo(3.14));
    }

    public void testFloatWidenedToDouble() {
        ESLogMessage msg = new ESLogMessage().field("k", 1.5f);
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.doubleKey("k")), equalTo((double) 1.5f));
    }

    public void testBooleanAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", true);
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.booleanKey("k")), equalTo(true));
    }

    public void testUnknownTypeToString() {
        ESLogMessage msg = new ESLogMessage().field("k", new Object() {
            @Override
            public String toString() {
                return "custom";
            }
        });
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.stringKey("k")), equalTo("custom"));
    }

    // --- array type dispatch ---

    public void testStringListAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", List.of("a", "b", "c"));
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.stringArrayKey("k")), equalTo(List.of("a", "b", "c")));
    }

    public void testLongListAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", List.of(1L, 2L, 3L));
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.longArrayKey("k")), equalTo(List.of(1L, 2L, 3L)));
    }

    public void testIntegerListWidenedToLong() {
        ESLogMessage msg = new ESLogMessage().field("k", List.of(1, 2, 3));
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.longArrayKey("k")), equalTo(List.of(1L, 2L, 3L)));
    }

    public void testBooleanListAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", List.of(true, false));
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.booleanArrayKey("k")), equalTo(List.of(true, false)));
    }

    // --- primitive and object array type dispatch ---

    public void testStringArrayAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", new String[] { "a", "b" });
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.stringArrayKey("k")), equalTo(List.of("a", "b")));
    }

    public void testLongPrimitiveArrayAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", new long[] { 1L, 2L, 3L });
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.longArrayKey("k")), equalTo(List.of(1L, 2L, 3L)));
    }

    public void testIntPrimitiveArrayWidenedToLong() {
        ESLogMessage msg = new ESLogMessage().field("k", new int[] { 4, 5 });
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.longArrayKey("k")), equalTo(List.of(4L, 5L)));
    }

    public void testDoublePrimitiveArrayAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", new double[] { 1.0, 2.5 });
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.doubleArrayKey("k")), equalTo(List.of(1.0, 2.5)));
    }

    public void testFloatPrimitiveArrayWidenedToDouble() {
        ESLogMessage msg = new ESLogMessage().field("k", new float[] { 1.0f, 2.0f });
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.doubleArrayKey("k")), equalTo(List.of((double) 1.0f, (double) 2.0f)));
    }

    public void testBooleanPrimitiveArrayAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", new boolean[] { true, false, true });
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.booleanArrayKey("k")), equalTo(List.of(true, false, true)));
    }

    public void testLongBoxedArrayAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", new Long[] { 10L, 20L });
        LogRecordData record = emitMapMessage(msg);
        assertThat(record.getAttributes().get(AttributeKey.longArrayKey("k")), equalTo(List.of(10L, 20L)));
    }

    // --- map type dispatch ---

    public void testMapAttribute() {
        ESLogMessage msg = new ESLogMessage().field("k", Map.of("x", "hello", "n", 42L));
        LogRecordData record = emitMapMessage(msg);
        Value<?> v = record.getAttributes().get(AttributeKey.valueKey("k"));
        assertNotNull(v);
        // Value.of(Map) returns Value<List<KeyValue>>
        @SuppressWarnings("unchecked")
        List<KeyValue> kvList = (List<KeyValue>) v.getValue();
        Map<String, Value<?>> map = new java.util.HashMap<>();
        kvList.forEach(kv -> map.put(kv.getKey(), kv.getValue()));
        assertThat(map.get("x"), equalTo(Value.of("hello")));
        assertThat(map.get("n"), equalTo(Value.of(42L)));
    }

    public void testNestedMapAttribute() {
        Map<String, Object> inner = Map.of("a", 1L);
        ESLogMessage msg = new ESLogMessage().field("k", Map.of("nested", inner));
        LogRecordData record = emitMapMessage(msg);
        Value<?> v = record.getAttributes().get(AttributeKey.valueKey("k"));
        assertNotNull(v);
        @SuppressWarnings("unchecked")
        List<KeyValue> outerKvList = (List<KeyValue>) v.getValue();
        Map<String, Value<?>> outer = new java.util.HashMap<>();
        outerKvList.forEach(kv -> outer.put(kv.getKey(), kv.getValue()));
        Value<?> nestedVal = outer.get("nested");
        assertNotNull(nestedVal);
        @SuppressWarnings("unchecked")
        List<KeyValue> innerKvList = (List<KeyValue>) nestedVal.getValue();
        Map<String, Value<?>> inner2 = new java.util.HashMap<>();
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
        assertThat(record.getAttributes().get(AttributeKey.stringArrayKey("k")), nullValue());
    }

    // --- severity mapping ---

    public void testSeverityMapping() {
        for (var pair : List.of(
            Map.entry(Level.TRACE, Severity.TRACE),
            Map.entry(Level.DEBUG, Severity.DEBUG),
            Map.entry(Level.INFO, Severity.INFO),
            Map.entry(Level.WARN, Severity.WARN),
            Map.entry(Level.ERROR, Severity.ERROR),
            Map.entry(Level.FATAL, Severity.FATAL)
        )) {
            exporter.reset();
            appender.append(
                Log4jLogEvent.newBuilder().setLoggerName("test.logger").setLevel(pair.getKey()).setMessage(new SimpleMessage("m")).build()
            );
            LogRecordData record = exporter.getFinishedLogRecordItems().getFirst();
            assertThat("severity for " + pair.getKey(), record.getSeverity(), equalTo(pair.getValue()));
        }
    }

    // --- replay ---

    public void testReplayOnSetOpenTelemetry() {
        // Appender with no OTel instance initially
        ElasticsearchOtelAppender replayAppender = new ElasticsearchOtelAppender("replay", null);
        replayAppender.start();
        try {
            // Emit before OTel is set — goes into the replay queue
            replayAppender.append(
                Log4jLogEvent.newBuilder()
                    .setLoggerName("replay.logger")
                    .setLevel(Level.INFO)
                    .setMessage(new SimpleMessage("queued"))
                    .build()
            );
            assertThat("no records yet", exporter.getFinishedLogRecordItems(), hasSize(0));

            // Now wire up OTel — queued event must be replayed
            replayAppender.setOpenTelemetry(sdk);
            assertThat("replayed record", exporter.getFinishedLogRecordItems(), hasSize(1));
            assertThat(exporter.getFinishedLogRecordItems().getFirst().getBodyValue().asString(), equalTo("queued"));
        } finally {
            replayAppender.stop();
        }
    }
}
