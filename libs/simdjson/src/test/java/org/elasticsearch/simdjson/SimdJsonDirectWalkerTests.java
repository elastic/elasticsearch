/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.BitIndexes;
import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.simdjson.SimdJsonTestSupport.newParser;
import static org.elasticsearch.simdjson.SimdJsonTestSupport.walkJson;

public class SimdJsonDirectWalkerTests extends ESTestCase {

    public void testEmptyObject() {
        List<String> events = walkJson("{}");
        assertEquals(List.of(), events);
    }

    public void testSingleStringField() {
        List<String> events = walkJson("{\"a\":\"hello\"}");
        assertEquals(List.of("string(a=hello)"), events);
    }

    public void testSingleIntField() {
        List<String> events = walkJson("{\"n\":42}");
        assertEquals(List.of("long(n=42,fitsInt=true)"), events);
    }

    public void testSingleLongField() {
        List<String> events = walkJson("{\"n\":9999999999}");
        assertEquals(List.of("long(n=9999999999,fitsInt=false)"), events);
    }

    public void testSingleDoubleField() {
        List<String> events = walkJson("{\"d\":3.14}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(d=3.14,"));
    }

    public void testBooleanTrue() {
        List<String> events = walkJson("{\"b\":true}");
        assertEquals(List.of("bool(b=true)"), events);
    }

    public void testBooleanFalse() {
        List<String> events = walkJson("{\"b\":false}");
        assertEquals(List.of("bool(b=false)"), events);
    }

    public void testNullField() {
        List<String> events = walkJson("{\"n\":null}");
        assertEquals(List.of("null(n)"), events);
    }

    public void testMultipleFields() {
        List<String> events = walkJson("{\"a\":1,\"b\":\"x\",\"c\":true}");
        assertEquals(3, events.size());
        assertEquals("long(a=1,fitsInt=true)", events.get(0));
        assertEquals("string(b=x)", events.get(1));
        assertEquals("bool(c=true)", events.get(2));
    }

    public void testNestedObject() {
        List<String> events = walkJson("{\"o\":{\"inner\":1}}");
        assertEquals(List.of("startObject(o)", "long(inner=1,fitsInt=true)", "endObject()"), events);
    }

    public void testEmptyNestedObject() {
        List<String> events = walkJson("{\"o\":{}}");
        assertEquals(List.of("emptyObject(o)"), events);
    }

    public void testDeeplyNested() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < 10; i++) {
            sb.append("\"l").append(i).append("\":{");
        }
        sb.append("\"v\":1");
        for (int i = 0; i < 10; i++) {
            sb.append("}");
        }
        sb.append("}");

        List<String> events = walkJson(sb.toString());
        int startCount = 0;
        int endCount = 0;
        for (String event : events) {
            if (event.startsWith("startObject(")) startCount++;
            if (event.equals("endObject()")) endCount++;
        }
        assertEquals(10, startCount);
        assertEquals(10, endCount);
        assertTrue(events.contains("long(v=1,fitsInt=true)"));
    }

    public void testMaxDepthExceeded() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < 65; i++) {
            sb.append("\"l").append(i).append("\":{");
        }
        sb.append("\"v\":1");
        for (int i = 0; i < 65; i++) {
            sb.append("}");
        }
        sb.append("}");

        expectThrows(JsonParsingException.class, () -> walkJson(sb.toString()));
    }

    public void testSimpleIntArray() {
        List<String> events = walkJson("{\"a\":[1,2,3]}");
        assertEquals(
            List.of(
                "startArray(a)",
                "arrayElemLong(1,fitsInt=true)",
                "arrayElemLong(2,fitsInt=true)",
                "arrayElemLong(3,fitsInt=true)",
                "endArray()"
            ),
            events
        );
    }

    public void testMixedArray() {
        List<String> events = walkJson("{\"a\":[1,\"s\",true,null,3.14]}");
        assertEquals(7, events.size());
        assertEquals("startArray(a)", events.get(0));
        assertEquals("arrayElemLong(1,fitsInt=true)", events.get(1));
        assertEquals("arrayElemString(s)", events.get(2));
        assertEquals("arrayElemBoolean(true)", events.get(3));
        assertEquals("arrayElemNull()", events.get(4));
        assertTrue(events.get(5).startsWith("arrayElemDouble(3.14,"));
        assertEquals("endArray()", events.get(6));
    }

    public void testNestedArrayInArray() {
        List<String> events = walkJson("{\"a\":[[1,2],[3]]}");
        assertEquals(
            List.of(
                "startArray(a)",
                "arrayElemStartArray()",
                "arrayElemLong(1,fitsInt=true)",
                "arrayElemLong(2,fitsInt=true)",
                "arrayElemEndArray()",
                "arrayElemStartArray()",
                "arrayElemLong(3,fitsInt=true)",
                "arrayElemEndArray()",
                "endArray()"
            ),
            events
        );
    }

    public void testObjectInArray() {
        List<String> events = walkJson("{\"a\":[{\"k\":\"v\"}]}");
        assertEquals(List.of("startArray(a)", "arrayElemStartObject()", "string(k=v)", "arrayElemEndObject()", "endArray()"), events);
    }

    public void testEscapedStringField() {
        List<String> events = walkJson("{\"a\":\"hello\\nworld\"}");
        assertEquals(1, events.size());
        assertEquals("string(a=hello\nworld)", events.get(0));
    }

    public void testNegativeNumber() {
        List<String> events = walkJson("{\"n\":-42}");
        assertEquals(List.of("long(n=-42,fitsInt=true)"), events);
    }

    public void testNegativeDouble() {
        List<String> events = walkJson("{\"n\":-3.14}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(n=-3.14,"));
    }

    public void testScientificNotation() {
        List<String> events = walkJson("{\"n\":1.5e10}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(n=1.5E10,"));
    }

    public void testDocumentStartingWithArray() {
        expectThrows(JsonParsingException.class, () -> walkJson("[1,2]"));
    }

    public void testEmptyBitIndexesThrows() {
        byte[] buffer = new byte[0];
        BitIndexes bitIndexes = new BitIndexes(64);
        bitIndexes.reset();
        bitIndexes.setReadWindow(0, 0);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);
        SimdJsonTestSupport.RecordingHandler handler = new SimdJsonTestSupport.RecordingHandler();

        expectThrows(JsonParsingException.class, () -> walker.walkDocument(buffer, 0, bitIndexes, handler));
    }

    public void testFieldNameCaching() {
        String json = "{\"field\":1}";
        byte[] buffer = json.getBytes(StandardCharsets.UTF_8);
        int len = buffer.length;

        SimdJsonBatchParser parser = newParser(len);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);

        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);
        SimdJsonTestSupport.RecordingHandler handler1 = new SimdJsonTestSupport.RecordingHandler();
        walker.walkDocument(buffer, len, parser.bitIndexes(), handler1);

        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);
        SimdJsonTestSupport.RecordingHandler handler2 = new SimdJsonTestSupport.RecordingHandler();
        walker.walkDocument(buffer, len, parser.bitIndexes(), handler2);

        String name1 = handler1.events.get(0).substring("long(".length(), handler1.events.get(0).indexOf('='));
        String name2 = handler2.events.get(0).substring("long(".length(), handler2.events.get(0).indexOf('='));
        assertEquals("field", name1);
        assertEquals("field", name2);
    }

    public void testTinyDocument2Bytes() {
        List<String> events = walkJson("{}");
        assertEquals(List.of(), events);
    }

    public void testDocumentExactly16Bytes() {
        String json = "{\"a\":\"b\"       }";
        assertEquals(16, json.getBytes(StandardCharsets.UTF_8).length);
        List<String> events = walkJson(json);
        assertEquals(List.of("string(a=b)"), events);
    }

    public void testDocumentExactly32Bytes() {
        String json = "{\"abcd\":\"efghijklmnopqr\"       }";
        assertEquals(32, json.getBytes(StandardCharsets.UTF_8).length);
        List<String> events = walkJson(json);
        assertEquals(List.of("string(abcd=efghijklmnopqr)"), events);
    }

    public void testDocumentExactly64Bytes() {
        String json = "{\"abcdefghijk\":\"lmnopqrstuvwxyz0123456789ABCDEFGHIJKLMN\"       }";
        assertEquals(64, json.getBytes(StandardCharsets.UTF_8).length);
        List<String> events = walkJson(json);
        assertEquals(List.of("string(abcdefghijk=lmnopqrstuvwxyz0123456789ABCDEFGHIJKLMN)"), events);
    }

    public void testEmptyArray() {
        List<String> events = walkJson("{\"a\":[]}");
        assertEquals(List.of("startArray(a)", "endArray()"), events);
    }

    public void testEmptyString() {
        List<String> events = walkJson("{\"a\":\"\"}");
        assertEquals(List.of("string(a=)"), events);
    }

    public void testUnicodeStringValue() {
        List<String> events = walkJson("{\"a\":\"caf\u00e9\"}");
        assertEquals(List.of("string(a=caf\u00e9)"), events);
    }

    public void testObjectsInNestedArray() {
        List<String> events = walkJson("{\"a\":[{\"x\":1},{\"y\":2}]}");
        assertEquals(
            List.of(
                "startArray(a)",
                "arrayElemStartObject()",
                "long(x=1,fitsInt=true)",
                "arrayElemEndObject()",
                "arrayElemStartObject()",
                "long(y=2,fitsInt=true)",
                "arrayElemEndObject()",
                "endArray()"
            ),
            events
        );
    }

    // ---- Zero-padding tests ----
    // All code paths have scalar tail fallbacks, so no trailing padding is needed.

    public void testZeroPaddingForAllValueTypes() {
        String[] docs = {
            "{}",
            "{\"a\":1}",
            "{\"s\":\"hello\"}",
            "{\"b\":true}",
            "{\"b\":false}",
            "{\"n\":null}",
            "{\"d\":3.14}",
            "{\"sci\":1.5e10}",
            "{\"neg\":-42}",
            "{\"arr\":[1,\"s\",true,null,3.14]}",
            "{\"o\":{\"inner\":\"val\"}}",
            "{\"esc\":\"line1\\nline2\"}",
            "{\"q\":\"say \\\"hi\\\"\"}",
            "{\"u\":\"\\u0041\"}",
            "{\"long\":9999999999}",
            "{\"last_esc\":\"a\\nb\"}" };

        for (String json : docs) {
            walkWithExactPadding(json, 0);
        }
    }

    public void testFieldNameLengthSweepZeroPadding() {
        // Exercises resolveFieldName with field names of varying lengths (1..20) in an
        // exact-length buffer (zero padding). Short names near the buffer end force the
        // resolveFieldNameScalar tail path. Longer names may go through the 8-byte SIMD loop.
        for (int nameLen = 1; nameLen <= 20; nameLen++) {
            String name = "x".repeat(nameLen);
            String json = "{\"" + name + "\":1}";
            walkWithExactPadding(json, 0);
        }
    }

    public void testFieldNameScalarTailWithEscape() {
        // Field name with backslash near the buffer end — exercises resolveFieldNameScalar's
        // backslash detection which delegates to resolveEscapedFieldName.
        walkWithExactPadding("{\"a\\nb\":1}", 0);
        walkWithExactPadding("{\"x\\\"y\":1}", 0);
        walkWithExactPadding("{\"\\u0041\":1}", 0);
    }

    public void testFieldNameResolutionConsistencyAcrossBufferSizes() {
        // The same field name must resolve to the same String regardless of whether
        // resolveFieldName took the SIMD path or the scalar tail path.
        String name = "test_field";
        String json = "{\"" + name + "\":42}";

        SimdJsonTestSupport.RecordingHandler h1 = walkAndRecord(json, 0);
        SimdJsonTestSupport.RecordingHandler h2 = walkAndRecord(json, 64);

        assertEquals(h1.events, h2.events);
    }

    public void testMultipleFieldsLastNearBufferEnd() {
        // The last field's name is near the end of the buffer, exercising the scalar tail.
        walkWithExactPadding("{\"first\":1,\"x\":2}", 0);
        walkWithExactPadding("{\"first\":1,\"ab\":2}", 0);
        walkWithExactPadding("{\"first\":1,\"abcdefg\":2}", 0);
        walkWithExactPadding("{\"first\":1,\"abcdefgh\":2}", 0);
        walkWithExactPadding("{\"first\":1,\"abcdefghi\":2}", 0);
    }

    /**
     * Walks a JSON document using a buffer of exactly {@code docLen + paddingBytes}.
     */
    private void walkWithExactPadding(String json, int paddingBytes) {
        walkAndRecord(json, paddingBytes);
    }

    /**
     * Walks a JSON document and returns the recording handler for assertion.
     */
    private SimdJsonTestSupport.RecordingHandler walkAndRecord(String json, int paddingBytes) {
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        int len = jsonBytes.length;
        byte[] buffer = Arrays.copyOf(jsonBytes, len + paddingBytes);

        SimdJsonBatchParser parser = new SimdJsonBatchParser(buffer.length, SimdJsonTestSupport::scalarStage1);
        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);

        SimdJsonTestSupport.RecordingHandler handler = new SimdJsonTestSupport.RecordingHandler();
        walker.walkDocument(buffer, len, parser.bitIndexes(), handler);
        return handler;
    }
}
