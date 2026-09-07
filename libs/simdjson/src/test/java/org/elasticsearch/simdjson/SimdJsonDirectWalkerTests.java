/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;
import org.elasticsearch.simdjson.internal.parsers.BitIndexes;

import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

// Unit tests for SimdJsonDirectWalker event emission (simdjson-only, no Jackson comparison).
public class SimdJsonDirectWalkerTests extends SimdJsonTestCase {

    // ---- Scalars and root object ----

    // Root {} emits no handler events (emptyObject is only for nested {}).
    public void testEmptyObject() {
        List<String> events = walkJson("{}");
        assertEquals(List.of(), events);
    }

    // Each scalar JSON type maps to one handler event string.
    public void testSingleStringField() {
        List<String> events = walkJson("{\"a\":\"hello\"}");
        assertEquals(List.of("string(a=hello)"), events);
    }

    public void testSingleIntField() {
        List<String> events = walkJson("{\"n\":42}");
        assertEquals(List.of("long(n=42,fitsInt=true)"), events);
    }

    // Values beyond int range still emit long with fitsInt=false.
    public void testSingleLongField() {
        List<String> events = walkJson("{\"n\":9999999999}");
        assertEquals(List.of("long(n=9999999999,fitsInt=false)"), events);
    }

    // Decimal point forces double classification.
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

    // Field order is preserved; each type maps to one handler event.
    public void testMultipleFields() {
        List<String> events = walkJson("{\"a\":1,\"b\":\"x\",\"c\":true}");
        assertEquals(3, events.size());
        assertEquals("long(a=1,fitsInt=true)", events.get(0));
        assertEquals("string(b=x)", events.get(1));
        assertEquals("bool(c=true)", events.get(2));
    }

    // ---- Nesting and depth limits ----

    public void testNestedObject() {
        List<String> events = walkJson("{\"o\":{\"inner\":1}}");
        assertEquals(List.of("startObject(o)", "long(inner=1,fitsInt=true)", "endObject()"), events);
    }

    // Walker uses emptyObject() for {} (Jackson comparison mode normalizes to start/end).
    public void testEmptyNestedObject() {
        List<String> events = walkJson("{\"o\":{}}");
        assertEquals(List.of("emptyObject(o)"), events);
    }

    // 10 levels of nesting — startObject/endObject pairs must balance.
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

    // MAX_DEPTH is 64; 65 nested objects must fail.
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

    // ---- Arrays ----

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

    // Mixed scalar types in one array.
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

    // Array of arrays — nested start/end array events.
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

    // Object as array element uses arrayElemStartObject/EndObject wrappers.
    public void testObjectInArray() {
        List<String> events = walkJson("{\"a\":[{\"k\":\"v\"}]}");
        assertEquals(List.of("startArray(a)", "arrayElemStartObject()", "string(k=v)", "arrayElemEndObject()", "endArray()"), events);
    }

    // ---- Escapes, signs, and scientific notation ----

    // \\n in a string value is decoded to a real newline.
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

    // Exponent form produces double event (not long).
    public void testScientificNotation() {
        List<String> events = walkJson("{\"n\":1.5e10}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(n=1.5E10,"));
    }

    // Root must be an object; top-level arrays are rejected.
    public void testDocumentStartingWithArray() {
        expectThrows(JsonParsingException.class, () -> walkJson("[1,2]"));
    }

    // ---- Parser/walker integration edge cases ----

    // Empty BitIndexes (no structurals) must fail before value parsing.
    public void testEmptyBitIndexesThrows() {
        byte[] buffer = new byte[0];
        BitIndexes bitIndexes = new BitIndexes(64);
        bitIndexes.reset();
        bitIndexes.setReadWindow(0, 0);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);
        RecordingHandler handler = new RecordingHandler();

        expectThrows(JsonParsingException.class, () -> walker.walkDocument(buffer, bitIndexes, handler));
    }

    // Repeated walks must resolve the same field name String from FrozenFieldNameTable.
    public void testFieldNameCaching() {
        String json = "{\"field\":1}";
        byte[] buffer = json.getBytes(UTF_8);
        int len = buffer.length;

        SimdJsonParser parser = newParser(len);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);

        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);
        RecordingHandler handler1 = new RecordingHandler();
        walker.walkDocument(buffer, parser.bitIndexes(), handler1);

        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);
        RecordingHandler handler2 = new RecordingHandler();
        walker.walkDocument(buffer, parser.bitIndexes(), handler2);

        String name1 = handler1.events.get(0).substring("long(".length(), handler1.events.get(0).indexOf('='));
        String name2 = handler2.events.get(0).substring("long(".length(), handler2.events.get(0).indexOf('='));
        assertEquals("field", name1);
        assertEquals("field", name2);
    }

    // ---- Exact buffer length and padding invariance ----

    public void testWalksDocumentsWithExactBufferLength() {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            List<String> expected = walkJson(json);
            assertEquals("exact buffer length walk for: " + json, expected, walkAndRecord(json, 0).events);
        }
    }

    public void testTrailingBufferPaddingDoesNotChangeEvents() {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            List<String> tight = walkAndRecord(json, 0).events;
            List<String> padded = walkAndRecord(json, 64).events;
            assertEquals("padding must not change events for: " + json, tight, padded);
        }
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

    // Truncated JSON must not complete a successful walk.
    public void testTruncatedJsonMustNotWalkSuccessfully() {
        byte[] buffer = "{\"a\":1".getBytes(UTF_8);
        try (SimdJsonParser parser = newParser(buffer.length)) {
            parser.stage1(buffer, buffer.length);
            parser.prepareDocumentWindow(0, buffer.length);
            FrozenFieldNameTable parent = new FrozenFieldNameTable();
            SimdJsonDirectWalker walker = new SimdJsonDirectWalker(parent.makeChild());
            RecordingHandler handler = new RecordingHandler();
            expectThrows(Exception.class, () -> walker.walkDocument(buffer, parser, handler));
        } catch (JsonParsingException e) {
            // stage 1 rejection is acceptable
        }
    }

    private RecordingHandler walkAndRecord(String json, int paddingBytes) {
        byte[] jsonBytes = json.getBytes(UTF_8);
        int len = jsonBytes.length;
        byte[] buffer = Arrays.copyOf(jsonBytes, len + paddingBytes);

        SimdJsonParser parser = newParser(buffer.length);
        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);

        RecordingHandler handler = new RecordingHandler();
        walker.walkDocument(buffer, parser.bitIndexes(), handler);
        return handler;
    }
}
