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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;

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

    // ---- Escapes ----

    // \\n in a string value is decoded to a real newline.
    public void testEscapedStringField() {
        List<String> events = walkJson("{\"a\":\"hello\\nworld\"}");
        assertEquals(1, events.size());
        assertEquals("string(a=hello\nworld)", events.get(0));
    }

    /**
     * Unescaped values stay readable after later calls in the same document.
     */
    public void testRetainedUnescapedValuesDoNotAlias() {
        record Slice(byte[] buf, int off, int len) {}
        List<Slice> retained = new ArrayList<>();
        RecordingHandler handler = new RecordingHandler(false) {
            @Override
            public void stringField(String fieldName, byte[] buf, int off, int len) {
                retained.add(new Slice(buf, off, len));
            }

            @Override
            public void arrayElemString(byte[] buf, int off, int len) {
                retained.add(new Slice(buf, off, len));
            }
        };

        byte[] buffer = "{\"a\":[\"x\\ny\",\"p\\nq\"],\"b\":\"r\\ns\"}".getBytes(UTF_8);
        try (SimdJsonParser parser = newParser(buffer.length)) {
            FrozenFieldNameTable parent = new FrozenFieldNameTable();
            SimdJsonDirectWalker walker = new SimdJsonDirectWalker(parent.makeChild());
            parser.stage1(buffer, 0, buffer.length);
            parser.prepareDocumentWindow(0, buffer.length);
            walker.walkDocument(buffer, parser, handler);
        }

        List<String> decoded = retained.stream().map(s -> new String(s.buf(), s.off(), s.len(), UTF_8)).toList();
        assertEquals(List.of("x\ny", "p\nq", "r\ns"), decoded);
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

    // Field name prediction remembers which name followed which. It is only a hint, so a
    // walker that has learned name sequences from earlier documents must emit exactly what a fresh walker
    // emits for any document, however the names are reordered, shortened, extended, escaped or nested.
    public void testFieldNamePredictionNeverChangesEvents() {
        // Raw JSON key text, as it appears between the quotes. Several share prefixes or straddle the
        // 8 and 16 byte boundaries the predictor and hash paths branch on; others are written with escapes.
        String[] keys = {
            "a",
            "ab",
            "abc",
            "ab\\u0063",
            "a\\\"",
            "a\\\\",
            "abcdefg",
            "abcdefgh",
            "abcdefghi",
            "abcdefghijklmno",
            "abcdefghijklmnop",
            "abcdefghijklmnopq",
            "abcdefghijklmnopqrstuvwxyz0123456789",
            "",
            "caf\\u00e9",
            "caf\u00e9",
            "x.y.z" };
        List<String> docs = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            docs.add(randomObject(keys, 3));
        }
        List<List<String>> expected = new ArrayList<>();
        for (String doc : docs) {
            expected.add(walkJson(doc));
        }

        SimdJsonDirectWalker shared = newWalker();
        for (int round = 0; round < 6; round++) {
            for (int i = 0; i < docs.size(); i++) {
                // mostly in order so name sequences get learned and hit, sometimes shuffled so predictions miss
                int which = rarely() ? randomIntBetween(0, docs.size() - 1) : i;
                int padding = randomFrom(0, 0, 1, 7, 8, 9, 64);
                assertEquals(
                    "round " + round + " doc " + which + ": " + docs.get(which),
                    expected.get(which),
                    walkWith(shared, docs.get(which), padding)
                );
            }
            if (rarely()) {
                shared.releaseNames();
            }
        }
    }

    // A shape change in the middle of a stream (what a bulk spanning several indices looks like on one
    // thread) must keep producing correct names after every switch.
    public void testFieldNamePredictionSurvivesInterleavedShapes() {
        String shapeA = "{\"alpha\":1,\"beta\":2,\"gamma\":{\"delta\":3,\"epsilon\":4}}";
        String shapeB = "{\"beta\":1,\"alpha\":2,\"gamma\":{\"epsilon\":3,\"delta\":4},\"extra\":5}";
        List<String> expectedA = walkJson(shapeA);
        List<String> expectedB = walkJson(shapeB);
        SimdJsonDirectWalker shared = newWalker();
        for (int i = 0; i < 200; i++) {
            boolean a = randomBoolean();
            assertEquals(a ? expectedA : expectedB, walkWith(shared, a ? shapeA : shapeB, randomFrom(0, 8)));
        }
    }

    // A key with an escape must never be remembered as its raw bytes: after a document whose key is
    // a, backslash, backslash (the name a\), a document whose key is a, backslash, quote (the name a")
    // starts with the same bytes up to a quote.
    public void testEscapedKeysAreNeverPredicted() {
        String[] docs = { "{\"a\\\\\":1}", "{\"a\\\"\":1}", "{\"a\\\\\":1}", "{\"\\\"\":1}", "{\"\\\\\":1}", "{\"\\\"\":1}" };
        SimdJsonDirectWalker shared = newWalker();
        for (int round = 0; round < 3; round++) {
            for (String doc : docs) {
                assertEquals(doc, walkJson(doc), walkWith(shared, doc, randomFrom(0, 8)));
            }
        }
    }

    private static String randomObject(String[] keys, int depth) {
        List<String> shuffled = new ArrayList<>(List.of(keys));
        Collections.shuffle(shuffled, random());
        int count = randomIntBetween(0, 6);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(shuffled.get(i)).append("\":");
            if (depth > 0 && rarely()) {
                sb.append(randomObject(keys, depth - 1));
            } else {
                sb.append(randomIntBetween(0, 1000));
            }
        }
        return sb.append('}').toString();
    }

    /** Walks {@code json} with the given (reused) walker, in a buffer with {@code padding} spare bytes after it. */
    private List<String> walkWith(SimdJsonDirectWalker walker, String json, int padding) {
        byte[] jsonBytes = json.getBytes(UTF_8);
        int len = jsonBytes.length;
        byte[] buffer = Arrays.copyOf(jsonBytes, len + padding);
        try (SimdJsonParser parser = newParser(buffer.length)) {
            parser.stage1(buffer, 0, len);
            parser.prepareDocumentWindow(0, len);
            RecordingHandler handler = new RecordingHandler();
            walker.walkDocument(buffer, parser, handler);
            return handler.events;
        }
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
            for (int padding : new int[] { 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65 }) {
                assertEquals("padding must not change events for: " + json, tight, walkAndRecord(json, padding).events);
            }
        }
    }

    public void testNonZeroStartOffsetDoesNotChangeEvents() {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            List<String> expected = walkJson(json);
            for (int offset : new int[] { 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65 }) {
                assertEquals("offset=" + offset + " walk for: " + json, expected, walkAndRecordAtOffset(json, offset).events);
            }
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
            expectThrows(JsonParsingException.class, () -> walker.walkDocument(buffer, parser, handler));
        } catch (JsonParsingException e) {
            // stage 1 rejection is acceptable
        }
    }

    // ---- Content after the document ----

    /** The first documents the tests below put other content after, with that content appended. */
    private record Followed(String json, String first) {}

    private static List<Followed> documentsFollowedByContent() {
        return List.of(
            new Followed("{\"a\":1}{\"b\":2}", "{\"a\":1}"),
            new Followed("{\"a\":1} x", "{\"a\":1}"),
            new Followed("{\"a\":1}x", "{\"a\":1}"),
            new Followed("{\"a\":1}}", "{\"a\":1}"),
            new Followed("{\"a\":1}]", "{\"a\":1}"),
            new Followed("{\"a\":1},", "{\"a\":1}"),
            new Followed("{\"a\":1}:", "{\"a\":1}"),
            new Followed("{\"a\":1}\"s\"", "{\"a\":1}"),
            new Followed("{\"a\":1} 2", "{\"a\":1}"),
            new Followed("{\"a\":1} null", "{\"a\":1}"),
            new Followed("{\"a\":[1]}[2]", "{\"a\":[1]}"),
            new Followed("{\"a\":{\"b\":1}}{", "{\"a\":{\"b\":1}}"),
            new Followed("{}{}", "{}"),
            new Followed("{} x", "{}"),
            new Followed("{}}", "{}"),
            new Followed("{}[]", "{}")
        );
    }

    /** Prepared parser for walking {@code [offset, offset + len)} of {@code buffer}, indexed over the whole buffer. */
    private SimdJsonParser parserForWindow(byte[] buffer, int offset, int len) {
        SimdJsonParser parser = newParser(buffer.length);
        parser.stage1(buffer, 0, buffer.length);
        parser.prepareDocumentWindow(offset, len);
        return parser;
    }

    private static SimdJsonDirectWalker newWalker() {
        return new SimdJsonDirectWalker(new FrozenFieldNameTable().makeChild());
    }

    private static String randomWhitespace() {
        StringBuilder sb = new StringBuilder();
        for (int i = between(1, 8); i > 0; i--) {
            sb.append(randomFrom(' ', '\n', '\r', '\t'));
        }
        return sb.toString();
    }

    // After a valid document the walker has consumed every structural index in the document's window.
    // Whitespace has no structural indexes, so the same holds with any whitespace before and after it.
    public void testValidDocumentConsumesAllStructuralIndexes() {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            List<String> variants = new ArrayList<>(List.of(json));
            for (int i = 0; i < 3; i++) {
                variants.add(randomWhitespace() + json + randomWhitespace());
            }
            for (String variant : variants) {
                byte[] buffer = variant.getBytes(UTF_8);
                try (SimdJsonParser parser = parserForWindow(buffer, 0, buffer.length)) {
                    newWalker().walkDocument(buffer, parser, new RecordingHandler());
                    assertTrue("walker left structural indexes unread for: " + variant, parser.bitIndexes().isEnd());
                }
            }
        }
    }

    // Structural content after the top-level object closes is rejected, for the empty object and a
    // non-empty one, whatever kind of token follows. The first object's events have been emitted by then.
    public void testContentAfterDocumentRejected() {
        for (Followed f : documentsFollowedByContent()) {
            byte[] buffer = f.json().getBytes(UTF_8);
            try (SimdJsonParser parser = parserForWindow(buffer, 0, buffer.length)) {
                RecordingHandler handler = new RecordingHandler();
                JsonParsingException e = expectThrows(JsonParsingException.class, () -> newWalker().walkDocument(buffer, parser, handler));
                assertThat(f.json(), e.getMessage(), containsString("Unexpected content after end of document"));
                assertEquals(f.json(), walkJson(f.first()), handler.events);
                assertFalse(f.json(), parser.bitIndexes().isEnd());
            }
        }
    }

    // Appending anything structural to any valid document must be rejected, not just to hand-picked ones.
    public void testContentAppendedToEveryValidDocumentRejected() {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            for (String suffix : List.of("{}", "}", "]", ",", "x", "1", "\"s\"", " {\"a\":1}")) {
                byte[] buffer = (json + suffix).getBytes(UTF_8);
                try (SimdJsonParser parser = parserForWindow(buffer, 0, buffer.length)) {
                    expectThrows(
                        JsonParsingException.class,
                        "[" + json + suffix + "] must be rejected",
                        () -> newWalker().walkDocument(buffer, parser, new RecordingHandler())
                    );
                }
            }
        }
    }

    // What follows is judged against the window passed to the walker, not the whole buffer. Two concatenated
    // documents are each fine through their own window, and rejected when one window spans both.
    public void testContentIsJudgedWithinTheWindow() {
        byte[] buffer = "{\"a\":1}{\"b\":2}".getBytes(UTF_8);
        int half = buffer.length / 2;

        try (SimdJsonParser first = parserForWindow(buffer, 0, half); SimdJsonParser second = parserForWindow(buffer, half, half)) {
            newWalker().walkDocument(buffer, first, new RecordingHandler());
            newWalker().walkDocument(buffer, second, new RecordingHandler());
        }

        try (SimdJsonParser both = parserForWindow(buffer, 0, buffer.length)) {
            expectThrows(JsonParsingException.class, () -> newWalker().walkDocument(buffer, both, new RecordingHandler()));
        }
    }

}
