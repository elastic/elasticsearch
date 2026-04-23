/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class JsonXContentParserByteOffsetTests extends ESTestCase {

    public void testTokenLocationByteOffsets() throws IOException {
        byte[] json = "{\"key\":\"val\"}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            // START_OBJECT at byte 0
            assertEquals(Token.START_OBJECT, parser.nextToken());
            XContentLocation loc = parser.getTokenLocation();
            assertEquals(0L, loc.byteOffset());

            // FIELD_NAME "key" at byte 1
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            loc = parser.getTokenLocation();
            assertEquals(1L, loc.byteOffset());

            // VALUE_STRING "val" at byte 7
            assertEquals(Token.VALUE_STRING, parser.nextToken());
            loc = parser.getTokenLocation();
            assertEquals(7L, loc.byteOffset());

            // END_OBJECT at byte 12
            assertEquals(Token.END_OBJECT, parser.nextToken());
            loc = parser.getTokenLocation();
            assertEquals(12L, loc.byteOffset());
        }
    }

    public void testGetCurrentLocationAdvancesPastToken() throws IOException {
        byte[] json = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            XContentLocation current = parser.getCurrentLocation();
            // After consuming START_OBJECT '{', current location should be past it
            assertTrue(current.byteOffset() > 0);
        }
    }

    public void testByteSlicingWithSkipChildren() throws IOException {
        // The inner object {"b":2} starts at byte 5 and ends at byte 11 (inclusive)
        byte[] json = "{\"a\":{\"b\":2},\"c\":3}".getBytes(StandardCharsets.UTF_8);
        // 0123456789012345678
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken()); // outer {
            assertEquals(Token.FIELD_NAME, parser.nextToken()); // "a"
            assertEquals(Token.START_OBJECT, parser.nextToken()); // inner {

            long startOffset = parser.getTokenLocation().byteOffset();
            assertEquals(5L, startOffset);

            parser.skipChildren();

            long endOffset = parser.getCurrentLocation().byteOffset();
            // After skipChildren() from START_OBJECT, getCurrentLocation should point past '}'
            assertTrue(endOffset > startOffset);

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            String sliced = new String(slice, StandardCharsets.UTF_8);
            assertEquals("{\"b\":2}", sliced);
        }
    }

    public void testByteSlicingArray() throws IOException {
        byte[] json = "{\"a\":[1,2,3]}".getBytes(StandardCharsets.UTF_8);
        // 0123456789012
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.START_ARRAY, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            assertEquals(5L, startOffset);

            parser.skipChildren();

            long endOffset = parser.getCurrentLocation().byteOffset();
            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            String sliced = new String(slice, StandardCharsets.UTF_8);
            assertEquals("[1,2,3]", sliced);
        }
    }

    public void testNestedObjectByteSlicing() throws IOException {
        byte[] json = "{\"a\":{\"b\":{\"c\":true}}}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken()); // outer
            assertEquals(Token.FIELD_NAME, parser.nextToken()); // "a"
            assertEquals(Token.START_OBJECT, parser.nextToken()); // {"b":{"c":true}}

            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            String sliced = new String(slice, StandardCharsets.UTF_8);
            assertEquals("{\"b\":{\"c\":true}}", sliced);
        }
    }

    public void testGetCurrentLocationIsNotNull() throws IOException {
        byte[] json = "{\"x\":42}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertNotNull(parser.getCurrentLocation());
            assertTrue(parser.getCurrentLocation().byteOffset() >= 0);
        }
    }

    public void testScalarValueSlicing() throws IOException {
        byte[] json = "{\"n\":12345}".getBytes(StandardCharsets.UTF_8);
        // 01234567890
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.VALUE_NUMBER, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            long endOffset = parser.getCurrentLocation().byteOffset();

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            String sliced = new String(slice, StandardCharsets.UTF_8);
            assertEquals("12345", sliced);
        }
    }

    public void testMultiByteUtf8ObjectSlicing() throws IOException {
        // Multi-byte UTF-8: \u00e9 = 2 bytes (c3 a9), \u2603 = 3 bytes (e2 98 83)
        // Verify byte offsets are counted in bytes, not characters
        byte[] json = "{\"k\":{\"x\":\"\u00e9\u2603\"}}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.START_OBJECT, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            String sliced = new String(slice, StandardCharsets.UTF_8);
            assertEquals("{\"x\":\"\u00e9\u2603\"}", sliced);
        }
    }

    public void testSurrogatePairObjectSlicing() throws IOException {
        // U+1F389 (🎉) = 4 bytes in UTF-8 (f0 9f 8e 89), 2 chars in Java (surrogate pair)
        // This is the case most likely to expose byte-vs-character offset confusion
        byte[] json = "{\"k\":{\"x\":\"\uD83C\uDF89\"}}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.START_OBJECT, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            String sliced = new String(slice, StandardCharsets.UTF_8);
            assertEquals("{\"x\":\"\uD83C\uDF89\"}", sliced);
        }
    }

    public void testEmptyObjectSlicing() throws IOException {
        byte[] json = "{\"a\":{}}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.START_OBJECT, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            assertEquals("{}", new String(slice, StandardCharsets.UTF_8));
        }
    }

    public void testEmptyArraySlicing() throws IOException {
        byte[] json = "{\"a\":[]}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.START_ARRAY, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            assertEquals("[]", new String(slice, StandardCharsets.UTF_8));
        }
    }

    public void testBooleanValueSlicing() throws IOException {
        byte[] json = "{\"b\":false}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.VALUE_BOOLEAN, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            long endOffset = parser.getCurrentLocation().byteOffset();

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            assertEquals("false", new String(slice, StandardCharsets.UTF_8));
        }
    }

    public void testNullValueSlicing() throws IOException {
        byte[] json = "{\"n\":null}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.VALUE_NULL, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            long endOffset = parser.getCurrentLocation().byteOffset();

            byte[] slice = Arrays.copyOfRange(json, (int) startOffset, (int) endOffset);
            assertEquals("null", new String(slice, StandardCharsets.UTF_8));
        }
    }

    public void testMultiLineJsonByteOffsets() throws IOException {
        String multiLine = "{\n  \"a\": 1,\n  \"b\": 2\n}";
        byte[] json = multiLine.getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(0L, parser.getTokenLocation().byteOffset());

            assertEquals(Token.FIELD_NAME, parser.nextToken()); // "a"
            long aFieldOffset = parser.getTokenLocation().byteOffset();
            assertEquals(4L, aFieldOffset); // after "{\n "

            assertEquals(Token.VALUE_NUMBER, parser.nextToken()); // 1
            assertEquals(Token.FIELD_NAME, parser.nextToken()); // "b"
            long bFieldOffset = parser.getTokenLocation().byteOffset();
            // "b" starts on line 3: "{\n \"a\": 1,\n " = 14 bytes
            assertEquals(14L, bFieldOffset);
        }
    }

    public void testGetCurrentLocationAfterEveryTokenType() throws IOException {
        byte[] json = "{\"s\":\"v\",\"n\":42,\"b\":true,\"z\":null,\"a\":[1],\"o\":{}}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            Token token;
            while ((token = parser.nextToken()) != null) {
                XContentLocation current = parser.getCurrentLocation();
                assertNotNull("getCurrentLocation() should not be null after " + token, current);
                assertTrue(
                    "byte offset should be non-negative after " + token + " but was " + current.byteOffset(),
                    current.byteOffset() >= 0
                );
            }
        }
    }

    // ===== Streaming JSON navigation + byte-offset slicing =====
    // These tests validate end-to-end scenarios: navigate to a value by key or path using standard
    // streaming token walking, then extract it via byte-offset slicing instead of copyCurrentStructure().

    /**
     * Navigate to an object field by key, skipping non-matching fields, then byte-slice the value.
     */
    public void testExtractObjectByKeyByteSlice() throws IOException {
        byte[] json = "{\"skip\":1,\"target\":{\"nested\":true},\"after\":2}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT (root)

            // Walk fields to find "target"
            Token token;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken(); // advance to value
                    if (fieldName.equals("target")) {
                        // Found it — byte-slice the object value
                        assertEquals(Token.START_OBJECT, parser.currentToken());
                        long startOffset = parser.getTokenLocation().byteOffset();
                        parser.skipChildren();
                        long endOffset = parser.getCurrentLocation().byteOffset();
                        String sliced = new String(Arrays.copyOfRange(json, (int) startOffset, (int) endOffset), StandardCharsets.UTF_8);
                        assertEquals("{\"nested\":true}", sliced);
                        return;
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            fail("field 'target' not found");
        }
    }

    /**
     * Navigate to an array field by key, then byte-slice the entire array.
     */
    public void testExtractArrayByKeyByteSlice() throws IOException {
        byte[] json = "{\"x\":\"skip\",\"items\":[1,{\"a\":2},3]}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT (root)

            Token token;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    if (fieldName.equals("items")) {
                        assertEquals(Token.START_ARRAY, parser.currentToken());
                        long startOffset = parser.getTokenLocation().byteOffset();
                        parser.skipChildren();
                        long endOffset = parser.getCurrentLocation().byteOffset();
                        String sliced = new String(Arrays.copyOfRange(json, (int) startOffset, (int) endOffset), StandardCharsets.UTF_8);
                        assertEquals("[1,{\"a\":2},3]", sliced);
                        return;
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            fail("field 'items' not found");
        }
    }

    /**
     * Navigate a nested object path (a → b → c), skipping non-matching fields at each level, then byte-slice.
     */
    public void testExtractNestedPathByteSlice() throws IOException {
        byte[] json = "{\"a\":{\"x\":0,\"b\":{\"c\":{\"deep\":42}}}}".getBytes(StandardCharsets.UTF_8);
        String[] path = { "a", "b", "c" };
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT (root)

            // Navigate path segments: at each level, walk fields to find the matching key
            for (String segment : path) {
                assertEquals(Token.START_OBJECT, parser.currentToken());
                boolean found = false;
                Token token;
                while ((token = parser.nextToken()) != Token.END_OBJECT) {
                    if (token == Token.FIELD_NAME) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        if (fieldName.equals(segment)) {
                            found = true;
                            break;
                        } else {
                            parser.skipChildren();
                        }
                    }
                }
                assertTrue("segment '" + segment + "' not found", found);
            }

            // At the target: extract via byte slice
            assertEquals(Token.START_OBJECT, parser.currentToken());
            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();
            String sliced = new String(Arrays.copyOfRange(json, (int) startOffset, (int) endOffset), StandardCharsets.UTF_8);
            assertEquals("{\"deep\":42}", sliced);
        }
    }

    /**
     * Walk an array to a specific index, skipping preceding elements, then byte-slice the target element.
     */
    public void testExtractArrayIndexByteSlice() throws IOException {
        byte[] json = "{\"arr\":[\"skip\",{\"target\":true},[3,4]]}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME "arr"
            parser.nextToken(); // START_ARRAY

            // Walk array to index 1
            int targetIndex = 1;
            int currentIndex = 0;
            while (parser.nextToken() != Token.END_ARRAY) {
                if (currentIndex == targetIndex) {
                    assertEquals(Token.START_OBJECT, parser.currentToken());
                    long startOffset = parser.getTokenLocation().byteOffset();
                    parser.skipChildren();
                    long endOffset = parser.getCurrentLocation().byteOffset();
                    String sliced = new String(Arrays.copyOfRange(json, (int) startOffset, (int) endOffset), StandardCharsets.UTF_8);
                    assertEquals("{\"target\":true}", sliced);
                    return;
                }
                parser.skipChildren();
                currentIndex++;
            }
            fail("index " + targetIndex + " not found");
        }
    }

    /**
     * Navigate to scalar values by key and extract them using parser.text() / parser.booleanValue().
     * Numbers, booleans, and nulls can also be byte-sliced; strings cannot because Jackson's
     * getCurrentLocation() for strings points past the opening quote, not the closing quote.
     * This is fine — only objects and arrays benefit from byte slicing in practice.
     */
    public void testExtractScalarsByKey() throws IOException {
        byte[] json = "{\"s\":\"hello\",\"n\":42,\"b\":true,\"z\":null}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT

            Token token;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();

                    // Scalars: use parser.text() / parser.booleanValue()
                    switch (fieldName) {
                        case "s" -> {
                            assertEquals(Token.VALUE_STRING, parser.currentToken());
                            assertEquals("hello", parser.text());
                        }
                        case "n" -> {
                            assertEquals(Token.VALUE_NUMBER, parser.currentToken());
                            assertEquals("42", parser.text());
                            // Numbers can also be byte-sliced
                            long start = parser.getTokenLocation().byteOffset();
                            long end = parser.getCurrentLocation().byteOffset();
                            assertEquals("42", new String(Arrays.copyOfRange(json, (int) start, (int) end), StandardCharsets.UTF_8));
                        }
                        case "b" -> {
                            assertEquals(Token.VALUE_BOOLEAN, parser.currentToken());
                            assertTrue(parser.booleanValue());
                            // Booleans can also be byte-sliced
                            long start = parser.getTokenLocation().byteOffset();
                            long end = parser.getCurrentLocation().byteOffset();
                            assertEquals("true", new String(Arrays.copyOfRange(json, (int) start, (int) end), StandardCharsets.UTF_8));
                        }
                        case "z" -> {
                            assertEquals(Token.VALUE_NULL, parser.currentToken());
                            // Nulls can also be byte-sliced
                            long start = parser.getTokenLocation().byteOffset();
                            long end = parser.getCurrentLocation().byteOffset();
                            assertEquals("null", new String(Arrays.copyOfRange(json, (int) start, (int) end), StandardCharsets.UTF_8));
                        }
                    }
                }
            }
        }
    }

    /**
     * Proves byte-offset slicing produces identical output to copyCurrentStructure().
     * Both approaches are run on the same input and their results compared.
     */
    public void testByteSliceMatchesCopyCurrentStructure() throws IOException {
        byte[] json = "{\"data\":{\"users\":[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\"}],\"count\":2}}".getBytes(
            StandardCharsets.UTF_8
        );

        // First pass: extract via copyCurrentStructure
        String fromCopy;
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME "data"
            parser.nextToken(); // START_OBJECT (the value)
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                builder.copyCurrentStructure(parser);
                fromCopy = BytesReference.bytes(builder).utf8ToString();
            }
        }

        // Second pass: extract via byte-offset slicing
        String fromSlice;
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME "data"
            parser.nextToken(); // START_OBJECT (the value)
            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();
            fromSlice = new String(Arrays.copyOfRange(json, (int) startOffset, (int) endOffset), StandardCharsets.UTF_8);
        }

        assertEquals(fromCopy, fromSlice);
    }

    /**
     * Combined navigation: object key → array index → object key, then byte-slice.
     * Validates that mixed object/array navigation produces correct byte ranges.
     */
    public void testExtractNestedPathWithArrayIndex() throws IOException {
        byte[] json = "{\"a\":[0,1,{\"k\":{\"found\":true}}]}".getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT (root)

            // Step 1: find field "a"
            while (parser.nextToken() != Token.END_OBJECT) {
                if (parser.currentToken() == Token.FIELD_NAME && parser.currentName().equals("a")) {
                    parser.nextToken(); // START_ARRAY
                    break;
                }
                parser.nextToken();
                parser.skipChildren();
            }
            assertEquals(Token.START_ARRAY, parser.currentToken());

            // Step 2: walk array to index 2
            int currentIndex = 0;
            while (parser.nextToken() != Token.END_ARRAY) {
                if (currentIndex == 2) {
                    break;
                }
                parser.skipChildren();
                currentIndex++;
            }
            assertEquals(Token.START_OBJECT, parser.currentToken());

            // Step 3: find field "k"
            while (parser.nextToken() != Token.END_OBJECT) {
                if (parser.currentToken() == Token.FIELD_NAME && parser.currentName().equals("k")) {
                    parser.nextToken(); // the value
                    break;
                }
                parser.nextToken();
                parser.skipChildren();
            }
            assertEquals(Token.START_OBJECT, parser.currentToken());

            // Extract via byte slice
            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();
            String sliced = new String(Arrays.copyOfRange(json, (int) startOffset, (int) endOffset), StandardCharsets.UTF_8);
            assertEquals("{\"found\":true}", sliced);
        }
    }

    /**
     * Navigate and byte-slice from pretty-printed JSON.
     * Byte offsets account for whitespace; the sliced result includes it verbatim.
     */
    public void testExtractFromPrettyPrintedJson() throws IOException {
        String pretty = "{\n  \"info\": {\n    \"version\": 1,\n    \"name\": \"test\"\n  }\n}";
        byte[] json = pretty.getBytes(StandardCharsets.UTF_8);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT

            // Find field "info"
            while (parser.nextToken() != Token.END_OBJECT) {
                if (parser.currentToken() == Token.FIELD_NAME && parser.currentName().equals("info")) {
                    parser.nextToken();
                    break;
                }
                parser.nextToken();
                parser.skipChildren();
            }
            assertEquals(Token.START_OBJECT, parser.currentToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            parser.skipChildren();
            long endOffset = parser.getCurrentLocation().byteOffset();
            String sliced = new String(Arrays.copyOfRange(json, (int) startOffset, (int) endOffset), StandardCharsets.UTF_8);

            // The slice preserves original whitespace — still valid JSON, just formatted
            // differently than what copyCurrentStructure() would produce
            assertEquals("{\n    \"version\": 1,\n    \"name\": \"test\"\n  }", sliced);
        }
    }

    public void testStringTokenLocationAtOpeningQuote() throws IOException {
        byte[] json = "{\"s\":\"hello\"}".getBytes(StandardCharsets.UTF_8);
        // 0123456789012
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals(Token.VALUE_STRING, parser.nextToken());

            long startOffset = parser.getTokenLocation().byteOffset();
            // Token location for VALUE_STRING points to the opening quote
            assertEquals(5L, startOffset);
            assertEquals('"', (char) json[(int) startOffset]);
        }
    }

}
