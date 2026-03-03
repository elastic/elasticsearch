/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import org.elasticsearch.test.ESTestCase;
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
