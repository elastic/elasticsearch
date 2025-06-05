/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentParserTests.generateRandomObject;

public class MapXContentParserTests extends ESTestCase {

    public void testSimpleMap() throws IOException {
        compareTokens(builder -> {
            builder.startObject();
            builder.field("string", "foo");
            builder.field("number", 42);
            builder.field("double", 42.5);
            builder.field("bool", false);
            builder.startArray("arr");
            {
                builder.value(10).value(20.0).value("30");
                builder.startArray();
                builder.value(30);
                builder.endArray();
            }
            builder.endArray();
            builder.startArray("nested_arr");
            {
                builder.startArray();
                builder.value(10);
                builder.endArray();
            }
            builder.endArray();
            builder.startObject("obj");
            {
                builder.field("inner_string", "bar");
                builder.startObject("inner_empty_obj");
                builder.field("f", "a");
                builder.endObject();
            }
            builder.endObject();
            builder.field("bytes", new byte[] { 1, 2, 3 });
            builder.nullField("nothing");
            builder.endObject();
        });
    }

    public void testRandomObject() throws IOException {
        compareTokens(builder -> generateRandomObject(builder, randomIntBetween(0, 10)));
    }

    /**
     * Assert that {@link XContentParser#hasTextCharacters()} returns false because
     * we don't support {@link XContentParser#textCharacters()}.
     */
    public void testHasTextCharacters() throws IOException {
        assertFalse(
            new MapXContentParser(
                xContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                Map.of("a", "b"),
                randomFrom(XContentType.values())
            ).hasTextCharacters()
        );
    }

    public void testCopyCurrentStructure() throws IOException {
        try (
            XContentParser parser = new MapXContentParser(
                xContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                Map.of("a", "b"),
                randomFrom(XContentType.values())
            )
        ) {
            try (
                XContentBuilder builder = JsonXContent.contentBuilder().copyCurrentStructure(parser);
                XContentParser copied = createParser(builder)
            ) {
                assertEquals(copied.map(), Map.of("a", "b"));
            }
        }
    }

    public void testParseBooleanStringValue() throws IOException {
        try (
            XContentParser parser = new MapXContentParser(
                xContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                Map.of("bool_key", "true"),
                randomFrom(XContentType.values())
            )
        ) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            assertTrue(parser.isBooleanValue());
            assertTrue(parser.booleanValue());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        }
    }

    private void compareTokens(CheckedConsumer<XContentBuilder, IOException> consumer) throws IOException {
        for (XContentType xContentType : EnumSet.allOf(XContentType.class)) {
            logger.info("--> testing with xcontent type: {}", xContentType);
            compareTokens(consumer, xContentType);
        }
    }

    private void compareTokens(CheckedConsumer<XContentBuilder, IOException> consumer, XContentType xContentType) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            consumer.accept(builder);
            final Map<String, Object> map;
            try (XContentParser parser = createParser(xContentType.xContent(), BytesReference.bytes(builder))) {
                map = parser.mapOrdered();
            }

            try (XContentParser parser = createParser(xContentType.xContent(), BytesReference.bytes(builder))) {
                try (
                    XContentParser mapParser = new MapXContentParser(
                        xContentRegistry(),
                        LoggingDeprecationHandler.INSTANCE,
                        map,
                        xContentType
                    )
                ) {
                    assertEquals(parser.contentType(), mapParser.contentType().canonical());
                    XContentParser.Token token;
                    assertEquals(parser.currentToken(), mapParser.currentToken());
                    assertEquals(parser.currentName(), mapParser.currentName());
                    do {
                        token = parser.nextToken();
                        XContentParser.Token mapToken = mapParser.nextToken();
                        assertEquals(token, mapToken);
                        assertEquals(parser.currentName(), mapParser.currentName());
                        if (token != null && (token.isValue() || token == XContentParser.Token.VALUE_NULL)) {
                            if ((xContentType.canonical() != XContentType.YAML) || token != XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                                // YAML struggles with converting byte arrays into text, because it
                                // does weird base64 decoding to the values. We don't do this
                                // weirdness in the MapXContentParser, so don't try to stringify it.
                                // The .binaryValue() comparison below still works correctly though.
                                assertEquals(parser.textOrNull(), mapParser.textOrNull());
                            }
                            switch (token) {
                                case VALUE_STRING -> assertEquals(parser.text(), mapParser.text());
                                case VALUE_NUMBER -> {
                                    assertEquals(parser.numberType(), mapParser.numberType());
                                    assertEquals(parser.numberValue(), mapParser.numberValue());
                                    if (parser.numberType() == XContentParser.NumberType.LONG
                                        || parser.numberType() == XContentParser.NumberType.INT) {
                                        assertEquals(parser.longValue(), mapParser.longValue());
                                        if (parser.longValue() <= Integer.MAX_VALUE && parser.longValue() >= Integer.MIN_VALUE) {
                                            assertEquals(parser.intValue(), mapParser.intValue());
                                            if (parser.longValue() <= Short.MAX_VALUE && parser.longValue() >= Short.MIN_VALUE) {
                                                assertEquals(parser.shortValue(), mapParser.shortValue());
                                            }
                                        }
                                    } else {
                                        assertEquals(parser.doubleValue(), mapParser.doubleValue(), 0.000001);
                                    }
                                }
                                case VALUE_BOOLEAN -> assertEquals(parser.booleanValue(), mapParser.booleanValue());
                                case VALUE_EMBEDDED_OBJECT -> assertArrayEquals(parser.binaryValue(), mapParser.binaryValue());
                                case VALUE_NULL -> assertNull(mapParser.textOrNull());
                            }
                            assertEquals(parser.currentName(), mapParser.currentName());
                            assertEquals(parser.isClosed(), mapParser.isClosed());
                        } else if (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT) {
                            if (randomInt(5) == 0) {
                                parser.skipChildren();
                                mapParser.skipChildren();
                            }
                        }
                    } while (token != null);
                    assertEquals(parser.nextToken(), mapParser.nextToken());
                    parser.close();
                    mapParser.close();
                    assertEquals(parser.isClosed(), mapParser.isClosed());
                    assertTrue(mapParser.isClosed());
                }
            }

        }
    }
}
