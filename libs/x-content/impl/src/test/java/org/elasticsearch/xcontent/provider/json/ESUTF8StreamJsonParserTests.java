/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class ESUTF8StreamJsonParserTests extends ESTestCase {

    private void testParseJson(String input, CheckedConsumer<ESUTF8StreamJsonParser, IOException> test) throws IOException {
        JsonFactory factory = new ESJsonFactoryBuilder().build();
        assertThat(factory, Matchers.instanceOf(ESJsonFactory.class));

        JsonParser parser = factory.createParser(StandardCharsets.UTF_8.encode(input).array());
        assertThat(parser, Matchers.instanceOf(ESUTF8StreamJsonParser.class));
        test.accept((ESUTF8StreamJsonParser) parser);
    }

    private void assertTextRef(XContentString.UTF8Bytes textRef, String expectedValue) {
        assertThat(textRef, Matchers.equalTo(new XContentString.UTF8Bytes(expectedValue.getBytes(StandardCharsets.UTF_8))));
    }

    public void testGetValueAsText() throws IOException {
        testParseJson("{\"foo\": \"bar\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            var text = parser.getValueAsText();
            assertThat(text, Matchers.notNullValue());

            var bytes = text.bytes();
            assertThat(bytes.offset(), Matchers.equalTo(9));
            assertThat(bytes.offset() + bytes.length(), Matchers.equalTo(12));
            assertTextRef(bytes, "bar");

            assertThat(parser.getValueAsString(), Matchers.equalTo("bar"));
            assertThat(parser.getValueAsText(), Matchers.nullValue());

            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.END_OBJECT));
        });

        testParseJson("{\"foo\": \"bar\\\"baz\\\"\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            var text = parser.getValueAsText();
            assertThat(text, Matchers.notNullValue());
            assertTextRef(text.bytes(), "bar\"baz\"");
        });

        testParseJson("{\"foo\": \"b\\u00e5r\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            assertThat(parser.getValueAsText(), Matchers.nullValue());
            assertThat(parser.getValueAsString(), Matchers.equalTo("bår"));
        });

        testParseJson("{\"foo\": \"bår\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            var text = parser.getValueAsText();
            assertThat(text, Matchers.notNullValue());

            var bytes = text.bytes();
            assertThat(bytes.offset(), Matchers.equalTo(9));
            assertThat(bytes.offset() + bytes.length(), Matchers.equalTo(13));
            assertTextRef(bytes, "bår");

            assertThat(parser.getValueAsString(), Matchers.equalTo("bår"));

            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.END_OBJECT));
        });

        testParseJson("{\"foo\": [\"lorem\", \"ipsum\", \"dolor\"]}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.START_ARRAY));

            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));
            {
                var textRef = parser.getValueAsText().bytes();
                assertThat(textRef, Matchers.notNullValue());
                assertThat(textRef.offset(), Matchers.equalTo(10));
                assertThat(textRef.offset() + textRef.length(), Matchers.equalTo(15));
                assertTextRef(textRef, "lorem");
            }

            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));
            {
                var textRef = parser.getValueAsText().bytes();
                assertThat(textRef, Matchers.notNullValue());
                assertThat(textRef.offset(), Matchers.equalTo(19));
                assertThat(textRef.offset() + textRef.length(), Matchers.equalTo(24));
                assertTextRef(textRef, "ipsum");
            }

            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));
            {
                var textRef = parser.getValueAsText().bytes();
                assertThat(textRef, Matchers.notNullValue());
                assertThat(textRef.offset(), Matchers.equalTo(28));
                assertThat(textRef.offset() + textRef.length(), Matchers.equalTo(33));
                assertTextRef(textRef, "dolor");
            }

            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.END_ARRAY));
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.END_OBJECT));
        });
    }

    private record TestInput(String input, String result, boolean supportsOptimized) {}

    private static final TestInput[] ESCAPE_SEQUENCES = {
        new TestInput("\\b", "\b", false),
        new TestInput("\\t", "\t", false),
        new TestInput("\\n", "\n", false),
        new TestInput("\\f", "\f", false),
        new TestInput("\\r", "\r", false),
        new TestInput("\\\"", "\"", true),
        new TestInput("\\/", "/", true),
        new TestInput("\\\\", "\\", true) };

    private int randomCodepoint(boolean includeAscii) {
        while (true) {
            char val = Character.toChars(randomInt(0xFFFF))[0];
            if (val <= 0x7f && includeAscii == false) {
                continue;
            }
            if (val >= Character.MIN_SURROGATE && val <= Character.MAX_SURROGATE) {
                continue;
            }
            return val;
        }
    }

    private TestInput buildRandomInput(int length) {
        StringBuilder input = new StringBuilder(length);
        StringBuilder result = new StringBuilder(length);
        boolean forceSupportOptimized = randomBoolean();
        boolean doesSupportOptimized = true;
        for (int i = 0; i < length; ++i) {
            if (forceSupportOptimized == false && randomBoolean()) {
                switch (randomInt(9)) {
                    case 0 -> {
                        var escape = randomFrom(ESCAPE_SEQUENCES);
                        input.append(escape.input());
                        result.append(escape.result());
                        doesSupportOptimized = doesSupportOptimized && escape.supportsOptimized();
                    }
                    case 1 -> {
                        int value = randomCodepoint(true);
                        input.append(String.format(Locale.ENGLISH, "\\u%04x", value));
                        result.append(Character.toChars(value));
                        doesSupportOptimized = false;
                    }
                    default -> {
                        var value = Character.toChars(randomCodepoint(false));
                        input.append(value);
                        result.append(value);
                    }
                }
            } else {
                var value = randomAlphanumericOfLength(1);
                input.append(value);
                result.append(value);
            }
        }
        return new TestInput(input.toString(), result.toString(), doesSupportOptimized);
    }

    public void testGetValueRandomized() throws IOException {
        StringBuilder inputBuilder = new StringBuilder();
        inputBuilder.append('{');

        final int numKeys = 128;
        String[] keys = new String[numKeys];
        TestInput[] inputs = new TestInput[numKeys];
        for (int i = 0; i < numKeys; i++) {
            String currKey = randomAlphanumericOfLength(6);
            var currVal = buildRandomInput(randomInt(512));
            inputBuilder.append('"');
            inputBuilder.append(currKey);
            inputBuilder.append("\":\"");
            inputBuilder.append(currVal.input());
            inputBuilder.append('"');
            if (i < numKeys - 1) {
                inputBuilder.append(',');
            }
            keys[i] = currKey;
            inputs[i] = currVal;
        }

        inputBuilder.append('}');
        testParseJson(inputBuilder.toString(), parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            for (int i = 0; i < numKeys; i++) {
                assertThat(parser.nextFieldName(), Matchers.equalTo(keys[i]));
                assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

                String currVal = inputs[i].result();
                if (inputs[i].supportsOptimized()) {
                    assertTextRef(parser.getValueAsText().bytes(), currVal);
                } else {
                    assertThat(parser.getValueAsText(), Matchers.nullValue());
                    assertThat(parser.getValueAsString(), Matchers.equalTo(currVal));
                }
            }
        });
    }

}
