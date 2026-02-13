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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.provider.XContentParserConfigurationImpl;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

        testParseJson("{\"foo\": [\"bar\\\"baz\\\"\", \"foobar\"]}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));

            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.START_ARRAY));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            var firstText = parser.getValueAsText();
            assertThat(firstText, Matchers.notNullValue());
            assertTextRef(firstText.bytes(), "bar\"baz\"");
            // Retrieve the value for a second time to ensure the last value is available
            firstText = parser.getValueAsText();
            assertThat(firstText, Matchers.notNullValue());
            assertTextRef(firstText.bytes(), "bar\"baz\"");

            // Ensure values lastOptimisedValue is reset
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));
            var secondTest = parser.getValueAsText();
            assertThat(secondTest, Matchers.notNullValue());
            assertTextRef(secondTest.bytes(), "foobar");
            secondTest = parser.getValueAsText();
            assertThat(secondTest, Matchers.notNullValue());
            assertTextRef(secondTest.bytes(), "foobar");
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.END_ARRAY));
        });

        testParseJson("{\"foo\": \"b\\u00e5r\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            assertThat(parser.getValueAsText(), Matchers.nullValue());
            assertThat(parser.getValueAsString(), Matchers.equalTo("bår"));
        });

        testParseJson("{\"foo\": \"\uD83D\uDE0A\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            var text = parser.getValueAsText();
            assertThat(text, Matchers.notNullValue());
            var bytes = text.bytes();
            assertTextRef(bytes, "\uD83D\uDE0A");
            assertThat(text.stringLength(), Matchers.equalTo(2));
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

    private int randomCodepointIncludeAscii() {
        while (true) {
            char val = Character.toChars(randomInt(0xFFFF))[0];
            if (val >= Character.MIN_SURROGATE && val <= Character.MAX_SURROGATE) {
                continue;
            }
            return val;
        }
    }

    private int randomCodepointIncludeOutsideBMP(int remainingLength) {
        while (true) {
            int codePoint = randomInt(0x10FFFF);
            char[] val = Character.toChars(codePoint);
            // Don't include ascii
            if (val.length == 1 && val[0] <= 0x7F) {
                continue;
            }
            boolean surrogate = val[0] >= Character.MIN_SURROGATE && val[0] <= Character.MAX_SURROGATE;
            // Single surrogate is invalid
            if (val.length == 1 && surrogate) {
                continue;
            }
            // Not enough remaining space for a surrogate pair
            if (remainingLength < 2 && surrogate) {
                continue;
            }
            return codePoint;
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
                        int value = randomCodepointIncludeAscii();
                        input.append(String.format(Locale.ENGLISH, "\\u%04x", value));
                        result.append(Character.toChars(value));
                        doesSupportOptimized = false;
                    }
                    default -> {
                        var remainingLength = length - i;
                        var value = Character.toChars(randomCodepointIncludeOutsideBMP(remainingLength));
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
                    var text = parser.getValueAsText();
                    assertTextRef(text.bytes(), currVal);
                    assertThat(text.stringLength(), Matchers.equalTo(currVal.length()));
                    // Retrieve it a second time
                    text = parser.getValueAsText();
                    assertThat(text, Matchers.notNullValue());
                    assertTextRef(text.bytes(), currVal);
                    assertThat(text.stringLength(), Matchers.equalTo(currVal.length()));
                    // Use getText()
                    assertThat(parser.getText(), Matchers.equalTo(text.string()));
                    // After retrieving it with getText() we do not use the optimised value anymore.
                    assertThat(parser.getValueAsText(), Matchers.nullValue());
                } else {
                    assertThat(parser.getText(), Matchers.notNullValue());
                    assertThat(parser.getValueAsText(), Matchers.nullValue());
                    assertThat(parser.getValueAsString(), Matchers.equalTo(currVal));
                    // Retrieve it twice to ensure it works as expected
                    assertThat(parser.getValueAsText(), Matchers.nullValue());
                    assertThat(parser.getValueAsString(), Matchers.equalTo(currVal));
                }
            }
        });
    }

    /**
     * This test compares the retrieval of an optimised text against the baseline.
     */
    public void testOptimisedParser() throws Exception {
        for (int i = 0; i < 200; i++) {
            String json = randomJsonInput(randomIntBetween(1, 6));
            try (
                var baselineParser = XContentHelper.createParser(
                    XContentParserConfiguration.EMPTY,
                    new BytesArray(json),
                    XContentType.JSON
                );
                var optimisedParser = TestXContentParser.create(json)
            ) {
                var expected = baselineParser.mapOrdered();
                var actual = optimisedParser.mapOrdered();
                assertThat(expected, Matchers.equalTo(actual));
            }
        }
    }

    private String randomJsonInput(int depth) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        int numberOfFields = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfFields; i++) {
            sb.append("\"k-").append(randomAlphanumericOfLength(10)).append("\":");
            if (depth == 0 || randomBoolean()) {
                if (randomIntBetween(0, 9) == 0) {
                    sb.append(
                        IntStream.range(0, randomIntBetween(1, 10))
                            .mapToObj(ignored -> randomUTF8Value())
                            .collect(Collectors.joining(",", "[", "]"))
                    );
                } else {
                    sb.append(randomUTF8Value());
                }
            } else {
                sb.append(randomJsonInput(depth - 1));
            }
            if (i < numberOfFields - 1) {
                sb.append(',');
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private String randomUTF8Value() {
        return "\"" + buildRandomInput(randomIntBetween(10, 50)).input + "\"";
    }

    /**
     * This XContentParser introduces a random mix of getText() and getOptimisedText()
     * to simulate different access patterns for optimised fields.
     */
    private static class TestXContentParser extends FilterXContentParserWrapper {

        TestXContentParser(XContentParser delegate) {
            super(delegate);
        }

        static TestXContentParser create(String input) throws IOException {
            JsonFactory factory = new ESJsonFactoryBuilder().build();
            assertThat(factory, Matchers.instanceOf(ESJsonFactory.class));

            JsonParser parser = factory.createParser(StandardCharsets.UTF_8.encode(input).array());
            assertThat(parser, Matchers.instanceOf(ESUTF8StreamJsonParser.class));
            return new TestXContentParser(new JsonXContentParser(XContentParserConfigurationImpl.EMPTY, parser));
        }

        @Override
        public String text() throws IOException {
            if (randomIntBetween(0, 9) < 8) {
                return super.text();
            } else {
                return super.optimizedText().string();
            }
        }

        @Override
        public XContentString optimizedText() throws IOException {
            int extraCalls = randomIntBetween(0, 5);
            for (int i = 0; i < extraCalls; i++) {
                if (randomBoolean()) {
                    super.optimizedText();
                } else {
                    super.text();
                }
            }
            return super.optimizedText();
        }
    }
}
