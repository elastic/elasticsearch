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

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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

            var textRef = parser.getValueAsText().bytes();
            assertThat(textRef, Matchers.notNullValue());
            assertThat(textRef.offset(), Matchers.equalTo(9));
            assertThat(textRef.offset() + textRef.length(), Matchers.equalTo(12));
            assertTextRef(textRef, "bar");

            assertThat(parser.getValueAsString(), Matchers.equalTo("bar"));
            assertThat(parser.getValueAsText(), Matchers.nullValue());

            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.END_OBJECT));
        });

        testParseJson("{\"foo\": \"bar\\\"baz\\\"\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            assertThat(parser.getValueAsText(), Matchers.nullValue());
            assertThat(parser.getValueAsString(), Matchers.equalTo("bar\"baz\""));
        });

        testParseJson("{\"foo\": \"bår\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            assertThat(parser.getValueAsText(), Matchers.nullValue());
            assertThat(parser.getValueAsString(), Matchers.equalTo("bår"));
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

    private boolean validForTextRef(String value) {
        for (char c : value.toCharArray()) {
            if (c == '"') {
                return false;
            }
            if (c == '\\') {
                return false;
            }
            if ((int) c < 32 || (int) c >= 128) {
                return false;
            }
        }
        return true;
    }

    public void testGetValueRandomized() throws IOException {
        XContentBuilder jsonBuilder = JsonXContent.contentBuilder().startObject();
        final int numKeys = 128;
        String[] keys = new String[numKeys];
        String[] values = new String[numKeys];
        for (int i = 0; i < numKeys; i++) {
            String currKey = randomAlphanumericOfLength(6);
            String currVal = randomUnicodeOfLengthBetween(0, 512);
            jsonBuilder.field(currKey, currVal);
            keys[i] = currKey;
            values[i] = currVal;
        }

        jsonBuilder.endObject();
        testParseJson(Strings.toString(jsonBuilder), parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            for (int i = 0; i < numKeys; i++) {
                assertThat(parser.nextFieldName(), Matchers.equalTo(keys[i]));
                assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

                String currVal = values[i];
                if (validForTextRef(currVal)) {
                    assertTextRef(parser.getValueAsText().bytes(), currVal);
                } else {
                    assertThat(parser.getValueAsText(), Matchers.nullValue());
                    assertThat(parser.getValueAsString(), Matchers.equalTo(currVal));
                }
            }
        });
    }

}
