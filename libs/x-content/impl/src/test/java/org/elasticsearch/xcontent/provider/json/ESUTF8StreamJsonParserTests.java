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
import org.elasticsearch.xcontent.XBytesRef;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ESUTF8StreamJsonParserTests extends ESTestCase {

    private void testParseJson(String input, CheckedConsumer<ESUTF8StreamJsonParser, IOException> test) throws IOException {
        JsonFactory factory = new ESJsonFactoryBuilder().build();
        assertThat(factory, Matchers.instanceOf(ESJsonFactory.class));

        JsonParser parser = factory.createParser(StandardCharsets.UTF_8.encode(input).array());
        assertThat(parser, Matchers.instanceOf(ESUTF8StreamJsonParser.class));
        test.accept((ESUTF8StreamJsonParser) parser);
    }

    private void assertTextRef(XBytesRef textRef, String expectedValue) {
        var data = Arrays.copyOfRange(textRef.bytes(), textRef.start(), textRef.end());
        assertThat(data, Matchers.equalTo(StandardCharsets.UTF_8.encode(expectedValue).array()));
    }

    public void testGetValueAsByteRef() throws IOException {
        testParseJson("{\"foo\": \"bar\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            XBytesRef textRef = parser.getValueAsByteRef();
            assertThat(textRef, Matchers.notNullValue());
            assertThat(textRef.start(), Matchers.equalTo(9));
            assertThat(textRef.end(), Matchers.equalTo(12));
            assertTextRef(textRef, "bar");

            assertThat(parser.getValueAsString(), Matchers.equalTo("bar"));
            assertThat(parser.getValueAsByteRef(), Matchers.nullValue());

            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.END_OBJECT));
        });

        testParseJson("{\"foo\": \"bar\\\"baz\\\"\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            assertThat(parser.getValueAsByteRef(), Matchers.nullValue());
            assertThat(parser.getValueAsString(), Matchers.equalTo("bar\"baz\""));
        });

        testParseJson("{\"foo\": \"bår\"}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));

            assertThat(parser.getValueAsByteRef(), Matchers.nullValue());
            assertThat(parser.getValueAsString(), Matchers.equalTo("bår"));
        });

        testParseJson("{\"foo\": [\"lorem\", \"ipsum\", \"dolor\"]}", parser -> {
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
            assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.START_ARRAY));

            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));
            {
                XBytesRef textRef = parser.getValueAsByteRef();
                assertThat(textRef, Matchers.notNullValue());
                assertThat(textRef.start(), Matchers.equalTo(10));
                assertThat(textRef.end(), Matchers.equalTo(15));
                assertTextRef(textRef, "lorem");
            }

            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));
            {
                XBytesRef textRef = parser.getValueAsByteRef();
                assertThat(textRef, Matchers.notNullValue());
                assertThat(textRef.start(), Matchers.equalTo(19));
                assertThat(textRef.end(), Matchers.equalTo(24));
                assertTextRef(textRef, "ipsum");
            }

            assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));
            {
                XBytesRef textRef = parser.getValueAsByteRef();
                assertThat(textRef, Matchers.notNullValue());
                assertThat(textRef.start(), Matchers.equalTo(28));
                assertThat(textRef.end(), Matchers.equalTo(33));
                assertTextRef(textRef, "dolor");
            }

            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.END_ARRAY));
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.END_OBJECT));
        });
    }

}
