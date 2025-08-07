/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.cbor;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ESCborParserTests extends ESTestCase {

    @Test
    public void testParseText() throws IOException {
        testStringValue("foo");
        testStringValue("føø");
        testStringValue("f\u00F8\u00F8");
        testStringValue("🐔");
        testStringValue(randomUnicodeOfLengthBetween(1, 1_000));
    }

    private void testStringValue(String value) throws IOException {
        CBORFactory factory = new ESCborFactoryBuilder().build();
        assertThat(factory, Matchers.instanceOf(ESCborFactory.class));

        ByteArrayOutputStream outputStream;
        try (XContentBuilder builder = CborXContent.contentBuilder()) {
            builder.map(Map.of("text", value));
            outputStream = (ByteArrayOutputStream) builder.getOutputStream();
        }
        ESCborParser parser = (ESCborParser) factory.createParser(outputStream.toByteArray());

        assertThat(parser, Matchers.instanceOf(ESCborParser.class));
        assertThat(parser.nextToken(), equalTo(JsonToken.START_OBJECT));
        assertThat(parser.nextFieldName(), equalTo("text"));
        assertThat(parser.nextToken(), equalTo(JsonToken.VALUE_STRING));
        Text valueAsText = parser.getValueAsText();
        assertThat(valueAsText.string(), equalTo(value));
        assertThat(valueAsText.hasBytes(), equalTo(true));
        assertThat(parser.nextToken(), equalTo(JsonToken.END_OBJECT));
    }
}
