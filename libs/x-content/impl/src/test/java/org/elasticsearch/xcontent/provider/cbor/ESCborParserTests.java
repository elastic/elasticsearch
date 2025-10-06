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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ESCborParserTests extends ESTestCase {

    public void testParseText() throws IOException {
        testStringValue("foo");
        testStringValue("føø");
        testStringValue("f\u00F8\u00F8");
        testStringValue("ツ"); // 3 bytes in UTF-8, counts as 1 character
        testStringValue("🐔"); // 4 bytes in UTF-8, counts as 2 characters
        testStringValue(randomUnicodeOfLengthBetween(1, 1_000));
    }

    private void testStringValue(String expected) throws IOException {
        CBORFactory factory = new ESCborFactoryBuilder().build();
        assertThat(factory, Matchers.instanceOf(ESCborFactory.class));

        ByteArrayOutputStream outputStream;
        try (XContentBuilder builder = CborXContent.contentBuilder()) {
            builder.map(Map.of("text", expected));
            outputStream = (ByteArrayOutputStream) builder.getOutputStream();
        }
        ESCborParser parser = (ESCborParser) factory.createParser(outputStream.toByteArray());

        assertThat(parser, Matchers.instanceOf(ESCborParser.class));
        assertThat(parser.nextToken(), equalTo(JsonToken.START_OBJECT));
        assertThat(parser.nextFieldName(), equalTo("text"));
        assertThat(parser.nextToken(), equalTo(JsonToken.VALUE_STRING));
        Text text = parser.getValueAsText();
        assertThat(text.hasBytes(), equalTo(true));
        assertThat(text.stringLength(), equalTo(expected.length()));
        assertThat(text.string(), equalTo(expected));
        // Retrieve twice
        assertThat(parser.getValueAsText().string(), equalTo(expected));
        assertThat(parser.getValueAsString(), equalTo(expected));
        // Use the getText() to ensure _tokenIncomplete works
        assertThat(parser.getText(), equalTo(expected));
        // The optimisation is not used after the getText()
        assertThat(parser.getValueAsText(), nullValue());
        // Original CBOR getValueAsString works.
        assertThat(parser.getValueAsString(), equalTo(expected));
        assertThat(parser.nextToken(), equalTo(JsonToken.END_OBJECT));
    }
}
