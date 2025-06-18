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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.ReaderBasedJsonParser;
import com.fasterxml.jackson.core.json.UTF8StreamJsonParser;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ESJsonFactoryTests extends ESTestCase {

    public void testCreateParser() throws IOException {
        JsonFactory factory = new ESJsonFactoryBuilder().build();
        assertThat(factory, Matchers.instanceOf(ESJsonFactory.class));

        // \ufeff is the BOM
        String[] inputs = { "{\"foo\": \"bar\"}", "\ufeff{\"foo\": \"bar\"}" };
        Charset[] charsets = { StandardCharsets.UTF_8, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE };
        Class<?>[] expectedParsers = { ESUTF8StreamJsonParser.class, ReaderBasedJsonParser.class, ReaderBasedJsonParser.class };

        for (String input : inputs) {
            for (int i = 0; i < charsets.length; i++) {
                ByteBuffer encoded = charsets[i].encode(input);
                JsonParser parser = factory.createParser(encoded.array());
                assertThat(parser, Matchers.instanceOf(expectedParsers[i]));
                assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
                assertThat(parser.nextFieldName(), Matchers.equalTo("foo"));
                assertThat(parser.nextValue(), Matchers.equalTo(JsonToken.VALUE_STRING));
                assertThat(parser.getValueAsString(), Matchers.equalTo("bar"));
            }
        }

        // Valid BOM
        {
            JsonParser parser = factory.createParser(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF, '{', '}' });
            assertThat(parser, Matchers.instanceOf(ESUTF8StreamJsonParser.class));
            assertThat(parser.nextToken(), Matchers.equalTo(JsonToken.START_OBJECT));
        }

        // Invalid BOMs
        {
            JsonParser parser = factory.createParser(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBB, '{', '}' });
            assertThat(parser, Matchers.instanceOf(UTF8StreamJsonParser.class));
            assertThrows("Invalid UTF-8 start byte 0xbb", JsonParseException.class, parser::nextToken);
        }

        {
            JsonParser parser = factory.createParser(new byte[] { (byte) 0xEF, '{', '}' });
            assertThat(parser, Matchers.instanceOf(UTF8StreamJsonParser.class));
            assertThrows("Invalid UTF-8 start byte 0x7b", JsonParseException.class, parser::nextToken);
        }
    }
}
