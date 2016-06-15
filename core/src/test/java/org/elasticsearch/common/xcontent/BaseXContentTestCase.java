/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class BaseXContentTestCase extends ESTestCase {

    public abstract XContentType xcontentType();

    public void testBasics() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeStartObject();
            generator.writeEndObject();
        }
        byte[] data = os.toByteArray();
        assertEquals(xcontentType(), XContentFactory.xContentType(data));
    }

    public void testMissingEndObject() throws IOException {
        IOException e = expectThrows(IOException.class, () -> {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
                generator.writeStartObject();
                generator.writeFieldName("foo");
                generator.writeNumber(2L);
            }
        });
        assertEquals(e.getMessage(), "unclosed object or array found");
    }

    public void testMissingEndArray() throws IOException {
        IOException e = expectThrows(IOException.class, () -> {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
                generator.writeStartArray();
                generator.writeNumber(2L);
            }
        });
        assertEquals(e.getMessage(), "unclosed object or array found");
    }

    public void testRawField() throws Exception {
        for (boolean useStream : new boolean[] {false, true}) {
            for (XContentType xcontentType : XContentType.values()) {
                doTestRawField(xcontentType.xContent(), useStream);
            }
        }
    }

    void doTestRawField(XContent source, boolean useStream) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (XContentGenerator generator = source.createGenerator(os)) {
            generator.writeStartObject();
            generator.writeFieldName("foo");
            generator.writeNull();
            generator.writeEndObject();
        }
        final byte[] rawData = os.toByteArray();

        os = new ByteArrayOutputStream();
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeStartObject();
            if (useStream) {
                generator.writeRawField("bar", new ByteArrayInputStream(rawData));
            } else {
                generator.writeRawField("bar", new BytesArray(rawData));
            }
            generator.writeEndObject();
        }

        XContentParser parser = xcontentType().xContent().createParser(os.toByteArray());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("bar", parser.currentName());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(Token.VALUE_NULL, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    public void testRawValue() throws Exception {
        for (XContentType xcontentType : XContentType.values()) {
            doTestRawValue(xcontentType.xContent());
        }
    }

    void doTestRawValue(XContent source) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (XContentGenerator generator = source.createGenerator(os)) {
            generator.writeStartObject();
            generator.writeFieldName("foo");
            generator.writeNull();
            generator.writeEndObject();
        }
        final byte[] rawData = os.toByteArray();

        os = new ByteArrayOutputStream();
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeRawValue(new BytesArray(rawData));
        }

        XContentParser parser = xcontentType().xContent().createParser(os.toByteArray());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(Token.VALUE_NULL, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());

        os = new ByteArrayOutputStream();
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeStartObject();
            generator.writeFieldName("test");
            generator.writeRawValue(new BytesArray(rawData));
            generator.writeEndObject();
        }

        parser = xcontentType().xContent().createParser(os.toByteArray());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("test", parser.currentName());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(Token.VALUE_NULL, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());

    }
}
