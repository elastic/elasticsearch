/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class XContentHelperTests extends ESTestCase {

    Map<String, Object> getMap(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i++) {
            map.put((String) keyValues[i], keyValues[++i]);
        }
        return map;
    }

    Map<String, Object> getNamedMap(String name, Object... keyValues) {
        Map<String, Object> map = getMap(keyValues);

        Map<String, Object> namedMap = Maps.newMapWithExpectedSize(1);
        namedMap.put(name, map);
        return namedMap;
    }

    List<Object> getList(Object... values) {
        return Arrays.asList(values);
    }

    public void testMergingListValuesAreMapsOfOne() {
        Map<String, Object> defaults = getMap("test", getList(getNamedMap("name1", "t1", "1"), getNamedMap("name2", "t2", "2")));
        Map<String, Object> content = getMap("test", getList(getNamedMap("name2", "t3", "3"), getNamedMap("name4", "t4", "4")));
        Map<String, Object> expected = getMap(
            "test",
            getList(getNamedMap("name2", "t2", "2", "t3", "3"), getNamedMap("name4", "t4", "4"), getNamedMap("name1", "t1", "1"))
        );

        XContentHelper.mergeDefaults(content, defaults);

        assertThat(content, equalTo(expected));
    }

    public void testToXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        final ToXContent toXContent;
        final boolean error;
        if (randomBoolean()) {
            if (randomBoolean()) {
                error = false;
                toXContent = (builder, params) -> builder.field("field", "value");
            } else {
                error = true;
                toXContent = (builder, params) -> builder.startObject().field("field", "value").endObject();
            }
        } else {
            if (randomBoolean()) {
                error = false;
                toXContent = (ToXContentObject) (builder, params) -> builder.startObject().field("field", "value").endObject();
            } else {
                error = true;
                toXContent = (ToXContentObject) (builder, params) -> builder.field("field", "value");
            }
        }
        if (error) {
            expectThrows(IOException.class, () -> XContentHelper.toXContent(toXContent, xContentType, randomBoolean()));
        } else {
            BytesReference bytes = XContentHelper.toXContent(toXContent, xContentType, randomBoolean());
            try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, bytes.streamInput())) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertTrue(parser.nextToken().isValue());
                assertEquals("value", parser.text());
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                assertNull(parser.nextToken());
            }
        }
    }

    public void testChildBytes() throws IOException {

        for (XContentType xContentType : XContentType.values()) {

            XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
            builder.startObject().startObject("level1");
            builder.startObject("level2")
                .startObject("object")
                .field("text", "string")
                .field("number", 10)
                .endObject()
                .startObject("object2")
                .field("boolean", true)
                .nullField("null")
                .startArray("array_of_strings")
                .value("string1")
                .value("string2")
                .endArray()
                .endObject()
                .endObject();
            builder.field("field", "value");
            builder.endObject().endObject();
            BytesReference input = BytesReference.bytes(builder);

            BytesReference bytes;
            try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, input.streamInput())) {

                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("level2", parser.currentName());
                // Extract everything under 'level2' as a bytestream
                bytes = XContentHelper.childBytes(parser);
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("field", parser.currentName());
            }

            // now parse the contents of 'level2'
            try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, bytes.streamInput())) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("object", parser.currentName());
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("text", parser.currentName());
                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                assertEquals("string", parser.text());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("number", parser.currentName());
                assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
                assertEquals(10, parser.numberValue());
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("object2", parser.currentName());
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("boolean", parser.currentName());
                assertEquals(XContentParser.Token.VALUE_BOOLEAN, parser.nextToken());
                assertTrue(parser.booleanValue());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("null", parser.currentName());
                assertEquals(XContentParser.Token.VALUE_NULL, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("array_of_strings", parser.currentName());
                assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                assertEquals("string1", parser.text());
                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                assertEquals("string2", parser.text());
                assertEquals(XContentParser.Token.END_ARRAY, parser.nextToken());
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                assertNull(parser.nextToken());
            }

        }
    }

    public void testEmbeddedObject() throws IOException {
        // Need to test this separately as XContentType.JSON never produces VALUE_EMBEDDED_OBJECT
        XContentBuilder builder = XContentBuilder.builder(XContentType.CBOR.xContent());
        builder.startObject().startObject("root");
        CompressedXContent embedded = new CompressedXContent("{\"field\":\"value\"}");
        builder.field("bytes", embedded.compressed());
        builder.endObject().endObject();
        BytesReference bytes = BytesReference.bytes(builder);

        BytesReference inner;
        try (XContentParser parser = XContentType.CBOR.xContent().createParser(XContentParserConfiguration.EMPTY, bytes.streamInput())) {

            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            inner = XContentHelper.childBytes(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }

        try (XContentParser parser = XContentType.CBOR.xContent().createParser(XContentParserConfiguration.EMPTY, inner.streamInput())) {

            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("bytes", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_EMBEDDED_OBJECT, parser.nextToken());
            assertEquals(embedded, new CompressedXContent(parser.binaryValue()));
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());

        }
    }

    public void testEmptyChildBytes() throws IOException {

        String inputJson = "{ \"mappings\" : {} }";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, inputJson)) {

            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            BytesReference bytes = XContentHelper.childBytes(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());

            assertEquals("{}", bytes.utf8ToString());

        }

    }

    public void testParseToType() throws IOException {
        String json = """
            { "a": "b", "c": "d"}
            """;
        Set<String> names = XContentHelper.parseToType(parser -> {
            Set<String> fields = new HashSet<>();
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.START_OBJECT) {
                fields.add(parser.nextFieldName());
            }
            for (token = parser.nextToken(); token != null; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fields.add(parser.currentName());
                }
            }
            return fields;
        }, new BytesArray(json), XContentType.JSON, null).v2();

        assertThat(names, equalTo(Set.of("a", "c")));
    }
}
