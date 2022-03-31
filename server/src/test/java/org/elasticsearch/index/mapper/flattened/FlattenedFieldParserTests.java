/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MockFieldMapper.FakeFieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

public class FlattenedFieldParserTests extends ESTestCase {
    private FlattenedFieldParser parser;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        parser = new FlattenedFieldParser("field", "field._keyed", new FakeFieldType("field"), Integer.MAX_VALUE, Integer.MAX_VALUE, null);
    }

    public void testTextValues() throws Exception {
        String input = """
            { "key1": "value1", "key2": "value2" }""";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(4, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("value1"), field1.binaryValue());

        IndexableField keyedField1 = fields.get(1);
        assertEquals("field._keyed", keyedField1.name());
        assertEquals(new BytesRef("key1\0value1"), keyedField1.binaryValue());

        IndexableField field2 = fields.get(2);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("value2"), field2.binaryValue());

        IndexableField keyedField2 = fields.get(3);
        assertEquals("field._keyed", keyedField2.name());
        assertEquals(new BytesRef("key2\0value2"), keyedField2.binaryValue());
    }

    public void testNumericValues() throws Exception {
        String input = "{ \"key\": 2.718 }";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(2, fields.size());

        IndexableField field = fields.get(0);
        assertEquals("field", field.name());
        assertEquals(new BytesRef("2.718"), field.binaryValue());

        IndexableField keyedField = fields.get(1);
        assertEquals("field._keyed", keyedField.name());
        assertEquals(new BytesRef("key" + '\0' + "2.718"), keyedField.binaryValue());
    }

    public void testBooleanValues() throws Exception {
        String input = "{ \"key\": false }";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(2, fields.size());

        IndexableField field = fields.get(0);
        assertEquals("field", field.name());
        assertEquals(new BytesRef("false"), field.binaryValue());

        IndexableField keyedField = fields.get(1);
        assertEquals("field._keyed", keyedField.name());
        assertEquals(new BytesRef("key\0false"), keyedField.binaryValue());
    }

    public void testBasicArrays() throws Exception {
        String input = """
            { "key": [true, false] }""";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(4, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("true"), field1.binaryValue());

        IndexableField keyedField1 = fields.get(1);
        assertEquals("field._keyed", keyedField1.name());
        assertEquals(new BytesRef("key\0true"), keyedField1.binaryValue());

        IndexableField field2 = fields.get(2);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("false"), field2.binaryValue());

        IndexableField keyedField2 = fields.get(3);
        assertEquals("field._keyed", keyedField2.name());
        assertEquals(new BytesRef("key\0false"), keyedField2.binaryValue());
    }

    public void testArrayOfArrays() throws Exception {
        String input = """
            { "key": [[true, "value"], 3] }""";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(6, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("true"), field1.binaryValue());

        IndexableField keyedField1 = fields.get(1);
        assertEquals("field._keyed", keyedField1.name());
        assertEquals(new BytesRef("key\0true"), keyedField1.binaryValue());

        IndexableField field2 = fields.get(2);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("value"), field2.binaryValue());

        IndexableField keyedField2 = fields.get(3);
        assertEquals("field._keyed", keyedField2.name());
        assertEquals(new BytesRef("key\0value"), keyedField2.binaryValue());

        IndexableField field3 = fields.get(4);
        assertEquals("field", field3.name());
        assertEquals(new BytesRef("3"), field3.binaryValue());

        IndexableField keyedField3 = fields.get(5);
        assertEquals("field._keyed", keyedField3.name());
        assertEquals(new BytesRef("key" + "\0" + "3"), keyedField3.binaryValue());
    }

    public void testArraysOfObjects() throws Exception {
        String input = """
            {
              "key1": [ { "key2": true }, false ],
              "key4": "other"
            }""";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(6, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("true"), field1.binaryValue());

        IndexableField keyedField1 = fields.get(1);
        assertEquals("field._keyed", keyedField1.name());
        assertEquals(new BytesRef("key1.key2\0true"), keyedField1.binaryValue());

        IndexableField field2 = fields.get(2);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("false"), field2.binaryValue());

        IndexableField keyedField2 = fields.get(3);
        assertEquals("field._keyed", keyedField2.name());
        assertEquals(new BytesRef("key1\0false"), keyedField2.binaryValue());

        IndexableField field3 = fields.get(4);
        assertEquals("field", field3.name());
        assertEquals(new BytesRef("other"), field3.binaryValue());

        IndexableField keyedField3 = fields.get(5);
        assertEquals("field._keyed", keyedField3.name());
        assertEquals(new BytesRef("key4\0other"), keyedField3.binaryValue());
    }

    public void testNestedObjects() throws Exception {
        String input = """
            {
              "parent1": {
                "key": "value"
              },
              "parent2": {
                "key": "value"
              }
            }""";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(4, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("value"), field1.binaryValue());

        IndexableField keyedField1 = fields.get(1);
        assertEquals("field._keyed", keyedField1.name());
        assertEquals(new BytesRef("parent1.key\0value"), keyedField1.binaryValue());

        IndexableField field2 = fields.get(2);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("value"), field2.binaryValue());

        IndexableField keyedField2 = fields.get(3);
        assertEquals("field._keyed", keyedField2.name());
        assertEquals(new BytesRef("parent2.key\0value"), keyedField2.binaryValue());
    }

    /**
     * Test that we are lenient in accepting dotted paths:
     *   * Dotted paths are allowed to be prefixes of each other.
     *   * The same field name can be specified as a dotted path and using object notation.
     */
    public void testDottedPaths() throws Exception {
        String input = """
            {
              "object1.object2": "value1",
              "object1.object2.object3": "value2",
              "object1": {
                "object2": "value3"
              }
            }""";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(6, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("value1"), field1.binaryValue());

        IndexableField keyedField1 = fields.get(1);
        assertEquals("field._keyed", keyedField1.name());
        assertEquals(new BytesRef("object1.object2\0value1"), keyedField1.binaryValue());

        IndexableField field2 = fields.get(2);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("value2"), field2.binaryValue());

        IndexableField keyedField2 = fields.get(3);
        assertEquals("field._keyed", keyedField2.name());
        assertEquals(new BytesRef("object1.object2.object3\0value2"), keyedField2.binaryValue());

        IndexableField field3 = fields.get(4);
        assertEquals("field", field3.name());
        assertEquals(new BytesRef("value3"), field3.binaryValue());

        IndexableField keyedField3 = fields.get(5);
        assertEquals("field._keyed", keyedField3.name());
        assertEquals(new BytesRef("object1.object2\0value3"), keyedField3.binaryValue());
    }

    public void testDepthLimit() throws Exception {
        String input = """
            {
              "parent1": {
                "key": "value"
              },
              "parent2": [ { "key": { "key": "value" } } ]
            }""";
        XContentParser xContentParser = createXContentParser(input);
        FlattenedFieldParser configuredParser = new FlattenedFieldParser(
            "field",
            "field._keyed",
            new FakeFieldType("field"),
            2,
            Integer.MAX_VALUE,
            null
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> configuredParser.parse(xContentParser));
        assertEquals("The provided [flattened] field [field] exceeds the maximum depth limit of [2].", e.getMessage());
    }

    public void testDepthLimitBoundary() throws Exception {
        String input = """
            {
              "parent1": {
                "key": "value"
              },
              "parent2": [ { "key": { "key": "value" } } ]
            }""";
        XContentParser xContentParser = createXContentParser(input);
        FlattenedFieldParser configuredParser = new FlattenedFieldParser(
            "field",
            "field._keyed",
            new FakeFieldType("field"),
            3,
            Integer.MAX_VALUE,
            null
        );

        List<IndexableField> fields = configuredParser.parse(xContentParser);
        assertEquals(4, fields.size());
    }

    public void testIgnoreAbove() throws Exception {
        String input = "{ \"key\": \"a longer field than usual\" }";
        XContentParser xContentParser = createXContentParser(input);
        FlattenedFieldParser configuredParser = new FlattenedFieldParser(
            "field",
            "field._keyed",
            new FakeFieldType("field"),
            Integer.MAX_VALUE,
            10,
            null
        );

        List<IndexableField> fields = configuredParser.parse(xContentParser);
        assertEquals(0, fields.size());
    }

    public void testNullValues() throws Exception {
        String input = "{ \"key\": null}";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(0, fields.size());

        xContentParser = createXContentParser(input);

        MappedFieldType fieldType = new FakeFieldType("field");
        FlattenedFieldParser configuredParser = new FlattenedFieldParser(
            "field",
            "field._keyed",
            fieldType,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            "placeholder"
        );

        fields = configuredParser.parse(xContentParser);
        assertEquals(2, fields.size());

        IndexableField field = fields.get(0);
        assertEquals("field", field.name());
        assertEquals(new BytesRef("placeholder"), field.binaryValue());

        IndexableField keyedField = fields.get(1);
        assertEquals("field._keyed", keyedField.name());
        assertEquals(new BytesRef("key\0placeholder"), keyedField.binaryValue());
    }

    public void testMalformedJson() throws Exception {
        String input = "{ \"key\": [true, false }";
        XContentParser xContentParser = createXContentParser(input);

        expectThrows(XContentParseException.class, () -> parser.parse(xContentParser));
    }

    public void testEmptyObject() throws Exception {
        String input = "{}";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(0, fields.size());
    }

    public void testRandomFields() throws Exception {
        BytesReference input = BytesReference.bytes(
            XContentBuilder.builder(JsonXContent.jsonXContent)
                .startObject()
                .startObject("object")
                .field("key", "value")
                .endObject()
                .startArray("array")
                .value(2.718)
                .endArray()
                .endObject()
        );

        input = XContentTestUtils.insertRandomFields(XContentType.JSON, input, null, random());
        XContentParser xContentParser = createXContentParser(input.utf8ToString());

        List<IndexableField> fields = parser.parse(xContentParser);
        assertTrue(fields.size() > 4);
    }

    public void testReservedCharacters() throws Exception {
        BytesReference input = BytesReference.bytes(
            XContentBuilder.builder(JsonXContent.jsonXContent).startObject().field("k\0y", "value").endObject()
        );
        XContentParser xContentParser = createXContentParser(input.utf8ToString());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parser.parse(xContentParser));
        assertEquals("Keys in [flattened] fields cannot contain the reserved character \\0. Offending key: [k\0y].", e.getMessage());
    }

    private XContentParser createXContentParser(String input) throws IOException {
        XContentParser xContentParser = createParser(JsonXContent.jsonXContent, input);
        xContentParser.nextToken();
        return xContentParser;
    }
}
