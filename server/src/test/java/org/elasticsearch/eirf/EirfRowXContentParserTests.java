/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EirfRowXContentParserTests extends ESTestCase {

    public void testSimpleFlatDocument() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("title", "hello");
            builder.setInt("count", 42);
            builder.setBoolean("active", true);
            builder.endDocument();

            try (EirfBatch batch = builder.build()) {
                EirfRowReader row = batch.getRowReader(0);
                EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
                try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                    assertToken(parser, Token.START_OBJECT);
                    assertFieldName(parser, "title");
                    assertToken(parser, Token.VALUE_STRING);
                    assertEquals("hello", parser.text());
                    assertFieldName(parser, "count");
                    assertToken(parser, Token.VALUE_NUMBER);
                    assertEquals(42, parser.intValue());
                    assertFieldName(parser, "active");
                    assertToken(parser, Token.VALUE_BOOLEAN);
                    assertTrue(parser.booleanValue());
                    assertToken(parser, Token.END_OBJECT);
                    assertNull(parser.nextToken());
                }
            }
        }
    }

    public void testNestedObject() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("user.name", "alice");
            builder.setInt("user.age", 30);
            builder.setString("status", "ok");
            builder.endDocument();

            try (EirfBatch batch = builder.build()) {
                EirfRowReader row = batch.getRowReader(0);
                EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
                try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                    assertToken(parser, Token.START_OBJECT);
                    assertFieldName(parser, "user");
                    assertToken(parser, Token.START_OBJECT);
                    assertFieldName(parser, "name");
                    assertToken(parser, Token.VALUE_STRING);
                    assertEquals("alice", parser.text());
                    assertFieldName(parser, "age");
                    assertToken(parser, Token.VALUE_NUMBER);
                    assertEquals(30, parser.intValue());
                    assertToken(parser, Token.END_OBJECT); // close user
                    assertFieldName(parser, "status");
                    assertToken(parser, Token.VALUE_STRING);
                    assertEquals("ok", parser.text());
                    assertToken(parser, Token.END_OBJECT); // close root
                    assertNull(parser.nextToken());
                }
            }
        }
    }

    public void testNullColumnsSkipped() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            // First doc sets schema with 3 columns
            builder.startDocument();
            builder.setString("a", "val");
            builder.setInt("b", 1);
            builder.setString("c", "end");
            builder.endDocument();

            // Second doc only sets a and c (b is null)
            builder.startDocument();
            builder.setString("a", "hello");
            builder.setString("c", "world");
            builder.endDocument();

            try (EirfBatch batch = builder.build()) {
                EirfRowReader row = batch.getRowReader(1);
                EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
                try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                    assertToken(parser, Token.START_OBJECT);
                    assertFieldName(parser, "a");
                    assertToken(parser, Token.VALUE_STRING);
                    assertEquals("hello", parser.text());
                    // b is null, should be skipped
                    assertFieldName(parser, "c");
                    assertToken(parser, Token.VALUE_STRING);
                    assertEquals("world", parser.text());
                    assertToken(parser, Token.END_OBJECT);
                    assertNull(parser.nextToken());
                }
            }
        }
    }

    public void testAllScalarTypes() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setInt("i", 123);
            builder.setLong("l", 9876543210L);
            builder.setFloat("f", 1.5f);
            builder.setDouble("d", 3.14);
            builder.setString("s", "text");
            builder.setBoolean("bt", true);
            builder.setBoolean("bf", false);
            builder.endDocument();

            try (EirfBatch batch = builder.build()) {
                EirfRowReader row = batch.getRowReader(0);
                EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
                try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                    assertToken(parser, Token.START_OBJECT);

                    assertFieldName(parser, "i");
                    assertToken(parser, Token.VALUE_NUMBER);
                    assertEquals(XContentParser.NumberType.INT, parser.numberType());
                    assertEquals(123, parser.intValue());

                    assertFieldName(parser, "l");
                    assertToken(parser, Token.VALUE_NUMBER);
                    assertEquals(XContentParser.NumberType.LONG, parser.numberType());
                    assertEquals(9876543210L, parser.longValue());

                    assertFieldName(parser, "f");
                    assertToken(parser, Token.VALUE_NUMBER);
                    assertEquals(XContentParser.NumberType.FLOAT, parser.numberType());
                    assertEquals(1.5f, parser.floatValue(), 0.001f);

                    assertFieldName(parser, "d");
                    assertToken(parser, Token.VALUE_NUMBER);
                    assertEquals(XContentParser.NumberType.DOUBLE, parser.numberType());
                    assertEquals(3.14, parser.doubleValue(), 0.001);

                    assertFieldName(parser, "s");
                    assertToken(parser, Token.VALUE_STRING);
                    assertEquals("text", parser.text());

                    assertFieldName(parser, "bt");
                    assertToken(parser, Token.VALUE_BOOLEAN);
                    assertTrue(parser.booleanValue());

                    assertFieldName(parser, "bf");
                    assertToken(parser, Token.VALUE_BOOLEAN);
                    assertFalse(parser.booleanValue());

                    assertToken(parser, Token.END_OBJECT);
                }
            }
        }
    }

    public void testMapParsing() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("title", "test");
            builder.setInt("count", 7);
            builder.endDocument();

            try (EirfBatch batch = builder.build()) {
                EirfRowReader row = batch.getRowReader(0);
                EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
                try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                    Map<String, Object> map = parser.map();
                    assertEquals("test", map.get("title"));
                    assertEquals(7, map.get("count"));
                }
            }
        }
    }

    public void testArrayOfObjects() throws IOException {
        // {"items": [{"name": "a", "val": 1}, {"name": "b", "val": 2}]}
        BytesReference source = new BytesArray("""
            {"items": [{"name": "a", "val": 1}, {"name": "b", "val": 2}]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            EirfRowReader row = batch.getRowReader(0);
            EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
            try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "items");
                assertToken(parser, Token.START_ARRAY);

                // First object
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "name");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("a", parser.text());
                assertFieldName(parser, "val");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(1, parser.intValue());
                assertToken(parser, Token.END_OBJECT);

                // Second object
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "name");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("b", parser.text());
                assertFieldName(parser, "val");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(2, parser.intValue());
                assertToken(parser, Token.END_OBJECT);

                assertToken(parser, Token.END_ARRAY);
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    public void testNestedArrays() throws IOException {
        // {"matrix": [[1, 2], [3, 4]]}
        BytesReference source = new BytesArray("""
            {"matrix": [[1, 2], [3, 4]]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            EirfRowReader row = batch.getRowReader(0);
            EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
            try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "matrix");
                assertToken(parser, Token.START_ARRAY);

                // First inner array
                assertToken(parser, Token.START_ARRAY);
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(1, parser.intValue());
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(2, parser.intValue());
                assertToken(parser, Token.END_ARRAY);

                // Second inner array
                assertToken(parser, Token.START_ARRAY);
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(3, parser.intValue());
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(4, parser.intValue());
                assertToken(parser, Token.END_ARRAY);

                assertToken(parser, Token.END_ARRAY);
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    public void testArrayOfObjectsWithNestedArrays() throws IOException {
        // {"items": [{"tags": ["x", "y"], "id": 1}]}
        BytesReference source = new BytesArray("""
            {"items": [{"tags": ["x", "y"], "id": 1}]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            EirfRowReader row = batch.getRowReader(0);
            EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
            try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "items");
                assertToken(parser, Token.START_ARRAY);

                // Object with nested array
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "tags");
                assertToken(parser, Token.START_ARRAY);
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("x", parser.text());
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("y", parser.text());
                assertToken(parser, Token.END_ARRAY);
                assertFieldName(parser, "id");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(1, parser.intValue());
                assertToken(parser, Token.END_OBJECT);

                assertToken(parser, Token.END_ARRAY);
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testArrayOfObjectsMapParsing() throws IOException {
        // End-to-end: parse array-of-objects via map() and verify structure
        BytesReference source = new BytesArray("""
            {"items": [{"k": "a", "v": 1}, {"k": "b", "v": 2}], "count": 2}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            EirfRowReader row = batch.getRowReader(0);
            EirfRowXContentParser.SchemaNode tree = EirfRowXContentParser.buildSchemaTree(batch.schema());
            try (EirfRowXContentParser parser = new EirfRowXContentParser(tree, row)) {
                Map<String, Object> map = parser.map();
                assertEquals(2, map.get("count"));
                List<Map<String, Object>> items = (List<Map<String, Object>>) map.get("items");
                assertEquals(2, items.size());
                assertEquals("a", items.get(0).get("k"));
                assertEquals(1, items.get(0).get("v"));
                assertEquals("b", items.get(1).get("k"));
                assertEquals(2, items.get(1).get("v"));
            }
        }
    }

    private static void assertToken(EirfRowXContentParser parser, Token expected) throws IOException {
        assertEquals(expected, parser.nextToken());
    }

    private static void assertFieldName(EirfRowXContentParser parser, String expected) throws IOException {
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals(expected, parser.currentName());
    }
}
