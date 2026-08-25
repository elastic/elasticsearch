/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KeyValueWriterTests extends ESTestCase {

    private static byte[] expectedKv(String json) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(json),
                XContentType.JSON
            )
        ) {
            parser.nextToken(); // START_OBJECT
            return SourceBatchEncodeHelper.serializeKeyValue(parser);
        }
    }

    private static void assertKvEquals(String json, byte[] actual) throws IOException {
        assertArrayEquals(expectedKv(json), actual);
    }

    public void testEmptyObjectPayload() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        assertKvEquals("{}", writer.toBytes());
    }

    public void testScalars() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeLongField("i", 42, true);
        writer.writeLongField("l", 10_000_000_000L, false);
        writer.writeDoubleField("d", 1.5, true);
        writer.writeStringField("s", "hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        writer.writeBooleanField("t", true);
        writer.writeBooleanField("f", false);
        writer.writeNullField("n");
        assertKvEquals("""
            {"i":42,"l":10000000000,"d":1.5,"s":"hello","t":true,"f":false,"n":null}""", writer.toBytes());
    }

    public void testNestedObjectField() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("outer");
        writer.writeLongField("inner", 100, true);
        writer.endObjectField();
        assertKvEquals("""
            {"outer":{"inner":100}}""", writer.toBytes());
    }

    public void testEmptyNestedObjectField() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeEmptyObjectField("empty");
        assertKvEquals("""
            {"empty":{}}""", writer.toBytes());
    }

    public void testNestedFixedArrayField() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        byte[] packed = SourceBatchEncodeHelper.packFixedArray(SourceValueType.INT, new long[] { 10, 20, 30 }, new Object[3], 3);
        writer.writeArrayField("nums", new SourceBatchEncodeHelper.PackedArray(SourceValueType.FIXED_ARRAY, packed));
        assertKvEquals("""
            {"nums":[10,20,30]}""", writer.toBytes());
    }

    public void testNestedUnionArrayField() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        byte[] packed = SourceBatchEncodeHelper.packUnionArray(
            new byte[] { SourceValueType.INT, SourceValueType.STRING },
            new long[] { 42, 0 },
            new Object[] { null, new org.elasticsearch.xcontent.XContentString.UTF8Bytes("hello".getBytes(StandardCharsets.UTF_8), 0, 5) },
            2
        );
        writer.writeArrayField("mixed", new SourceBatchEncodeHelper.PackedArray(SourceValueType.UNION_ARRAY, packed));
        assertKvEquals("""
            {"mixed":[42,"hello"]}""", writer.toBytes());
    }

    public void testArrayAndScalarFields() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        byte[] packed = SourceBatchEncodeHelper.packFixedArray(
            SourceValueType.STRING,
            new long[2],
            new Object[] {
                new org.elasticsearch.xcontent.XContentString.UTF8Bytes("a".getBytes(StandardCharsets.UTF_8), 0, 1),
                new org.elasticsearch.xcontent.XContentString.UTF8Bytes("b".getBytes(StandardCharsets.UTF_8), 0, 1) },
            2
        );
        writer.writeArrayField("tags", new SourceBatchEncodeHelper.PackedArray(SourceValueType.FIXED_ARRAY, packed));
        writer.writeLongField("n", 1, true);
        assertKvEquals("""
            {"tags":["a","b"],"n":1}""", writer.toBytes());
    }

    public void testDeepNestedObjectFields() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("l0");
        writer.beginObjectField("l1");
        writer.writeLongField("leaf", 1, true);
        writer.endObjectField();
        writer.endObjectField();
        assertKvEquals("""
            {"l0":{"l1":{"leaf":1}}}""", writer.toBytes());
    }

    public void testDoubleFieldUsesDoubleType() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeDoubleField("d", 3.14, false);
        assertKvEquals("""
            {"d":3.14}""", writer.toBytes());

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals("d", reader.key());
        assertEquals(SourceValueType.DOUBLE, reader.type());
        assertEquals(3.14, reader.doubleValue(), 0.0);
        assertFalse(reader.next());
    }

    public void testWriteNestedObjectField() throws IOException {
        KeyValueWriter inner = KeyValueWriter.forObjectPayload();
        inner.writeLongField("inner", 100, true);

        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeNestedObjectField("outer", inner.toBytes());
        assertKvEquals("""
            {"outer":{"inner":100}}""", writer.toBytes());
    }

    public void testUnionArrayWithNestedObjectElement() throws IOException {
        KeyValueWriter objectWriter = KeyValueWriter.forObjectPayload();
        objectWriter.writeLongField("a", 42, true);
        byte[] objectKv = objectWriter.toBytes();

        byte[] packed = SourceBatchEncodeHelper.packUnionArray(
            new byte[] { SourceValueType.KEY_VALUE },
            new long[1],
            new Object[] { objectKv },
            1
        );

        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeArrayField("items", new SourceBatchEncodeHelper.PackedArray(SourceValueType.UNION_ARRAY, packed));
        assertKvEquals("""
            {"items":[{"a":42}]}""", writer.toBytes());

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals("items", reader.key());
        InlineArrayReader arr = reader.nestedArray();
        assertTrue(arr.next());
        assertEquals(SourceValueType.KEY_VALUE, arr.type());
        KeyValueReader nested = arr.nestedKeyValue();
        assertTrue(nested.next());
        assertEquals("a", nested.key());
        assertEquals(42, nested.intValue());
        assertFalse(nested.next());
        assertFalse(arr.next());
        assertFalse(reader.next());
    }

    public void testMatchesSerializeKeyValueForComplexObject() throws IOException {
        String json = """
            {"str":"hello","nested":{"a":1},"tags":["x","y"],"after":2}""";
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeStringField("str", "hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        writer.beginObjectField("nested");
        writer.writeLongField("a", 1, true);
        writer.endObjectField();
        byte[] tagPacked = SourceBatchEncodeHelper.packFixedArray(
            SourceValueType.STRING,
            new long[2],
            new Object[] {
                new org.elasticsearch.xcontent.XContentString.UTF8Bytes("x".getBytes(StandardCharsets.UTF_8), 0, 1),
                new org.elasticsearch.xcontent.XContentString.UTF8Bytes("y".getBytes(StandardCharsets.UTF_8), 0, 1) },
            2
        );
        writer.writeArrayField("tags", new SourceBatchEncodeHelper.PackedArray(SourceValueType.FIXED_ARRAY, tagPacked));
        writer.writeLongField("after", 2, true);
        assertArrayEquals(expectedKv(json), writer.toBytes());
    }

    public void testWriterBytesEqualReaderRoundTrip() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("outer");
        writer.writeLongField("inner", 1, true);
        writer.endObjectField();
        byte[] arrPacked = SourceBatchEncodeHelper.packFixedArray(SourceValueType.INT, new long[] { 1, 2 }, new Object[2], 2);
        writer.writeArrayField("arr", new SourceBatchEncodeHelper.PackedArray(SourceValueType.FIXED_ARRAY, arrPacked));

        byte[] kv = writer.toBytes();
        KeyValueReader reader = new KeyValueReader(kv);
        assertTrue(reader.next());
        assertEquals("outer", reader.key());
        assertEquals(SourceValueType.KEY_VALUE, reader.type());
        KeyValueReader nested = reader.nestedKeyValue();
        assertTrue(nested.next());
        assertEquals("inner", nested.key());
        assertEquals(1, nested.intValue());
        assertFalse(nested.next());
        assertTrue(reader.next());
        assertEquals("arr", reader.key());
        InlineArrayReader arr = reader.nestedArray();
        assertTrue(arr.next());
        assertEquals(1, arr.intValue());
        assertTrue(arr.next());
        assertEquals(2, arr.intValue());
        assertFalse(arr.next());
        assertFalse(reader.next());
    }
}
