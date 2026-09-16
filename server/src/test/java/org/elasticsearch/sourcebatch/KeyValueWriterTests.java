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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class KeyValueWriterTests extends ESTestCase {

    private static byte[] expectedKv(String json) throws IOException {
        return expectedKv(new BytesArray(json), XContentType.JSON);
    }

    private static byte[] expectedKv(BytesReference source, XContentType xContentType) throws IOException {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
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
        writer.writeIntField("i", 42);
        writer.writeLongField("l", 10_000_000_000L);
        writer.writeDoubleField("d", 1.5);
        writer.writeStringField("s", "hello".getBytes(UTF_8), 0, 5);
        writer.writeBooleanField("t", true);
        writer.writeBooleanField("f", false);
        writer.writeNullField("n");
        assertKvEquals("""
            {"i":42,"l":10000000000,"d":1.5,"s":"hello","t":true,"f":false,"n":null}""", writer.toBytes());
    }

    public void testNestedObjectField() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("outer");
        writer.writeIntField("inner", 100);
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
            new Object[] { null, new org.elasticsearch.xcontent.XContentString.UTF8Bytes("hello".getBytes(UTF_8), 0, 5) },
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
                new org.elasticsearch.xcontent.XContentString.UTF8Bytes("a".getBytes(UTF_8), 0, 1),
                new org.elasticsearch.xcontent.XContentString.UTF8Bytes("b".getBytes(UTF_8), 0, 1) },
            2
        );
        writer.writeArrayField("tags", new SourceBatchEncodeHelper.PackedArray(SourceValueType.FIXED_ARRAY, packed));
        writer.writeIntField("n", 1);
        assertKvEquals("""
            {"tags":["a","b"],"n":1}""", writer.toBytes());
    }

    public void testDeepNestedObjectFields() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("l0");
        writer.beginObjectField("l1");
        writer.writeIntField("leaf", 1);
        writer.endObjectField();
        writer.endObjectField();
        assertKvEquals("""
            {"l0":{"l1":{"leaf":1}}}""", writer.toBytes());
    }

    public void testDoubleFieldUsesDoubleType() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeDoubleField("d", 3.14);
        assertKvEquals("""
            {"d":3.14}""", writer.toBytes());

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals("d", reader.key());
        assertEquals(SourceValueType.DOUBLE, reader.type());
        assertEquals(3.14, reader.doubleValue(), 0.0);
        assertFalse(reader.next());
    }

    public void testFloatFieldUsesFloatType() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeFloatField("f", 3.14f);

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals("f", reader.key());
        assertEquals(SourceValueType.FLOAT, reader.type());
        assertEquals(3.14f, reader.floatValue(), 0.0f);
        assertFalse(reader.next());
    }

    public void testWriteNestedObjectField() throws IOException {
        KeyValueWriter inner = KeyValueWriter.forObjectPayload();
        inner.writeIntField("inner", 100);

        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeNestedObjectField("outer", inner.toBytes());
        assertKvEquals("""
            {"outer":{"inner":100}}""", writer.toBytes());
    }

    public void testUnionArrayWithNestedObjectElement() throws IOException {
        KeyValueWriter objectWriter = KeyValueWriter.forObjectPayload();
        objectWriter.writeIntField("a", 42);
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
        writer.writeStringField("str", "hello".getBytes(UTF_8), 0, 5);
        writer.beginObjectField("nested");
        writer.writeIntField("a", 1);
        writer.endObjectField();
        byte[] tagPacked = SourceBatchEncodeHelper.packFixedArray(
            SourceValueType.STRING,
            new long[2],
            new Object[] {
                new org.elasticsearch.xcontent.XContentString.UTF8Bytes("x".getBytes(UTF_8), 0, 1),
                new org.elasticsearch.xcontent.XContentString.UTF8Bytes("y".getBytes(UTF_8), 0, 1) },
            2
        );
        writer.writeArrayField("tags", new SourceBatchEncodeHelper.PackedArray(SourceValueType.FIXED_ARRAY, tagPacked));
        writer.writeIntField("after", 2);
        assertArrayEquals(expectedKv(json), writer.toBytes());
    }

    public void testWriterBytesEqualReaderRoundTrip() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("outer");
        writer.writeIntField("inner", 1);
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

    /**
     * {@link KeyValueWriter#writeLongField} emits {@link SourceValueType#LONG} even when the value
     * fits in an int; {@link KeyValueWriter#writeIntField} must be used explicitly for INT.
     */
    public void testWriteLongFieldDoesNotNarrowToInt() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeLongField("n", 42L);

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals("n", reader.key());
        assertEquals(SourceValueType.LONG, reader.type());
        assertEquals(42L, reader.longValue());
        assertFalse(reader.next());
    }

    /** Parser-reported LONG inside int range must not be narrowed to INT when serializing. */
    public void testSerializeKeyValuePreservesParserLongWidth() throws IOException {
        try (
            MapXContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                Map.of("n", 42L),
                XContentType.JSON
            )
        ) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            byte[] kv = SourceBatchEncodeHelper.serializeKeyValue(parser);
            KeyValueReader reader = new KeyValueReader(kv);
            assertTrue(reader.next());
            assertEquals("n", reader.key());
            assertEquals(SourceValueType.LONG, reader.type());
            assertEquals(42L, reader.longValue());
            assertFalse(reader.next());
        }
    }

    /** JSON floating-point literals are DOUBLE-typed; encode must not narrow to FLOAT. */
    public void testSerializeKeyValuePreservesParserDoubleWidth() throws IOException {
        KeyValueReader reader = new KeyValueReader(expectedKv("{\"n\":1.5}"));
        assertTrue(reader.next());
        assertEquals("n", reader.key());
        assertEquals(SourceValueType.DOUBLE, reader.type());
        assertEquals(1.5, reader.doubleValue(), 0.0);
        assertFalse(reader.next());
    }

    public void testWriteStringFieldWithSlice() throws IOException {
        byte[] buf = "xxhelloxx".getBytes(UTF_8);
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeStringField("s", buf, 2, 5);
        assertKvEquals("""
            {"s":"hello"}""", writer.toBytes());
    }

    public void testBeginEndEmptyNestedObjectField() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("empty");
        writer.endObjectField();
        assertKvEquals("""
            {"empty":{}}""", writer.toBytes());
        KeyValueWriter emptyFieldWriter = KeyValueWriter.forObjectPayload();
        emptyFieldWriter.writeEmptyObjectField("empty");
        assertArrayEquals(emptyFieldWriter.toBytes(), writer.toBytes());
    }

    /** Exercises nested-object stack growth beyond the initial capacity of four. */
    public void testDeepNestedObjectStackGrowth() throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("l0");
        writer.beginObjectField("l1");
        writer.beginObjectField("l2");
        writer.beginObjectField("l3");
        writer.beginObjectField("l4");
        writer.writeIntField("leaf", 7);
        writer.endObjectField();
        writer.endObjectField();
        writer.endObjectField();
        writer.endObjectField();
        writer.endObjectField();
        assertKvEquals("""
            {"l0":{"l1":{"l2":{"l3":{"l4":{"leaf":7}}}}}}""", writer.toBytes());
    }

    public void testEndObjectFieldWithoutBeginThrows() {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        expectThrows(IllegalStateException.class, writer::endObjectField);
    }

    public void testUnclosedNestedObjectToBytesThrows() {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.beginObjectField("outer");
        writer.writeIntField("inner", 1);
        expectThrows(IllegalStateException.class, writer::toBytes);
    }

    public void testWriteStringFieldWithOutOfBoundsSliceThrows() {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        byte[] buf = "abc".getBytes(UTF_8);
        expectThrows(IndexOutOfBoundsException.class, () -> writer.writeStringField("s", buf, 1, 5));
    }

    public void testWriteStringFieldWithZeroLengthSlice() throws IOException {
        byte[] buf = "hello".getBytes(UTF_8);
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeStringField("s", buf, 3, 0);
        assertKvEquals("""
            {"s":""}""", writer.toBytes());
    }

    public void testDuplicateKeysAreRetained() {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeIntField("dup", 1);
        writer.writeIntField("dup", 2);

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals("dup", reader.key());
        assertEquals(1, reader.intValue());
        assertTrue(reader.next());
        assertEquals("dup", reader.key());
        assertEquals(2, reader.intValue());
        assertFalse(reader.next());
    }

    public void testNonFiniteFloatAndDoubleRoundTripBitExact() {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeFloatField("nan", Float.NaN);
        writer.writeFloatField("inf", Float.POSITIVE_INFINITY);
        writer.writeDoubleField("dnan", Double.NaN);
        writer.writeDoubleField("ninf", Double.NEGATIVE_INFINITY);

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertTrue(Float.isNaN(reader.floatValue()));
        assertTrue(reader.next());
        assertEquals(Float.POSITIVE_INFINITY, reader.floatValue(), 0.0f);
        assertTrue(reader.next());
        assertTrue(Double.isNaN(reader.doubleValue()));
        assertTrue(reader.next());
        assertEquals(Double.NEGATIVE_INFINITY, reader.doubleValue(), 0.0);
        assertFalse(reader.next());
    }

    public void testIntegerExtremesRoundTrip() {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeIntField("imin", Integer.MIN_VALUE);
        writer.writeIntField("imax", Integer.MAX_VALUE);
        writer.writeLongField("lmin", Long.MIN_VALUE);
        writer.writeLongField("lmax", Long.MAX_VALUE);

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals(Integer.MIN_VALUE, reader.intValue());
        assertTrue(reader.next());
        assertEquals(Integer.MAX_VALUE, reader.intValue());
        assertTrue(reader.next());
        assertEquals(Long.MIN_VALUE, reader.longValue());
        assertTrue(reader.next());
        assertEquals(Long.MAX_VALUE, reader.longValue());
        assertFalse(reader.next());
    }

    public void testEmptyStringAndEmptyKeyRoundTrip() {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeStringField("", new byte[0], 0, 0);
        writer.writeNullField("");

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals("", reader.key());
        assertEquals(SourceValueType.STRING, reader.type());
        assertEquals("", reader.stringValue());
        assertTrue(reader.next());
        assertEquals("", reader.key());
        assertEquals(SourceValueType.NULL, reader.type());
        assertFalse(reader.next());
    }

    public void testUtf8KeyRoundTrip() {
        String key = "caf\u00e9";
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        writer.writeIntField(key, 7);

        KeyValueReader reader = new KeyValueReader(writer.toBytes());
        assertTrue(reader.next());
        assertEquals(key, reader.key());
        assertEquals(7, reader.intValue());
        assertFalse(reader.next());
    }
}
