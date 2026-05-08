/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

public class EirfArrayReaderTests extends ESTestCase {

    public void testEmptyUnionArray() {
        byte[] packed = EirfEncoder.packUnionArray(new byte[0], new long[0], new Object[0], 0);
        EirfArrayReader reader = new EirfArrayReader(packed, false);
        assertFalse(reader.next());
    }

    public void testEmptyFixedArray() {
        // Fixed array with 0 elements: empty byte array
        byte[] packed = new byte[0];
        EirfArrayReader reader = new EirfArrayReader(packed, true);
        assertFalse(reader.next());
    }

    public void testUnionArraySingleInt() {
        byte[] elemTypes = { EirfType.INT };
        long[] elemNumeric = { 42L };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemNumeric, new Object[1], 1);

        EirfArrayReader reader = new EirfArrayReader(packed, false);
        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(42, reader.intValue());
        assertFalse(reader.next());
    }

    public void testFixedArrayInts() {
        long[] elemNumeric = { 1L, 2L, 3L };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.INT, elemNumeric, new Object[3], 3);

        EirfArrayReader reader = new EirfArrayReader(packed, true);
        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(1, reader.intValue());
        assertTrue(reader.next());
        assertEquals(2, reader.intValue());
        assertTrue(reader.next());
        assertEquals(3, reader.intValue());
        assertFalse(reader.next());
    }

    public void testFixedArrayStrings() {
        byte[] utf8a = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] utf8b = "world".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Object[] elemVar = { new XContentString.UTF8Bytes(utf8a, 0, utf8a.length), new XContentString.UTF8Bytes(utf8b, 0, utf8b.length) };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.STRING, new long[2], elemVar, 2);

        EirfArrayReader reader = new EirfArrayReader(packed, true);
        assertTrue(reader.next());
        assertEquals("hello", reader.stringValue());
        assertTrue(reader.next());
        assertEquals("world", reader.stringValue());
        assertFalse(reader.next());
    }

    public void testUnionArrayMixedTypes() {
        byte[] utf8 = "world".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] elemTypes = { EirfType.INT, EirfType.STRING, EirfType.TRUE, EirfType.NULL, EirfType.FLOAT };
        long[] elemNumeric = new long[5];
        Object[] elemVar = new Object[5];
        elemNumeric[0] = 42L;
        elemVar[1] = new XContentString.UTF8Bytes(utf8, 0, utf8.length);
        // TRUE and NULL have no data
        elemNumeric[4] = Float.floatToRawIntBits(3.14f);

        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemNumeric, elemVar, 5);
        EirfArrayReader reader = new EirfArrayReader(packed, false);

        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(42, reader.intValue());

        assertTrue(reader.next());
        assertEquals(EirfType.STRING, reader.type());
        assertEquals("world", reader.stringValue());

        assertTrue(reader.next());
        assertEquals(EirfType.TRUE, reader.type());
        assertTrue(reader.booleanValue());

        assertTrue(reader.next());
        assertEquals(EirfType.NULL, reader.type());
        assertTrue(reader.isNull());

        assertTrue(reader.next());
        assertEquals(EirfType.FLOAT, reader.type());
        assertEquals(3.14f, reader.floatValue(), 0.001f);

        assertFalse(reader.next());
    }

    public void testBooleanValues() {
        byte[] elemTypes = { EirfType.TRUE, EirfType.FALSE };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, new long[2], new Object[2], 2);

        EirfArrayReader reader = new EirfArrayReader(packed, false);
        assertTrue(reader.next());
        assertTrue(reader.booleanValue());

        assertTrue(reader.next());
        assertFalse(reader.booleanValue());

        assertFalse(reader.next());
    }

    public void testWithOffset() {
        byte[] elemTypes = { EirfType.INT };
        long[] elemNumeric = { 99L };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemNumeric, new Object[1], 1);

        byte[] withPrefix = new byte[5 + packed.length];
        System.arraycopy(packed, 0, withPrefix, 5, packed.length);

        EirfArrayReader reader = new EirfArrayReader(withPrefix, 5, packed.length, false);
        assertTrue(reader.next());
        assertEquals(99, reader.intValue());
    }

    public void testFixedArrayLongs() {
        long[] elemNumeric = { Long.MAX_VALUE, Long.MIN_VALUE };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.LONG, elemNumeric, new Object[2], 2);

        EirfArrayReader reader = new EirfArrayReader(packed, true);
        assertTrue(reader.next());
        assertEquals(Long.MAX_VALUE, reader.longValue());
        assertTrue(reader.next());
        assertEquals(Long.MIN_VALUE, reader.longValue());
        assertFalse(reader.next());
    }

    public void testFixedArrayDoubles() {
        long[] elemNumeric = { Double.doubleToRawLongBits(3.14), Double.doubleToRawLongBits(-2.718) };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.DOUBLE, elemNumeric, new Object[2], 2);

        EirfArrayReader reader = new EirfArrayReader(packed, true);
        assertTrue(reader.next());
        assertEquals(3.14, reader.doubleValue(), 0.001);
        assertTrue(reader.next());
        assertEquals(-2.718, reader.doubleValue(), 0.001);
        assertFalse(reader.next());
    }

    public void testUnionArrayWithCompoundKeyValue() {
        // Create a KEY_VALUE element: key_length(i32 LE)=1, key="a", type=INT, value=42 (LE)
        byte[] kvPayload = new byte[] {
            1,
            0,
            0,
            0,      // key_length = 1 (i32 LE)
            'a',              // key bytes
            EirfType.INT,     // type
            42,
            0,
            0,
            0       // INT value = 42 (LE)
        };

        byte[] elemTypes = { EirfType.KEY_VALUE };
        Object[] elemVar = { kvPayload };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, new long[1], elemVar, 1);

        EirfArrayReader reader = new EirfArrayReader(packed, false);
        assertTrue(reader.next());
        assertEquals(EirfType.KEY_VALUE, reader.type());
        EirfKeyValueReader kv = reader.nestedKeyValue();
        assertTrue(kv.next());
        assertEquals("a", kv.key());
        assertEquals(EirfType.INT, kv.type());
        assertEquals(42, kv.intValue());
        assertFalse(kv.next());
        assertFalse(reader.next());
    }
}
