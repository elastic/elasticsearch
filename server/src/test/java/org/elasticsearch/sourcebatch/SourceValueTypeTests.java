/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.test.ESTestCase;

public class SourceValueTypeTests extends ESTestCase {

    public void testFixedSizeZeroByte() {
        assertEquals(0, SourceValueType.fixedSize(SourceValueType.ABSENT, true));
        assertEquals(0, SourceValueType.fixedSize(SourceValueType.NULL, true));
        assertEquals(0, SourceValueType.fixedSize(SourceValueType.TRUE, true));
        assertEquals(0, SourceValueType.fixedSize(SourceValueType.FALSE, true));
        assertEquals(0, SourceValueType.fixedSize(SourceValueType.ABSENT, false));
        assertEquals(0, SourceValueType.fixedSize(SourceValueType.NULL, false));
        assertEquals(0, SourceValueType.fixedSize(SourceValueType.TRUE, false));
        assertEquals(0, SourceValueType.fixedSize(SourceValueType.FALSE, false));
    }

    public void testFixedSizeSmallRow() {
        // Small row: types 0x04-0x0A → 4 bytes, 0x0B-0x0C → 8 bytes
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.INT, true));
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.FLOAT, true));
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.STRING, true));
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.BINARY, true));
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.UNION_ARRAY, true));
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.FIXED_ARRAY, true));
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.KEY_VALUE, true));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.LONG, true));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.DOUBLE, true));
    }

    public void testFixedSizeLargeRow() {
        // Large row: types 0x04-0x05 → 4 bytes, 0x06-0x0C → 8 bytes
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.INT, false));
        assertEquals(4, SourceValueType.fixedSize(SourceValueType.FLOAT, false));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.STRING, false));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.BINARY, false));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.UNION_ARRAY, false));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.FIXED_ARRAY, false));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.KEY_VALUE, false));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.LONG, false));
        assertEquals(8, SourceValueType.fixedSize(SourceValueType.DOUBLE, false));
    }

    public void testIsVariable() {
        assertTrue(SourceValueType.isVariable(SourceValueType.STRING));
        assertTrue(SourceValueType.isVariable(SourceValueType.BINARY));
        assertTrue(SourceValueType.isVariable(SourceValueType.UNION_ARRAY));
        assertTrue(SourceValueType.isVariable(SourceValueType.FIXED_ARRAY));
        assertTrue(SourceValueType.isVariable(SourceValueType.KEY_VALUE));
        assertFalse(SourceValueType.isVariable(SourceValueType.INT));
        assertFalse(SourceValueType.isVariable(SourceValueType.FLOAT));
        assertFalse(SourceValueType.isVariable(SourceValueType.LONG));
        assertFalse(SourceValueType.isVariable(SourceValueType.DOUBLE));
        assertFalse(SourceValueType.isVariable(SourceValueType.ABSENT));
        assertFalse(SourceValueType.isVariable(SourceValueType.NULL));
        assertFalse(SourceValueType.isVariable(SourceValueType.TRUE));
        assertFalse(SourceValueType.isVariable(SourceValueType.FALSE));
    }

    public void testIsCompound() {
        assertTrue(SourceValueType.isCompound(SourceValueType.KEY_VALUE));
        assertTrue(SourceValueType.isCompound(SourceValueType.UNION_ARRAY));
        assertTrue(SourceValueType.isCompound(SourceValueType.FIXED_ARRAY));
        assertFalse(SourceValueType.isCompound(SourceValueType.STRING));
        assertFalse(SourceValueType.isCompound(SourceValueType.INT));
    }

    public void testNameForAllTypes() {
        assertEquals("ABSENT", SourceValueType.name(SourceValueType.ABSENT));
        assertEquals("NULL", SourceValueType.name(SourceValueType.NULL));
        assertEquals("TRUE", SourceValueType.name(SourceValueType.TRUE));
        assertEquals("FALSE", SourceValueType.name(SourceValueType.FALSE));
        assertEquals("INT", SourceValueType.name(SourceValueType.INT));
        assertEquals("FLOAT", SourceValueType.name(SourceValueType.FLOAT));
        assertEquals("STRING", SourceValueType.name(SourceValueType.STRING));
        assertEquals("BINARY", SourceValueType.name(SourceValueType.BINARY));
        assertEquals("UNION_ARRAY", SourceValueType.name(SourceValueType.UNION_ARRAY));
        assertEquals("FIXED_ARRAY", SourceValueType.name(SourceValueType.FIXED_ARRAY));
        assertEquals("KEY_VALUE", SourceValueType.name(SourceValueType.KEY_VALUE));
        assertEquals("LONG", SourceValueType.name(SourceValueType.LONG));
        assertEquals("DOUBLE", SourceValueType.name(SourceValueType.DOUBLE));
    }

    public void testNameForUnknownType() {
        String name = SourceValueType.name((byte) 0xFF);
        assertTrue(name.startsWith("UNKNOWN"));
    }

    public void testElemDataSize() {
        assertEquals(0, SourceValueType.elemDataSize(SourceValueType.ABSENT));
        assertEquals(0, SourceValueType.elemDataSize(SourceValueType.NULL));
        assertEquals(0, SourceValueType.elemDataSize(SourceValueType.TRUE));
        assertEquals(0, SourceValueType.elemDataSize(SourceValueType.FALSE));
        assertEquals(4, SourceValueType.elemDataSize(SourceValueType.INT));
        assertEquals(4, SourceValueType.elemDataSize(SourceValueType.FLOAT));
        assertEquals(8, SourceValueType.elemDataSize(SourceValueType.LONG));
        assertEquals(8, SourceValueType.elemDataSize(SourceValueType.DOUBLE));
        assertEquals(-1, SourceValueType.elemDataSize(SourceValueType.STRING));
        assertEquals(-1, SourceValueType.elemDataSize(SourceValueType.KEY_VALUE));
        assertEquals(-1, SourceValueType.elemDataSize(SourceValueType.UNION_ARRAY));
        assertEquals(-1, SourceValueType.elemDataSize(SourceValueType.FIXED_ARRAY));
    }
}
