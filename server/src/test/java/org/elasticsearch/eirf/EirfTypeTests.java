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

public class EirfTypeTests extends ESTestCase {

    public void testFixedSizeZeroByte() {
        assertEquals(0, EirfType.fixedSize(EirfType.ABSENT, true));
        assertEquals(0, EirfType.fixedSize(EirfType.NULL, true));
        assertEquals(0, EirfType.fixedSize(EirfType.TRUE, true));
        assertEquals(0, EirfType.fixedSize(EirfType.FALSE, true));
        assertEquals(0, EirfType.fixedSize(EirfType.ABSENT, false));
        assertEquals(0, EirfType.fixedSize(EirfType.NULL, false));
        assertEquals(0, EirfType.fixedSize(EirfType.TRUE, false));
        assertEquals(0, EirfType.fixedSize(EirfType.FALSE, false));
    }

    public void testFixedSizeSmallRow() {
        // Small row: types 0x04-0x0A → 4 bytes, 0x0B-0x0C → 8 bytes
        assertEquals(4, EirfType.fixedSize(EirfType.INT, true));
        assertEquals(4, EirfType.fixedSize(EirfType.FLOAT, true));
        assertEquals(4, EirfType.fixedSize(EirfType.STRING, true));
        assertEquals(4, EirfType.fixedSize(EirfType.BINARY, true));
        assertEquals(4, EirfType.fixedSize(EirfType.UNION_ARRAY, true));
        assertEquals(4, EirfType.fixedSize(EirfType.FIXED_ARRAY, true));
        assertEquals(4, EirfType.fixedSize(EirfType.KEY_VALUE, true));
        assertEquals(8, EirfType.fixedSize(EirfType.LONG, true));
        assertEquals(8, EirfType.fixedSize(EirfType.DOUBLE, true));
    }

    public void testFixedSizeLargeRow() {
        // Large row: types 0x04-0x05 → 4 bytes, 0x06-0x0C → 8 bytes
        assertEquals(4, EirfType.fixedSize(EirfType.INT, false));
        assertEquals(4, EirfType.fixedSize(EirfType.FLOAT, false));
        assertEquals(8, EirfType.fixedSize(EirfType.STRING, false));
        assertEquals(8, EirfType.fixedSize(EirfType.BINARY, false));
        assertEquals(8, EirfType.fixedSize(EirfType.UNION_ARRAY, false));
        assertEquals(8, EirfType.fixedSize(EirfType.FIXED_ARRAY, false));
        assertEquals(8, EirfType.fixedSize(EirfType.KEY_VALUE, false));
        assertEquals(8, EirfType.fixedSize(EirfType.LONG, false));
        assertEquals(8, EirfType.fixedSize(EirfType.DOUBLE, false));
    }

    public void testIsVariable() {
        assertTrue(EirfType.isVariable(EirfType.STRING));
        assertTrue(EirfType.isVariable(EirfType.BINARY));
        assertTrue(EirfType.isVariable(EirfType.UNION_ARRAY));
        assertTrue(EirfType.isVariable(EirfType.FIXED_ARRAY));
        assertTrue(EirfType.isVariable(EirfType.KEY_VALUE));
        assertFalse(EirfType.isVariable(EirfType.INT));
        assertFalse(EirfType.isVariable(EirfType.FLOAT));
        assertFalse(EirfType.isVariable(EirfType.LONG));
        assertFalse(EirfType.isVariable(EirfType.DOUBLE));
        assertFalse(EirfType.isVariable(EirfType.ABSENT));
        assertFalse(EirfType.isVariable(EirfType.NULL));
        assertFalse(EirfType.isVariable(EirfType.TRUE));
        assertFalse(EirfType.isVariable(EirfType.FALSE));
    }

    public void testIsCompound() {
        assertTrue(EirfType.isCompound(EirfType.KEY_VALUE));
        assertTrue(EirfType.isCompound(EirfType.UNION_ARRAY));
        assertTrue(EirfType.isCompound(EirfType.FIXED_ARRAY));
        assertFalse(EirfType.isCompound(EirfType.STRING));
        assertFalse(EirfType.isCompound(EirfType.INT));
    }

    public void testNameForAllTypes() {
        assertEquals("ABSENT", EirfType.name(EirfType.ABSENT));
        assertEquals("NULL", EirfType.name(EirfType.NULL));
        assertEquals("TRUE", EirfType.name(EirfType.TRUE));
        assertEquals("FALSE", EirfType.name(EirfType.FALSE));
        assertEquals("INT", EirfType.name(EirfType.INT));
        assertEquals("FLOAT", EirfType.name(EirfType.FLOAT));
        assertEquals("STRING", EirfType.name(EirfType.STRING));
        assertEquals("BINARY", EirfType.name(EirfType.BINARY));
        assertEquals("UNION_ARRAY", EirfType.name(EirfType.UNION_ARRAY));
        assertEquals("FIXED_ARRAY", EirfType.name(EirfType.FIXED_ARRAY));
        assertEquals("KEY_VALUE", EirfType.name(EirfType.KEY_VALUE));
        assertEquals("LONG", EirfType.name(EirfType.LONG));
        assertEquals("DOUBLE", EirfType.name(EirfType.DOUBLE));
    }

    public void testNameForUnknownType() {
        String name = EirfType.name((byte) 0xFF);
        assertTrue(name.startsWith("UNKNOWN"));
    }

    public void testElemDataSize() {
        assertEquals(0, EirfType.elemDataSize(EirfType.ABSENT));
        assertEquals(0, EirfType.elemDataSize(EirfType.NULL));
        assertEquals(0, EirfType.elemDataSize(EirfType.TRUE));
        assertEquals(0, EirfType.elemDataSize(EirfType.FALSE));
        assertEquals(4, EirfType.elemDataSize(EirfType.INT));
        assertEquals(4, EirfType.elemDataSize(EirfType.FLOAT));
        assertEquals(8, EirfType.elemDataSize(EirfType.LONG));
        assertEquals(8, EirfType.elemDataSize(EirfType.DOUBLE));
        assertEquals(-1, EirfType.elemDataSize(EirfType.STRING));
        assertEquals(-1, EirfType.elemDataSize(EirfType.KEY_VALUE));
        assertEquals(-1, EirfType.elemDataSize(EirfType.UNION_ARRAY));
        assertEquals(-1, EirfType.elemDataSize(EirfType.FIXED_ARRAY));
    }
}
