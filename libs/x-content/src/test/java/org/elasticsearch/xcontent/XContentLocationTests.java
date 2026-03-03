/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.test.ESTestCase;

public class XContentLocationTests extends ESTestCase {

    public void testTwoArgConstructorDefaultsByteOffset() {
        XContentLocation loc = new XContentLocation(1, 5);
        assertEquals(1, loc.lineNumber());
        assertEquals(5, loc.columnNumber());
        assertEquals(-1L, loc.byteOffset());
    }

    public void testThreeArgConstructorPreservesAllFields() {
        XContentLocation loc = new XContentLocation(3, 10, 42L);
        assertEquals(3, loc.lineNumber());
        assertEquals(10, loc.columnNumber());
        assertEquals(42L, loc.byteOffset());
    }

    public void testUnknownHasByteOffsetMinusOne() {
        assertEquals(-1, XContentLocation.UNKNOWN.lineNumber());
        assertEquals(-1, XContentLocation.UNKNOWN.columnNumber());
        assertEquals(-1L, XContentLocation.UNKNOWN.byteOffset());
    }

    public void testEqualityIncludesByteOffset() {
        XContentLocation a = new XContentLocation(1, 1, 0L);
        XContentLocation b = new XContentLocation(1, 1, 99L);
        XContentLocation c = new XContentLocation(1, 1, 0L);
        assertNotEquals(a, b);
        assertEquals(a, c);
        assertEquals(a.hashCode(), c.hashCode());
    }

    public void testUndefinedHasZeroLineColumnAndMinusOneByteOffset() {
        assertEquals(0, XContentLocation.UNDEFINED.lineNumber());
        assertEquals(0, XContentLocation.UNDEFINED.columnNumber());
        assertEquals(-1L, XContentLocation.UNDEFINED.byteOffset());
    }

    public void testToStringOmitsByteOffset() {
        XContentLocation loc = new XContentLocation(5, 12, 100L);
        assertEquals("5:12", loc.toString());
    }
}
