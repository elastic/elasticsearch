/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

public class ByteRegionInputStreamTests extends ESTestCase {

    public void testConstructorRejectsNullBuf() {
        expectThrows(IllegalArgumentException.class, () -> new ByteRegionInputStream(null, 0, 0));
    }

    public void testConstructorRejectsNegativeOffset() {
        expectThrows(IllegalArgumentException.class, () -> new ByteRegionInputStream(new byte[4], -1, 1));
    }

    public void testConstructorRejectsNegativeLength() {
        expectThrows(IllegalArgumentException.class, () -> new ByteRegionInputStream(new byte[4], 0, -1));
    }

    public void testConstructorRejectsOverflowingOffsetPlusLength() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ByteRegionInputStream(new byte[16], Integer.MAX_VALUE - 8, Integer.MAX_VALUE - 8)
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("overflows int"));
    }

    public void testConstructorRejectsRegionPastArrayEnd() {
        expectThrows(IllegalArgumentException.class, () -> new ByteRegionInputStream(new byte[8], 3, 6));
    }

    public void testEmptyRegionAtOffset() {
        byte[] data = new byte[8];
        ByteRegionInputStream stream = new ByteRegionInputStream(data, 3, 0);
        assertSame(data, stream.array());
        assertEquals(3, stream.position());
        assertEquals(3, stream.limit());
    }
}
