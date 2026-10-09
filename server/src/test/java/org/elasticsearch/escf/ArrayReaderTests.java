/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Verifies that the ESCF columnar-list {@link ArrayReader} ({@link ColumnarArrayReader}) iterates a
 * row's array with the correct element types and values, in order.
 */
public class ArrayReaderTests extends ESTestCase {

    public void testLongArray() throws IOException {
        try (SourceBatch batch = encode("{\"a\":[1,2,3,4]}")) {
            ArrayReader reader = batch.row(0).getArrayValue(columnOf(batch, "a"));
            assertLong(reader, 1L);
            assertLong(reader, 2L);
            assertLong(reader, 3L);
            assertLong(reader, 4L);
            assertFalse("expected exactly 4 elements", reader.next());
        }
    }

    public void testDoubleArray() throws IOException {
        try (SourceBatch batch = encode("{\"a\":[1.5,2.5,-3.25]}")) {
            ArrayReader reader = batch.row(0).getArrayValue(columnOf(batch, "a"));
            assertDouble(reader, 1.5);
            assertDouble(reader, 2.5);
            assertDouble(reader, -3.25);
            assertFalse("expected exactly 3 elements", reader.next());
        }
    }

    public void testStringArray() throws IOException {
        try (SourceBatch batch = encode("{\"a\":[\"x\",\"yy\",\"zzz\"]}")) {
            ArrayReader reader = batch.row(0).getArrayValue(columnOf(batch, "a"));
            assertString(reader, "x");
            assertString(reader, "yy");
            assertString(reader, "zzz");
            assertFalse("expected exactly 3 elements", reader.next());
        }
    }

    private static SourceBatch encode(String json) throws IOException {
        return EscfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON);
    }

    private static void assertLong(ArrayReader reader, long expected) {
        assertTrue("expected another element", reader.next());
        // ESCF columnar arrays store integers as LONG.
        assertEquals(SourceValueType.LONG, reader.type());
        assertEquals(expected, reader.longValue());
    }

    private static void assertDouble(ArrayReader reader, double expected) {
        assertTrue("expected another element", reader.next());
        // ESCF columnar arrays store floating-point values as DOUBLE.
        assertEquals(SourceValueType.DOUBLE, reader.type());
        assertEquals(expected, reader.doubleValue(), 0.0);
    }

    private static void assertString(ArrayReader reader, String expected) {
        assertTrue("expected another element", reader.next());
        assertEquals(SourceValueType.STRING, reader.type());
        assertEquals(expected, reader.stringValue());
    }

    private static int columnOf(SourceBatch batch, String path) {
        for (int i = 0; i < batch.columnCount(); i++) {
            if (batch.schema().getFullPath(i).equals(path)) {
                return i;
            }
        }
        throw new AssertionError("no column [" + path + "]");
    }
}
