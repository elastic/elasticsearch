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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Verifies the ESCF columnar-list {@link ArrayReader} ({@link ColumnarArrayReader}) iterates a row's
 * array identically to the EIRF inline reader ({@link org.elasticsearch.sourcebatch.InlineArrayReader}) for the
 * same source document — same element types and values, in order.
 */
public class ArrayReaderTests extends ESTestCase {

    public void testLongArrayParity() throws IOException {
        assertReaderParity("{\"a\":[1,2,3,4]}", "a");
    }

    public void testDoubleArrayParity() throws IOException {
        assertReaderParity("{\"a\":[1.5,2.5,-3.25]}", "a");
    }

    public void testStringArrayParity() throws IOException {
        assertReaderParity("{\"a\":[\"x\",\"yy\",\"zzz\"]}", "a");
    }

    private static void assertReaderParity(String json, String path) throws IOException {
        List<BytesReference> sources = List.of(new BytesArray(json));
        try (
            SourceBatch escf = EscfEncoder.encode(sources, XContentType.JSON);
            SourceBatch eirf = EirfEncoder.encode(sources, XContentType.JSON)
        ) {
            ArrayReader escfReader = escf.row(0).getArrayValue(columnOf(escf, path));
            ArrayReader eirfReader = eirf.row(0).getArrayValue(columnOf(eirf, path));
            int count = 0;
            while (true) {
                boolean a = escfReader.next();
                boolean b = eirfReader.next();
                assertEquals("readers ended at different points after " + count + " elements", a, b);
                if (a == false) {
                    break;
                }
                count++;
                byte type = escfReader.type();
                // The EIRF reader may report the narrower INT/FLOAT element type; the ESCF columnar child
                // upcasts to LONG/DOUBLE. Compare on the upcast value, which is what reconstruction emits.
                switch (type) {
                    case SourceValueType.LONG -> assertEquals(escfReader.longValue(), eirfLong(eirfReader));
                    case SourceValueType.DOUBLE -> assertEquals(escfReader.doubleValue(), eirfDouble(eirfReader), 0.0);
                    case SourceValueType.STRING -> assertEquals(escfReader.stringValue(), eirfReader.stringValue());
                    default -> throw new AssertionError("unexpected ESCF array element type " + SourceValueType.name(type));
                }
            }
            assertTrue("expected a non-empty array", count > 0);
        }
    }

    private static long eirfLong(ArrayReader reader) {
        return reader.type() == SourceValueType.INT ? reader.intValue() : reader.longValue();
    }

    private static double eirfDouble(ArrayReader reader) {
        return reader.type() == SourceValueType.FLOAT ? reader.floatValue() : reader.doubleValue();
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
