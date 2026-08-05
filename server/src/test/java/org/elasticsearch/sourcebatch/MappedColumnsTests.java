/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class MappedColumnsTests extends ESTestCase {

    private static final FieldType LONG_FIELD_TYPE;
    private static final FieldType BINARY_FIELD_TYPE;

    static {
        LONG_FIELD_TYPE = new FieldType();
        LONG_FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        LONG_FIELD_TYPE.freeze();

        BINARY_FIELD_TYPE = new FieldType();
        BINARY_FIELD_TYPE.setDocValuesType(DocValuesType.BINARY);
        BINARY_FIELD_TYPE.freeze();
    }

    private static byte[] longBytes(long... values) {
        byte[] buf = new byte[values.length * 8];
        for (int i = 0; i < values.length; i++) {
            ByteUtils.writeLongLE(values[i], buf, i * 8);
        }
        return buf;
    }

    public void testSliceOfSliceLongColumn() {
        // 6 docs with values 10..60; double-slice down to original rows 3 and 4 (values 40, 50).
        byte[] data = longBytes(10L, 20L, 30L, 40L, 50L, 60L);
        MappedColumns mc = new MappedColumns(
            0,
            6,
            null,
            null,
            null,
            List.of(MappedColumns.longColumn(data, "val", LONG_FIELD_TYPE, LongColumn.NumericKind.LONG))
        );

        // [2, 6) → rows 2..5; then [1, 3) of that → original rows 3..4
        MappedColumns sliced = mc.slice(2, 6).slice(1, 3);
        assertEquals(2, sliced.docCount());

        MappedColumns.RowCursor cursor = sliced.rowCursor();
        cursor.advance();
        assertEquals(40L, cursor.fields().get(0).numericValue().longValue());
        cursor.advance();
        assertEquals(50L, cursor.fields().get(0).numericValue().longValue());
    }

    public void testSliceOfSliceBinaryColumn() {
        // 6 docs with values "a".."f"; double-slice down to original rows 2 and 3 ("c", "d").
        BytesRef[] values = {
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("c"),
            new BytesRef("d"),
            new BytesRef("e"),
            new BytesRef("f") };
        MappedColumns mc = new MappedColumns(
            0,
            6,
            null,
            null,
            null,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        // [1, 5) → rows 1..4; then [1, 3) of that → original rows 2..3
        MappedColumns sliced = mc.slice(1, 5).slice(1, 3);
        assertEquals(2, sliced.docCount());

        MappedColumns.RowCursor cursor = sliced.rowCursor();
        cursor.advance();
        assertEquals(new BytesRef("c"), cursor.fields().get(0).binaryValue());
        cursor.advance();
        assertEquals(new BytesRef("d"), cursor.fields().get(0).binaryValue());
    }

    public void testSliceOfSliceSeqNoOffset() {
        byte[] seqNos = new byte[6 * 8]; // zero-initialised
        MappedColumns mc = new MappedColumns(0, 6, seqNos, null, null, List.of());

        // [2, 6) then [1, 3) → offset = 3 in the backing array
        MappedColumns sliced = mc.slice(2, 6).slice(1, 3);
        sliced.setSeqNo(0, 100L);
        sliced.setSeqNo(1, 200L);

        assertEquals(100L, ByteUtils.readLongLE(seqNos, 3 * 8));
        assertEquals(200L, ByteUtils.readLongLE(seqNos, 4 * 8));
        // neighbours must be untouched
        assertEquals(0L, ByteUtils.readLongLE(seqNos, 2 * 8));
        assertEquals(0L, ByteUtils.readLongLE(seqNos, 5 * 8));
    }
}
