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
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        BytesRef data = new BytesRef(longBytes(10L, 20L, 30L, 40L, 50L, 60L));
        BytesRef seqNos = new BytesRef(new byte[6 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[6 * 8]);
        BytesRef versions = new BytesRef(new byte[6 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            6,
            seqNos,
            primaryTerms,
            versions,
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
        BytesRef seqNos = new BytesRef(new byte[6 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[6 * 8]);
        BytesRef versions = new BytesRef(new byte[6 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            6,
            seqNos,
            primaryTerms,
            versions,
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

    // -------------------------------------------------------------------------
    // withFilter — WindowedBinaryColumn
    // -------------------------------------------------------------------------

    private static Map<Integer, BytesRef> drainBinaryTuples(MappedColumns mc) {
        Map<Integer, BytesRef> result = new LinkedHashMap<>();
        ObjectTupleCursor<BytesRef> cursor = ((BinaryColumn) mc.toColumnBatch().columns().iterator().next()).tuples();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            result.put(doc, BytesRef.deepCopyOf(cursor.value()));
        }
        return result;
    }

    private static Map<Integer, BytesRef> drainBinaryRowCursor(MappedColumns mc) {
        Map<Integer, BytesRef> result = new LinkedHashMap<>();
        MappedColumns.RowCursor cursor = mc.rowCursor();
        int doc = 0;
        while (doc < mc.docCount()) {
            cursor.advance();
            List<IndexableField> fields = cursor.fields();
            if (fields.isEmpty() == false) {
                result.put(doc, BytesRef.deepCopyOf(fields.get(0).binaryValue()));
            }
            doc++;
        }
        return result;
    }

    private static Map<Integer, Long> drainLongTuples(MappedColumns mc) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        LongTupleCursor cursor = ((LongColumn) mc.toColumnBatch().columns().iterator().next()).tuples();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            result.put(doc, cursor.longValue());
        }
        return result;
    }

    public void testWithFilterBinaryColumnFiltersCorrectly() {
        // 4 docs "a".."d"; filter passes docs 1 and 3
        BytesRef[] values = { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") };
        BytesRef seqNos = new BytesRef(new byte[4 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[4 * 8]);
        BytesRef versions = new BytesRef(new byte[4 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            4,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        FixedBitSet filter = new FixedBitSet(4);
        filter.set(1);
        filter.set(3);
        MappedColumns filtered = mc.withFilter(filter);

        assertEquals(Map.of(1, new BytesRef("b"), 3, new BytesRef("d")), drainBinaryTuples(filtered));
        assertEquals(Map.of(1, new BytesRef("b"), 3, new BytesRef("d")), drainBinaryRowCursor(filtered));
    }

    public void testWithFilterBinaryColumnForcesSparse() {
        // All docs present with all-set filter — should still be SPARSE (filter non-null → SPARSE).
        BytesRef[] values = { new BytesRef("x"), new BytesRef("y") };
        BytesRef seqNos = new BytesRef(new byte[2 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[2 * 8]);
        BytesRef versions = new BytesRef(new byte[2 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            2,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        FixedBitSet filter = new FixedBitSet(2);
        filter.set(0);
        filter.set(1);
        MappedColumns filtered = mc.withFilter(filter);
        assertEquals(Column.Density.SPARSE, filtered.toColumnBatch().columns().iterator().next().density());
    }

    public void testWithFilterNullReturnsSelf() {
        BytesRef[] values = { new BytesRef("a") };
        BytesRef seqNos = new BytesRef(new byte[8]);
        BytesRef primaryTerms = new BytesRef(new byte[8]);
        BytesRef versions = new BytesRef(new byte[8]);
        MappedColumns mc = new MappedColumns(
            0,
            1,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );
        assertSame(mc, mc.withFilter(null));
    }

    public void testWithFilterLongColumn() {
        // 5 docs with values 10..50; filter passes docs 0 and 2
        BytesRef data = new BytesRef(longBytes(10L, 20L, 30L, 40L, 50L));
        BytesRef seqNos = new BytesRef(new byte[5 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[5 * 8]);
        BytesRef versions = new BytesRef(new byte[5 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            5,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.longColumn(data, "val", LONG_FIELD_TYPE, LongColumn.NumericKind.LONG))
        );

        FixedBitSet filter = new FixedBitSet(5);
        filter.set(0);
        filter.set(2);
        MappedColumns filtered = mc.withFilter(filter);

        assertEquals(Map.of(0, 10L, 2, 30L), drainLongTuples(filtered));
    }

    public void testWithFilterSlicePreservesFilter() {
        // 6 docs "a".."f"; filter passes {1, 3, 5}; slice [2, 6) → filter windowed to {1, 3}.
        BytesRef[] values = {
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("c"),
            new BytesRef("d"),
            new BytesRef("e"),
            new BytesRef("f") };
        BytesRef seqNos = new BytesRef(new byte[6 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[6 * 8]);
        BytesRef versions = new BytesRef(new byte[6 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            6,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        FixedBitSet filter = new FixedBitSet(6);
        filter.set(1);
        filter.set(3);
        filter.set(5);
        // slice(2, 6) → docs 2,3,4,5 become local 0,1,2,3; only original docs 3 and 5 pass filter
        MappedColumns sliced = mc.withFilter(filter).slice(2, 6);

        assertEquals(Map.of(1, new BytesRef("d"), 3, new BytesRef("f")), drainBinaryTuples(sliced));
    }

    public void testWithFilterAllSetWindowBecomesNull() {
        // All 4 docs present; filter passes all 4; windowed to slice [1, 3) — both set → filter becomes null.
        BytesRef[] values = { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") };
        BytesRef seqNos = new BytesRef(new byte[4 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[4 * 8]);
        BytesRef versions = new BytesRef(new byte[4 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            4,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        FixedBitSet filter = new FixedBitSet(4);
        filter.set(0);
        filter.set(1);
        filter.set(2);
        filter.set(3);
        // slice(1, 3) covers docs 1,2 — both set in filter → windowFilter returns null → DENSE
        MappedColumns sliced = mc.withFilter(filter).slice(1, 3);
        assertEquals(Column.Density.DENSE, sliced.toColumnBatch().columns().iterator().next().density());
        assertEquals(Map.of(0, new BytesRef("b"), 1, new BytesRef("c")), drainBinaryTuples(sliced));
    }

    public void testSliceOfSliceSeqNoOffset() {
        BytesRef seqNos = new BytesRef(new byte[6 * 8]); // zero-initialised
        BytesRef primaryTerms = new BytesRef(new byte[6 * 8]);
        BytesRef versions = new BytesRef(new byte[6 * 8]);
        MappedColumns mc = new MappedColumns(0, 6, seqNos, primaryTerms, versions, List.of());

        // [2, 6) then [1, 3) → offset = 3 in the backing array
        MappedColumns sliced = mc.slice(2, 6).slice(1, 3);
        sliced.setSeqNo(0, 100L);
        sliced.setSeqNo(1, 200L);

        assertEquals(100L, ByteUtils.readLongLE(seqNos.bytes, 3 * 8));
        assertEquals(200L, ByteUtils.readLongLE(seqNos.bytes, 4 * 8));
        // neighbours must be untouched
        assertEquals(0L, ByteUtils.readLongLE(seqNos.bytes, 2 * 8));
        assertEquals(0L, ByteUtils.readLongLE(seqNos.bytes, 5 * 8));
    }
}
