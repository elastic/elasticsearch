/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

/**
 * Unit tests for {@link LuceneLongColumn.Builder} and {@link LuceneLongColumn.Spec}. Verifies that
 * the builder selects the correct {@link IndexableFieldType} for every combination of
 * {@link IndexType}, {@code indexed}, {@code stored}, and {@link LongColumn.NumericKind}.
 */
public class LuceneLongColumnBuilderTests extends ESTestCase {

    /** A minimal one-doc long column — just enough to call {@link LuceneLongColumn.Spec#build}. */
    private static EscfColumnData minimalLongData() {
        EscfColumnBuilder builder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE, BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setLong(0, 42L);
        return builder.finish(1);
    }

    /** Returns a stored variant of {@code base} — mirrors the private helper in {@link LuceneLongColumn}. */
    private static IndexableFieldType storedVariant(IndexableFieldType base) {
        FieldType ft = new FieldType(base);
        ft.setStored(true);
        ft.freeze();
        return ft;
    }

    // ---- name and null-check ----

    public void testNamePropagates() {
        LuceneLongColumn col = LuceneLongColumn.builder()
            .name("my_field")
            .indexType(IndexType.docValuesOnly())
            .build()
            .build(minimalLongData());
        assertEquals("my_field", col.name());
    }

    public void testMissingNameThrows() {
        expectThrows(NullPointerException.class, () -> LuceneLongColumn.builder().indexType(IndexType.docValuesOnly()).build());
    }

    // ---- numericKind ----

    public void testNumericKindDefaultsToLong() {
        LuceneLongColumn col = LuceneLongColumn.builder().name("f").indexType(IndexType.docValuesOnly()).build().build(minimalLongData());
        assertEquals(LongColumn.NumericKind.LONG, col.numericKind());
    }

    public void testNumericKindPropagates() {
        for (LongColumn.NumericKind kind : LongColumn.NumericKind.values()) {
            LuceneLongColumn col = LuceneLongColumn.builder()
                .name("f")
                .indexType(IndexType.docValuesOnly())
                .numericKind(kind)
                .build()
                .build(minimalLongData());
            assertEquals(kind, col.numericKind());
        }
    }

    // ---- stored-only path (indexType == null) ----

    public void testStoredOnlyLong() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .stored(true)
            .numericKind(LongColumn.NumericKind.LONG)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(new StoredField("_", 0L).fieldType(), ft);
        assertStoredOnly(ft);
    }

    public void testStoredOnlyInt() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .stored(true)
            .numericKind(LongColumn.NumericKind.INT)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(new StoredField("_", 0).fieldType(), ft);
        assertStoredOnly(ft);
    }

    public void testStoredOnlyFloat() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .stored(true)
            .numericKind(LongColumn.NumericKind.FLOAT)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(new StoredField("_", 0f).fieldType(), ft);
        assertStoredOnly(ft);
    }

    public void testStoredOnlyDouble() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .stored(true)
            .numericKind(LongColumn.NumericKind.DOUBLE)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(new StoredField("_", 0.0).fieldType(), ft);
        assertStoredOnly(ft);
    }

    // ---- doc-values-skipper path (indexType.hasDocValuesSkipper() == true) ----

    public void testDocValuesSkipperNotStored() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.skippers())
            .stored(false)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(SortedNumericDocValuesField.indexedField("_", 0L).fieldType(), ft);
        assertFalse(ft.stored());
        assertEquals(DocValuesType.SORTED_NUMERIC, ft.docValuesType());
        assertNotEquals(DocValuesSkipIndexType.NONE, ft.docValuesSkipIndexType());
        assertEquals(0, ft.pointDimensionCount());
    }

    public void testDocValuesSkipperStored() {
        IndexableFieldType expected = storedVariant(SortedNumericDocValuesField.indexedField("_", 0L).fieldType());
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.skippers())
            .stored(true)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(expected, ft);
        assertTrue(ft.stored());
        assertEquals(DocValuesType.SORTED_NUMERIC, ft.docValuesType());
        assertNotEquals(DocValuesSkipIndexType.NONE, ft.docValuesSkipIndexType());
        assertEquals(0, ft.pointDimensionCount());
    }

    // ---- indexed (BKD + DV) path ----

    public void testIndexedLong() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.points(true, true))
            .indexed(true)
            .numericKind(LongColumn.NumericKind.LONG)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(new LongField("_", 0L, Field.Store.NO).fieldType(), ft);
        assertFalse(ft.stored());
        assertEquals(DocValuesType.SORTED_NUMERIC, ft.docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, ft.docValuesSkipIndexType());
        assertEquals(1, ft.pointDimensionCount());
        assertEquals(Long.BYTES, ft.pointNumBytes());
    }

    public void testIndexedInt() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.points(true, true))
            .indexed(true)
            .numericKind(LongColumn.NumericKind.INT)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(new IntField("_", 0, Field.Store.NO).fieldType(), ft);
        assertFalse(ft.stored());
        assertEquals(1, ft.pointDimensionCount());
        assertEquals(Integer.BYTES, ft.pointNumBytes());
    }

    public void testIndexedFloat() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.points(true, true))
            .indexed(true)
            .numericKind(LongColumn.NumericKind.FLOAT)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(new FloatField("_", 0f, Field.Store.NO).fieldType(), ft);
        assertFalse(ft.stored());
        assertEquals(1, ft.pointDimensionCount());
        assertEquals(Integer.BYTES, ft.pointNumBytes());
    }

    public void testIndexedDouble() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.points(true, true))
            .indexed(true)
            .numericKind(LongColumn.NumericKind.DOUBLE)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(new DoubleField("_", 0.0, Field.Store.NO).fieldType(), ft);
        assertFalse(ft.stored());
        assertEquals(1, ft.pointDimensionCount());
        assertEquals(Long.BYTES, ft.pointNumBytes());
    }

    public void testIndexedLongStored() {
        IndexableFieldType expected = storedVariant(new LongField("_", 0L, Field.Store.NO).fieldType());
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.points(true, true))
            .indexed(true)
            .stored(true)
            .numericKind(LongColumn.NumericKind.LONG)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(expected, ft);
        assertTrue(ft.stored());
        assertEquals(DocValuesType.SORTED_NUMERIC, ft.docValuesType());
        assertEquals(1, ft.pointDimensionCount());
        assertEquals(Long.BYTES, ft.pointNumBytes());
    }

    public void testIndexedIntStored() {
        IndexableFieldType expected = storedVariant(new IntField("_", 0, Field.Store.NO).fieldType());
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.points(true, true))
            .indexed(true)
            .stored(true)
            .numericKind(LongColumn.NumericKind.INT)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(expected, ft);
        assertTrue(ft.stored());
        assertEquals(1, ft.pointDimensionCount());
        assertEquals(Integer.BYTES, ft.pointNumBytes());
    }

    // ---- doc-values-only path (indexed=false, no DV skipper) ----

    public void testDocValuesOnly() {
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.docValuesOnly())
            .indexed(false)
            .stored(false)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(SortedNumericDocValuesField.TYPE, ft);
        assertFalse(ft.stored());
        assertEquals(DocValuesType.SORTED_NUMERIC, ft.docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, ft.docValuesSkipIndexType());
        assertEquals(0, ft.pointDimensionCount());
    }

    public void testDocValuesOnlyStored() {
        IndexableFieldType expected = storedVariant(SortedNumericDocValuesField.TYPE);
        IndexableFieldType ft = LuceneLongColumn.builder()
            .name("f")
            .indexType(IndexType.docValuesOnly())
            .indexed(false)
            .stored(true)
            .build()
            .build(minimalLongData())
            .fieldType();
        assertEquals(expected, ft);
        assertTrue(ft.stored());
        assertEquals(DocValuesType.SORTED_NUMERIC, ft.docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, ft.docValuesSkipIndexType());
        assertEquals(0, ft.pointDimensionCount());
    }

    // ---- Spec reuse ----

    /** The same {@link LuceneLongColumn.Spec} can be called multiple times without side effects. */
    public void testSpecIsReusable() {
        LuceneLongColumn.Spec spec = LuceneLongColumn.builder()
            .name("reused")
            .indexType(IndexType.docValuesOnly())
            .numericKind(LongColumn.NumericKind.LONG)
            .build();
        LuceneLongColumn col1 = spec.build(minimalLongData());
        LuceneLongColumn col2 = spec.build(minimalLongData());
        assertEquals(col1.name(), col2.name());
        assertEquals(col1.fieldType(), col2.fieldType());
        assertEquals(col1.numericKind(), col2.numericKind());
    }

    private static void assertStoredOnly(IndexableFieldType ft) {
        assertTrue(ft.stored());
        assertEquals(DocValuesType.NONE, ft.docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, ft.docValuesSkipIndexType());
        assertEquals(0, ft.pointDimensionCount());
    }
}
