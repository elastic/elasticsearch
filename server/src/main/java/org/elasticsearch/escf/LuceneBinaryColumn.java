/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;

/**
 * A {@link BinaryColumn} backed by an ESCF data column, bridging both the Lucene
 * {@code IndexWriter.addBatch} columnar path and the row-oriented soft-update path.
 *
 * <p>Two data shapes are supported through a single adaptor:
 * <ul>
 *   <li><b>Dense (STRING/BINARY)</b> — one byte-string value per row. Density is
 *       {@link Density#DENSE}. Create via {@link #stringColumn}.
 *   <li><b>Sparse array (ARRAY with a var-width child)</b> — zero or more byte-string elements
 *       per row; rows with no elements are skipped. Density is {@link Density#SPARSE}. Create
 *       via {@link #arrayColumn}.
 * </ul>
 *
 * <p>Both shapes expose iteration via {@link EscfColumn#bytesRefCursor()}: a dense cursor for
 * var-width columns, and an element-granular cursor that repeats the same row-id for multi-valued
 * documents and skips empty/absent rows for array columns.
 */
public final class LuceneBinaryColumn extends BinaryColumn implements LuceneColumn {

    private final EscfColumn data;

    private LuceneBinaryColumn(EscfColumn data, String name, IndexableFieldType fieldType, Density density) {
        super(name, fieldType, density);
        this.data = data;
    }

    /**
     * Creates a dense {@link LuceneBinaryColumn} backed by a STRING or BINARY ESCF column. Each
     * row in the column data maps to exactly one Lucene field.
     *
     * @param data      the ESCF column data; must have kind {@link EscfColumnKind#STRING} or
     *                  {@link EscfColumnKind#BINARY}
     * @param name      Lucene field name
     * @param fieldType Lucene field type
     */
    public static LuceneBinaryColumn stringColumn(EscfColumnData data, String name, IndexableFieldType fieldType) {
        return new LuceneBinaryColumn(EscfColumn.from(data), name, fieldType, Density.DENSE);
    }

    /**
     * Creates a sparse {@link LuceneBinaryColumn} backed by an ARRAY ESCF column whose child is a
     * var-width (STRING or BINARY) column. Multi-valued rows produce multiple elements; empty and
     * absent rows are skipped.
     *
     * @param data      the ESCF column data; must have kind {@link EscfColumnKind#ARRAY}
     * @param name      Lucene field name
     * @param fieldType Lucene field type
     */
    public static LuceneBinaryColumn arrayColumn(EscfColumnData data, String name, IndexableFieldType fieldType) {
        assert data.kind() == EscfColumnKind.ARRAY : "expected ARRAY, got " + EscfColumnKind.name(data.kind());
        return new LuceneBinaryColumn(EscfColumn.from(data), name, fieldType, Density.SPARSE);
    }

    @Override
    public LuceneBinaryColumn slice(int from, int count) {
        return new LuceneBinaryColumn(data.sliceInternal(from, count), name(), fieldType(), density());
    }

    @Override
    public Column toLuceneColumn() {
        return this;
    }

    @Override
    public LuceneColumn.RowFieldCursor rowFieldCursor() {
        // A reusable mutable field whose bytes value is updated per position. The IndexWriter reads
        // binaryValue() synchronously during addDocument, so reusing the same field object is safe.
        final BytesRef sentinel = new BytesRef();
        final Field field = new Field(name(), sentinel, fieldType());
        final ObjectTupleCursor<BytesRef> cursor = data.bytesRefCursor();
        return new LuceneColumn.RowFieldCursor() {
            @Override
            public int nextDoc() {
                return cursor.nextDoc();
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                field.setBytesValue(cursor.value());
                out.add(field);
            }
        };
    }

    @Override
    public ObjectTupleCursor<BytesRef> tuples() {
        return data.bytesRefCursor();
    }

    @Override
    public BytesRefValuesCursor values() {
        if (density() == Density.SPARSE) {
            // SPARSE columns are never consulted via the dense values cursor.
            return super.values();
        }
        final int count = data.docCount;
        return new BytesRefValuesCursor(count) {
            private int pos;

            @Override
            public BytesRef nextValue() {
                if (pos >= size()) {
                    throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
                }
                return data.getBinaryValue(pos++);
            }
        };
    }
}
