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
 * A {@link BinaryColumn} backed by an ESCF data column. Supports dense STRING/BINARY columns
 * (one value per row) and sparse ARRAY columns (zero or more elements per row, absent rows skipped).
 * Create via {@link #stringColumn}, {@link #arrayColumn}, or the dispatch helper {@link #of}.
 */
public final class LuceneBinaryColumn extends BinaryColumn implements LuceneColumn {

    private final EscfColumn data;

    private LuceneBinaryColumn(EscfColumn data, String name, IndexableFieldType fieldType, Density density) {
        super(name, fieldType, density);
        this.data = data;
    }

    /** Creates a dense ({@link Density#DENSE}) column from a STRING or BINARY {@link EscfColumnData}. */
    public static LuceneBinaryColumn stringColumn(EscfColumnData data, String name, IndexableFieldType fieldType) {
        return new LuceneBinaryColumn(EscfColumn.from(data), name, fieldType, Density.DENSE);
    }

    /** Creates a sparse ({@link Density#SPARSE}) column from an ARRAY {@link EscfColumnData}. */
    public static LuceneBinaryColumn arrayColumn(EscfColumnData data, String name, IndexableFieldType fieldType) {
        assert data.kind() == EscfColumnKind.ARRAY : "expected ARRAY, got " + EscfColumnKind.name(data.kind());
        return new LuceneBinaryColumn(EscfColumn.from(data), name, fieldType, Density.SPARSE);
    }

    /**
     * Dispatch helper: STRING/BINARY → {@link Density#DENSE} (or SPARSE when validity is non-null);
     * ARRAY → {@link Density#SPARSE}. Use when the kind is determined at runtime.
     */
    public static LuceneBinaryColumn of(EscfColumnData data, String name, IndexableFieldType fieldType) {
        return switch (data.kind()) {
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> {
                // Use SPARSE when validity is non-null so absent docs are skipped by tuples().
                Density density = data.validity() == null ? Density.DENSE : Density.SPARSE;
                yield new LuceneBinaryColumn(EscfColumn.from(data), name, fieldType, density);
            }
            case EscfColumnKind.ARRAY -> arrayColumn(data, name, fieldType);
            default -> throw new AssertionError("unexpected column kind: " + EscfColumnKind.name(data.kind()));
        };
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
        final ObjectTupleCursor<BytesRef> cursor = data.bytesRefCursor();
        return new LuceneColumn.RowFieldCursor() {
            @Override
            public int nextDoc() {
                return cursor.nextDoc();
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                // A distinct Field per element: for multi-valued (array) rows appendCurrentFields is
                // called more than once for the same document and every emitted field is retained in
                // the caller's list, so a single reused field object would collapse all values to the
                // last one. cursor.value() already returns a fresh BytesRef per element.
                out.add(new Field(name(), cursor.value(), fieldType()));
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
