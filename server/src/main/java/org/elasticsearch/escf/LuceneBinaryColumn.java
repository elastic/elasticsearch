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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;

import static org.elasticsearch.escf.EscfColumn.windowValidity;

/**
 * A {@link BinaryColumn} backed by an ESCF data column. Supports dense STRING/BINARY columns
 * (one value per row) and sparse ARRAY columns (zero or more elements per row, absent rows skipped).
 * Create via {@link #stringColumn}, {@link #arrayColumn}, or the dispatch helper {@link #of}.
 *
 * <p>An optional {@code filter} bitset (see {@link #withFilter}) can restrict which documents are
 * emitted to Lucene. When non-null the column is always {@link Density#SPARSE}, regardless of the
 * underlying data's density, and only documents whose bit is set in the filter appear in
 * {@link #tuples()}, {@link #rowFieldCursor()}, and {@link #values()}.
 */
public final class LuceneBinaryColumn extends BinaryColumn implements LuceneColumn {

    private final EscfColumn data;
    private final FixedBitSet filter;

    private LuceneBinaryColumn(EscfColumn data, String name, IndexableFieldType fieldType, Density density, FixedBitSet filter) {
        super(name, fieldType, filter != null ? Density.SPARSE : density);
        this.data = data;
        this.filter = filter;
    }

    /** Creates a dense ({@link Density#DENSE}) column from a STRING or BINARY {@link EscfColumnData}. */
    public static LuceneBinaryColumn stringColumn(EscfColumnData data, String name, IndexableFieldType fieldType) {
        return new LuceneBinaryColumn(EscfColumn.from(data), name, fieldType, Density.DENSE, null);
    }

    /** Creates a sparse ({@link Density#SPARSE}) column from an ARRAY {@link EscfColumnData}. */
    public static LuceneBinaryColumn arrayColumn(EscfColumnData data, String name, IndexableFieldType fieldType) {
        assert data.kind() == EscfColumnKind.ARRAY : "expected ARRAY, got " + EscfColumnKind.name(data.kind());
        return new LuceneBinaryColumn(EscfColumn.from(data), name, fieldType, Density.SPARSE, null);
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
                yield new LuceneBinaryColumn(EscfColumn.from(data), name, fieldType, density, null);
            }
            case EscfColumnKind.ARRAY -> arrayColumn(data, name, fieldType);
            default -> throw new AssertionError("unexpected column kind: " + EscfColumnKind.name(data.kind()));
        };
    }

    /**
     * Returns a copy of this column that passes only the documents whose bit is set in
     * {@code filter} to Lucene. The returned column is always {@link Density#SPARSE}. Pass
     * {@code null} to remove any existing filter.
     *
     * @param filter a bitset of length equal to this column's doc count, or {@code null}
     */
    public LuceneBinaryColumn withFilter(FixedBitSet filter) {
        assert filter == null || filter.length() == data.docCount;
        Density density = (data instanceof EscfArrayColumn || data.validity != null) ? Density.SPARSE : Density.DENSE;
        return new LuceneBinaryColumn(data, name(), fieldType(), density, LuceneColumn.singleFilter(this.filter, filter));
    }

    @Override
    public LuceneBinaryColumn slice(int from, int count) {
        EscfColumn sliced = data.sliceInternal(from, count);
        Density density = (sliced instanceof EscfArrayColumn || sliced.validity != null) ? Density.SPARSE : Density.DENSE;
        return new LuceneBinaryColumn(sliced, name(), fieldType(), density, windowValidity(filter, from, count));
    }

    @Override
    public Column toLuceneColumn() {
        return this;
    }

    @Override
    public LuceneColumn.RowFieldCursor rowFieldCursor() {
        // retainValues=true: see appendCurrentFields below — the emitted Fields outlive the cursor position.
        final ObjectTupleCursor<BytesRef> cursor = data.bytesRefCursor(true);
        return new LuceneColumn.RowFieldCursor() {
            @Override
            public int nextDoc() {
                if (filter == null) {
                    return cursor.nextDoc();
                }
                int doc;
                while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (filter.get(doc)) {
                        return doc;
                    }
                }
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                // A distinct Field per element: for multi-valued (array) rows appendCurrentFields is
                // called more than once for the same document and every emitted field is retained in
                // the caller's list, so a single reused field object would collapse all values to the
                // last one. cursor.value() already returns a fresh BytesRef per element.
                //
                // Lucene's Field(String, BytesRef, FieldType) constructor rejects tokenized+indexed
                // field types ("cannot set a BytesRef value on a tokenized field"). Indexed tokenized
                // fields require a String so Lucene can run an analyzer over them.
                if (fieldType().tokenized() && fieldType().indexOptions() != IndexOptions.NONE) {
                    out.add(new Field(name(), cursor.value().utf8ToString(), fieldType()));
                } else {
                    out.add(new Field(name(), cursor.value(), fieldType()));
                }
            }
        };
    }

    @Override
    public ObjectTupleCursor<BytesRef> tuples() {
        // retainValues=false: Lucene's indexing chain consumes each tuple value before advancing the
        // cursor, which is all ObjectTupleCursor#value() promises, so the shared BytesRef is enough.
        ObjectTupleCursor<BytesRef> inner = data.bytesRefCursor(false);
        if (filter == null) {
            return inner;
        }
        return new ObjectTupleCursor<>() {
            @Override
            public int nextDoc() {
                int doc;
                while ((doc = inner.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (filter.get(doc)) {
                        return doc;
                    }
                }
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public BytesRef value() {
                return inner.value();
            }
        };
    }

    @Override
    public BytesRefValuesCursor values() {
        if (density() == Density.SPARSE) {
            return super.values();
        }
        return ((AbstractVarColumn) data).bytesRefValuesCursor(false);
    }
}
