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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.sourcebatch.SliceableColumn;

import java.util.List;

/**
 * A {@link SliceableColumn} that binds an {@link EscfArrayColumn} with a STRING child to both the
 * Lucene {@code addBatch} path and the row (soft-update) path.
 *
 * <p><b>Batch path ({@link #toLuceneColumn}):</b> returns a Lucene {@link BinaryColumn} with
 * {@link Column.Density#SPARSE}. Its {@link ObjectTupleCursor} iterates every element in the child
 * column in order, returning each element's batch-local doc-id from {@code nextDoc()} and its byte
 * value from {@code value()}. The cursor returns the same doc-id for every element belonging to a
 * multi-valued document, and skips doc-ids for which the array is empty (absent).
 *
 * <p><b>Row path ({@link #rowFieldCursor}):</b> returns a {@link SliceableColumn.RowFieldCursor}
 * that emits one {@link IndexableField} per element per document, appended to the field list via
 * {@link SliceableColumn.RowFieldCursor#appendCurrentFields}. Multi-valued documents appear as
 * consecutive entries with the same doc-id in the cursor's iteration; the
 * {@link org.elasticsearch.sourcebatch.MappedColumns.RowCursor#advance()} drain loop handles this
 * naturally.
 *
 * <p>Create via {@link #fieldNamesColumn} from the {@link EscfColumnData} produced by
 * {@link EscfRowColumnBuilder#finish}.
 */
public final class EscfStringArrayColumn implements SliceableColumn {

    private final EscfArrayColumn array;
    private final String name;
    private final IndexableFieldType fieldType;

    private EscfStringArrayColumn(EscfArrayColumn array, String name, IndexableFieldType fieldType) {
        this.array = array;
        this.name = name;
        this.fieldType = fieldType;
    }

    /**
     * Wraps {@code data} — which must be an {@link EscfColumnKind#ARRAY} column with a
     * {@link EscfColumnKind#STRING} child, as produced by {@link EscfRowColumnBuilder#finish} —
     * into a {@link SliceableColumn} for use as a batch-mapping column.
     *
     * @param data      the array column data; must have kind {@link EscfColumnKind#ARRAY}
     * @param name      Lucene field name (e.g. {@code _field_names})
     * @param fieldType Lucene field type (e.g. {@code StringField.TYPE_NOT_STORED})
     */
    public static EscfStringArrayColumn fieldNamesColumn(EscfColumnData data, String name, IndexableFieldType fieldType) {
        assert data.kind() == EscfColumnKind.ARRAY : "expected ARRAY, got " + EscfColumnKind.name(data.kind());
        return new EscfStringArrayColumn((EscfArrayColumn) EscfColumn.from(data), name, fieldType);
    }

    /**
     * Returns a view over the sub-range {@code [from, from + count)} of this column's document
     * window. The child column is kept full and unsliced; the {@code rowOffsets} window is
     * adjusted to cover the requested sub-range.
     */
    @Override
    public SliceableColumn slice(int from, int count) {
        return new EscfStringArrayColumn((EscfArrayColumn) array.sliceInternal(from, count), name, fieldType);
    }

    /**
     * Returns a cursor that iterates over every element in every row, emitting one
     * {@link IndexableField} per element. The cursor advances through elements in order and returns
     * the batch-local doc-id from {@code nextDoc()} for every element (repeating the same doc-id
     * for multi-valued documents). Rows with no elements are silently skipped.
     *
     * <p>The returned {@link Field} object is reused across calls; its bytes value is updated in
     * place and is safe to hand to the IndexWriter because it reads values synchronously during
     * {@code addDocument}.
     */
    @Override
    public RowFieldCursor rowFieldCursor() {
        final int docCount = array.docCount;
        // Reusable field: constructor sets fieldsData to the sentinel BytesRef; setBytesValue
        // updates it in place.
        final BytesRef sentinel = new BytesRef();
        final Field field = new Field(name, sentinel, fieldType);
        return new RowFieldCursor() {
            /**
             * Current element position in the (absolute, unsliced) child column.
             * Initialized to {@code rowElemFrom(0) - 1} so that the first {@code nextDoc()}
             * advances to the absolute start of this window's first row, which is 0 for a
             * freshly built (non-sliced) column but may be greater after {@link #slice}.
             */
            private int elemPos = array.rowElemFrom(0) - 1;
            /** Current batch-local doc-id. Tracks which row elemPos belongs to. */
            private int currentDoc = 0;

            @Override
            public int nextDoc() {
                elemPos++;
                // Advance currentDoc until it owns elemPos (or until exhausted).
                while (currentDoc < docCount && array.rowElemTo(currentDoc) <= elemPos) {
                    currentDoc++;
                }
                return currentDoc < docCount ? currentDoc : DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                field.setBytesValue(array.child().getBinaryValue(elemPos));
                out.add(field);
            }
        };
    }

    /**
     * Returns a Lucene {@link BinaryColumn} with {@link Column.Density#SPARSE} whose tuple cursor
     * iterates every element in the child, repeating each batch-local doc-id once per element and
     * skipping doc-ids with no elements.
     *
     * <p>The {@code values()} cursor throws {@link UnsupportedOperationException} because
     * {@code SPARSE} columns are never consulted via the dense values cursor.
     */
    @Override
    public Column toLuceneColumn() {
        final int docCount = array.docCount;
        return new BinaryColumn(name, fieldType, Column.Density.SPARSE) {
            @Override
            public ObjectTupleCursor<BytesRef> tuples() {
                return new ObjectTupleCursor<>() {
                    /**
                     * Current element position in the (absolute, unsliced) child column.
                     * Initialized to {@code rowElemFrom(0) - 1} so the first {@code nextDoc()}
                     * advances to the absolute start of this window's first row (mirrors
                     * the row cursor initialization).
                     */
                    private int elemPos = array.rowElemFrom(0) - 1;
                    /** Current batch-local doc-id. */
                    private int currentDoc = 0;

                    @Override
                    public int nextDoc() {
                        elemPos++;
                        while (currentDoc < docCount && array.rowElemTo(currentDoc) <= elemPos) {
                            currentDoc++;
                        }
                        return currentDoc < docCount ? currentDoc : DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public BytesRef value() {
                        return array.child().getBinaryValue(elemPos);
                    }
                };
            }

            @Override
            public BytesRefValuesCursor values() {
                // SPARSE columns are never consulted via the dense values cursor.
                throw new UnsupportedOperationException("values() is not supported on SPARSE _field_names column");
            }
        };
    }
}
