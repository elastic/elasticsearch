/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.LuceneLongColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class MappedColumns {

    private final int offset;
    private final int count;

    @Nullable
    private final byte[] seqNos;

    @Nullable
    private final byte[] primaryTerms;

    @Nullable
    private final byte[] versions;

    private final List<LuceneColumn> columns;

    /**
     * Constructs a {@code MappedColumns} covering the window {@code [from, from + count)} of the
     * given backing arrays and columns.
     */
    public MappedColumns(
        int offset,
        int count,
        @Nullable byte[] seqNos,
        @Nullable byte[] primaryTerms,
        @Nullable byte[] versions,
        List<LuceneColumn> columns
    ) {
        this.offset = offset;
        this.count = count;
        this.seqNos = seqNos;
        this.primaryTerms = primaryTerms;
        this.versions = versions;
        this.columns = List.copyOf(columns);
    }

    public int docCount() {
        return count;
    }

    public void setSeqNo(int doc, long value) {
        assert doc >= 0 && doc < count;
        if (seqNos != null) {
            ByteUtils.writeLongLE(value, seqNos, (offset + doc) * 8);
        }
    }

    public void fillPrimaryTerm(long value) {
        if (primaryTerms != null) {
            for (int i = 0; i < count; i++) {
                ByteUtils.writeLongLE(value, primaryTerms, (offset + i) * 8);
            }
        }
    }

    public void setVersion(int doc, long value) {
        assert doc >= 0 && doc < count;
        if (versions != null) {
            ByteUtils.writeLongLE(value, versions, (offset + doc) * 8);
        }
    }

    public MappedColumns slice(int from, int to) {
        Objects.checkFromIndexSize(from, to - from, this.count);
        int newCount = to - from;
        List<LuceneColumn> slicedColumns = new ArrayList<>(columns.size());
        for (LuceneColumn c : columns) {
            slicedColumns.add(c.slice(from, newCount));
        }
        return new MappedColumns(this.offset + from, newCount, seqNos, primaryTerms, versions, slicedColumns);
    }

    public ColumnBatch toColumnBatch() {
        final List<Column> luceneColumns = columns.stream().map(LuceneColumn::toLuceneColumn).toList();
        return new SliceableColumnBatch(luceneColumns, count);
    }

    /**
     * Returns a {@link RowCursor} positioned before the first document. The caller must invoke
     * {@link RowCursor#advance()} once per document — unconditionally, even for documents that will
     * not be written to Lucene — to keep all column cursors correctly positioned.
     */
    public RowCursor rowCursor() {
        return new RowCursor(columns);
    }

    /**
     * A forward-only cursor over all columns that assembles per-document {@link IndexableField} lists
     * for the row-oriented (soft-update / non-{@code addBatch}) indexing path. Encapsulates the
     * per-column {@link LuceneColumn.RowFieldCursor} instances and their position tracking.
     */
    public static final class RowCursor {
        private final List<LuceneColumn.RowFieldCursor> cursors;
        private final int[] heads;
        private final List<IndexableField> fields;
        private int currentDoc = 0;

        private RowCursor(List<LuceneColumn> columns) {
            cursors = new ArrayList<>(columns.size());
            for (LuceneColumn col : columns) {
                cursors.add(col.rowFieldCursor());
            }
            heads = new int[cursors.size()];
            for (int c = 0; c < cursors.size(); c++) {
                heads[c] = cursors.get(c).nextDoc();
            }
            fields = new ArrayList<>(columns.size());
        }

        /**
         * Advances all column cursors to the next document and collects that document's fields.
         * Must be called once per document in order, even when the document will not be written,
         * so that every column cursor stays correctly positioned.
         */
        public void advance() {
            fields.clear();
            for (int c = 0; c < cursors.size(); c++) {
                while (heads[c] == currentDoc) {
                    cursors.get(c).appendCurrentFields(fields);
                    heads[c] = cursors.get(c).nextDoc();
                }
            }
            currentDoc++;
        }

        /**
         * Returns the fields collected for the current document. The returned list is only valid
         * until the next call to {@link #advance()}.
         */
        public List<IndexableField> fields() {
            return fields;
        }
    }

    private static final class SliceableColumnBatch extends ColumnBatch {

        private final List<Column> columns;
        private final int numDocs;

        SliceableColumnBatch(List<Column> columns, int numDocs) {
            this.columns = columns;
            this.numDocs = numDocs;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public Iterable<Column> columns() {
            return columns;
        }
    }

    public static LuceneColumn longColumn(byte[] values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        return LuceneLongColumn.longColumn(values, name, fieldType, kind);
    }

    public static LuceneColumn binaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType) {
        return new WindowedBinaryColumn(values, name, fieldType, 0, values.length);
    }

    private static final class WindowedBinaryColumn extends BinaryColumn implements LuceneColumn {

        private final BytesRef[] values;
        private final int from;
        private final int count;
        private final IndexableFieldType fieldType;

        WindowedBinaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType, int from, int count) {
            super(name, fieldType, allPresent(values, from, count) ? Density.DENSE : Density.SPARSE);
            this.values = values;
            this.from = from;
            this.count = count;
            this.fieldType = fieldType;
        }

        private static boolean allPresent(BytesRef[] values, int from, int count) {
            for (int i = from; i < from + count; i++) {
                if (values[i] == null) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public WindowedBinaryColumn slice(int from, int count) {
            Objects.checkFromIndexSize(from, count, this.count);
            return new WindowedBinaryColumn(values, name(), fieldType, this.from + from, count);
        }

        @Override
        public Column toLuceneColumn() {
            return this;
        }

        @Override
        public LuceneColumn.RowFieldCursor rowFieldCursor() {
            // A reusable mutable field whose bytes value is updated per document. Using the public
            // Field(String, BytesRef, IndexableFieldType) constructor sets fieldsData to the given
            // BytesRef; subsequent setBytesValue calls update fieldsData in place. The IndexWriter
            // reads binaryValue() synchronously during addDocument, so the same field object is safe
            // to reuse across documents.
            final BytesRef sentinel = new BytesRef(); // placeholder; overwritten in appendCurrentFields
            final Field field = new Field(name(), sentinel, fieldType);
            return new LuceneColumn.RowFieldCursor() {
                private int doc = DocIdSetIterator.NO_MORE_DOCS;
                private int srcIdx = from - 1;

                @Override
                public int nextDoc() {
                    srcIdx++;
                    final int end = from + count;
                    while (srcIdx < end && values[srcIdx] == null) {
                        srcIdx++;
                    }
                    doc = srcIdx < end ? srcIdx - from : DocIdSetIterator.NO_MORE_DOCS;
                    return doc;
                }

                @Override
                public void appendCurrentFields(List<? super IndexableField> out) {
                    field.setBytesValue(values[srcIdx]);
                    out.add(field);
                }
            };
        }

        @Override
        public ObjectTupleCursor<BytesRef> tuples() {
            // srcIdx tracks position in the full backing array; doc is the batch-local id.
            return new ObjectTupleCursor<>() {
                private int srcIdx = from - 1;

                @Override
                public int nextDoc() {
                    srcIdx++;
                    final int end = from + count;
                    while (srcIdx < end && values[srcIdx] == null) {
                        srcIdx++;
                    }
                    int doc;
                    if (srcIdx >= end) {
                        doc = DocIdSetIterator.NO_MORE_DOCS;
                    } else {
                        doc = srcIdx - from;
                    }
                    return doc;
                }

                @Override
                public BytesRef value() {
                    return values[srcIdx];
                }
            };
        }

        @Override
        public BytesRefValuesCursor values() {
            if (density() == Density.SPARSE) {
                return super.values(); // throws; never consulted for SPARSE columns
            }
            return new BytesRefValuesCursor(count) {
                private int pos;

                @Override
                public BytesRef nextValue() {
                    if (pos >= size()) {
                        throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
                    }
                    return values[from + pos++];
                }
            };
        }
    }
}
