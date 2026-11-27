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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.escf.LuceneLongColumn;
import org.elasticsearch.index.mapper.LuceneDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.escf.EscfColumn.windowValidity;

/**
 * The Lucene columns that the mappers produced for the documents of a window of a batch, and the engine-assigned sequence numbers,
 * primary terms and versions that the columns alias.
 *
 * <p>A document can have nested children, which Lucene indexes in the same block as the document, children first. The children of one
 * nested path are the rows of their own {@code MappedColumns} (see {@link NestedColumns}), laid out contiguously for each parent.
 * {@link #toColumnBatch()} cannot write blocks, so a window with children is written through {@link #blockCursor()}.
 */
public final class MappedColumns {

    /**
     * The children of one nested path.
     *
     * @param nestedPath the value of {@code _nested_path} of the children
     * @param children one row for each child
     * @param parentOffsets the children of parent {@code d} are the rows {@code [parentOffsets[d], parentOffsets[d + 1])} of
     *                      {@code children}, in source order; the array has one more entry than the parent window has documents
     */
    public record NestedColumns(String nestedPath, MappedColumns children, int[] parentOffsets) {

        public NestedColumns {
            assert parentOffsets[0] == 0 : "the offsets of a window start at 0";
            assert parentOffsets[parentOffsets.length - 1] == children.docCount() : "the children must be the rows of the offsets";
            assert isAscending(parentOffsets) : "the children of a parent are contiguous and the parents are in source order";
        }

        private static boolean isAscending(int[] offsets) {
            for (int i = 1; i < offsets.length; i++) {
                if (offsets[i] < offsets[i - 1]) {
                    return false;
                }
            }
            return true;
        }

        int childCount(int parent) {
            return parentOffsets[parent + 1] - parentOffsets[parent];
        }
    }

    private final int offset;
    private final int count;

    private final BytesRef seqNos;
    private final BytesRef primaryTerms;
    private final BytesRef versions;

    private final List<LuceneColumn> columns;
    /**
     * The documents of this window that are written to Lucene, or {@code null} for all of them. Every column carries the same
     * filter; it is kept here as well because {@link #toColumnBatch()} needs it to renumber the remaining documents.
     */
    private final FixedBitSet filter;
    /** The nested children of the documents of this window, one entry for each nested path that has children, by nested path. */
    private final List<NestedColumns> nested;

    /**
     * Constructs a {@code MappedColumns} covering the window {@code [from, from + count)} of the
     * given backing arrays and columns.
     */
    public MappedColumns(int offset, int count, BytesRef seqNos, BytesRef primaryTerms, BytesRef versions, List<LuceneColumn> columns) {
        this(offset, count, seqNos, primaryTerms, versions, columns, null, List.of());
    }

    /** Like above, for documents that have nested children. {@code nested} are the children of this window, which start at 0. */
    public MappedColumns(
        int offset,
        int count,
        BytesRef seqNos,
        BytesRef primaryTerms,
        BytesRef versions,
        List<LuceneColumn> columns,
        List<NestedColumns> nested
    ) {
        this(offset, count, seqNos, primaryTerms, versions, columns, null, nested);
    }

    private MappedColumns(
        int offset,
        int count,
        BytesRef seqNos,
        BytesRef primaryTerms,
        BytesRef versions,
        List<LuceneColumn> columns,
        FixedBitSet filter,
        List<NestedColumns> nested
    ) {
        assert filter == null || filter.length() == count;
        assert nested.stream().allMatch(n -> n.parentOffsets().length == count + 1) : "an offset for each document and one more";
        this.nested = List.copyOf(nested);
        this.offset = offset;
        this.count = count;
        this.seqNos = Objects.requireNonNull(seqNos, "seqNos");
        this.primaryTerms = Objects.requireNonNull(primaryTerms, "primaryTerms");
        this.versions = Objects.requireNonNull(versions, "versions");
        this.columns = List.copyOf(columns);
        this.filter = filter;
    }

    public int docCount() {
        return count;
    }

    /** Sets the sequence number of document {@code doc} and of its children, which share it. */
    public void setSeqNo(int doc, long value) {
        assert doc >= 0 && doc < count;
        ByteUtils.writeLongLE(value, seqNos.bytes, seqNos.offset + ((offset + doc) * 8));
        for (NestedColumns n : nested) {
            for (int c = n.parentOffsets()[doc]; c < n.parentOffsets()[doc + 1]; c++) {
                n.children().setSeqNo(c, value);
            }
        }
    }

    public void fillPrimaryTerm(long value) {
        for (int i = 0; i < count; i++) {
            ByteUtils.writeLongLE(value, primaryTerms.bytes, primaryTerms.offset + ((offset + i) * 8));
        }
    }

    /** Sets the version of document {@code doc} and of its children, which share it. */
    public void setVersion(int doc, long value) {
        assert doc >= 0 && doc < count;
        ByteUtils.writeLongLE(value, versions.bytes, versions.offset + ((offset + doc) * 8));
        for (NestedColumns n : nested) {
            for (int c = n.parentOffsets()[doc]; c < n.parentOffsets()[doc + 1]; c++) {
                n.children().setVersion(c, value);
            }
        }
    }

    /** Whether any document of this window has nested children. */
    public boolean hasChildren() {
        for (NestedColumns n : nested) {
            if (n.children().docCount() > 0) {
                return true;
            }
        }
        return false;
    }

    /** The number of nested children, over all nested paths, of document {@code doc}. */
    public int childCount(int doc) {
        assert doc >= 0 && doc < count;
        int children = 0;
        for (NestedColumns n : nested) {
            children += n.childCount(doc);
        }
        return children;
    }

    public MappedColumns slice(int from, int to) {
        Objects.checkFromIndexSize(from, to - from, this.count);
        int newCount = to - from;
        List<LuceneColumn> slicedColumns = new ArrayList<>(columns.size());
        for (LuceneColumn c : columns) {
            slicedColumns.add(c.slice(from, newCount));
        }
        List<NestedColumns> slicedNested = new ArrayList<>(nested.size());
        for (NestedColumns n : nested) {
            final int[] offsets = n.parentOffsets();
            final int childFrom = offsets[from];
            final int[] slicedOffsets = new int[newCount + 1];
            for (int i = 0; i <= newCount; i++) {
                slicedOffsets[i] = offsets[from + i] - childFrom;
            }
            slicedNested.add(new NestedColumns(n.nestedPath(), n.children().slice(childFrom, offsets[to]), slicedOffsets));
        }
        return new MappedColumns(
            this.offset + from,
            newCount,
            seqNos,
            primaryTerms,
            versions,
            slicedColumns,
            windowValidity(filter, from, newCount),
            slicedNested
        );
    }

    /**
     * Returns a copy of this {@code MappedColumns} where every column is filtered to emit only
     * the documents whose bit is set in {@code filter}. Returns {@code this} when {@code filter}
     * is {@code null}.
     *
     * <p>The filter does not change {@link #docCount()} or the row numbering of {@link #rowCursor()}, which still has to be
     * advanced once per document. {@link #toColumnBatch()} on the other hand holds the remaining documents only.
     *
     * @param filter a bitset of length {@link #docCount()}, or {@code null}
     */
    public MappedColumns withFilter(FixedBitSet filter) {
        if (filter == null) {
            return this;
        }
        assert filter.length() == count;
        List<LuceneColumn> filtered = new ArrayList<>(columns.size());
        for (LuceneColumn c : columns) {
            filtered.add(c.withFilter(filter));
        }
        assert hasChildren() == false : "a block of nested documents cannot be filtered, it is written through the block cursor";
        return new MappedColumns(
            offset,
            count,
            seqNos,
            primaryTerms,
            versions,
            filtered,
            LuceneColumn.singleFilter(this.filter, filter),
            nested
        );
    }

    /**
     * The batch to hand to {@code IndexWriter.addBatch}. Lucene adds one document per batch-local doc id in {@code [0, numDocs)},
     * whether or not any column has a value for it, so a document that a {@link #withFilter filter} excludes must not keep a doc id
     * or it is indexed as a document without any field. The remaining documents are therefore renumbered densely, in order, and
     * {@code numDocs} is the number of them.
     */
    public ColumnBatch toColumnBatch() {
        assert hasChildren() == false : "a column batch has no blocks, a window with nested children is written through the block cursor";
        if (filter == null) {
            final List<Column> luceneColumns = columns.stream().map(LuceneColumn::toLuceneColumn).toList();
            return new SliceableColumnBatch(luceneColumns, count);
        }
        final int[] compactedDocIds = new int[count];
        int numDocs = 0;
        for (int doc = 0; doc < count; doc++) {
            compactedDocIds[doc] = filter.get(doc) ? numDocs++ : -1;
        }
        final List<Column> luceneColumns = new ArrayList<>(columns.size());
        for (LuceneColumn column : columns) {
            luceneColumns.add(column.toLuceneColumn(compactedDocIds));
        }
        return new SliceableColumnBatch(luceneColumns, numDocs);
    }

    /**
     * Returns a {@link RowCursor} positioned before the first document. The caller must invoke
     * {@link RowCursor#advance()} once per document — unconditionally, even for documents that will
     * not be written to Lucene — to keep all column cursors correctly positioned.
     */
    public RowCursor rowCursor() {
        return rowCursor(column -> true);
    }

    /**
     * Like {@link #rowCursor()}, but the cursor only reads the columns accepted by {@code include}, and so the fields it assembles for
     * a document leave out those of the other columns. Use it when only some fields are needed, as skipping a column also skips
     * building its fields.
     */
    public RowCursor rowCursor(Predicate<? super LuceneColumn> include) {
        return new RowCursor(columns.stream().filter(include).toList(), false);
    }

    /**
     * Returns a {@link BlockCursor} positioned before the first document, which yields each document with its nested children, as the
     * engine indexes them in one block. Like {@link #rowCursor()} it must be advanced once per document.
     */
    public BlockCursor blockCursor() {
        return blockCursor(column -> true);
    }

    /**
     * Like {@link #blockCursor()}, and the cursor only reads the columns that {@code include} accepts, like
     * {@link #rowCursor(Predicate)}.
     */
    public BlockCursor blockCursor(Predicate<? super LuceneColumn> include) {
        return new BlockCursor(this, include);
    }

    /**
     * A forward-only cursor over the blocks of this window: for each document its nested children, then the document itself, which is
     * the order of the documents of a block in Lucene. The children of the nested paths are in the order of the paths, and in source
     * order for each path. Each document of a block has its own fields, because one column emits a field for each child.
     */
    public static final class BlockCursor {
        private final RowCursor root;
        private final List<NestedColumns> nested;
        private final List<RowCursor> children;
        private int doc = 0;

        private BlockCursor(MappedColumns columns, Predicate<? super LuceneColumn> include) {
            this.root = new RowCursor(columns.columns.stream().filter(include).toList(), false);
            this.nested = columns.nested;
            this.children = new ArrayList<>(nested.size());
            for (NestedColumns n : nested) {
                children.add(new RowCursor(n.children().columns.stream().filter(include).toList(), true));
            }
        }

        /**
         * Advances to the next document and returns its block: the children, then the root document, which is the last. The list is only
         * valid until the next call, and the root document wraps a list of fields that the next call also reuses, while the fields of
         * the children are not reused.
         */
        public List<LuceneDocument> advance() {
            root.advance();
            final LuceneDocument rootDocument = new LuceneDocument(root.fields());
            final List<LuceneDocument> block = new ArrayList<>(1);
            for (int n = 0; n < nested.size(); n++) {
                final NestedColumns columns = nested.get(n);
                final RowCursor cursor = children.get(n);
                for (int c = columns.childCount(doc); c > 0; c--) {
                    cursor.advance();
                    block.add(new LuceneDocument(columns.nestedPath(), rootDocument, new ArrayList<>(cursor.fields())));
                }
            }
            block.add(rootDocument);
            doc++;
            return block;
        }
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

        private RowCursor(List<LuceneColumn> columns, boolean independentFields) {
            cursors = new ArrayList<>(columns.size());
            for (LuceneColumn col : columns) {
                cursors.add(independentFields ? col.independentRowFieldCursor() : col.rowFieldCursor());
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

    public static LuceneColumn longColumn(BytesRef values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        return LuceneLongColumn.longColumn(values, name, fieldType, kind);
    }

    public static LuceneColumn binaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType) {
        return new WindowedBinaryColumn(values, name, fieldType, 0, values.length, false);
    }

    /**
     * Like {@link #binaryColumn} but the row-path cursor is a no-op: {@code rowFieldCursor()} always
     * returns {@link DocIdSetIterator#NO_MORE_DOCS}. Use when a companion column owns the row-path
     * emission for the same field (e.g. a token-stream column that also writes binary doc values).
     */
    public static LuceneColumn binaryDvOnlyColumn(BytesRef[] values, String name, IndexableFieldType fieldType) {
        return new WindowedBinaryColumn(values, name, fieldType, 0, values.length, true);
    }

    private static final class WindowedBinaryColumn extends BinaryColumn implements LuceneColumn {

        private final BytesRef[] values;
        private final int from;
        private final int count;
        private final IndexableFieldType fieldType;
        // When true, rowFieldCursor() is a no-op. Use this only when a companion column emits the
        // complete field (including any doc values) on the row path, so this column's row emission
        // would be redundant or conflicting.
        private final boolean noOpRowPath;
        private final FixedBitSet filter;

        WindowedBinaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType, int from, int count, boolean noOpRowPath) {
            this(values, name, fieldType, from, count, noOpRowPath, null);
        }

        private WindowedBinaryColumn(
            BytesRef[] values,
            String name,
            IndexableFieldType fieldType,
            int from,
            int count,
            boolean noOpRowPath,
            FixedBitSet filter
        ) {
            super(name, fieldType, filter != null ? Density.SPARSE : (allPresent(values, from, count) ? Density.DENSE : Density.SPARSE));
            this.values = values;
            this.from = from;
            this.count = count;
            this.fieldType = fieldType;
            this.noOpRowPath = noOpRowPath;
            this.filter = filter;
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
        public WindowedBinaryColumn withFilter(FixedBitSet newFilter) {
            assert newFilter == null || newFilter.length() == count;
            return new WindowedBinaryColumn(
                values,
                name(),
                fieldType,
                from,
                count,
                noOpRowPath,
                LuceneColumn.singleFilter(filter, newFilter)
            );
        }

        @Override
        public WindowedBinaryColumn slice(int from, int count) {
            Objects.checkFromIndexSize(from, count, this.count);
            return new WindowedBinaryColumn(
                values,
                name(),
                fieldType,
                this.from + from,
                count,
                noOpRowPath,
                windowValidity(filter, from, count)
            );
        }

        @Override
        public Column toLuceneColumn() {
            return this;
        }

        @Override
        public Column toLuceneColumn(int[] compactedDocIds) {
            return CompactedColumns.of(this, compactedDocIds);
        }

        @Override
        public LuceneColumn.RowFieldCursor rowFieldCursor() {
            return rowFieldCursor(false);
        }

        @Override
        public LuceneColumn.RowFieldCursor independentRowFieldCursor() {
            return rowFieldCursor(true);
        }

        private LuceneColumn.RowFieldCursor rowFieldCursor(boolean independentFields) {
            if (noOpRowPath) {
                return new LuceneColumn.RowFieldCursor() {
                    @Override
                    public int nextDoc() {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public void appendCurrentFields(List<? super IndexableField> out) {}
                };
            }
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
                    while (srcIdx < end && (values[srcIdx] == null || (filter != null && filter.get(srcIdx - from) == false))) {
                        srcIdx++;
                    }
                    doc = srcIdx < end ? srcIdx - from : DocIdSetIterator.NO_MORE_DOCS;
                    return doc;
                }

                @Override
                public void appendCurrentFields(List<? super IndexableField> out) {
                    if (independentFields) {
                        out.add(new Field(name(), values[srcIdx], fieldType));
                        return;
                    }
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
                    while (srcIdx < end && (values[srcIdx] == null || (filter != null && filter.get(srcIdx - from) == false))) {
                        srcIdx++;
                    }
                    return srcIdx >= end ? DocIdSetIterator.NO_MORE_DOCS : srcIdx - from;
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
