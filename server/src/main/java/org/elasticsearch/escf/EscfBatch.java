/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.transport.BytesRefRecycler;

/**
 * An Elasticsearch Column Format batch: a column-major {@link SourceBatch} backed by an array
 * of {@link EscfColumnData}. Built in memory by {@link EscfEncoder} or reconstructed from
 * serialized bytes via {@link #parse(BytesReference, Releasable)}.
 *
 * <p>This class is the in-memory container (schema, columns, row/column access, slicing, and RAM
 * accounting). The on-wire byte layout and the field-level codecs live in {@link EscfBatchCodec}.
 */
public final class EscfBatch implements SourceBatch {

    private final SourceSchema schema;
    private final int docCount;
    /** Original column data, kept for RAM accounting. {@code null} for slice views. */
    @Nullable
    private final EscfColumnData[] columnData;
    private final EscfColumn[] columns;
    private final Releasable releasable;
    @Nullable
    private final Recycler<BytesRef> recycler;
    @Nullable
    private final EscfBatch fullRangeParent;
    private BytesReference serialized;
    @Nullable
    private Releasable serializedPages;
    private boolean closed;

    /** In-memory construction path used by {@link EscfEncoder#buildPartition(int)}. */
    EscfBatch(SourceSchema schema, int docCount, EscfColumnData[] columnData, Recycler<BytesRef> recycler, Releasable releasable) {
        this(schema, docCount, columnData, null, recycler, releasable);
    }

    /** Full construction path, used by {@link EscfBatchCodec#parse} once the serialized bytes are already in hand. */
    EscfBatch(SourceSchema schema, int docCount, EscfColumnData[] columnData, BytesReference serialized, Releasable releasable) {
        this(schema, docCount, columnData, serialized, null, releasable);
    }

    private EscfBatch(
        SourceSchema schema,
        int docCount,
        EscfColumnData[] columnData,
        @Nullable BytesReference serialized,
        @Nullable Recycler<BytesRef> recycler,
        Releasable releasable
    ) {
        this.schema = schema;
        this.docCount = docCount;
        this.columnData = columnData;
        this.columns = buildColumns(columnData);
        this.releasable = releasable;
        this.recycler = recycler;
        this.fullRangeParent = null;
        this.serialized = serialized;
    }

    /** Slice construction — shares backing data with the parent via adjusted column bases. */
    private EscfBatch(SourceSchema schema, int docCount, EscfColumn[] columns, @Nullable EscfBatch fullRangeParent) {
        this.schema = schema;
        this.docCount = docCount;
        this.columnData = null; // slice views share the parent's RAM; no separate accounting
        this.columns = columns;
        this.releasable = () -> {};
        this.recycler = null;
        this.fullRangeParent = fullRangeParent;
        this.serialized = null;
    }

    private static EscfColumn[] buildColumns(EscfColumnData[] data) {
        EscfColumn[] cols = new EscfColumn[data.length];
        for (int i = 0; i < data.length; i++) {
            cols[i] = EscfColumn.from(data[i]);
        }
        return cols;
    }

    /** Serialized construction path: parse a batch from its wire/translog bytes via {@link EscfBatchCodec}. */
    public static EscfBatch parse(BytesReference data, Releasable releasable) {
        return EscfBatchCodec.parse(data, releasable);
    }

    @Override
    public int docCount() {
        return docCount;
    }

    @Override
    public SourceSchema schema() {
        return schema;
    }

    @Override
    public BytesReference data() {
        assert closed == false : "batch already closed";
        // TODO: Eventually optimize to be more stream like on the serialization path.
        if (serialized == null) {
            if (fullRangeParent != null) {
                serialized = fullRangeParent.data();
            } else {
                EscfColumnData[] dataForSerialize = new EscfColumnData[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    dataForSerialize[i] = columns[i].toColumnData();
                }
                ReleasableBytesReference bytes = EscfBatchCodec.serialize(
                    schema,
                    docCount,
                    dataForSerialize,
                    recycler != null ? recycler : BytesRefRecycler.NON_RECYCLING_INSTANCE
                );
                serializedPages = bytes;
                serialized = bytes;
            }
        }
        return serialized;
    }

    @Override
    public int columnCount() {
        return schema.leafCount();
    }

    @Override
    public SourceRow row(int docIndex) {
        assert closed == false : "batch already closed";
        if (docIndex < 0 || docIndex >= docCount) {
            throw new IndexOutOfBoundsException("docIndex " + docIndex + " out of range [0, " + docCount + ")");
        }
        return new EscfRow(this, docIndex);
    }

    /** The typed view for {@code columnIndex}. */
    public EscfColumn column(int columnIndex) {
        assert closed == false : "batch already closed";
        return columns[columnIndex];
    }

    @Override
    public boolean isEmptyObjectColumn(int columnIndex) {
        return EscfColumnTransforms.allNullOrEmptyObject(columns[columnIndex]);
    }

    @Override
    public SourceBatch slice(int from, int to) {
        assert closed == false : "batch already closed";
        if (from < 0 || to > docCount || from > to) {
            throw new IndexOutOfBoundsException("slice [" + from + ", " + to + ") out of [0, " + docCount + ")");
        }
        int newDocCount = to - from;
        EscfColumn[] slicedColumns = new EscfColumn[columns.length];
        for (int c = 0; c < columns.length; c++) {
            slicedColumns[c] = columns[c].sliceInternal(from, newDocCount);
        }
        // A full-range slice serializes through its parent; partial slices must be re-serialized at their
        // new base (slices must not inherit mismatched wire bytes).
        return new EscfBatch(schema, newDocCount, slicedColumns, (from == 0 && to == docCount) ? this : null);
    }

    @Override
    public void close() {
        assert closed == false : "batch already closed";
        closed = true;
        Releasables.close(serializedPages, releasable);
    }

    /**
     * Estimates the bytes this batch contributes to the Lucene indexing buffer.
     */
    @Override
    public int estimatedBytes() {
        int size = schemaBytes(schema);
        if (columnData != null) {
            for (EscfColumnData col : columnData) {
                size += valueBytes(col);
            }
            return size;
        }
        // Slice views share the parent's backing; materialize each column's parent's data.
        for (EscfColumn column : columns) {
            size += valueBytes(column.toColumnData());
        }
        return size;
    }

    /** Raw value bytes of one column: the payload ref length, or the kind-specific equivalent when there is no payload. */
    private static int valueBytes(EscfColumnData col) {
        return switch (col.kind()) {
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE, EscfColumnKind.STRING, EscfColumnKind.BINARY, EscfColumnKind.UNION ->
                (int) refLen(col.data());
            // BOOL has no byte payload: values are one bit per document
            case EscfColumnKind.BOOL -> EscfBatchCodec.bitsetBytes(col.docCount());
            // ARRAY has no payload of its own: the elements live in the child column
            case EscfColumnKind.ARRAY -> valueBytes(col.child());
            default -> throw new IllegalStateException("Unknown ESCF column kind: " + EscfColumnKind.name(col.kind()));
        };
    }

    /** UTF-8 bytes of every leaf's full field path. */
    private static int schemaBytes(SourceSchema schema) {
        int size = 0;
        for (int i = 0; i < schema.leafCount(); i++) {
            String path = schema.getFullPath(i);
            size += UnicodeUtil.calcUTF16toUTF8Length(path, 0, path.length());
        }
        return size;
    }

    @Override
    public long ramBytesUsed() {
        if (serialized != null) {
            return serialized.length() + 64L;
        }
        if (columnData != null) {
            long total = 64L;
            for (EscfColumnData col : columnData) {
                total += columnRamBytes(col);
            }
            return total;
        }
        // Slice views share the parent's backing; the parent already accounts for all RAM.
        return 64L;
    }

    /** Sums a column's own live storage plus, for ARRAY, its nested {@code child} column's storage. */
    private static long columnRamBytes(EscfColumnData col) {
        long total = bitsetRam(col.validity()) + bitsetRam(col.values()) + (col.typeVector() != null ? col.typeVector().length : 0L) + (col
            .offsets() != null ? col.offsets().length * 4L : 0L) + refLen(col.data());
        if (col.child() != null) {
            total += columnRamBytes(col.child());
        }
        return total;
    }

    private static long bitsetRam(FixedBitSet bs) {
        return bs == null ? 0L : (long) bs.getBits().length * 8;
    }

    private static long refLen(BytesReference ref) {
        return ref == null ? 0L : ref.length();
    }
}
