/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceSchema;

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
    private BytesReference serialized;

    /** In-memory construction path used by {@link EscfEncoder#buildPartition(int)}. */
    EscfBatch(SourceSchema schema, int docCount, EscfColumnData[] columnData, Releasable releasable) {
        this(schema, docCount, columnData, null, releasable);
    }

    /** Full construction path, used by {@link EscfBatchCodec#parse} once the serialized bytes are already in hand. */
    EscfBatch(SourceSchema schema, int docCount, EscfColumnData[] columnData, BytesReference serialized, Releasable releasable) {
        this.schema = schema;
        this.docCount = docCount;
        this.columnData = columnData;
        this.columns = buildColumns(columnData);
        this.releasable = releasable;
        this.serialized = serialized;
    }

    /** Slice construction — shares backing data with the parent via adjusted column bases. */
    private EscfBatch(SourceSchema schema, int docCount, EscfColumn[] columns, @Nullable BytesReference serialized) {
        this.schema = schema;
        this.docCount = docCount;
        this.columnData = null; // slice views share the parent's RAM; no separate accounting
        this.columns = columns;
        this.releasable = () -> {};
        this.serialized = serialized;
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
        // TODO: Eventually optimize to be more stream like on the serialization path.
        if (serialized == null) {
            EscfColumnData[] dataForSerialize = new EscfColumnData[columns.length];
            for (int i = 0; i < columns.length; i++) {
                dataForSerialize[i] = columns[i].toColumnData();
            }
            serialized = EscfBatchCodec.serialize(schema, docCount, dataForSerialize);
        }
        return serialized;
    }

    @Override
    public int columnCount() {
        return schema.leafCount();
    }

    @Override
    public SourceRow row(int docIndex) {
        if (docIndex < 0 || docIndex >= docCount) {
            throw new IndexOutOfBoundsException("docIndex " + docIndex + " out of range [0, " + docCount + ")");
        }
        return new EscfRow(this, docIndex);
    }

    /** The typed view for {@code columnIndex}. Package-private: used by {@link EscfRow}. */
    EscfColumn column(int columnIndex) {
        return columns[columnIndex];
    }

    @Override
    public SourceBatch slice(int from, int to) {
        if (from < 0 || to > docCount || from > to) {
            throw new IndexOutOfBoundsException("slice [" + from + ", " + to + ") out of [0, " + docCount + ")");
        }
        int newDocCount = to - from;
        EscfColumn[] slicedColumns = new EscfColumn[columns.length];
        for (int c = 0; c < columns.length; c++) {
            slicedColumns[c] = columns[c].sliceInternal(from, newDocCount);
        }
        // Preserve the cached serialized bytes only for a full-range slice; partial slices must be
        // re-serialized at their new base (slices must not inherit mismatched wire bytes).
        BytesReference slicedSerialized = (from == 0 && to == docCount) ? serialized : null;
        return new EscfBatch(schema, newDocCount, slicedColumns, slicedSerialized);
    }

    @Override
    public void close() {
        releasable.close();
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
        long total = bitsetRam(col.absent()) + bitsetRam(col.values()) + (col.typeVector() != null ? col.typeVector().length : 0L) + (col
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
