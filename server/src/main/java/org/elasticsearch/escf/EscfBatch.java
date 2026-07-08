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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceSchema;

import java.util.Arrays;

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
    private final EscfColumnData[] columns;
    private final EscfColumn[] columnCache;
    private final Releasable releasable;
    private BytesReference serialized;

    /** In-memory construction path used by {@link EscfEncoder#buildPartition(int)}. */
    EscfBatch(SourceSchema schema, int docCount, EscfColumnData[] columns, Releasable releasable) {
        this(schema, docCount, columns, null, releasable);
    }

    /** Full construction path, used by {@link EscfBatchCodec#parse} once the serialized bytes are already in hand. */
    EscfBatch(SourceSchema schema, int docCount, EscfColumnData[] columns, BytesReference serialized, Releasable releasable) {
        this.schema = schema;
        this.docCount = docCount;
        this.columns = columns;
        this.columnCache = new EscfColumn[columns.length];
        this.releasable = releasable;
        this.serialized = serialized;
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
        if (serialized == null) {
            serialized = EscfBatchCodec.serialize(schema, docCount, columns);
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

    /** The typed view for {@code columnIndex}, lazily built and cached. Package-private: used by {@link EscfRow}. */
    EscfColumn column(int columnIndex) {
        EscfColumn cached = columnCache[columnIndex];
        if (cached != null) {
            return cached;
        }
        EscfColumn built = EscfColumn.from(columns[columnIndex]);
        columnCache[columnIndex] = built;
        return built;
    }

    /**
     * Returns a view of this batch containing rows in {@code [from, to)}. Column data is shared with
     * the parent via {@link BytesReference#slice}, not copied. The returned batch holds no ownership
     * over the parent's underlying buffers — closing it is a no-op; the parent must be closed instead.
     */
    @Override
    public SourceBatch slice(int from, int to) {
        if (from < 0 || to > docCount || from > to) {
            throw new IndexOutOfBoundsException("slice [" + from + ", " + to + ") out of [0, " + docCount + ")");
        }
        if (from == 0 && to == docCount) {
            return new EscfBatch(schema, docCount, columns, () -> {});
        }
        int newDocCount = to - from;
        EscfColumnData[] newColumns = new EscfColumnData[columns.length];
        for (int c = 0; c < columns.length; c++) {
            newColumns[c] = sliceColumn(columns[c], from, newDocCount);
        }
        return new EscfBatch(schema, newDocCount, newColumns, () -> {});
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
        long total = 64L;
        for (EscfColumnData col : columns) {
            total += columnRamBytes(col);
        }
        return total;
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

    private static EscfColumnData sliceColumn(EscfColumnData col, int from, int newCount) {
        FixedBitSet absent = col.absent() != null ? sliceBitset(col.absent(), from, newCount) : null;
        if (col.kind() == EscfColumnKind.ARRAY) {
            int[] rowOffsets = col.offsets();
            int elemFrom = rowOffsets[from];
            int elemTo = rowOffsets[from + newCount];
            int[] newRowOffsets = rebasedOffsets(rowOffsets, from, newCount, elemFrom);
            // The child is a native EscfColumnData (STRING or LONG/DOUBLE), so it slices via the
            // same generic paths below rather than hand-parsed bytes.
            EscfColumnData childSlice = sliceColumn(col.child(), elemFrom, elemTo - elemFrom);
            return EscfColumnData.ofArray(newCount, absent, newRowOffsets, childSlice);
        }
        if (col.offsets() != null) {
            byte[] typeVector = col.typeVector() != null ? Arrays.copyOfRange(col.typeVector(), from, from + newCount) : null;
            int[] srcOffsets = col.offsets();
            int byteFrom = srcOffsets[from];
            int byteTo = srcOffsets[from + newCount];
            BytesReference data = col.data().slice(byteFrom, byteTo - byteFrom);
            int[] offsets = rebasedOffsets(srcOffsets, from, newCount, byteFrom);
            return typeVector != null
                ? EscfColumnData.ofUnion(newCount, absent, typeVector, offsets, data)
                : EscfColumnData.ofVarWidth(col.kind(), newCount, absent, offsets, data);
        }
        if (col.kind() == EscfColumnKind.BOOL) {
            FixedBitSet values = col.values() != null ? sliceBitset(col.values(), from, newCount) : null;
            return EscfColumnData.ofBool(newCount, absent, values);
        }
        // LONG / DOUBLE: 8-byte slots
        BytesReference data = col.data().slice(from * 8, newCount * 8);
        return EscfColumnData.ofFixed64(col.kind(), newCount, absent, data);
    }

    // TODO Zero copy with Lucene IntsRef
    private static int[] rebasedOffsets(int[] offsets, int from, int newCount, int rebase) {
        int[] out = new int[newCount + 1];
        for (int i = 0; i <= newCount; i++) {
            out[i] = offsets[from + i] - rebase;
        }
        return out;
    }

    private static FixedBitSet sliceBitset(FixedBitSet src, int from, int count) {
        FixedBitSet out = new FixedBitSet(Math.max(1, count));
        int cap = src.length();
        for (int i = 0; i < count; i++) {
            int idx = from + i;
            if (idx < cap && src.get(idx)) {
                out.set(i);
            }
        }
        return out;
    }
}
