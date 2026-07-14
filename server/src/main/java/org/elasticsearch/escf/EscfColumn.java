/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SliceableColumn;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;

/**
 * A direct-access view over a single ESCF leaf column, windowed to a contiguous sub-range
 * of the column's backing data. Each kind is a subtype that reads its payload in place from
 * the column's native, possibly-paged {@link BytesReference} (plus {@link IntsRef} offsets /
 * {@link org.apache.lucene.util.BytesRef} type vector / {@link FixedBitSet} metadata).
 *
 * <p>All backing arrays are shared between a parent column and its slices; windowing for
 * array-backed fields is expressed via the {@code offset} and {@code length} of the
 * appropriate Ref. The {@link FixedBitSet}s ({@code absent} and the BOOL {@code values}
 * bitset) do not support an offset, so they are rewritten to a zero-based window on each
 * {@link #slice} call. Fixed-width data payloads are windowed via a zero-copy
 * {@link BytesReference#slice}; variable-width data payloads are kept full and addressed
 * through the windowed offsets.
 *
 * <p>Layout is shared further down via {@link AbstractFixed64Column} (long/double) and
 * {@link AbstractVarColumn} (string/binary). Typed value getters default to throwing; each
 * subtype overrides only what it supports.
 */
abstract class EscfColumn implements SliceableColumn {

    final int docCount;

    /**
     * Absent set (bit set = absent), or {@code null} when every document is present (dense).
     * Always zero-based and covers {@code [0, docCount)} — either {@code null}, or a
     * {@link FixedBitSet} of size {@code Math.max(1, docCount)}.
     */
    final FixedBitSet absent;

    EscfColumn(int docCount, FixedBitSet absent) {
        this.docCount = docCount;
        this.absent = absent;
    }

    /** The column kind (see {@link EscfColumnKind}). */
    abstract byte kind();

    /**
     * Builds the typed column view for {@code col}, dispatching on its kind. The fields are
     * already in their native (zero-based, full-window) form. Array-backed factors are wrapped
     * into Refs so slicing can adjust {@code offset}/{@code length} without copying.
     * The {@code absent} bitset is normalized to {@code null} (no absent documents) or a
     * {@link FixedBitSet} that covers {@code [0, docCount)}.
     */
    static EscfColumn from(EscfColumnData col) {
        int docCount = col.docCount();
        // Normalize the absent bitset to [0, docCount): windowBitSet returns null when no bits
        // are set (same semantics) and a properly-sized FixedBitSet otherwise.
        FixedBitSet absent = windowBitSet(col.absent(), 0, docCount);
        return switch (col.kind()) {
            case EscfColumnKind.LONG -> new EscfLongColumn(docCount, absent, col.data());
            case EscfColumnKind.DOUBLE -> new EscfDoubleColumn(docCount, absent, col.data());
            case EscfColumnKind.BOOL -> new EscfBoolColumn(docCount, absent, windowBitSet(col.values(), 0, docCount));
            case EscfColumnKind.STRING -> new EscfStringColumn(docCount, absent, col.data(), new IntsRef(col.offsets(), 0, docCount + 1));
            case EscfColumnKind.BINARY -> new EscfBinaryColumn(docCount, absent, col.data(), new IntsRef(col.offsets(), 0, docCount + 1));
            case EscfColumnKind.ARRAY -> new EscfArrayColumn(
                docCount,
                absent,
                from(col.child()),
                new IntsRef(col.offsets(), 0, docCount + 1)
            );
            case EscfColumnKind.UNION -> new EscfUnionColumn(
                docCount,
                absent,
                new BytesRef(col.typeVector(), 0, docCount),
                new IntsRef(col.offsets(), 0, docCount + 1),
                col.data()
            );
            default -> throw new IllegalStateException("Unknown ESCF column kind: " + EscfColumnKind.name(col.kind()));
        };
    }

    final boolean isAbsent(int d) {
        if (d < 0 || d >= docCount) {
            return true;
        }
        // absent is always null or a FixedBitSet covering [0, docCount), so no length guard is needed.
        return absent != null && absent.get(d);
    }

    final byte getTypeByte(int d) {
        if (d < 0 || d >= docCount || isAbsent(d)) {
            return SourceValueType.ABSENT;
        }
        return typeByteForPresent(d);
    }

    /** The {@link SourceValueType} byte for document {@code d}, which is known to be present. */
    abstract byte typeByteForPresent(int d);

    final boolean isNull(int d) {
        return getTypeByte(d) == SourceValueType.NULL;
    }

    // Typed value getters — default to throwing; subtypes override what they support.

    boolean getBooleanValue(int d) {
        throw notA("boolean");
    }

    long getLongValue(int d) {
        throw notA("long");
    }

    double getDoubleValue(int d) {
        throw notA("double");
    }

    /** Narrows {@link #getLongValue} to an {@code int}, throwing if out of range. */
    int getIntValue(int d) {
        long val = getLongValue(d);
        if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
            throw new ArithmeticException("Long value " + val + " does not fit in int");
        }
        return (int) val;
    }

    /** Narrows {@link #getDoubleValue} to a {@code float}. */
    float getFloatValue(int d) {
        return (float) getDoubleValue(d);
    }

    Text getStringValue(int d) {
        throw notA("string");
    }

    BytesRef getBinaryValue(int d) {
        throw notA("binary");
    }

    ArrayReader getArrayValue(int d) {
        throw notA("array");
    }

    KeyValueReader getKeyValue(int d) {
        throw notA("key-value");
    }

    private IllegalStateException notA(String what) {
        return new IllegalStateException("Column kind=" + EscfColumnKind.name(kind()) + " has no " + what + " values");
    }

    /**
     * Returns a new column of the same subtype sharing this column's backing data, windowed to
     * the sub-range {@code [from, from + count)} of this column's current window {@code [0, docCount)}.
     * Array-backed factors (offsets, type vector) are re-windowed via Ref adjustment; the
     * {@code absent} bitset is rewritten zero-based for the new window.
     */
    @Override
    public final SliceableColumn slice(int from, int count) {
        return sliceInternal(from, count);
    }

    /**
     * Throws {@link UnsupportedOperationException} — direct ESCF→Lucene column conversion is not
     * yet implemented. Use {@link EscfLuceneColumn} for engine-metadata long columns.
     */
    @Override
    public Column toLuceneColumn() {
        throw new UnsupportedOperationException(
            "Direct ESCF-to-Lucene column conversion is not yet implemented for kind "
                + EscfColumnKind.name(kind())
                + "; wrap with EscfLuceneColumn for engine-metadata columns"
        );
    }

    /**
     * Throws {@link UnsupportedOperationException} — ESCF user-data column → per-doc Lucene field
     * conversion is not yet implemented. Use {@link EscfLuceneColumn} for engine-metadata longs.
     */
    @Override
    public RowFieldCursor rowFieldCursor() {
        throw new UnsupportedOperationException(
            "ESCF user-data column to row-field cursor is not yet implemented for kind "
                + EscfColumnKind.name(kind())
                + "; use EscfLuceneColumn for engine-metadata columns"
        );
    }

    /**
     * Returns a new column of the same subtype sharing all backing data, windowed to the
     * sub-range {@code [from, from + count)} of this column's current window. Package-private:
     * callers outside this package use {@link #slice} via the {@link SliceableColumn} interface.
     */
    abstract EscfColumn sliceInternal(int from, int count);

    /**
     * Materializes this column's current window as a zero-based {@link EscfColumnData} suitable
     * for serialization via {@link EscfBatchCodec}. Variable-width columns rebase their offset
     * vectors and slice their data payload; fixed-width and bool columns return their already-windowed
     * {@code data} / bitsets directly. Package-private.
     */
    abstract EscfColumnData toColumnData();

    /**
     * Returns a new {@link FixedBitSet} covering bits {@code [base, base + count)} of {@code src},
     * rebased to {@code [0, count)}, sized to {@code Math.max(1, count)}. Returns {@code null}
     * when {@code src} is {@code null} or no bits are set in the range (same semantics as a null
     * absent/values bitset).
     */
    static FixedBitSet windowBitSet(FixedBitSet src, int base, int count) {
        if (src == null) {
            return null;
        }
        FixedBitSet out = new FixedBitSet(Math.max(1, count));
        int cap = src.length();
        boolean anySet = false;
        for (int i = 0; i < count; i++) {
            int idx = base + i;
            if (idx < cap && src.get(idx)) {
                out.set(i);
                anySet = true;
            }
        }
        return anySet ? out : null;
    }

    /**
     * Returns a new {@code int[count + 1]} offset array covering entries {@code [ir.offset,
     * ir.offset + count]} of {@code ir.ints}, rebased so that {@code out[0] == 0}. Used by
     * variable-width, union, and array columns when materializing a window for serialization.
     *
     * @param ir    a windowed ref whose {@code offset} locates the first entry and whose backing
     *              {@code ints} array holds absolute byte/element offsets
     * @param count the number of documents in the window (the offset array has {@code count + 1}
     *              entries)
     */
    static int[] rebasedOffsets(IntsRef ir, int count) {
        int base = ir.offset;
        int rebase = ir.ints[base];
        int[] out = new int[count + 1];
        for (int i = 0; i <= count; i++) {
            out[i] = ir.ints[base + i] - rebase;
        }
        return out;
    }
}
