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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SliceableColumn;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;

/**
 * A direct-access view over a single ESCF leaf column, windowed to a contiguous sub-range
 * {@code [base, base + docCount)} of the column's backing data. Each kind is a subtype that reads
 * its payload in place from the column's native, possibly-paged {@link BytesReference}
 * (plus native {@code int[]} offsets / {@link FixedBitSet} metadata).
 *
 * <p>Layout is shared further down via {@link AbstractFixed64Column} (long/double) and
 * {@link AbstractVarColumn} (string/binary). Typed value getters default to throwing; each subtype
 * overrides only what it supports.
 *
 * <p>Implements {@link SliceableColumn}: {@link #slice} returns a new same-subtype view sharing
 * all backing data, with {@code base'} = {@code base + from}; no copying occurs. Direct
 * ESCF→Lucene conversion ({@link #toLuceneColumn}) is not yet implemented and throws; use
 * {@link EscfLuceneColumn} for engine-metadata longs.
 */
abstract class EscfColumn implements SliceableColumn {

    /** Number of documents in this window (may be smaller than the full backing column). */
    final int docCount;

    /**
     * Absent set (bit set = absent), or {@code null} when every document is present (dense).
     * Indexed by absolute position ({@code base + d}) — shared with parent windows.
     */
    final FixedBitSet absent;

    /**
     * Absolute start offset into the backing data. All reads use {@code base + d} as the
     * absolute index. For freshly-parsed columns this is 0; for slices it equals the parent
     * column's {@code base + from}.
     */
    final int base;

    EscfColumn(int docCount, FixedBitSet absent) {
        this(docCount, absent, 0);
    }

    EscfColumn(int docCount, FixedBitSet absent, int base) {
        this.docCount = docCount;
        this.absent = absent;
        this.base = base;
    }

    /** The column kind (see {@link EscfColumnKind}). */
    abstract byte kind();

    /** Builds the typed column view for {@code col}, dispatching on its kind. The fields are already native. */
    static EscfColumn from(EscfColumnData col) {
        int docCount = col.docCount();
        FixedBitSet absent = col.absent();
        return switch (col.kind()) {
            case EscfColumnKind.LONG -> new EscfLongColumn(docCount, absent, col.data());
            case EscfColumnKind.DOUBLE -> new EscfDoubleColumn(docCount, absent, col.data());
            case EscfColumnKind.BOOL -> new EscfBoolColumn(docCount, absent, col.values());
            case EscfColumnKind.STRING -> new EscfStringColumn(docCount, absent, col.data(), col.offsets());
            case EscfColumnKind.BINARY -> new EscfBinaryColumn(docCount, absent, col.data(), col.offsets());
            case EscfColumnKind.ARRAY -> new EscfArrayColumn(docCount, absent, from(col.child()), col.offsets());
            case EscfColumnKind.UNION -> new EscfUnionColumn(docCount, absent, col.typeVector(), col.offsets(), col.data());
            default -> throw new IllegalStateException("Unknown ESCF column kind: " + EscfColumnKind.name(col.kind()));
        };
    }

    final boolean isAbsent(int d) {
        if (d < 0 || d >= docCount) {
            return true;
        }
        // The absent bitset is indexed by the absolute position (base + d) and only sized to the last
        // absent document (it may be narrower than the full column), so any position beyond its length is present.
        int idx = base + d;
        return absent != null && idx < absent.length() && absent.get(idx);
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

    // =========================================================================
    // SliceableColumn implementation
    // =========================================================================

    /**
     * Returns a new column of the same subtype sharing this column's backing data, windowed to
     * {@code [base + from, base + from + count)}. No copying occurs. {@code from} is relative to
     * this column's current window {@code [0, docCount)}.
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
     * Returns a new column of the same subtype sharing all backing data, windowed to
     * {@code [base + from, base + from + count)}. Package-private: callers outside this package
     * use {@link #slice} via the {@link SliceableColumn} interface.
     */
    abstract EscfColumn sliceInternal(int from, int count);

    /**
     * Materializes this column's current window {@code [base, base + docCount)} as a zero-based,
     * dense {@link EscfColumnData} suitable for serialization via {@link EscfBatchCodec}. For
     * columns with {@code base == 0} and no window adjustment, the original backing data may be
     * returned directly to avoid copies. Package-private.
     */
    abstract EscfColumnData toColumnData();

    // =========================================================================
    // Static helpers shared by subtypes
    // =========================================================================

    /**
     * Returns a new {@link FixedBitSet} covering bits {@code [base, base + count)} of {@code src},
     * rebased to {@code [0, count)}. Returns {@code null} when no bits are set in the range (same
     * semantics as a null absent/values bitset).
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
     * Returns a new {@code int[count + 1]} offset array that covers entries {@code [base, base + count]}
     * of {@code offsets}, rebased so that {@code out[0] == 0}. Used by variable-width and union
     * columns when materializing a window for serialization.
     */
    static int[] rebasedOffsets(int[] offsets, int base, int count) {
        int rebase = offsets[base];
        int[] out = new int[count + 1];
        for (int i = 0; i <= count; i++) {
            out[i] = offsets[base + i] - rebase;
        }
        return out;
    }
}
