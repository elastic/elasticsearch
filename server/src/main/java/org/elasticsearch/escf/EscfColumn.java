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
 * {@link BytesRef} type vector / {@link FixedBitSet} metadata).
 */
abstract class EscfColumn implements SliceableColumn {

    final int docCount;

    /** Absent set (bit set = absent), or {@code null} when every document is present (dense). */
    final FixedBitSet absent;

    EscfColumn(int docCount, FixedBitSet absent) {
        this.docCount = docCount;
        this.absent = absent;
    }

    /** The column kind (see {@link EscfColumnKind}). */
    abstract byte kind();

    /** Builds the typed column view for {@code col}, dispatching on its kind. The fields are already native. */
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
                col.typeVector(),
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

    @Override
    public final SliceableColumn slice(int from, int count) {
        return sliceInternal(from, count);
    }

    abstract EscfColumn sliceInternal(int from, int count);

    abstract EscfColumnData toColumnData();

    /**
     * Extracts a {@code count}-bit window starting at {@code base} from {@code src}, re-indexed to
     * {@code [0, count)}. Returns {@code null} when {@code src} is {@code null} (dense) or when no
     * bits in the window are set (also dense), preserving the invariant that a {@code null} absent set
     * means every document is present.
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
     * Materializes the {@code count + 1} offset entries from {@code ir}'s current window into a fresh
     * {@code int[]}, subtracting the first entry so the result always starts at zero. Used when
     * serializing a windowed column back to {@link EscfColumnData}, where offsets must be
     * self-contained (not relative to a larger backing array).
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

    /**
     * Returns a window into {@code offsets} starting at entry {@code from} and covering {@code count}
     * rows (i.e. {@code count + 1} offset entries — one fence post per row boundary).
     */
    static IntsRef sliceOffsets(IntsRef offsets, int from, int count) {
        return new IntsRef(offsets.ints, offsets.offset + from, count + 1);
    }

    /**
     * Slices {@code data} to the byte range referenced by the current window of {@code offsets}
     * ({@code [offsets[0], offsets[count])}).
     */
    static BytesReference sliceData(IntsRef offsets, BytesReference data, int count) {
        int byteFrom = offsets.ints[offsets.offset];
        return data.slice(byteFrom, offsets.ints[offsets.offset + count] - byteFrom);
    }
}
