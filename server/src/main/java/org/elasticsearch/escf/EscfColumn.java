/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
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
public abstract class EscfColumn implements SliceableColumn {

    final int docCount;

    /**
     * Validity bitset (bit set = present/valid), or {@code null} when every document is
     * present (dense). Always zero-based and covers {@code [0, docCount)} when non-null.
     */
    final FixedBitSet validity;

    EscfColumn(int docCount, FixedBitSet validity) {
        if (docCount >= DocIdSetIterator.NO_MORE_DOCS) {
            throw new IllegalArgumentException(
                "docCount " + docCount + " must be less than DocIdSetIterator.NO_MORE_DOCS (" + DocIdSetIterator.NO_MORE_DOCS + ")"
            );
        }
        assert validity == null || validity.length() == docCount : "validity length " + validity.length() + " != docCount " + docCount;
        this.docCount = docCount;
        this.validity = validity;
    }

    /** The column kind (see {@link EscfColumnKind}). */
    public abstract byte kind();

    /**
     * The kind of this column's leaf (scalar) values: this column's own {@link #kind()} for scalar
     * columns, or the element child's kind for an {@link EscfArrayColumn}.
     */
    public byte leafValueKind() {
        return kind();
    }

    /**
     * Returns this column's backing data as an {@link EscfColumnData}, reusing the existing byte
     * storage (no per-value copy). Symmetric with {@link #from(EscfColumnData)}, this enables
     * mapper code outside this package to re-wrap a source column under a different Lucene field
     * type without going through the value-at-a-time {@link EscfColumnBuilder}.
     */
    public final EscfColumnData columnData() {
        return toColumnData();
    }

    /** Builds the typed column view for {@code col}, dispatching on its kind. The fields are already native. */
    public static EscfColumn from(EscfColumnData col) {
        int docCount = col.docCount();
        FixedBitSet validity = windowValidity(col.validity(), 0, docCount);
        return switch (col.kind()) {
            case EscfColumnKind.LONG -> new EscfLongColumn(docCount, validity, col.data());
            case EscfColumnKind.DOUBLE -> new EscfDoubleColumn(docCount, validity, col.data());
            case EscfColumnKind.BOOL -> new EscfBoolColumn(docCount, validity, windowBitSet(col.values(), 0, docCount));
            case EscfColumnKind.STRING -> new EscfStringColumn(docCount, validity, col.data(), new IntsRef(col.offsets(), 0, docCount + 1));
            case EscfColumnKind.BINARY -> new EscfBinaryColumn(docCount, validity, col.data(), new IntsRef(col.offsets(), 0, docCount + 1));
            case EscfColumnKind.ARRAY -> new EscfArrayColumn(
                docCount,
                validity,
                from(col.child()),
                new IntsRef(col.offsets(), 0, docCount + 1)
            );
            case EscfColumnKind.UNION -> new EscfUnionColumn(
                docCount,
                validity,
                col.typeVector(),
                new IntsRef(col.offsets(), 0, docCount + 1),
                col.data()
            );
            default -> throw new IllegalStateException("Unknown ESCF column kind: " + EscfColumnKind.name(col.kind()));
        };
    }

    /** The number of documents in this column window (present and absent). */
    public final int docCount() {
        return docCount;
    }

    /** A forward-only iterator over this column's present (non-absent) doc ids. */
    public final PresentDocIterator presentDocs() {
        return new PresentDocIterator(validity, docCount);
    }

    final boolean isAbsent(int row) {
        if (row < 0 || row >= docCount) {
            return true;
        }
        // validity is always null (all-present) or a FixedBitSet covering [0, docCount), so no length guard needed.
        // A set bit means present; a clear bit or null means all-present (dense).
        return validity != null && validity.get(row) == false;
    }

    /** Returns {@code true} if the document at {@code row} is present (has a value). */
    public final boolean isPresent(int row) {
        return isAbsent(row) == false;
    }

    /**
     * Returns the {@link SourceValueType} byte for document {@code row}. Returns
     * {@link SourceValueType#ABSENT} when the row is out of bounds or absent.
     *
     * <p>These getters are intentionally public — the same information is already publicly
     * reachable via {@code EscfBatch.row(d)} (which allocates an {@code EscfRow}); widening the
     * modifier avoids that per-row allocation for callers that scan an entire column.
     */
    public final byte getTypeByte(int row) {
        if (row < 0 || row >= docCount || isAbsent(row)) {
            return SourceValueType.ABSENT;
        }
        return typeByteForPresent(row);
    }

    /** The {@link SourceValueType} byte for document {@code row}, which is known to be present. */
    abstract byte typeByteForPresent(int row);

    /** Returns {@code true} if document {@code row} holds an explicit JSON {@code null}. */
    public final boolean isNull(int row) {
        return getTypeByte(row) == SourceValueType.NULL;
    }

    // Typed value getters — default to throwing; subtypes override what they support.
    // Public modifier: the same values are already reachable via EscfBatch.row(d) (which returns
    // an EscfRow that delegates to these methods). Making them public removes the per-row
    // EscfRow allocation for column-scanning callers.

    /** Returns the boolean value at {@code row}. The column kind must be {@link EscfColumnKind#BOOL}. */
    public boolean getBooleanValue(int row) {
        throw notA("boolean");
    }

    /** Returns the long value at {@code row}. The column kind must be {@link EscfColumnKind#LONG}. */
    public long getLongValue(int row) {
        throw notA("long");
    }

    /** Returns the double value at {@code row}. The column kind must be {@link EscfColumnKind#DOUBLE}. */
    public double getDoubleValue(int row) {
        throw notA("double");
    }

    /**
     * Narrows {@link #getLongValue} to an {@code int}, throwing if out of range.
     * Valid for {@link EscfColumnKind#UNION} columns whose row type is {@link org.elasticsearch.sourcebatch.SourceValueType#INT}.
     */
    public int getIntValue(int row) {
        long val = getLongValue(row);
        if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
            throw new ArithmeticException("Long value " + val + " does not fit in int");
        }
        return (int) val;
    }

    /**
     * Narrows {@link #getDoubleValue} to a {@code float}.
     * Valid for {@link EscfColumnKind#UNION} columns whose row type is {@link org.elasticsearch.sourcebatch.SourceValueType#FLOAT}.
     */
    public float getFloatValue(int row) {
        return (float) getDoubleValue(row);
    }

    /** Returns the string value at {@code row}. The column kind must be {@link EscfColumnKind#STRING}. */
    public Text getStringValue(int row) {
        throw notA("string");
    }

    /** Returns the binary value at {@code row}. The column kind must be {@link EscfColumnKind#BINARY}. */
    public BytesRef getBinaryValue(int row) {
        throw notA("binary");
    }

    /** Returns the array value at {@code row}. The column kind must be {@link EscfColumnKind#ARRAY}. */
    public ArrayReader getArrayValue(int row) {
        throw notA("array");
    }

    /** Returns the key-value reader at {@code row}. The column kind must be a key-value type. */
    public KeyValueReader getKeyValue(int row) {
        throw notA("key-value");
    }

    /**
     * Returns a forward-only {@link LongTupleCursor} positioned before the first row. Subtypes that
     * hold long values override this; the default throws.
     */
    public LongTupleCursor longCursor() {
        throw notA("long");
    }

    /**
     * Returns a forward-only {@link ObjectTupleCursor}{@code <BytesRef>} positioned before the first
     * row. Subtypes that hold byte-string values override this; the default throws.
     *
     * @param retainValues {@code false} to reuse a single {@link BytesRef} across the whole scan (valid
     *                     only until the next {@link ObjectTupleCursor#nextDoc()}, and allocation-free);
     *                     {@code true} to hand back a fresh {@link BytesRef} per value, for callers that
     *                     keep values past the cursor position
     */
    public ObjectTupleCursor<BytesRef> bytesRefCursor(boolean retainValues) {
        throw notA("binary");
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
     * Extracts a {@code count}-bit window of an validity bitset (bit set = present) starting
     * at {@code base} from {@code src}, re-indexed to {@code [0, count)}. Returns {@code null} when
     * {@code src} is {@code null} (all-present / dense) or when every bit in the window is set (also
     * all-present), preserving the invariant that a {@code null} validity means every document is present.
     */
    static FixedBitSet windowValidity(FixedBitSet src, int base, int count) {
        if (src == null) {
            return null;
        }
        FixedBitSet out = null;
        for (int i = 0; i < count; i++) {
            if (src.get(base + i)) {
                if (out != null) {
                    out.set(i);
                }
            } else {
                if (out == null) {
                    out = new FixedBitSet(count);
                    out.set(0, i); // backfill all prior docs in the window as present
                }
                // leave bit[i] clear — this doc is absent
            }
        }
        return out;
    }

    /**
     * Extracts a {@code count}-bit window starting at {@code base} from {@code src}, re-indexed to
     * {@code [0, count)}. Returns {@code null} when {@code src} is {@code null} or when no bits in the
     * window are set, preserving the invariant that a {@code null} bitset means all bits are clear.
     * Used for the BOOL {@code values} bitset (bit set = {@code true}); not for validity.
     */
    static FixedBitSet windowBitSet(FixedBitSet src, int base, int count) {
        if (src == null) {
            return null;
        }
        FixedBitSet out = null;
        int cap = src.length();
        for (int i = 0; i < count; i++) {
            int idx = base + i;
            if (idx < cap && src.get(idx)) {
                if (out == null) {
                    out = new FixedBitSet(count);
                }
                out.set(i);
            }
        }
        return out;
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
        if (rebase == 0 && base == 0 && ir.ints.length == count + 1) {
            return ir.ints;
        }
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
        if (from == 0 && offsets.length == count + 1) {
            return offsets;
        }
        return new IntsRef(offsets.ints, offsets.offset + from, count + 1);
    }

    /**
     * Slices {@code data} to the byte range referenced by the current window of {@code offsets}
     * ({@code [offsets[0], offsets[count])}).
     */
    static BytesReference sliceData(IntsRef offsets, BytesReference data, int count) {
        int byteFrom = intAt(offsets, 0);
        int byteTo = intAt(offsets, count);
        if (byteFrom == 0 && byteTo == data.length()) {
            return data;
        }
        return data.slice(byteFrom, byteTo - byteFrom);
    }

    /** Returns the {@code i}-th logical entry of an {@link IntsRef}, accounting for its {@code offset}. */
    static int intAt(IntsRef ir, int i) {
        return ir.ints[ir.offset + i];
    }

    /** Returns the {@code i}-th logical byte of a {@link BytesRef}, accounting for its {@code offset}. */
    static byte byteAt(BytesRef br, int i) {
        return br.bytes[br.offset + i];
    }
}
