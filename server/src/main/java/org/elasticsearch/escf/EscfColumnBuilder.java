/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.sourcebatch.InlineArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

/**
 * Value-at-a-time accumulator for a single ESCF leaf column, serialized into an {@link EscfColumnData}
 * by {@link #finish(int)}. The {@link CollisionPolicy} governs scalar&harr;array collision behavior:
 * {@link CollisionPolicy#MERGE} coalesces a scalar and a compatible-kind array into one
 * {@link EscfColumnKind#ARRAY} column; {@link CollisionPolicy#SPLIT} promotes them to
 * {@link EscfColumnKind#UNION} to preserve per-row shape for faithful round-trip.
 *
 * <p>Two call surfaces: an <b>append</b> surface ({@code addX}/{@code addAbsent}, in row order) and a
 * <b>positional</b> surface ({@code setX(row, value)}, non-decreasing rows) plus element-append
 * ({@code beginArray}/{@code appendX}/{@code endArray}). Both share one {@code lastWrittenRow} cursor.
 *
 * <p>Not thread-safe.
 */
public final class EscfColumnBuilder {

    /** Selects the scalar&harr;array collision behavior; see the class Javadoc. */
    public enum CollisionPolicy {
        /** Lucene: coalesce scalar and compatible array into an {@link EscfColumnKind#ARRAY} column. */
        MERGE,
        /** Format: split scalar and array into a {@link EscfColumnKind#UNION}, preserving per-row shape. */
        SPLIT
    }

    /** Sentinel for an array column whose child kind has not yet been resolved (only empty arrays / no element seen). */
    private static final byte UNSET_ARRAY_KIND = -1;
    /** Sentinel returned by {@link #arrayChildKind} for arrays that aren't a single fixed primitive kind. */
    private static final byte UNION_CHILD_KIND = -2;
    private static final byte[] EMPTY_BYTES = new byte[0];
    /** Reusable zero buffer for bulk absent writes in {@link FixedNumericBuilder#addAbsents}. */
    private static final byte[] ZERO_BYTES = new byte[512];

    private final CollisionPolicy policy;
    private final Recycler<BytesRef> recycler;
    private TypedBuilder current;
    private int leadingAbsents;
    /** Row index of the last written value (not an absent gap); {@code -1} before any value. */
    private int lastWrittenRow = -1;
    /** {@code true} between {@link #beginArray} and {@link #endArray}. */
    private boolean arrayOpen;
    /** When set via {@link #lockScalar}, asserts that every scalar write uses this exact kind. {@code -1} = unrestricted. */
    private byte lockedKind = -1;

    public EscfColumnBuilder(CollisionPolicy policy) {
        this(policy, BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    public EscfColumnBuilder(CollisionPolicy policy, Recycler<BytesRef> recycler) {
        this.policy = policy;
        this.recycler = recycler;
    }

    /** Returns {@code true} if no present value has been recorded (absents/hints don't count); callers may skip {@link #finish}. */
    public boolean isEmpty() {
        return lastWrittenRow == -1;
    }

    /**
     * Hints that this column is a scalar of {@code kind}.
     */
    public void hintScalar(byte kind) {
        if (canHint()) {
            current = newScalarWithBackfill(kind);
        }
    }

    /**
     * Like {@link #hintScalar}, but also locks the column to {@code kind}: any subsequent scalar write
     * of a different kind triggers an assertion error. Use when the caller statically knows the kind
     * and a type mismatch is always a programming error (e.g. Lucene column builders).
     */
    public void lockScalar(byte kind) {
        hintScalar(kind);
        lockedKind = kind;
    }

    /**
     * Hints that this column is an array with elements of {@code childKind}.
     */
    public void hintArray(byte childKind) {
        assert childKind == EscfColumnKind.LONG
            || childKind == EscfColumnKind.DOUBLE
            || childKind == EscfColumnKind.STRING
            || childKind == EscfColumnKind.BINARY : "invalid array child kind: " + EscfColumnKind.name(childKind);
        if (canHint()) {
            ArrayBuilder ab = new ArrayBuilder(childKind, policy == CollisionPolicy.SPLIT, recycler);
            backfillLeadingAbsents(ab);
            current = ab;
        }
    }

    /** Hints that this column is heterogeneous. */
    public void hintUnion() {
        if (canHint()) {
            UnionBuilder union = new UnionBuilder(recycler);
            backfillLeadingAbsents(union);
            current = union;
        }
    }

    private boolean canHint() {
        return current == null && arrayOpen == false;
    }

    public void addAbsent() {
        if (current == null) {
            leadingAbsents++;
        } else {
            current.addAbsent();
        }
    }

    /** Records {@code n} consecutive absent rows; more efficient than {@code n} {@link #addAbsent()} calls. */
    public void addAbsents(int n) {
        if (n <= 0) {
            return;
        }
        if (current == null) {
            leadingAbsents += n;
        } else {
            current.addAbsents(n);
        }
    }

    public void addLong(long value) {
        setLong(nextAppendRow(), value);
    }

    public void addDouble(double value) {
        setDouble(nextAppendRow(), value);
    }

    public void addBoolean(boolean value) {
        setBoolean(nextAppendRow(), value);
    }

    public void addString(XContentString.UTF8Bytes utf8) {
        setString(nextAppendRow(), utf8);
    }

    /**
     * Adds a packed inline array. Fixed single-kind arrays are decoded into the columnar child;
     * heterogeneous/nested arrays, or a child-kind mismatch, go inline on a union (rewriting if needed).
     */
    public void addArray(byte arrayType, byte[] packed) {
        int row = nextAppendRow();
        fillGapTo(row);
        byte childKind = arrayChildKind(arrayType, packed);
        if (childKind == UNION_CHILD_KIND) {
            promoteToUnion();
            ((UnionBuilder) current).addInlineArray(arrayType, packed);
        } else {
            ArrayBuilder ab = prepareColumnarArrayRow(childKind);
            if (ab != null) {
                ab.startRow();
                if (packed.length > 0) {
                    InlineArrayReader reader = new InlineArrayReader(packed, 0, packed.length, true);
                    while (reader.next()) {
                        appendReaderElement(ab, childKind, reader);
                    }
                }
            } else {
                ((UnionBuilder) current).addInlineArray(arrayType, packed);
            }
        }
        lastWrittenRow = row;
    }

    /**
     * Adds a {@code LONG} array row from {@code values[0, size)}, avoiding a packed {@code byte[]}
     * intermediate. The buffer may be larger than {@code size} but must not be mutated until committed.
     */
    public void addLongArray(long[] values, int size) {
        addColumnarFixedArray(EscfColumnKind.LONG, values, size);
    }

    /**
     * Adds a {@code DOUBLE} array row from raw-bit values ({@link Double#doubleToRawLongBits}).
     * See {@link #addLongArray} for buffer constraints.
     */
    public void addDoubleArray(long[] rawBits, int size) {
        addColumnarFixedArray(EscfColumnKind.DOUBLE, rawBits, size);
    }

    private void addColumnarFixedArray(byte elemKind, long[] values, int size) {
        int row = nextAppendRow();
        fillGapTo(row);
        ArrayBuilder ab = prepareColumnarArrayRow(elemKind);
        if (ab != null) {
            ab.startRow();
            for (int i = 0; i < size; i++) {
                ab.appendFixedBits(elemKind, values[i]);
            }
        } else {
            // Column is (or became) a union — scalar+array under SPLIT, or a child-kind mismatch.
            // Stream the array inline on the union as a FIXED_ARRAY slot, matching the byte layout of
            // rewriteArrayToUnion / SourceBatchEncodeHelper.packFixedArray ([elemType][raw LE values]).
            UnionBuilder ub = (UnionBuilder) current;
            ub.beginInlineSlot(SourceValueType.FIXED_ARRAY);
            ub.slotByte(childInlineType(elemKind));
            for (int i = 0; i < size; i++) {
                ub.slotLongLE(values[i]);
            }
            ub.endInlineSlot();
        }
        lastWrittenRow = row;
    }

    public void addNull() {
        setNull(nextAppendRow());
    }

    /** Adds an object's entries in inline kv form. No native key-value column kind; promotes to union. */
    public void addKeyValue(byte[] packed) {
        int row = nextAppendRow();
        fillGapTo(row);
        promoteToUnion();
        ((UnionBuilder) current).addInlineArray(SourceValueType.KEY_VALUE, packed);
        lastWrittenRow = row;
    }

    public void setLong(int row, long value) {
        if (row == lastWrittenRow) {
            sameRowScalar(EscfColumnKind.LONG, value, 0.0, null, 0, 0);
            return;
        }
        newRowScalar(row, EscfColumnKind.LONG, value, 0.0, null, 0, 0);
    }

    public void setDouble(int row, double value) {
        if (row == lastWrittenRow) {
            sameRowScalar(EscfColumnKind.DOUBLE, 0L, value, null, 0, 0);
            return;
        }
        newRowScalar(row, EscfColumnKind.DOUBLE, 0L, value, null, 0, 0);
    }

    public void setBoolean(int row, boolean value) {
        // BOOL is scalar-only in the columnar sense (bool arrays go inline-on-union); no same-row support.
        if (row == lastWrittenRow) {
            throw new IllegalStateException("a second same-row boolean is not representable");
        }
        assert row > lastWrittenRow : nonDecreasing(row);
        fillGapTo(row);
        if (current == null) {
            BoolBuilder b = new BoolBuilder();
            backfillLeadingAbsents(b);
            current = b;
            b.addBoolean(value);
        } else if (current.kind() == EscfColumnKind.BOOL || current.kind() == EscfColumnKind.UNION) {
            current.addBoolean(value);
        } else {
            promoteToUnion();
            current.addBoolean(value);
        }
        lastWrittenRow = row;
    }

    public void setString(int row, XContentString.UTF8Bytes value) {
        setBytes(row, EscfColumnKind.STRING, value.bytes(), value.offset(), value.length());
    }

    public void setString(int row, BytesRef value) {
        setBytes(row, EscfColumnKind.STRING, value.bytes, value.offset, value.length);
    }

    public void setString(int row, byte[] bytes, int offset, int length) {
        setBytes(row, EscfColumnKind.STRING, bytes, offset, length);
    }

    public void setBinary(int row, BytesRef value) {
        setBytes(row, EscfColumnKind.BINARY, value.bytes, value.offset, value.length);
    }

    public void setNull(int row) {
        assert row > lastWrittenRow : nonDecreasing(row);
        fillGapTo(row);
        promoteToUnion();
        current.addNull();
        lastWrittenRow = row;
    }

    /**
     * Positional raw-bits write for a fixed-64 (LONG or DOUBLE) column. The {@code bits} parameter
     * is the raw 64-bit little-endian word: for LONG that is the value itself; for DOUBLE it is
     * {@link Double#doubleToRawLongBits(double)}.
     */
    public void setRawFixed64(int row, byte kind, long bits) {
        assert kind == EscfColumnKind.LONG || kind == EscfColumnKind.DOUBLE;
        if (row == lastWrittenRow) {
            sameRowScalar(kind, bits, Double.longBitsToDouble(bits), null, 0, 0);
            return;
        }
        newRowScalar(row, kind, bits, Double.longBitsToDouble(bits), null, 0, 0);
    }

    /**
     * Appends a raw UNION row verbatim, bypassing all type-dispatch and value decoding. The column
     * must have been primed as a UNION via {@link #hintUnion()} before any row is written.
     *
     * <p>The type byte is stamped directly into the type vector; the payload bytes are copied into the
     * data buffer without interpretation. Zero-payload types ({@link SourceValueType#NULL},
     * {@link SourceValueType#TRUE}, {@link SourceValueType#FALSE}, {@link SourceValueType#ABSENT})
     * must be passed with a zero-length {@code payload}; numeric types must carry exactly 8 bytes.
     *
     * <p>Absent rows do not advance the write cursor ({@code lastWrittenRow} is not updated), matching
     * the convention of {@link #addAbsent()}.
     */
    public void addRawUnionRow(byte type, BytesRef payload) {
        assert current instanceof UnionBuilder : "addRawUnionRow requires hintUnion() to have been called first";
        UnionBuilder ub = (UnionBuilder) current;
        int row = ub.rowsConsumed();
        ub.beginInlineSlot(type);
        if (payload.length > 0) {
            ub.slotBytes(payload.bytes, payload.offset, payload.length);
        }
        if (type == SourceValueType.ABSENT) {
            ub.endAbsentSlot();
        } else {
            ub.endInlineSlot();
            lastWrittenRow = row;
        }
    }

    private void setBytes(int row, byte kind, byte[] bytes, int off, int len) {
        if (row == lastWrittenRow) {
            sameRowScalar(kind, 0L, 0.0, bytes, off, len);
            return;
        }
        newRowScalar(row, kind, 0L, 0.0, bytes, off, len);
    }

    /** Opens an array cell for {@code row}; elements are supplied via {@code appendX} until {@link #endArray}. */
    public void beginArray(int row) {
        assert arrayOpen == false : "beginArray without a matching endArray";
        assert row > lastWrittenRow : nonDecreasing(row);
        fillGapTo(row);
        ArrayBuilder ab = prepareColumnarArrayRow(UNSET_ARRAY_KIND);
        if (ab == null) {
            // A scalar column in SPLIT met an array: open an inline UNION_ARRAY slot on the union instead.
            ((UnionBuilder) current).beginInlineSlot(SourceValueType.UNION_ARRAY);
        } else {
            ab.startRow();
        }
        arrayOpen = true;
        lastWrittenRow = row;
    }

    public void appendLong(long value) {
        if (current instanceof ArrayBuilder ab) {
            if (ab.childKind() == UNSET_ARRAY_KIND || ab.childKind() == EscfColumnKind.LONG) {
                ab.appendLong(value);
                return;
            }
            heterogeneousArrayElement(SourceValueType.LONG, value, 0.0, null, 0, 0);
        } else {
            appendUnionArrayElement(SourceValueType.LONG, value, 0.0, null, 0, 0);
        }
    }

    public void appendDouble(double value) {
        if (current instanceof ArrayBuilder ab) {
            if (ab.childKind() == UNSET_ARRAY_KIND || ab.childKind() == EscfColumnKind.DOUBLE) {
                ab.appendDouble(value);
                return;
            }
            heterogeneousArrayElement(SourceValueType.DOUBLE, 0L, value, null, 0, 0);
        } else {
            appendUnionArrayElement(SourceValueType.DOUBLE, 0L, value, null, 0, 0);
        }
    }

    public void appendString(BytesRef value) {
        appendBytesElement(EscfColumnKind.STRING, SourceValueType.STRING, value.bytes, value.offset, value.length);
    }

    public void appendString(XContentString.UTF8Bytes value) {
        appendBytesElement(EscfColumnKind.STRING, SourceValueType.STRING, value.bytes(), value.offset(), value.length());
    }

    /**
     * Appends a BINARY element to the current array row. Counterpart to {@link #appendString(BytesRef)}.
     */
    public void appendBinary(BytesRef value) {
        appendBytesElement(EscfColumnKind.BINARY, SourceValueType.BINARY, value.bytes, value.offset, value.length);
    }

    /**
     * Appends a raw 64-bit element to the current array row. Unlike {@link #appendLong} and
     * {@link #appendDouble}, takes a raw bit pattern and passes it through to the builder without a
     * {@code Double.longBitsToDouble → doubleToRawLongBits} round trip.
     *
     * @param childKind the element kind ({@link EscfColumnKind#LONG} or {@link EscfColumnKind#DOUBLE})
     * @param bits      the raw 64-bit value (for DOUBLE: {@link Double#doubleToRawLongBits(double)})
     */
    public void appendFixedBits(byte childKind, long bits) {
        assert childKind == EscfColumnKind.LONG || childKind == EscfColumnKind.DOUBLE;
        if (current instanceof ArrayBuilder ab) {
            if (ab.childKind() == UNSET_ARRAY_KIND || ab.childKind() == childKind) {
                ab.appendFixedBits(childKind, bits);
                return;
            }
            byte typeByte = childKind == EscfColumnKind.LONG ? SourceValueType.LONG : SourceValueType.DOUBLE;
            heterogeneousArrayElement(typeByte, bits, Double.longBitsToDouble(bits), null, 0, 0);
        } else {
            byte typeByte = childKind == EscfColumnKind.LONG ? SourceValueType.LONG : SourceValueType.DOUBLE;
            appendUnionArrayElement(typeByte, bits, Double.longBitsToDouble(bits), null, 0, 0);
        }
    }

    /**
     * Appends an explicit JSON {@code null} element to the current array row. Requires an open array
     * (i.e. {@link #beginArray} has been called and {@link #endArray} has not). The element kind must
     * match the already-resolved child kind when one is set; a null as the very first element
     * conservatively promotes the row to an inline {@code UNION_ARRAY} (since the child kind is
     * unknown and deferred resolution is not supported).
     *
     * <p>If the column was already promoted to a union, appends a {@link SourceValueType#NULL} byte
     * into the open inline slot instead.
     */
    public void appendNull() {
        assert arrayOpen : "appendNull requires an open array (call beginArray first)";
        if (current instanceof ArrayBuilder ab) {
            byte kind = ab.childKind();
            if (kind == UNSET_ARRAY_KIND) {
                // No child kind resolved yet — conservatively emit a UNION_ARRAY row.
                heterogeneousArrayElement(SourceValueType.NULL, 0L, 0.0, null, 0, 0);
            } else {
                ab.appendNull(kind);
            }
        } else {
            // Already a union column (or SPLIT scalar-vs-array promoted).
            appendUnionArrayElement(SourceValueType.NULL, 0L, 0.0, null, 0, 0);
        }
    }

    private void appendBytesElement(byte childKind, byte typeByte, byte[] bytes, int off, int len) {
        if (current instanceof ArrayBuilder ab) {
            if (ab.childKind() == UNSET_ARRAY_KIND || ab.childKind() == childKind) {
                ab.appendBytes(childKind, bytes, off, len);
                return;
            }
            heterogeneousArrayElement(typeByte, 0L, 0.0, bytes, off, len);
        } else {
            appendUnionArrayElement(typeByte, 0L, 0.0, bytes, off, len);
        }
    }

    public void endArray() {
        assert arrayOpen : "endArray without a matching beginArray";
        if (current instanceof UnionBuilder ub) {
            ub.endInlineSlot();
        }
        arrayOpen = false;
    }

    /**
     * Determines the column kind and serialises it. An all-absent (or empty) column finishes as
     * {@link EscfColumnKind#LONG} with an all-absent bitset.
     */
    public EscfColumnData finish(int docCount) {
        assert arrayOpen == false : "finish while an array is open";
        if (current == null) {
            FixedNumericBuilder allAbsent = new FixedNumericBuilder(EscfColumnKind.LONG, recycler);
            backfillLeadingAbsents(allAbsent);
            current = allAbsent;
        }
        // A column of only empty arrays and absents cannot be a typed ARRAY; finish it as a union.
        if (current instanceof ArrayBuilder ab && ab.childKind() == UNSET_ARRAY_KIND) {
            current = rewriteArrayToUnion(ab);
        }
        // Fill trailing absent rows: the positional surface leaves rows after the last written value implicit.
        while (current.rowsConsumed() < docCount) {
            current.addAbsent();
        }
        return current.finish(docCount);
    }

    /** Releases the active builder's stream without producing a column. */
    public void discard() {
        if (current != null) {
            current.discard();
        }
    }

    /** The next fresh row index for the append surface. */
    private int nextAppendRow() {
        return current == null ? leadingAbsents : current.rowsConsumed();
    }

    /** Fills the absent gap so that the number of consumed rows becomes {@code row}. */
    private void fillGapTo(int row) {
        if (current == null) {
            assert row >= leadingAbsents : "cannot write row " + row + " below leading absents " + leadingAbsents;
            leadingAbsents = row;
        } else {
            assert row >= current.rowsConsumed() : "row " + row + " already consumed " + current.rowsConsumed();
            int gap = row - current.rowsConsumed();
            if (gap > 0) {
                current.addAbsents(gap);
            }
        }
    }

    private void backfillLeadingAbsents(TypedBuilder builder) {
        builder.addAbsents(leadingAbsents);
        leadingAbsents = 0;
    }

    /** Writes a scalar value at a fresh row {@code row}. */
    private void newRowScalar(int row, byte kind, long longBits, double dbl, byte[] bytes, int off, int len) {
        assert lockedKind == -1 || kind == lockedKind
            : "column locked to " + EscfColumnKind.name(lockedKind) + " but received " + EscfColumnKind.name(kind);
        assert row > lastWrittenRow : nonDecreasing(row);
        fillGapTo(row);
        if (current == null) {
            current = newScalarWithBackfill(kind);
            writeScalar(current, kind, longBits, dbl, bytes, off, len);
        } else if (current instanceof ArrayBuilder ab) {
            // A scalar row meeting an existing ARRAY column.
            if (policy == CollisionPolicy.MERGE && ab.childKind() == kind) {
                ab.startRow();
                appendScalarAsElement(ab, kind, longBits, dbl, bytes, off, len);
            } else {
                // SPLIT scalar-after-array, or MERGE child-kind mismatch → rewrite the array to a union.
                promoteToUnion();
                writeScalar(current, kind, longBits, dbl, bytes, off, len);
            }
        } else if (current.kind() == kind || current.kind() == EscfColumnKind.UNION) {
            writeScalar(current, kind, longBits, dbl, bytes, off, len);
        } else {
            promoteToUnion();
            writeScalar(current, kind, longBits, dbl, bytes, off, len);
        }
        lastWrittenRow = row;
    }

    /** Applies a second value for the last written row (multi-value). */
    private void sameRowScalar(byte kind, long longBits, double dbl, byte[] bytes, int off, int len) {
        assert lockedKind == -1 || kind == lockedKind
            : "column locked to " + EscfColumnKind.name(lockedKind) + " but received " + EscfColumnKind.name(kind);
        if (current instanceof ArrayBuilder ab) {
            if (ab.childKind() == kind || ab.childKind() == UNSET_ARRAY_KIND) {
                appendScalarAsElement(ab, kind, longBits, dbl, bytes, off, len);
            } else {
                // A same-row element of a different kind → heterogeneous inline array for this row.
                heterogeneousArrayElement(scalarTypeByte(kind), longBits, dbl, bytes, off, len);
            }
        } else if (policy == CollisionPolicy.MERGE && current != null && current.kind() == kind) {
            ArrayBuilder ab = current.promoteToArray(recycler);
            current = ab;
            appendScalarAsElement(ab, kind, longBits, dbl, bytes, off, len);
        } else {
            // SPLIT never legitimately sees a same-row second scalar (the row buffer rejects duplicate keys).
            throw new IllegalStateException("a second same-row scalar is not representable under " + policy);
        }
    }

    private TypedBuilder newScalarWithBackfill(byte kind) {
        TypedBuilder b = newScalar(kind, recycler);
        backfillLeadingAbsents(b);
        return b;
    }

    private static void writeScalar(TypedBuilder b, byte kind, long longBits, double dbl, byte[] bytes, int off, int len) {
        switch (kind) {
            case EscfColumnKind.LONG -> b.addLong(longBits);
            case EscfColumnKind.DOUBLE -> b.addDouble(dbl);
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> b.addBytes(kind, bytes, off, len);
            default -> throw new AssertionError("not a scalar kind: " + EscfColumnKind.name(kind));
        }
    }

    private static void appendScalarAsElement(ArrayBuilder ab, byte kind, long longBits, double dbl, byte[] bytes, int off, int len) {
        switch (kind) {
            case EscfColumnKind.LONG -> ab.appendLong(longBits);
            case EscfColumnKind.DOUBLE -> ab.appendDouble(dbl);
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> ab.appendBytes(kind, bytes, off, len);
            default -> throw new AssertionError("not a scalar kind: " + EscfColumnKind.name(kind));
        }
    }

    private static void appendReaderElement(ArrayBuilder ab, byte childKind, InlineArrayReader reader) {
        switch (childKind) {
            case EscfColumnKind.LONG -> ab.appendLong(reader.type() == SourceValueType.INT ? reader.intValue() : reader.longValue());
            case EscfColumnKind.DOUBLE -> ab.appendDouble(
                reader.type() == SourceValueType.FLOAT ? reader.floatValue() : reader.doubleValue()
            );
            case EscfColumnKind.STRING -> {
                XContentString.UTF8Bytes b = reader.textValue().bytes();
                ab.appendBytes(EscfColumnKind.STRING, b.bytes(), b.offset(), b.length());
            }
            default -> throw new AssertionError("not a columnar array child kind: " + EscfColumnKind.name(childKind));
        }
    }

    /**
     * Prepares {@code current} to accept a columnar array row of {@code childKind}. Returns the
     * {@link ArrayBuilder}, or {@code null} if the value must go inline on a union (in which case
     * {@code current} is a {@link UnionBuilder}).
     */
    private ArrayBuilder prepareColumnarArrayRow(byte childKind) {
        if (current == null) {
            ArrayBuilder ab = new ArrayBuilder(childKind, policy == CollisionPolicy.SPLIT, recycler);
            backfillLeadingAbsents(ab);
            current = ab;
            return ab;
        }
        if (current instanceof ArrayBuilder ab) {
            if (childKind == UNSET_ARRAY_KIND || ab.childKind() == UNSET_ARRAY_KIND || ab.childKind() == childKind) {
                return ab;
            }
            // Child-kind change → rewrite the columnar array to a union; the new array goes inline.
            current = rewriteArrayToUnion(ab);
            return null;
        }
        if (current.kind() == EscfColumnKind.UNION) {
            return null;
        }
        if (policy == CollisionPolicy.MERGE && (childKind == UNSET_ARRAY_KIND || current.kind() == childKind)) {
            ArrayBuilder ab = current.promoteToArray(recycler);
            current = ab;
            return ab;
        }
        // SPLIT scalar+array, or MERGE kind mismatch → union.
        promoteToUnion();
        return null;
    }

    /**
     * Handles a heterogeneous element inside an open element-append array: rewrites the columnar array to a
     * union, re-opens the current row as a {@code UNION_ARRAY} slot, then appends the conflicting element.
     */
    private void heterogeneousArrayElement(byte typeByte, long longBits, double dbl, byte[] bytes, int off, int len) {
        assert current instanceof ArrayBuilder;
        ArrayBuilder ab = (ArrayBuilder) current;
        // The open row is the last one; capture its elements before the rewrite consumes the child buffer.
        int openRow = ab.rowsConsumed() - 1;
        UnionBuilder union = rewriteArrayToUnion(ab, openRow); // rewrites rows [0, openRow) as committed slots
        // Re-open the current row as a UNION_ARRAY slot and replay its homogeneous elements with per-element types.
        union.beginInlineSlot(SourceValueType.UNION_ARRAY);
        replayRowAsUnionElements(union, ab, openRow);
        current = union;
        appendUnionArrayElement(typeByte, longBits, dbl, bytes, off, len);
    }

    /** Appends one element into the currently-open inline {@code UNION_ARRAY} slot on the union. */
    private void appendUnionArrayElement(byte typeByte, long longBits, double dbl, byte[] bytes, int off, int len) {
        UnionBuilder union = (UnionBuilder) current;
        union.slotByte(typeByte);
        switch (typeByte) {
            case SourceValueType.LONG -> union.slotLongLE(longBits);
            case SourceValueType.DOUBLE -> union.slotLongLE(Double.doubleToRawLongBits(dbl));
            case SourceValueType.STRING, SourceValueType.BINARY -> {
                union.slotIntLE(len);
                union.slotBytes(bytes, off, len);
            }
            case SourceValueType.NULL -> {
            } // type byte only; SourceValueType.elemDataSize(NULL) == 0
            default -> throw new AssertionError("unexpected union array element type " + SourceValueType.name(typeByte));
        }
    }

    private void promoteToUnion() {
        if (current != null && current.kind() == EscfColumnKind.UNION) {
            return;
        }
        if (current == null) {
            UnionBuilder union = new UnionBuilder(recycler);
            backfillLeadingAbsents(union);
            current = union;
        } else if (current instanceof ArrayBuilder ab) {
            current = rewriteArrayToUnion(ab);
        } else {
            current = current.promote(recycler);
        }
    }

    /** Rewrites all rows of {@code ab} into a union of inline arrays. */
    private UnionBuilder rewriteArrayToUnion(ArrayBuilder ab) {
        return rewriteArrayToUnion(ab, ab.rowsConsumed());
    }

    /**
     * Rewrites the first {@code upToRow} rows of {@code ab} into a fresh {@link UnionBuilder}: present
     * non-empty rows become {@code FIXED_ARRAY} or {@code UNION_ARRAY} slots, present empty rows
     * become zero-length {@code UNION_ARRAY} slots, absent rows become {@code ABSENT} slots.
     * Rows {@code [upToRow, rowsConsumed)} are left for the caller.
     *
     * <p>When the child has no null elements ({@code childValidity == null}), non-empty rows are
     * written as {@code FIXED_ARRAY} slots with a bulk byte-copy of the child data — the fast path.
     * When the child has null elements, each row is written as a {@code UNION_ARRAY} slot with
     * per-element type bytes: {@link SourceValueType#NULL} for nulls, {@code elemType} for non-nulls.
     */
    private UnionBuilder rewriteArrayToUnion(ArrayBuilder ab, int upToRow) {
        ab.seal();
        UnionBuilder union = new UnionBuilder(recycler);
        BytesReference childBytes = ab.childData.bytes();
        boolean fixed = ab.childKind == EscfColumnKind.LONG || ab.childKind == EscfColumnKind.DOUBLE;
        // Unresolved kind (only empty arrays/absents) never reaches the typed slot branch below.
        byte elemType = ab.childKind == UNSET_ARRAY_KIND ? 0 : childInlineType(ab.childKind);
        boolean hasNullElems = ab.childValidity != null;
        for (int r = 0; r < upToRow; r++) {
            int from = ab.rowOffsets[r];
            int to = ab.rowOffsets[r + 1];
            if (ab.isAbsentRow(r, from, to)) {
                union.addAbsent();
            } else if (from == to) {
                union.addInlineArray(SourceValueType.UNION_ARRAY, EMPTY_BYTES); // present empty array
            } else if (hasNullElems == false) {
                // Dense child — bulk-copy as a FIXED_ARRAY slot (fast path).
                union.beginInlineSlot(SourceValueType.FIXED_ARRAY);
                union.slotByte(elemType);
                if (fixed) {
                    union.slotSlice(childBytes, from * 8, (to - from) * 8);
                } else {
                    for (int e = from; e < to; e++) {
                        int bf = ab.childOffsets[e];
                        int bt = ab.childOffsets[e + 1];
                        union.slotIntLE(bt - bf);
                        union.slotSlice(childBytes, bf, bt - bf);
                    }
                }
                union.endInlineSlot();
            } else {
                // Nullable child — emit per-element types as a UNION_ARRAY slot.
                union.beginInlineSlot(SourceValueType.UNION_ARRAY);
                for (int e = from; e < to; e++) {
                    if (ab.childValidity.get(e) == false) {
                        union.slotByte(SourceValueType.NULL);
                    } else {
                        union.slotByte(elemType);
                        if (fixed) {
                            union.slotSlice(childBytes, e * 8, 8);
                        } else {
                            int bf = ab.childOffsets[e];
                            int bt = ab.childOffsets[e + 1];
                            union.slotIntLE(bt - bf);
                            union.slotSlice(childBytes, bf, bt - bf);
                        }
                    }
                }
                union.endInlineSlot();
            }
        }
        if (upToRow == ab.rowsConsumed()) {
            ab.childData.close();
            ab.consumed = true;
        }
        return union;
    }

    /** Replays row {@code r} of {@code ab} into the currently-open union slot as typed elements. */
    private void replayRowAsUnionElements(UnionBuilder union, ArrayBuilder ab, int r) {
        int from = ab.rowOffsets[r];
        int to = ab.elemCount;
        if (from < to) {
            // Defer childInlineType until we know there are elements to replay.
            boolean fixed = ab.childKind == EscfColumnKind.LONG || ab.childKind == EscfColumnKind.DOUBLE;
            byte elemType = childInlineType(ab.childKind);
            BytesReference childBytes = ab.childData.bytes();
            for (int e = from; e < to; e++) {
                if (ab.childValidity != null && ab.childValidity.get(e) == false) {
                    union.slotByte(SourceValueType.NULL);
                } else {
                    union.slotByte(elemType);
                    if (fixed) {
                        union.slotSlice(childBytes, e * 8, 8);
                    } else {
                        int bf = ab.childOffsets[e];
                        int bt = ab.childOffsets[e + 1];
                        union.slotIntLE(bt - bf);
                        union.slotSlice(childBytes, bf, bt - bf);
                    }
                }
            }
        }
        ab.childData.close();
        ab.consumed = true;
    }

    private static byte childInlineType(byte childKind) {
        return switch (childKind) {
            case EscfColumnKind.LONG -> SourceValueType.LONG;
            case EscfColumnKind.DOUBLE -> SourceValueType.DOUBLE;
            case EscfColumnKind.STRING -> SourceValueType.STRING;
            case EscfColumnKind.BINARY -> SourceValueType.BINARY;
            default -> throw new AssertionError("not a columnar array child kind: " + EscfColumnKind.name(childKind));
        };
    }

    private static byte scalarTypeByte(byte kind) {
        return switch (kind) {
            case EscfColumnKind.LONG -> SourceValueType.LONG;
            case EscfColumnKind.DOUBLE -> SourceValueType.DOUBLE;
            case EscfColumnKind.STRING -> SourceValueType.STRING;
            case EscfColumnKind.BINARY -> SourceValueType.BINARY;
            default -> throw new AssertionError("not a scalar kind: " + EscfColumnKind.name(kind));
        };
    }

    private String nonDecreasing(int row) {
        return "rows must be non-decreasing: " + row + " is not greater than the last written row " + lastWrittenRow;
    }

    private static TypedBuilder newScalar(byte kind, Recycler<BytesRef> recycler) {
        return switch (kind) {
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE -> new FixedNumericBuilder(kind, recycler);
            case EscfColumnKind.BOOL -> new BoolBuilder();
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> new VarBuilder(kind, recycler);
            default -> throw new IllegalArgumentException("No scalar builder for kind " + EscfColumnKind.name(kind));
        };
    }

    /**
     * Returns the fixed columnar child kind for a packed array, {@link #UNSET_ARRAY_KIND} for an empty
     * array, or {@link #UNION_CHILD_KIND} if the array must go inline.
     */
    private static byte arrayChildKind(byte arrayType, byte[] packed) {
        int len = packed.length;
        if (len == 0) {
            return UNSET_ARRAY_KIND;
        }
        if (arrayType != SourceValueType.FIXED_ARRAY) {
            return UNION_CHILD_KIND;
        }
        return switch (packed[0]) {
            case SourceValueType.INT, SourceValueType.LONG -> EscfColumnKind.LONG;
            case SourceValueType.FLOAT, SourceValueType.DOUBLE -> EscfColumnKind.DOUBLE;
            case SourceValueType.STRING -> EscfColumnKind.STRING;
            default -> UNION_CHILD_KIND;
        };
    }

    private interface TypedBuilder {

        byte kind();

        /** Number of rows accumulated so far (present + absent). */
        int rowsConsumed();

        void addLong(long value);

        void addDouble(double value);

        void addBoolean(boolean value);

        void addBytes(byte kind, byte[] bytes, int offset, int length);

        void addNull();

        void addAbsent();

        /** Records {@code n} consecutive absent rows. Default falls back to repeated {@link #addAbsent()}; override for bulk efficiency. */
        default void addAbsents(int n) {
            for (int i = 0; i < n; i++) {
                addAbsent();
            }
        }

        /** Near-free promotion to a union (adopts the value buffer where the layout allows). */
        UnionBuilder promote(Recycler<BytesRef> recycler);

        /** Promotion to a columnar ARRAY (scalar buffer becomes the array child); only scalar builders support it. */
        default ArrayBuilder promoteToArray(Recycler<BytesRef> recycler) {
            throw new AssertionError("column kind " + EscfColumnKind.name(kind()) + " cannot promote to array");
        }

        EscfColumnData finish(int docCount);

        void discard();
    }

    private abstract static class BaseBuilder implements TypedBuilder {

        int count;
        FixedBitSet validity;

        @Override
        public int rowsConsumed() {
            return count;
        }

        /** Marks the current row absent, materialising the validity bitset on first absence. */
        final void advanceAbsent() {
            if (validity == null) {
                validity = new FixedBitSet(Math.max(64, count + 1));
                validity.set(0, count); // [0, count) present
            } else {
                validity = FixedBitSet.ensureCapacity(validity, count + 1);
            }
            count++;
        }

        /** Marks {@code n} consecutive rows absent; materialises and grows the validity bitset once. */
        final void bulkAdvanceAbsent(int n) {
            if (n == 0) return;
            if (validity == null) {
                validity = new FixedBitSet(Math.max(64, count + n));
                validity.set(0, count); // [0, count) are present
            } else {
                validity = FixedBitSet.ensureCapacity(validity, count + n);
            }
            // bits [count, count+n) remain 0 (absent)
            count += n;
        }

        /** Marks the current document present and advances {@code count}. */
        final void advancePresent() {
            if (validity != null) {
                validity = FixedBitSet.ensureCapacity(validity, count + 1);
                validity.set(count);
            }
            count++;
        }

        final boolean isAbsentAt(int d) {
            return validity != null && validity.get(d) == false;
        }

        @Override
        public void addLong(long value) {
            throw unsupported("long");
        }

        @Override
        public void addDouble(double value) {
            throw unsupported("double");
        }

        @Override
        public void addBoolean(boolean value) {
            throw unsupported("boolean");
        }

        @Override
        public void addBytes(byte kind, byte[] bytes, int offset, int length) {
            throw unsupported("bytes");
        }

        @Override
        public void addNull() {
            throw unsupported("null");
        }

        @Override
        public void discard() {}

        private AssertionError unsupported(String type) {
            return new AssertionError("column kind " + EscfColumnKind.name(kind()) + " cannot accept a " + type + " value");
        }
    }

    /** LONG / DOUBLE: 8-byte slots (LE), one per document; absent slots are written as zero. */
    private static final class FixedNumericBuilder extends BaseBuilder {
        private final byte kind;
        private RecyclerBytesStreamOutput data;

        FixedNumericBuilder(byte kind, Recycler<BytesRef> recycler) {
            this.kind = kind;
            this.data = newStream(recycler);
        }

        @Override
        public byte kind() {
            return kind;
        }

        @Override
        public void addLong(long value) {
            writeLongLE(data, value);
            advancePresent();
        }

        @Override
        public void addDouble(double value) {
            writeLongLE(data, Double.doubleToRawLongBits(value));
            advancePresent();
        }

        @Override
        public void addAbsent() {
            writeLongLE(data, 0L);
            advanceAbsent();
        }

        @Override
        public void addAbsents(int n) {
            // Write n * 8 zero bytes in one shot, avoiding per-row stream-call overhead.
            int remaining = n * Long.BYTES;
            while (remaining > 0) {
                int chunk = Math.min(remaining, ZERO_BYTES.length);
                writeBytes(data, ZERO_BYTES, 0, chunk);
                remaining -= chunk;
            }
            bulkAdvanceAbsent(n);
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            byte present = kind == EscfColumnKind.LONG ? SourceValueType.LONG : SourceValueType.DOUBLE;
            byte[] typeVec = new byte[count];
            int[] offsets = new int[count + 1];
            for (int i = 0; i < count; i++) {
                typeVec[i] = isAbsentAt(i) ? SourceValueType.ABSENT : present;
                offsets[i] = i * 8;
            }
            offsets[count] = count * 8;
            // Adopts the positional data buffer verbatim; null out to prevent double-close via discard().
            RecyclerBytesStreamOutput adopted = data;
            data = null;
            return new UnionBuilder(adopted, typeVec, offsets, count * 8, count, validity);
        }

        @Override
        public ArrayBuilder promoteToArray(Recycler<BytesRef> recycler) {
            int[] rowOffsets = new int[Math.max(16, count + 1)];
            if (validity == null) {
                // Dense: positional == element-packed. Adopt the data buffer; null out to prevent double-close via discard().
                int[] childOffsets = new int[Math.max(16, count + 2)];
                for (int i = 0; i < count; i++) {
                    rowOffsets[i] = i;
                }
                RecyclerBytesStreamOutput adopted = data;
                data = null;
                return new ArrayBuilder(kind, adopted, childOffsets, rowOffsets, count, count * 8, count, null);
            }
            // Sparse: compact present slots into a fresh element-packed stream (the [swap]).
            BytesReference positional = data.bytes();
            RecyclerBytesStreamOutput childData = newStream(recycler);
            int[] childOffsets = new int[Math.max(16, count + 2)];
            int elemCount = 0;
            int childDataLen = 0;
            try {
                for (int r = 0; r < count; r++) {
                    rowOffsets[r] = elemCount;
                    if (validity.get(r)) {
                        childOffsets[elemCount] = childDataLen;
                        positional.slice(r * 8, 8).writeTo(childData);
                        childDataLen += 8;
                        elemCount++;
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            data.close();
            data = null; // handed off to the fresh childData above; null out so discard() does not double-close.
            return new ArrayBuilder(kind, childData, childOffsets, rowOffsets, elemCount, childDataLen, count, validity);
        }

        @Override
        public EscfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            return EscfColumnData.ofFixed64(kind, docCount, validity, data.moveToBytesReference());
        }

        @Override
        public void discard() {
            if (data != null) data.close();
        }
    }

    /** BOOL: a value bitset (bit set = true). */
    private static final class BoolBuilder extends BaseBuilder {

        private FixedBitSet values;

        @Override
        public byte kind() {
            return EscfColumnKind.BOOL;
        }

        @Override
        public void addBoolean(boolean value) {
            if (value) {
                values = values == null ? new FixedBitSet(Math.max(64, count + 1)) : FixedBitSet.ensureCapacity(values, count + 1);
                values.set(count);
            }
            advancePresent();
        }

        @Override
        public void addAbsent() {
            advanceAbsent();
        }

        @Override
        public void addAbsents(int n) {
            bulkAdvanceAbsent(n);
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            byte[] typeVec = new byte[count];
            for (int i = 0; i < count; i++) {
                if (isAbsentAt(i)) {
                    typeVec[i] = SourceValueType.ABSENT;
                } else {
                    typeVec[i] = (values != null && values.get(i)) ? SourceValueType.TRUE : SourceValueType.FALSE;
                }
            }
            return new UnionBuilder(newStream(recycler), typeVec, new int[count + 1], 0, count, validity);
        }

        @Override
        public EscfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            return EscfColumnData.ofBool(docCount, validity, values);
        }
    }

    /** STRING / BINARY: raw bytes plus an offset vector. */
    private static final class VarBuilder extends BaseBuilder {
        private final byte kind;
        private RecyclerBytesStreamOutput data;
        private int[] offsets = new int[16];
        private int dataLen;

        VarBuilder(byte kind, Recycler<BytesRef> recycler) {
            this.kind = kind;
            this.data = newStream(recycler);
        }

        @Override
        public byte kind() {
            return kind;
        }

        @Override
        public void addBytes(byte kind, byte[] bytes, int offset, int length) {
            assert kind == this.kind : "VarBuilder of " + EscfColumnKind.name(this.kind) + " given " + EscfColumnKind.name(kind);
            recordOffset();
            writeBytes(data, bytes, offset, length);
            dataLen += length;
            advancePresent();
        }

        @Override
        public void addAbsent() {
            recordOffset();
            advanceAbsent();
        }

        @Override
        public void addAbsents(int n) {
            // VarBuilder writes no bytes for absent rows; bulk-fill the offset slots and advance.
            offsets = ensureIntCapacity(offsets, count + n);
            Arrays.fill(offsets, count, count + n, dataLen);
            bulkAdvanceAbsent(n);
        }

        private void recordOffset() {
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            byte present = kind == EscfColumnKind.STRING ? SourceValueType.STRING : SourceValueType.BINARY;
            byte[] typeVec = new byte[count];
            for (int i = 0; i < count; i++) {
                typeVec[i] = isAbsentAt(i) ? SourceValueType.ABSENT : present;
            }
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            // Adopts both the data buffer and the offset array; null out to prevent double-close via discard().
            RecyclerBytesStreamOutput adopted = data;
            data = null;
            return new UnionBuilder(adopted, typeVec, offsets, dataLen, count, validity);
        }

        @Override
        public ArrayBuilder promoteToArray(Recycler<BytesRef> recycler) {
            // The data buffer already holds only present bytes (absent rows wrote none), so it is adopted
            // as-is in both dense and sparse cases; only the per-element offsets and row offsets differ.
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            int[] rowOffsets = new int[Math.max(16, count + 1)];
            // Adopts data; null out to prevent double-close via discard().
            RecyclerBytesStreamOutput adopted = data;
            data = null;
            if (validity == null) {
                // Dense: one element per row. Reuse the per-doc offsets as the per-element child offsets.
                for (int i = 0; i < count; i++) {
                    rowOffsets[i] = i;
                }
                return new ArrayBuilder(kind, adopted, offsets, rowOffsets, count, dataLen, count, null);
            }
            int[] childOffsets = new int[Math.max(16, count + 2)];
            int elemCount = 0;
            for (int r = 0; r < count; r++) {
                rowOffsets[r] = elemCount;
                if (validity.get(r)) {
                    childOffsets[elemCount] = offsets[r];
                    elemCount++;
                }
            }
            childOffsets[elemCount] = dataLen;
            return new ArrayBuilder(kind, adopted, childOffsets, rowOffsets, elemCount, dataLen, count, validity);
        }

        @Override
        public EscfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return EscfColumnData.ofVarWidth(kind, docCount, validity, Arrays.copyOf(offsets, docCount + 1), data.moveToBytesReference());
        }

        @Override
        public void discard() {
            if (data != null) data.close();
        }
    }

    /**
     * ARRAY: per-row element-range offsets over a dense element-packed child. Under SPLIT a validity
     * bitset distinguishes a present empty array from absent; under MERGE an absent row is an empty range.
     */
    private static final class ArrayBuilder extends BaseBuilder {
        private byte childKind;
        /** Whether to emit the validity bitset (SPLIT: distinguishes {@code []} from absent). */
        private final boolean splitValidity;
        private final RecyclerBytesStreamOutput childData;
        private int[] childOffsets;
        private int[] rowOffsets;
        private int elemCount;
        private int childDataLen;
        private boolean sealed;
        /** Set once {@code childData} has been closed by a rewrite/replay; makes {@link #discard()} idempotent. */
        private boolean consumed;
        /**
         * Element-level validity bitset (bit set = element non-null); {@code null} when every element is
         * non-null. Lazily materialised on the first null element, mirroring the pattern in
         * {@link BaseBuilder#advanceAbsent()}.
         */
        private FixedBitSet childValidity;

        ArrayBuilder(byte childKind, boolean splitValidity, Recycler<BytesRef> recycler) {
            this.childKind = childKind;
            this.splitValidity = splitValidity;
            this.childData = newStream(recycler);
            this.childOffsets = new int[16];
            this.rowOffsets = new int[16];
        }

        /** Adoption constructor for {@link TypedBuilder#promoteToArray} (MERGE only); takes ownership of {@code childData}. */
        ArrayBuilder(
            byte childKind,
            RecyclerBytesStreamOutput childData,
            int[] childOffsets,
            int[] rowOffsets,
            int elemCount,
            int childDataLen,
            int rows,
            FixedBitSet validity
        ) {
            this.childKind = childKind;
            this.splitValidity = false;
            this.childData = childData;
            this.childOffsets = childOffsets;
            this.rowOffsets = rowOffsets;
            this.elemCount = elemCount;
            this.childDataLen = childDataLen;
            this.count = rows;
            this.validity = validity;
        }

        @Override
        public byte kind() {
            return EscfColumnKind.ARRAY;
        }

        byte childKind() {
            return childKind;
        }

        @Override
        public void addAbsent() {
            rowOffsets = ensureIntCapacity(rowOffsets, count + 1);
            rowOffsets[count] = elemCount;
            advanceAbsent();
        }

        @Override
        public void addAbsents(int n) {
            rowOffsets = ensureIntCapacity(rowOffsets, count + n);
            Arrays.fill(rowOffsets, count, count + n, elemCount);
            bulkAdvanceAbsent(n);
        }

        /** Begins a new present row; the caller then appends zero or more elements. */
        void startRow() {
            rowOffsets = ensureIntCapacity(rowOffsets, count + 1);
            rowOffsets[count] = elemCount;
            advancePresent();
        }

        void appendLong(long value) {
            resolveKind(EscfColumnKind.LONG);
            recordElemOffset();
            markChildPresent();
            writeLongLE(childData, value);
            childDataLen += Long.BYTES;
            elemCount++;
        }

        void appendDouble(double value) {
            resolveKind(EscfColumnKind.DOUBLE);
            recordElemOffset();
            markChildPresent();
            writeLongLE(childData, Double.doubleToRawLongBits(value));
            childDataLen += Long.BYTES;
            elemCount++;
        }

        void appendBytes(byte elemKind, byte[] bytes, int offset, int length) {
            resolveKind(elemKind);
            recordElemOffset();
            markChildPresent();
            writeBytes(childData, bytes, offset, length);
            childDataLen += length;
            elemCount++;
        }

        /** Appends an 8-byte element from its raw bit pattern; avoids a bits→double→bits round trip. */
        void appendFixedBits(byte elemKind, long bits) {
            resolveKind(elemKind);
            recordElemOffset();
            markChildPresent();
            writeLongLE(childData, bits);
            childDataLen += Long.BYTES;
            elemCount++;
        }

        /**
         * Appends an explicit JSON {@code null} element of the given {@code elemKind}. A null element
         * occupies a placeholder slot in the child data so that positional cursors stay in step: 8 zero
         * bytes for fixed-64 kinds (reuses {@link #ZERO_BYTES}), a zero-length range for var-width kinds.
         * The child validity bitset is materialised and the bit for this slot is left clear (null).
         */
        void appendNull(byte elemKind) {
            resolveKind(elemKind);
            recordElemOffset();
            markChildNull();
            if (elemKind == EscfColumnKind.LONG || elemKind == EscfColumnKind.DOUBLE) {
                // 8 zero bytes as a placeholder; ZERO_BYTES is large enough (512 bytes).
                writeBytes(childData, ZERO_BYTES, 0, Long.BYTES);
                childDataLen += Long.BYTES;
            }
            // var-width: zero-length range — nothing written to childData; recordElemOffset() already captured the fence.
            elemCount++;
        }

        /** Marks the most-recently-added element present in the child validity bitset. */
        private void markChildPresent() {
            if (childValidity != null) {
                childValidity = FixedBitSet.ensureCapacity(childValidity, elemCount + 1);
                childValidity.set(elemCount);
            }
        }

        /**
         * Marks the most-recently-added element null in the child validity bitset.
         * Materialises the bitset on first null, backfilling all prior elements as present.
         */
        private void markChildNull() {
            if (childValidity == null) {
                childValidity = new FixedBitSet(Math.max(64, elemCount + 1));
                childValidity.set(0, elemCount); // [0, elemCount) are non-null
            } else {
                childValidity = FixedBitSet.ensureCapacity(childValidity, elemCount + 1);
            }
            // bit[elemCount] is left clear — this element is null
        }

        private void resolveKind(byte k) {
            if (childKind == UNSET_ARRAY_KIND) {
                childKind = k;
            } else {
                assert childKind == k : "array child kind " + EscfColumnKind.name(childKind) + " vs element " + EscfColumnKind.name(k);
            }
        }

        private void recordElemOffset() {
            childOffsets = ensureIntCapacity(childOffsets, elemCount + 1);
            childOffsets[elemCount] = childDataLen;
        }

        /** Whether row {@code r} (element range {@code [from, to)}) is absent. */
        boolean isAbsentRow(int r, int from, int to) {
            if (validity != null) {
                return validity.get(r) == false;
            }
            // No validity materialised: SPLIT never has absents here (empty ranges are empty arrays); MERGE
            // represents an absent row as an empty range.
            return splitValidity == false && from == to;
        }

        /** Seals the row-offset and child-offset fences up to the current position (idempotent). */
        void seal() {
            if (sealed) {
                return;
            }
            rowOffsets = ensureIntCapacity(rowOffsets, count + 1);
            rowOffsets[count] = elemCount;
            childOffsets = ensureIntCapacity(childOffsets, elemCount + 1);
            childOffsets[elemCount] = childDataLen;
            sealed = true;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            throw new AssertionError("ArrayBuilder is rewritten to a union via rewriteArrayToUnion, not promote()");
        }

        @Override
        public EscfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            assert childKind != UNSET_ARRAY_KIND : "unresolved array child kind should be rewritten to a union before finish";
            rowOffsets = ensureIntCapacity(rowOffsets, docCount + 1);
            rowOffsets[docCount] = elemCount;
            childOffsets = ensureIntCapacity(childOffsets, elemCount + 1);
            childOffsets[elemCount] = childDataLen;
            // childValidity may be over-sized; EscfColumn.from re-windows it to [0, elemCount).
            final EscfColumnData child;
            if (childKind == EscfColumnKind.LONG || childKind == EscfColumnKind.DOUBLE) {
                child = EscfColumnData.ofFixed64(childKind, elemCount, childValidity, childData.moveToBytesReference());
            } else {
                child = EscfColumnData.ofVarWidth(
                    childKind,
                    elemCount,
                    childValidity,
                    Arrays.copyOf(childOffsets, elemCount + 1),
                    childData.moveToBytesReference()
                );
            }
            FixedBitSet arrayValidity = splitValidity ? validity : null;
            return EscfColumnData.ofArray(docCount, arrayValidity, Arrays.copyOf(rowOffsets, docCount + 1), child);
        }

        @Override
        public void discard() {
            if (consumed == false) {
                childData.close();
            }
        }
    }

    /** UNION: a per-document {@link SourceValueType} vector, an offset vector, and a dense value buffer. */
    private static final class UnionBuilder extends BaseBuilder {
        private final RecyclerBytesStreamOutput data;
        private int[] offsets;
        private byte[] typeVec;
        private int dataLen;

        UnionBuilder(Recycler<BytesRef> recycler) {
            this.data = newStream(recycler);
            this.offsets = new int[16];
            this.typeVec = new byte[16];
        }

        UnionBuilder(RecyclerBytesStreamOutput data, byte[] typeVec, int[] offsets, int dataLen, int count, FixedBitSet validity) {
            this.data = data;
            this.typeVec = typeVec;
            this.offsets = offsets;
            this.dataLen = dataLen;
            this.count = count;
            this.validity = validity;
        }

        @Override
        public byte kind() {
            return EscfColumnKind.UNION;
        }

        @Override
        public void addLong(long value) {
            prep(SourceValueType.LONG);
            writeLongLE(data, value);
            dataLen += 8;
            advancePresent();
        }

        @Override
        public void addDouble(double value) {
            prep(SourceValueType.DOUBLE);
            writeLongLE(data, Double.doubleToRawLongBits(value));
            dataLen += 8;
            advancePresent();
        }

        @Override
        public void addBoolean(boolean value) {
            prep(value ? SourceValueType.TRUE : SourceValueType.FALSE);
            advancePresent();
        }

        @Override
        public void addBytes(byte kind, byte[] bytes, int offset, int length) {
            prep(kind == EscfColumnKind.BINARY ? SourceValueType.BINARY : SourceValueType.STRING);
            writeBytes(data, bytes, offset, length);
            dataLen += length;
            advancePresent();
        }

        void addInlineArray(byte arrayType, byte[] packed) {
            prep(arrayType);
            writeBytes(data, packed, 0, packed.length);
            dataLen += packed.length;
            advancePresent();
        }

        // Streaming inline-slot API: begin a slot, write its payload via slotX, then close it. Avoids an
        // intermediate byte[] when rewriting a columnar array or building a heterogeneous UNION_ARRAY row.
        void beginInlineSlot(byte arrayType) {
            prep(arrayType);
        }

        void slotByte(byte b) {
            writeByte(data, b);
            dataLen += 1;
        }

        void slotIntLE(int v) {
            writeIntLE(data, v);
            dataLen += 4;
        }

        void slotLongLE(long v) {
            writeLongLE(data, v);
            dataLen += 8;
        }

        void slotBytes(byte[] bytes, int offset, int length) {
            writeBytes(data, bytes, offset, length);
            dataLen += length;
        }

        void slotSlice(BytesReference src, int offset, int length) {
            writeSlice(data, src, offset, length);
            dataLen += length;
        }

        void endInlineSlot() {
            advancePresent();
        }

        /** Counterpart to {@link #endInlineSlot()} for absent rows. */
        void endAbsentSlot() {
            advanceAbsent();
        }

        @Override
        public void addNull() {
            prep(SourceValueType.NULL);
            advancePresent();
        }

        @Override
        public void addAbsent() {
            prep(SourceValueType.ABSENT);
            advanceAbsent();
        }

        @Override
        public void addAbsents(int n) {
            offsets = ensureIntCapacity(offsets, count + n);
            typeVec = ensureByteCapacity(typeVec, count + n);
            Arrays.fill(typeVec, count, count + n, SourceValueType.ABSENT);
            Arrays.fill(offsets, count, count + n, dataLen);
            bulkAdvanceAbsent(n);
        }

        private void prep(byte type) {
            offsets = ensureIntCapacity(offsets, count + 1);
            typeVec = ensureByteCapacity(typeVec, count + 1);
            typeVec[count] = type;
            offsets[count] = dataLen;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            throw new AssertionError("a union builder is terminal and is never promoted");
        }

        @Override
        public EscfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return EscfColumnData.ofUnion(
                docCount,
                validity,
                new BytesRef(Arrays.copyOf(typeVec, docCount)),
                Arrays.copyOf(offsets, docCount + 1),
                data.moveToBytesReference()
            );
        }

        @Override
        public void discard() {
            data.close();
        }
    }

    private static RecyclerBytesStreamOutput newStream(Recycler<BytesRef> recycler) {
        return new RecyclerBytesStreamOutput(recycler);
    }

    private static void writeLongLE(RecyclerBytesStreamOutput out, long value) {
        try {
            out.writeLongLE(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeByte(RecyclerBytesStreamOutput out, byte value) {
        out.writeByte(value);
    }

    // TODO: Add first class on RecyclerBytesStreamOutput
    private static void writeIntLE(RecyclerBytesStreamOutput out, int value) {
        out.writeByte((byte) value);
        out.writeByte((byte) (value >>> 8));
        out.writeByte((byte) (value >>> 16));
        out.writeByte((byte) (value >>> 24));
    }

    private static void writeBytes(RecyclerBytesStreamOutput out, byte[] bytes, int offset, int length) {
        out.writeBytes(bytes, offset, length);
    }

    private static void writeSlice(RecyclerBytesStreamOutput out, BytesReference src, int offset, int length) {
        try {
            src.slice(offset, length).writeTo(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int[] ensureIntCapacity(int[] array, int minSize) {
        return array.length >= minSize ? array : Arrays.copyOf(array, ArrayUtil.oversize(minSize, Integer.BYTES));
    }

    private static byte[] ensureByteCapacity(byte[] array, int minSize) {
        return array.length >= minSize ? array : Arrays.copyOf(array, Math.max(minSize, array.length * 2));
    }
}
