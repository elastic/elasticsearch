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
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.sourcebatch.InlineArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Accumulates the per-document values of a single ESCF leaf column and serializes them into an
 * {@link EscfColumnData} when {@link #finish(int)} is called.
 *
 * <p>The first non-absent value selects the kind. A conflicting later value, or an explicit
 * {@code null}, promotes the column to {@link EscfColumnKind#UNION}.
 */
final class EscfColumnBuilder {

    /** Sentinel returned by {@link #arrayChildKind} for arrays that aren't a single fixed primitive kind. */
    private static final byte NO_CHILD_KIND = -1;

    private final Recycler<BytesRef> recycler;
    private TypedBuilder current;
    private int leadingAbsents;

    EscfColumnBuilder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    EscfColumnBuilder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    void addAbsent() {
        if (current == null) {
            leadingAbsents++;
        } else {
            current.addAbsent();
        }
    }

    void addLong(long value) {
        ensureScalar(EscfColumnKind.LONG);
        current.addLong(value);
    }

    void addDouble(double value) {
        ensureScalar(EscfColumnKind.DOUBLE);
        current.addDouble(value);
    }

    void addBoolean(boolean value) {
        ensureScalar(EscfColumnKind.BOOL);
        current.addBoolean(value);
    }

    void addString(XContentString.UTF8Bytes utf8) {
        ensureScalar(EscfColumnKind.STRING);
        current.addString(utf8);
    }

    void addBinary(XContentString.UTF8Bytes bytes) {
        ensureScalar(EscfColumnKind.BINARY);
        current.addBinary(bytes);
    }

    /**
     * Adds an array value parsed into its inline form ({@code arrayType} is
     * {@code SourceValueType.FIXED_ARRAY} or {@code SourceValueType.UNION_ARRAY}). A fixed array of one primitive
     * element kind is accumulated in a columnar list layout; anything else (heterogeneous, nested, object
     * elements, empty, or a child-kind change) promotes the column to a union holding inline arrays.
     */
    void addArray(byte arrayType, byte[] packed) {
        byte childKind = arrayChildKind(arrayType, packed);
        if (childKind == NO_CHILD_KIND) {
            promoteToUnion();
            current.addInlineArray(arrayType, packed);
            return;
        }
        if (current == null) {
            ArrayBuilder array = new ArrayBuilder(childKind, recycler);
            for (int i = 0; i < leadingAbsents; i++) {
                array.addAbsent();
            }
            leadingAbsents = 0;
            current = array;
            array.addColumnarArray(packed);
        } else if (current.kind() == EscfColumnKind.ARRAY && current.childKind() == childKind) {
            current.addColumnarArray(packed);
        } else {
            promoteToUnion();
            current.addInlineArray(arrayType, packed);
        }
    }

    void addNull() {
        promoteToUnion();
        current.addNull();
    }

    /**
     * Adds a key-value value (an object's entries in inline kv {@code key_length | key_bytes | type | value}
     * form, e.g. zero bytes for an empty object). There is no native key-value column kind, so this always
     * promotes to a union holding the bytes inline.
     */
    void addKeyValue(byte[] packed) {
        promoteToUnion();
        current.addInlineArray(SourceValueType.KEY_VALUE, packed);
    }

    /**
     * Determines the column kind and serialises it. An all-absent (or empty) column finishes as
     * {@link EscfColumnKind#LONG} with an all-absent bitset.
     */
    EscfColumnData finish(int docCount) {
        if (current == null) {
            FixedNumericBuilder allAbsent = new FixedNumericBuilder(EscfColumnKind.LONG, recycler);
            for (int i = 0; i < leadingAbsents; i++) {
                allAbsent.addAbsent();
            }
            current = allAbsent;
        }
        return current.finish(docCount);
    }

    /** Releases the active builder's stream without producing a column. */
    void discard() {
        if (current != null) {
            current.discard();
        }
    }

    private void ensureScalar(byte kind) {
        if (current == null) {
            current = newTyped(kind, recycler);
            for (int i = 0; i < leadingAbsents; i++) {
                current.addAbsent();
            }
            leadingAbsents = 0;
        } else if (current.kind() != kind && current.kind() != EscfColumnKind.UNION) {
            promoteToUnion();
        }
    }

    private void promoteToUnion() {
        if (current != null && current.kind() == EscfColumnKind.UNION) {
            return;
        }
        if (current != null) {
            current = current.promote(recycler);
        } else {
            UnionBuilder union = new UnionBuilder(recycler);
            for (int i = 0; i < leadingAbsents; i++) {
                union.addAbsent();
            }
            current = union;
        }
        leadingAbsents = 0;
    }

    private static TypedBuilder newTyped(byte kind, Recycler<BytesRef> recycler) {
        return switch (kind) {
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE -> new FixedNumericBuilder(kind, recycler);
            case EscfColumnKind.BOOL -> new BoolBuilder();
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> new VarBuilder(kind, recycler);
            default -> throw new IllegalArgumentException("No typed builder for kind " + EscfColumnKind.name(kind));
        };
    }

    /** Returns the fixed columnar child kind for a packed array, or {@link #NO_CHILD_KIND} if it doesn't have one. */
    private static byte arrayChildKind(byte arrayType, byte[] packed) {
        if (arrayType != SourceValueType.FIXED_ARRAY || packed.length == 0) {
            return NO_CHILD_KIND;
        }
        return switch (packed[0]) {
            case SourceValueType.INT, SourceValueType.LONG -> EscfColumnKind.LONG;
            case SourceValueType.FLOAT, SourceValueType.DOUBLE -> EscfColumnKind.DOUBLE;
            case SourceValueType.STRING -> EscfColumnKind.STRING;
            default -> NO_CHILD_KIND;
        };
    }

    private interface TypedBuilder {

        byte kind();

        /** The fixed columnar child kind; only meaningful (and overridden) when {@link #kind()} is {@code ARRAY}. */
        byte childKind();

        void addLong(long value);

        void addDouble(double value);

        void addBoolean(boolean value);

        void addString(XContentString.UTF8Bytes utf8);

        void addBinary(XContentString.UTF8Bytes bytes);

        void addColumnarArray(byte[] packed);

        void addInlineArray(byte arrayType, byte[] packed);

        void addNull();

        void addAbsent();

        UnionBuilder promote(Recycler<BytesRef> recycler);

        EscfColumnData finish(int docCount);

        void discard();
    }

    private abstract static class BaseBuilder implements TypedBuilder {

        int count;
        FixedBitSet validity;

        /**
         * Marks the current document (at {@code count}) as absent and advances {@code count}. On the first
         * absence, the validity bitset is materialised and all prior documents are backfilled as present.
         */
        final void advanceAbsent() {
            if (validity == null) {
                // First absent: materialise and backfill all prior docs as present.
                validity = new FixedBitSet(Math.max(64, count + 1));
                validity.set(0, count); // [0, count) are present
            } else {
                validity = FixedBitSet.ensureCapacity(validity, count + 1);
            }
            // leave bit[count] clear — this document is absent
            count++;
        }

        /**
         * Marks the current document (at {@code count}) as present and advances {@code count}. A no-op on
         * the validity bitset when the column is still dense (null), since all documents are implicitly
         * present; once the bitset is materialised, the present bit is explicitly set.
         */
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
        public byte childKind() {
            // Only ArrayBuilder overrides this; every other kind's addArray path never reaches a childKind() call.
            throw new AssertionError("column kind " + EscfColumnKind.name(kind()) + " has no array child kind");
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
        public void addString(XContentString.UTF8Bytes utf8) {
            throw unsupported("string");
        }

        @Override
        public void addBinary(XContentString.UTF8Bytes bytes) {
            throw unsupported("binary");
        }

        @Override
        public void addColumnarArray(byte[] packed) {
            throw unsupported("array");
        }

        @Override
        public void addInlineArray(byte arrayType, byte[] packed) {
            throw unsupported("array");
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
        private final RecyclerBytesStreamOutput data;

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
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            byte present = kind == EscfColumnKind.LONG ? SourceValueType.LONG : SourceValueType.DOUBLE;
            byte[] typeVec = new byte[count];
            int[] offsets = new int[count + 1];
            for (int i = 0; i < count; i++) {
                typeVec[i] = isAbsentAt(i) ? SourceValueType.ABSENT : present;
                offsets[i] = i * 8;
            }
            offsets[count] = count * 8;
            return new UnionBuilder(data, typeVec, offsets, count * 8, count, validity);
        }

        @Override
        public EscfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            return EscfColumnData.ofFixed64(kind, docCount, validity, data.moveToBytesReference());
        }

        @Override
        public void discard() {
            data.close();
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
        private final RecyclerBytesStreamOutput data;
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
        public void addString(XContentString.UTF8Bytes utf8) {
            addBytes(utf8);
        }

        @Override
        public void addBinary(XContentString.UTF8Bytes bytes) {
            addBytes(bytes);
        }

        private void addBytes(XContentString.UTF8Bytes value) {
            recordOffset();
            writeBytes(data, value.bytes(), value.offset(), value.length());
            dataLen += value.length();
            advancePresent();
        }

        @Override
        public void addAbsent() {
            recordOffset();
            advanceAbsent();
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
            return new UnionBuilder(data, typeVec, offsets, dataLen, count, validity);
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
            data.close();
        }
    }

    /**
     * ARRAY: arrays of a single fixed primitive child kind, kept as their inline bytes per row
     * during building (so promotion to a union is a cheap replay) and materialised into a native
     * {@code child} sub-column at {@link #finish}.
     */
    private static final class ArrayBuilder extends BaseBuilder {
        private final byte childKind;
        private final Recycler<BytesRef> recycler;
        /** Per-row inline FIXED_ARRAY bytes; {@code null} marks an absent row. */
        private final List<byte[]> rows = new ArrayList<>();

        ArrayBuilder(byte childKind, Recycler<BytesRef> recycler) {
            this.childKind = childKind;
            this.recycler = recycler;
        }

        @Override
        public byte kind() {
            return EscfColumnKind.ARRAY;
        }

        @Override
        public byte childKind() {
            return childKind;
        }

        @Override
        public void addColumnarArray(byte[] packed) {
            rows.add(packed);
            advancePresent();
        }

        @Override
        public void addAbsent() {
            rows.add(null);
            advanceAbsent();
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            UnionBuilder union = new UnionBuilder(recycler);
            for (byte[] packed : rows) {
                if (packed == null) {
                    union.addAbsent();
                } else {
                    union.addInlineArray(SourceValueType.FIXED_ARRAY, packed);
                }
            }
            return union;
        }

        @Override
        public EscfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            int[] rowOffsets = new int[docCount + 1];
            RecyclerBytesStreamOutput childData = newStream(recycler);
            EscfColumnData child;
            try {
                int elemTotal = 0;
                if (childKind == EscfColumnKind.STRING) {
                    List<BytesRef> elems = new ArrayList<>();
                    for (int r = 0; r < docCount; r++) {
                        byte[] packed = rows.get(r);
                        rowOffsets[r] = elemTotal;
                        if (packed != null) {
                            InlineArrayReader reader = new InlineArrayReader(packed, 0, packed.length, true);
                            while (reader.next()) {
                                XContentString.UTF8Bytes bytes = reader.textValue().bytes();
                                elems.add(new BytesRef(bytes.bytes(), bytes.offset(), bytes.length()));
                                elemTotal++;
                            }
                        }
                    }
                    rowOffsets[docCount] = elemTotal;
                    // Child offsets are kept as a native int[] here, not written into childData; only the wire
                    // serializer in EscfBatch flattens them to bytes.
                    int[] childOffsets = new int[elemTotal + 1];
                    int cumulative = 0;
                    for (int i = 0; i < elemTotal; i++) {
                        BytesRef e = elems.get(i);
                        childOffsets[i] = cumulative;
                        cumulative += e.length;
                        childData.writeBytes(e.bytes, e.offset, e.length);
                    }
                    childOffsets[elemTotal] = cumulative;
                    child = EscfColumnData.ofVarWidth(
                        EscfColumnKind.STRING,
                        elemTotal,
                        null,
                        childOffsets,
                        childData.moveToBytesReference()
                    );
                } else {
                    boolean isDouble = childKind == EscfColumnKind.DOUBLE;
                    for (int r = 0; r < docCount; r++) {
                        byte[] packed = rows.get(r);
                        rowOffsets[r] = elemTotal;
                        if (packed != null) {
                            InlineArrayReader reader = new InlineArrayReader(packed, 0, packed.length, true);
                            while (reader.next()) {
                                long bits;
                                if (isDouble) {
                                    double v = reader.type() == SourceValueType.FLOAT ? reader.floatValue() : reader.doubleValue();
                                    bits = Double.doubleToRawLongBits(v);
                                } else {
                                    bits = reader.type() == SourceValueType.INT ? reader.intValue() : reader.longValue();
                                }
                                childData.writeLongLE(bits);
                                elemTotal++;
                            }
                        }
                    }
                    rowOffsets[docCount] = elemTotal;
                    child = EscfColumnData.ofFixed64(childKind, elemTotal, null, childData.moveToBytesReference());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e); // in-memory stream never performs IO
            }
            return EscfColumnData.ofArray(docCount, validity, rowOffsets, child);
        }
    }

    /** UNION: a per-document {@link SourceValueType} vector, an offset vector, and a dense value buffer. */
    private static final class UnionBuilder extends BaseBuilder {
        private final RecyclerBytesStreamOutput data;
        private int[] offsets = new int[16];
        private byte[] typeVec = new byte[16];
        private int dataLen;

        UnionBuilder(Recycler<BytesRef> recycler) {
            this.data = newStream(recycler);
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
        public void addString(XContentString.UTF8Bytes utf8) {
            prep(SourceValueType.STRING);
            writeBytes(data, utf8.bytes(), utf8.offset(), utf8.length());
            dataLen += utf8.length();
            advancePresent();
        }

        @Override
        public void addBinary(XContentString.UTF8Bytes bytes) {
            prep(SourceValueType.BINARY);
            writeBytes(data, bytes.bytes(), bytes.offset(), bytes.length());
            dataLen += bytes.length();
            advancePresent();
        }

        @Override
        public void addInlineArray(byte arrayType, byte[] packed) {
            prep(arrayType);
            writeBytes(data, packed, 0, packed.length);
            dataLen += packed.length;
            advancePresent();
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

    private static void writeBytes(RecyclerBytesStreamOutput out, byte[] bytes, int offset, int length) {
        out.writeBytes(bytes, offset, length);
    }

    private static int[] ensureIntCapacity(int[] array, int minSize) {
        return array.length >= minSize ? array : Arrays.copyOf(array, Math.max(minSize, array.length * 2));
    }

    private static byte[] ensureByteCapacity(byte[] array, int minSize) {
        return array.length >= minSize ? array : Arrays.copyOf(array, Math.max(minSize, array.length * 2));
    }
}
