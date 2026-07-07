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
 * Accumulates the per-document values of a single ESCF leaf column and serialises them into an
 * {@link ElasticsearchColumnData} when {@link #finish(int)} is called.
 *
 * <p>The facade dispatches each {@code add*} call to a typed builder for the column's current kind.
 * The first non-absent value selects the kind. A conflicting later value, or an explicit {@code null},
 * promotes the column to {@link ElasticsearchColumnKind#UNION}. Scalar promotions adopt the existing
 * buffers with no replay; an array column promotes by replaying its rows as inline EIRF arrays into
 * the union.
 */
final class ElasticsearchColumnBuilder {

    private final Recycler<BytesRef> recycler;
    private TypedBuilder current;
    private byte currentKind = ElasticsearchColumnKind.NONE;
    /** The fixed columnar child kind when {@code currentKind == ARRAY}; otherwise {@code NONE}. */
    private byte arrayChildKind = ElasticsearchColumnKind.NONE;
    private int leadingAbsents;

    ElasticsearchColumnBuilder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    ElasticsearchColumnBuilder(Recycler<BytesRef> recycler) {
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
        ensureScalar(ElasticsearchColumnKind.LONG);
        current.addLong(value);
    }

    void addDouble(double value) {
        ensureScalar(ElasticsearchColumnKind.DOUBLE);
        current.addDouble(value);
    }

    void addBoolean(boolean value) {
        ensureScalar(ElasticsearchColumnKind.BOOL);
        current.addBoolean(value);
    }

    void addString(XContentString.UTF8Bytes utf8) {
        ensureScalar(ElasticsearchColumnKind.STRING);
        current.addString(utf8);
    }

    void addBinary(XContentString.UTF8Bytes bytes) {
        ensureScalar(ElasticsearchColumnKind.BINARY);
        current.addBinary(bytes);
    }

    /**
     * Adds an array value parsed into its inline EIRF form ({@code arrayType} is
     * {@code SourceValueType.FIXED_ARRAY} or {@code SourceValueType.UNION_ARRAY}). A fixed array of one primitive
     * element kind is accumulated in a columnar list layout; anything else (heterogeneous, nested, object
     * elements, empty, or a child-kind change) promotes the column to a union holding inline arrays.
     */
    void addArray(byte arrayType, byte[] packed) {
        byte childKind = arrayChildKind(arrayType, packed);
        if (childKind == ElasticsearchColumnKind.NONE) {
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
            currentKind = ElasticsearchColumnKind.ARRAY;
            arrayChildKind = childKind;
            array.addColumnarArray(packed);
        } else if (currentKind == ElasticsearchColumnKind.ARRAY && arrayChildKind == childKind) {
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
     * Determines the column kind and serialises it. An all-absent (or empty) column finishes as
     * {@link ElasticsearchColumnKind#LONG} with an all-absent bitset.
     */
    ElasticsearchColumnData finish(int docCount) {
        if (current == null) {
            FixedNumericBuilder allAbsent = new FixedNumericBuilder(ElasticsearchColumnKind.LONG, recycler);
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
            currentKind = kind;
            for (int i = 0; i < leadingAbsents; i++) {
                current.addAbsent();
            }
            leadingAbsents = 0;
        } else if (currentKind != kind && currentKind != ElasticsearchColumnKind.UNION) {
            promoteToUnion();
        }
    }

    private void promoteToUnion() {
        if (currentKind == ElasticsearchColumnKind.UNION) {
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
        currentKind = ElasticsearchColumnKind.UNION;
        arrayChildKind = ElasticsearchColumnKind.NONE;
        leadingAbsents = 0;
    }

    private static TypedBuilder newTyped(byte kind, Recycler<BytesRef> recycler) {
        return switch (kind) {
            case ElasticsearchColumnKind.LONG, ElasticsearchColumnKind.DOUBLE -> new FixedNumericBuilder(kind, recycler);
            case ElasticsearchColumnKind.BOOL -> new BoolBuilder();
            case ElasticsearchColumnKind.STRING, ElasticsearchColumnKind.BINARY -> new VarBuilder(kind, recycler);
            default -> throw new IllegalArgumentException("No typed builder for kind " + ElasticsearchColumnKind.name(kind));
        };
    }

    private static byte arrayChildKind(byte arrayType, byte[] packed) {
        if (arrayType != SourceValueType.FIXED_ARRAY || packed.length == 0) {
            return ElasticsearchColumnKind.NONE;
        }
        return switch (packed[0]) {
            case SourceValueType.INT, SourceValueType.LONG -> ElasticsearchColumnKind.LONG;
            case SourceValueType.FLOAT, SourceValueType.DOUBLE -> ElasticsearchColumnKind.DOUBLE;
            case SourceValueType.STRING -> ElasticsearchColumnKind.STRING;
            default -> ElasticsearchColumnKind.NONE;
        };
    }

    private interface TypedBuilder {

        byte kind();

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

        ElasticsearchColumnData finish(int docCount);

        void discard();
    }

    private abstract static class BaseBuilder implements TypedBuilder {

        int count;
        FixedBitSet absent;

        final void markAbsent() {
            absent = absent == null ? new FixedBitSet(Math.max(64, count + 1)) : FixedBitSet.ensureCapacity(absent, count + 1);
            absent.set(count);
        }

        final boolean isAbsentAt(int d) {
            return absent != null && absent.get(d);
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
            return new AssertionError("column kind " + ElasticsearchColumnKind.name(kind()) + " cannot accept a " + type + " value");
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
            count++;
        }

        @Override
        public void addDouble(double value) {
            writeLongLE(data, Double.doubleToRawLongBits(value));
            count++;
        }

        @Override
        public void addAbsent() {
            markAbsent();
            writeLongLE(data, 0L);
            count++;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            byte present = kind == ElasticsearchColumnKind.LONG ? SourceValueType.LONG : SourceValueType.DOUBLE;
            byte[] typeVec = new byte[count];
            int[] offsets = new int[count + 1];
            for (int i = 0; i < count; i++) {
                typeVec[i] = isAbsentAt(i) ? SourceValueType.ABSENT : present;
                offsets[i] = i * 8;
            }
            offsets[count] = count * 8;
            return new UnionBuilder(data, typeVec, offsets, count * 8, count, absent);
        }

        @Override
        public ElasticsearchColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            return new ElasticsearchColumnData(kind, docCount, absent, null, null, null, data.moveToBytesReference());
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
            return ElasticsearchColumnKind.BOOL;
        }

        @Override
        public void addBoolean(boolean value) {
            if (value) {
                values = values == null ? new FixedBitSet(Math.max(64, count + 1)) : FixedBitSet.ensureCapacity(values, count + 1);
                values.set(count);
            }
            count++;
        }

        @Override
        public void addAbsent() {
            markAbsent();
            count++;
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
            return new UnionBuilder(newStream(recycler), typeVec, new int[count + 1], 0, count, absent);
        }

        @Override
        public ElasticsearchColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            return new ElasticsearchColumnData(ElasticsearchColumnKind.BOOL, docCount, absent, values, null, null, null);
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
            count++;
        }

        @Override
        public void addAbsent() {
            recordOffset();
            markAbsent();
            count++;
        }

        private void recordOffset() {
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            byte present = kind == ElasticsearchColumnKind.STRING ? SourceValueType.STRING : SourceValueType.BINARY;
            byte[] typeVec = new byte[count];
            for (int i = 0; i < count; i++) {
                typeVec[i] = isAbsentAt(i) ? SourceValueType.ABSENT : present;
            }
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return new UnionBuilder(data, typeVec, offsets, dataLen, count, absent);
        }

        @Override
        public ElasticsearchColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return new ElasticsearchColumnData(
                kind,
                docCount,
                absent,
                null,
                null,
                Arrays.copyOf(offsets, docCount + 1),
                data.moveToBytesReference()
            );
        }

        @Override
        public void discard() {
            data.close();
        }
    }

    /**
     * ARRAY: arrays of a single fixed primitive child kind, kept as their inline EIRF bytes per row
     * during building (so promotion to a union is a cheap replay) and materialised into the columnar
     * {@code child_kind | child_values} layout at {@link #finish}.
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
            return ElasticsearchColumnKind.ARRAY;
        }

        @Override
        public void addColumnarArray(byte[] packed) {
            rows.add(packed);
            count++;
        }

        @Override
        public void addAbsent() {
            rows.add(null);
            markAbsent();
            count++;
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
        public ElasticsearchColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            int[] rowOffsets = new int[docCount + 1];
            RecyclerBytesStreamOutput childData = newStream(recycler);
            try {
                // child_kind prefix byte
                childData.writeByte(childKind);
                int elemTotal = 0;
                // For STRING children, element byte-offsets are written after the child-kind byte and before bytes.
                // We accumulate string element bytes separately, then assemble, because the offset vector precedes them.
                if (childKind == ElasticsearchColumnKind.STRING) {
                    List<byte[]> elems = new ArrayList<>();
                    for (int r = 0; r < docCount; r++) {
                        byte[] packed = rows.get(r);
                        rowOffsets[r] = elemTotal;
                        if (packed != null) {
                            InlineArrayReader reader = new InlineArrayReader(packed, 0, packed.length, true);
                            while (reader.next()) {
                                elems.add(reader.stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                                elemTotal++;
                            }
                        }
                    }
                    rowOffsets[docCount] = elemTotal;
                    int[] childOffsets = new int[elemTotal + 1];
                    int cum = 0;
                    for (int i = 0; i < elemTotal; i++) {
                        childOffsets[i] = cum;
                        cum += elems.get(i).length;
                    }
                    childOffsets[elemTotal] = cum;
                    for (int i = 0; i <= elemTotal; i++) {
                        childData.writeIntLE(childOffsets[i]);
                    }
                    for (byte[] e : elems) {
                        childData.writeBytes(e, 0, e.length);
                    }
                } else {
                    boolean isDouble = childKind == ElasticsearchColumnKind.DOUBLE;
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
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e); // in-memory stream never performs IO
            }
            return new ElasticsearchColumnData(
                ElasticsearchColumnKind.ARRAY,
                docCount,
                absent,
                null,
                null,
                rowOffsets,
                childData.moveToBytesReference()
            );
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

        UnionBuilder(RecyclerBytesStreamOutput data, byte[] typeVec, int[] offsets, int dataLen, int count, FixedBitSet absent) {
            this.data = data;
            this.typeVec = typeVec;
            this.offsets = offsets;
            this.dataLen = dataLen;
            this.count = count;
            this.absent = absent;
        }

        @Override
        public byte kind() {
            return ElasticsearchColumnKind.UNION;
        }

        @Override
        public void addLong(long value) {
            prep(SourceValueType.LONG);
            writeLongLE(data, value);
            dataLen += 8;
            count++;
        }

        @Override
        public void addDouble(double value) {
            prep(SourceValueType.DOUBLE);
            writeLongLE(data, Double.doubleToRawLongBits(value));
            dataLen += 8;
            count++;
        }

        @Override
        public void addBoolean(boolean value) {
            prep(value ? SourceValueType.TRUE : SourceValueType.FALSE);
            count++;
        }

        @Override
        public void addString(XContentString.UTF8Bytes utf8) {
            prep(SourceValueType.STRING);
            writeBytes(data, utf8.bytes(), utf8.offset(), utf8.length());
            dataLen += utf8.length();
            count++;
        }

        @Override
        public void addBinary(XContentString.UTF8Bytes bytes) {
            prep(SourceValueType.BINARY);
            writeBytes(data, bytes.bytes(), bytes.offset(), bytes.length());
            dataLen += bytes.length();
            count++;
        }

        @Override
        public void addInlineArray(byte arrayType, byte[] packed) {
            prep(arrayType);
            writeBytes(data, packed, 0, packed.length);
            dataLen += packed.length;
            count++;
        }

        @Override
        public void addNull() {
            prep(SourceValueType.NULL);
            count++;
        }

        @Override
        public void addAbsent() {
            prep(SourceValueType.ABSENT);
            markAbsent();
            count++;
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
        public ElasticsearchColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return new ElasticsearchColumnData(
                ElasticsearchColumnKind.UNION,
                docCount,
                absent,
                null,
                Arrays.copyOf(typeVec, docCount),
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
