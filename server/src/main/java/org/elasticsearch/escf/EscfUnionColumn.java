/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.InlineArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

/**
 * A heterogeneous ESCF column: a per-document {@link SourceValueType} vector gives each row's type, and a
 * dense value buffer delimited by a {@code (docCount + 1)}-entry offset vector holds the payload.
 * Zero-byte types (NULL/TRUE/FALSE/ABSENT) occupy no payload, fixed numerics (LONG/DOUBLE) occupy 8
 * bytes, and variable types occupy their offset-delta bytes. Array and key-value rows are stored as
 * inline bytes and read with {@link InlineArrayReader} / {@link KeyValueReader}.
 */
final class EscfUnionColumn extends EscfColumn {

    private final BytesRef typeVec;
    private final IntsRef offsets;
    private final BytesReference data;

    EscfUnionColumn(int docCount, FixedBitSet validity, BytesRef typeVec, IntsRef offsets, BytesReference data) {
        super(docCount, validity);
        this.typeVec = typeVec;
        this.offsets = offsets;
        this.data = data;
    }

    @Override
    public byte kind() {
        return EscfColumnKind.UNION;
    }

    @Override
    byte typeByteForPresent(int row) {
        return byteAt(typeVec, row);
    }

    @Override
    public boolean getBooleanValue(int row) {
        byte t = byteAt(typeVec, row);
        if (t == SourceValueType.TRUE) {
            return true;
        }
        if (t == SourceValueType.FALSE) {
            return false;
        }
        throw new IllegalStateException("Doc " + row + " is not boolean, type=" + SourceValueType.name(t));
    }

    @Override
    public int getIntValue(int row) {
        // INT values are stored as 4 bytes in the UNION column, so this reads directly rather than
        // narrowing getLongValue() as the base class does. Caller must have checked typeByteForPresent.
        return data.getIntLE(intAt(offsets, row));
    }

    @Override
    public float getFloatValue(int row) {
        // FLOAT values are stored as 4 raw bytes (bit-identical IEEE 754), not as a narrowed double.
        // Caller must have checked typeByteForPresent.
        return Float.intBitsToFloat(data.getIntLE(intAt(offsets, row)));
    }

    @Override
    public long getLongValue(int row) {
        return data.getLongLE(intAt(offsets, row));
    }

    @Override
    public double getDoubleValue(int row) {
        return Double.longBitsToDouble(data.getLongLE(intAt(offsets, row)));
    }

    @Override
    public Text getStringValue(int row) {
        BytesRef ref = value(row);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    @Override
    public BytesRef getBinaryValue(int row) {
        return value(row);
    }

    /**
     * A union row is multi-valued when it is an array holding more than one leaf value, counting through nested arrays the way the
     * mappers flatten them. An explicit {@code null} element counts only when {@code countNulls}: a mapper that substitutes
     * {@code null_value} for it writes a value, so {@code [x, null]} is two values on a single-valued field, but a mapper that drops
     * it writes one.
     *
     * <p>Costs one {@link ArrayReader} per array row, and stops reading a row at its second value.
     */
    @Override
    public boolean hasMultiValueDoc(boolean countNulls) {
        for (int row = 0; row < docCount; row++) {
            if (isAbsent(row)) {
                continue;
            }
            final byte type = byteAt(typeVec, row);
            if ((type == SourceValueType.FIXED_ARRAY || type == SourceValueType.UNION_ARRAY)
                && countLeaves(getArrayValue(row), 0, countNulls) > 1) {
                return true;
            }
        }
        return false;
    }

    private static int countLeaves(ArrayReader reader, int soFar, boolean countNulls) {
        int count = soFar;
        while (count <= 1 && reader.next()) {
            if (reader.isNull()) {
                if (countNulls) {
                    count++;
                }
                continue;
            }
            final byte type = reader.type();
            count = type == SourceValueType.FIXED_ARRAY || type == SourceValueType.UNION_ARRAY
                ? countLeaves(reader.nestedArray(), count, countNulls)
                : count + 1;
        }
        return count;
    }

    @Override
    public ArrayReader getArrayValue(int row) {
        boolean fixed = byteAt(typeVec, row) == SourceValueType.FIXED_ARRAY;
        // InlineArrayReader takes a byte[]; materialise this one value's bytes (zero-copy when contiguous).
        BytesRef ref = value(row);
        return new InlineArrayReader(ref.bytes, ref.offset, ref.length, fixed);
    }

    @Override
    public KeyValueReader getKeyValue(int row) {
        // KeyValueReader takes a byte[]; materialise this one value's bytes (zero-copy when contiguous).
        BytesRef ref = value(row);
        return new KeyValueReader(ref.bytes, ref.offset, ref.length);
    }

    // TODO: Union pretty much is a var column with a type byte vector. Refactor to make this extend var column.
    @Override
    public ObjectTupleCursor<BytesRef> bytesRefCursor(boolean retainValues) {
        return new AbstractVarColumn.BytesRefTupleCursor(
            presentDocs(),
            new AbstractVarColumn.DenseBytesRefValuesCursor(docCount, offsets, data, retainValues)
        );
    }

    /** The contiguous bytes for document {@code row}'s value, sliced from the payload (zero-copy when contiguous). */
    private BytesRef value(int row) {
        int off0 = intAt(offsets, row);
        return data.slice(off0, intAt(offsets, row + 1) - off0).toBytesRef();
    }

    @Override
    public int sizeInBytes(int row) {
        return isAbsent(row) ? 0 : sizeOfPresent(row);
    }

    @Override
    public void addSizesInBytes(int[] sizes) {
        if (validity == null) {
            for (int d = 0; d < docCount; d++) {
                sizes[d] += sizeOfPresent(d);
            }
            return;
        }
        PresentDocIterator present = presentDocs();
        for (int d = present.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = present.nextDoc()) {
            sizes[d] += sizeOfPresent(d);
        }
    }

    /**
     * Every row of a union carries its own type, so the size is settled per cell: a variable-length
     * value (string, binary, inline array or key-value) is its payload length, read from the offset fence
     * posts, and a fixed-width scalar is {@link SourceValueType#elemDataSize} of its type, floored at 0
     * for the zero-byte types.
     */
    private int sizeOfPresent(int row) {
        byte type = byteAt(typeVec, row);
        if (SourceValueType.isVariable(type)) {
            int base = offsets.offset + row;
            return offsets.ints[base + 1] - offsets.ints[base];
        }
        return Math.max(SourceValueType.elemDataSize(type), 0);
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfUnionColumn(
            count,
            windowValidity(validity, from, count),
            new BytesRef(typeVec.bytes, typeVec.offset + from, count),
            sliceOffsets(offsets, from, count),
            data
        );
    }

    @Override
    EscfColumnData toColumnData() {
        BytesReference newData = sliceData(offsets, data, docCount);
        int[] newOffsets = rebasedOffsets(offsets, docCount);
        return EscfColumnData.ofUnion(docCount, validity, typeVec, newOffsets, newData);
    }

}
