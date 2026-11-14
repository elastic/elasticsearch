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
import org.elasticsearch.plugins.internal.MeteringModel;
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
        // A numeric row's payload is the raw value, not text: render it canonically for numerics/
        byte type = byteAt(typeVec, row);
        if (type == SourceValueType.LONG) {
            return new Text(Long.toString(getLongValue(row)));
        }
        if (type == SourceValueType.DOUBLE) {
            return new Text(Double.toString(getDoubleValue(row)));
        }
        BytesRef ref = value(row);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    @Override
    public BytesRef getBinaryValue(int row) {
        return value(row);
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

    @Override
    public void addMeteredSizes(long[] sizes, MeteringModel model) {
        if (validity == null) {
            for (int d = 0; d < docCount; d++) {
                sizes[d] += meteredSizeOfPresent(d, model);
            }
            return;
        }
        PresentDocIterator present = presentDocs();
        for (int d = present.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = present.nextDoc()) {
            sizes[d] += meteredSizeOfPresent(d, model);
        }
    }

    /**
     * Settled per cell on its type: a string or binary by its payload length, a scalar by the model's constant, and an inline
     * array or object by walking its values.
     */
    private long meteredSizeOfPresent(int row, MeteringModel model) {
        final byte type = byteAt(typeVec, row);
        return switch (type) {
            case SourceValueType.STRING -> model.stringBytes(payloadLength(row));
            case SourceValueType.BINARY -> model.binaryBytes(payloadLength(row));
            case SourceValueType.INT, SourceValueType.FLOAT, SourceValueType.LONG, SourceValueType.DOUBLE -> model.numberBytes();
            case SourceValueType.TRUE, SourceValueType.FALSE -> model.booleanBytes();
            case SourceValueType.NULL -> 0;
            case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> meteredSize(getArrayValue(row), model);
            case SourceValueType.KEY_VALUE -> meteredSize(getKeyValue(row), model);
            default -> throw new IllegalStateException("unexpected union value type [" + SourceValueType.name(type) + "]");
        };
    }

    private int payloadLength(int row) {
        final int base = offsets.offset + row;
        return offsets.ints[base + 1] - offsets.ints[base];
    }

    /** The size {@code model} charges for the elements {@code array} yields, nested arrays and objects included. */
    private static long meteredSize(ArrayReader array, MeteringModel model) {
        long size = 0;
        while (array.next()) {
            final byte type = array.type();
            size += switch (type) {
                case SourceValueType.STRING -> model.stringBytes(array.valueLength());
                case SourceValueType.INT, SourceValueType.FLOAT, SourceValueType.LONG, SourceValueType.DOUBLE -> model.numberBytes();
                case SourceValueType.TRUE, SourceValueType.FALSE -> model.booleanBytes();
                case SourceValueType.NULL -> 0;
                case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> meteredSize(array.nestedArray(), model);
                case SourceValueType.KEY_VALUE -> meteredSize(array.nestedKeyValue(), model);
                default -> throw new IllegalStateException("unexpected array element type [" + SourceValueType.name(type) + "]");
            };
        }
        return size;
    }

    /** The size {@code model} charges for the values {@code keyValues} yields; keys are never charged. */
    private static long meteredSize(KeyValueReader keyValues, MeteringModel model) {
        long size = 0;
        while (keyValues.next()) {
            final byte type = keyValues.type();
            size += switch (type) {
                case SourceValueType.STRING -> model.stringBytes(keyValues.valueLength());
                case SourceValueType.INT, SourceValueType.FLOAT, SourceValueType.LONG, SourceValueType.DOUBLE -> model.numberBytes();
                case SourceValueType.TRUE, SourceValueType.FALSE -> model.booleanBytes();
                case SourceValueType.NULL -> 0;
                case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> meteredSize(keyValues.nestedArray(), model);
                case SourceValueType.KEY_VALUE -> meteredSize(keyValues.nestedKeyValue(), model);
                default -> throw new IllegalStateException("unexpected object value type [" + SourceValueType.name(type) + "]");
            };
        }
        return size;
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
