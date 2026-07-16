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

    EscfUnionColumn(int docCount, FixedBitSet absent, BytesRef typeVec, IntsRef offsets, BytesReference data) {
        super(docCount, absent);
        this.typeVec = typeVec;
        this.offsets = offsets;
        this.data = data;
    }

    @Override
    byte kind() {
        return EscfColumnKind.UNION;
    }

    @Override
    byte typeByteForPresent(int row) {
        return byteAt(typeVec, row);
    }

    @Override
    boolean getBooleanValue(int row) {
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
    long getLongValue(int row) {
        return data.getLongLE(intAt(offsets, row));
    }

    @Override
    double getDoubleValue(int row) {
        return Double.longBitsToDouble(data.getLongLE(intAt(offsets, row)));
    }

    @Override
    Text getStringValue(int row) {
        BytesRef ref = value(row);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    @Override
    BytesRef getBinaryValue(int row) {
        return value(row);
    }

    @Override
    ArrayReader getArrayValue(int row) {
        boolean fixed = byteAt(typeVec, row) == SourceValueType.FIXED_ARRAY;
        // InlineArrayReader takes a byte[]; materialise this one value's bytes (zero-copy when contiguous).
        BytesRef ref = value(row);
        return new InlineArrayReader(ref.bytes, ref.offset, ref.length, fixed);
    }

    @Override
    KeyValueReader getKeyValue(int row) {
        // KeyValueReader takes a byte[]; materialise this one value's bytes (zero-copy when contiguous).
        BytesRef ref = value(row);
        return new KeyValueReader(ref.bytes, ref.offset, ref.length);
    }

    /** The contiguous bytes for document {@code row}'s value, sliced from the payload (zero-copy when contiguous). */
    private BytesRef value(int row) {
        int off0 = intAt(offsets, row);
        return data.slice(off0, intAt(offsets, row + 1) - off0).toBytesRef();
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfUnionColumn(
            count,
            windowBitSet(absent, from, count),
            new BytesRef(typeVec.bytes, typeVec.offset + from, count),
            sliceOffsets(offsets, from, count),
            data
        );
    }

    @Override
    EscfColumnData toColumnData() {
        BytesReference newData = sliceData(offsets, data, docCount);
        int[] newOffsets = rebasedOffsets(offsets, docCount);
        return EscfColumnData.ofUnion(docCount, absent, typeVec, newOffsets, newData);
    }
}
