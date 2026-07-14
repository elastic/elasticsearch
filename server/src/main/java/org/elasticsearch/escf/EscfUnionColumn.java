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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.InlineArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

import java.util.Arrays;

/**
 * A heterogeneous ESCF column: a per-document {@link SourceValueType} vector gives each row's type, and a
 * dense value buffer delimited by a {@code (fullDocCount + 1)}-entry offset vector holds the payload.
 * Zero-byte types (NULL/TRUE/FALSE/ABSENT) occupy no payload, fixed numerics (LONG/DOUBLE) occupy 8
 * bytes, and variable types occupy their offset-delta bytes. Array and key-value rows are stored as
 * inline bytes and read with {@link InlineArrayReader} / {@link KeyValueReader}.
 *
 * <p>The former separate {@code typeVecBase} field has been folded into {@link EscfColumn#base}: all
 * accesses to {@code typeVec} and {@code offsets} use the absolute index {@code base + d}.
 */
final class EscfUnionColumn extends EscfColumn {

    private final byte[] typeVec;
    private final int[] offsets;
    private final BytesReference data;

    EscfUnionColumn(int docCount, FixedBitSet absent, byte[] typeVec, int[] offsets, BytesReference data) {
        super(docCount, absent);
        this.typeVec = typeVec;
        this.offsets = offsets;
        this.data = data;
    }

    private EscfUnionColumn(int docCount, FixedBitSet absent, byte[] typeVec, int[] offsets, BytesReference data, int base) {
        super(docCount, absent, base);
        this.typeVec = typeVec;
        this.offsets = offsets;
        this.data = data;
    }

    @Override
    byte kind() {
        return EscfColumnKind.UNION;
    }

    @Override
    byte typeByteForPresent(int d) {
        return typeVec[base + d];
    }

    @Override
    boolean getBooleanValue(int d) {
        byte t = typeVec[base + d];
        if (t == SourceValueType.TRUE) {
            return true;
        }
        if (t == SourceValueType.FALSE) {
            return false;
        }
        throw new IllegalStateException("Doc " + d + " is not boolean, type=" + SourceValueType.name(t));
    }

    @Override
    long getLongValue(int d) {
        return data.getLongLE(offsets[base + d]);
    }

    @Override
    double getDoubleValue(int d) {
        return Double.longBitsToDouble(data.getLongLE(offsets[base + d]));
    }

    @Override
    Text getStringValue(int d) {
        BytesRef ref = value(d);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    @Override
    BytesRef getBinaryValue(int d) {
        return value(d);
    }

    @Override
    ArrayReader getArrayValue(int d) {
        boolean fixed = typeVec[base + d] == SourceValueType.FIXED_ARRAY;
        // InlineArrayReader takes a byte[]; materialise this one value's bytes (zero-copy when contiguous).
        BytesRef ref = value(d);
        return new InlineArrayReader(ref.bytes, ref.offset, ref.length, fixed);
    }

    @Override
    KeyValueReader getKeyValue(int d) {
        // KeyValueReader takes a byte[]; materialise this one value's bytes (zero-copy when contiguous).
        BytesRef ref = value(d);
        return new KeyValueReader(ref.bytes, ref.offset, ref.length);
    }

    /** The contiguous bytes for document {@code d}'s value, sliced from the payload (zero-copy when contiguous). */
    private BytesRef value(int d) {
        int off0 = offsets[base + d];
        return data.slice(off0, offsets[base + d + 1] - off0).toBytesRef();
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfUnionColumn(count, absent, typeVec, offsets, data, base + from);
    }

    @Override
    EscfColumnData toColumnData() {
        FixedBitSet newAbsent = windowBitSet(absent, base, docCount);
        byte[] newTypeVec = Arrays.copyOfRange(typeVec, base, base + docCount);
        int byteFrom = offsets[base];
        BytesReference newData = data.slice(byteFrom, offsets[base + docCount] - byteFrom);
        int[] newOffsets = rebasedOffsets(offsets, base, docCount);
        return EscfColumnData.ofUnion(docCount, newAbsent, newTypeVec, newOffsets, newData);
    }
}
