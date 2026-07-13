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

/**
 * A heterogeneous ESCF column: a per-document {@link SourceValueType} vector gives each row's type, and a
 * dense value buffer delimited by a {@code (docCount + 1)}-entry offset vector holds the payload.
 * Zero-byte types (NULL/TRUE/FALSE/ABSENT) occupy no payload, fixed numerics (LONG/DOUBLE) occupy 8
 * bytes, and variable types occupy their offset-delta bytes. Array and key-value rows are stored as
 * inline bytes and read with {@link InlineArrayReader} / {@link KeyValueReader}.
 */
final class EscfUnionColumn extends EscfColumn {

    private final byte[] typeVec;
    private final int typeVecBase;
    private final int[] offsets;
    private final BytesReference data;

    EscfUnionColumn(int docCount, FixedBitSet absent, byte[] typeVec, int typeVecBase, int[] offsets, BytesReference data) {
        super(docCount, absent);
        this.typeVec = typeVec;
        this.typeVecBase = typeVecBase;
        this.offsets = offsets;
        this.data = data;
    }

    @Override
    byte kind() {
        return EscfColumnKind.UNION;
    }

    @Override
    byte typeByteForPresent(int d) {
        return typeVec[typeVecBase + d];
    }

    @Override
    boolean getBooleanValue(int d) {
        byte t = typeVec[typeVecBase + d];
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
        return data.getLongLE(offsets[d]);
    }

    @Override
    double getDoubleValue(int d) {
        return Double.longBitsToDouble(data.getLongLE(offsets[d]));
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
        boolean fixed = typeVec[typeVecBase + d] == SourceValueType.FIXED_ARRAY;
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
        int off0 = offsets[d];
        return data.slice(off0, offsets[d + 1] - off0).toBytesRef();
    }
}
