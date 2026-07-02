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
import org.elasticsearch.common.util.ByteUtils;
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
 * inline EIRF bytes and read with {@link InlineArrayReader} / {@link KeyValueReader}. This is the
 * one ESCF kind that diverges from Arrow and the only one that branches on type at read time.
 */
final class ElasticsearchUnionColumn extends ElasticsearchColumn {

    private final byte[] typeVec;
    private final int typeVecBase;
    private final int[] offsets;
    private final byte[] data;
    private final int base;

    ElasticsearchUnionColumn(
        int columnIndex,
        int docCount,
        FixedBitSet absent,
        byte[] typeVec,
        int typeVecBase,
        int[] offsets,
        byte[] data,
        int base
    ) {
        super(columnIndex, docCount, absent);
        this.typeVec = typeVec;
        this.typeVecBase = typeVecBase;
        this.offsets = offsets;
        this.data = data;
        this.base = base;
    }

    @Override
    byte kind() {
        return ElasticsearchColumnKind.UNION;
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
        throw new IllegalStateException("Column " + columnIndex + " doc " + d + " is not boolean, type=" + SourceValueType.name(t));
    }

    @Override
    long getLongValue(int d) {
        return ByteUtils.readLongLE(data, base + offsets[d]);
    }

    @Override
    double getDoubleValue(int d) {
        return Double.longBitsToDouble(ByteUtils.readLongLE(data, base + offsets[d]));
    }

    @Override
    Text getStringValue(int d) {
        int off0 = offsets[d];
        return new Text(new XContentString.UTF8Bytes(data, base + off0, offsets[d + 1] - off0));
    }

    @Override
    BytesRef getBinaryValue(int d) {
        int off0 = offsets[d];
        return new BytesRef(data, base + off0, offsets[d + 1] - off0);
    }

    @Override
    ArrayReader getArrayValue(int d) {
        boolean fixed = typeVec[typeVecBase + d] == SourceValueType.FIXED_ARRAY;
        int off0 = offsets[d];
        return new InlineArrayReader(data, base + off0, offsets[d + 1] - off0, fixed);
    }

    @Override
    KeyValueReader getKeyValue(int d) {
        int off0 = offsets[d];
        return new KeyValueReader(data, base + off0, offsets[d + 1] - off0);
    }
}
