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
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;

/**
 * A direct-access view over a single ESCF leaf column. Each kind is a subtype that reads its payload
 * in place from the column's native, possibly-paged {@link BytesReference}
 * (plus native {@code int[]} offsets / {@link FixedBitSet} metadata).
 *
 * <p>Layout is shared further down via {@link AbstractFixed64Column} (long/double) and
 * {@link AbstractVarColumn} (string/binary). Typed value getters default to throwing; each subtype
 * overrides only what it supports.
 */
abstract class EscfColumn {

    final int docCount;
    /** Absent set (bit set = absent), or {@code null} when every document is present (dense). */
    final FixedBitSet absent;

    EscfColumn(int docCount, FixedBitSet absent) {
        this.docCount = docCount;
        this.absent = absent;
    }

    /** The column kind (see {@link EscfColumnKind}). */
    abstract byte kind();

    /** Builds the typed column view for {@code col}, dispatching on its kind. The fields are already native. */
    static EscfColumn from(EscfColumnData col) {
        int docCount = col.docCount();
        FixedBitSet absent = col.absent();
        return switch (col.kind()) {
            case EscfColumnKind.LONG -> new EscfLongColumn(docCount, absent, col.data());
            case EscfColumnKind.DOUBLE -> new EscfDoubleColumn(docCount, absent, col.data());
            case EscfColumnKind.BOOL -> new EscfBoolColumn(docCount, absent, col.values());
            case EscfColumnKind.STRING -> new EscfStringColumn(docCount, absent, col.data(), col.offsets());
            case EscfColumnKind.BINARY -> new EscfBinaryColumn(docCount, absent, col.data(), col.offsets());
            case EscfColumnKind.ARRAY -> new EscfArrayColumn(docCount, absent, from(col.child()), col.offsets());
            case EscfColumnKind.UNION -> new EscfUnionColumn(docCount, absent, col.typeVector(), 0, col.offsets(), col.data());
            default -> throw new IllegalStateException("Unknown ESCF column kind: " + EscfColumnKind.name(col.kind()));
        };
    }

    final boolean isAbsent(int d) {
        if (d < 0 || d >= docCount) {
            return true;
        }
        // The absent bitset is only sized to the last absent document (it may be narrower than docCount when
        // the trailing documents are all present), so any doc beyond its length is present.
        return absent != null && d < absent.length() && absent.get(d);
    }

    final byte getTypeByte(int d) {
        if (d < 0 || d >= docCount || isAbsent(d)) {
            return SourceValueType.ABSENT;
        }
        return typeByteForPresent(d);
    }

    /** The {@link SourceValueType} byte for document {@code d}, which is known to be present. */
    abstract byte typeByteForPresent(int d);

    final boolean isNull(int d) {
        return getTypeByte(d) == SourceValueType.NULL;
    }

    // Typed value getters — default to throwing; subtypes override what they support.

    boolean getBooleanValue(int d) {
        throw notA("boolean");
    }

    long getLongValue(int d) {
        throw notA("long");
    }

    double getDoubleValue(int d) {
        throw notA("double");
    }

    /** Narrows {@link #getLongValue} to an {@code int}, throwing if out of range. */
    int getIntValue(int d) {
        long val = getLongValue(d);
        if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
            throw new ArithmeticException("Long value " + val + " does not fit in int");
        }
        return (int) val;
    }

    /** Narrows {@link #getDoubleValue} to a {@code float}. */
    float getFloatValue(int d) {
        return (float) getDoubleValue(d);
    }

    Text getStringValue(int d) {
        throw notA("string");
    }

    BytesRef getBinaryValue(int d) {
        throw notA("binary");
    }

    ArrayReader getArrayValue(int d) {
        throw notA("array");
    }

    KeyValueReader getKeyValue(int d) {
        throw notA("key-value");
    }

    private IllegalStateException notA(String what) {
        return new IllegalStateException("Column kind=" + EscfColumnKind.name(kind()) + " has no " + what + " values");
    }
}
