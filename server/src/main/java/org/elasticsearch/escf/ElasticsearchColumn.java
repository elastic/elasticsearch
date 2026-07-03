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
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;

/**
 * A direct-access view over a single ESCF leaf column. Each kind is a subtype that holds its data
 * unwrapped into the primitive representation it needs ({@code byte[]} / {@code int[]} /
 * {@link FixedBitSet}) rather than chained {@code BytesReference}s, removing per-read indirection.
 *
 * <p>The shared base owns identity ({@link #columnIndex()} / {@link #docCount()}) and the optional
 * validity (absent) set, and resolves {@link #getTypeByte}/{@link #isAbsent}/{@link #isNull} once.
 * Layout is shared further down via {@link AbstractFixed64Column} (long/double) and
 * {@link AbstractVarColumn} (string/binary). Typed value getters default to throwing; each subtype
 * overrides only what it supports. These columns are internal helpers backing {@link EscfRow} — they
 * are not part of any public {@code SourceColumn} contract.
 */
abstract class ElasticsearchColumn {

    final int columnIndex;
    final int docCount;
    /** Absent set (bit set = absent), or {@code null} when every document is present (dense). */
    final FixedBitSet absent;

    ElasticsearchColumn(int columnIndex, int docCount, FixedBitSet absent) {
        this.columnIndex = columnIndex;
        this.docCount = docCount;
        this.absent = absent;
    }

    final int columnIndex() {
        return columnIndex;
    }

    final int docCount() {
        return docCount;
    }

    /** The column kind (see {@link ElasticsearchColumnKind}). */
    abstract byte kind();

    /** Builds the typed column view for {@code col}, dispatching on its kind. The fields are already native. */
    static ElasticsearchColumn from(int columnIndex, ElasticsearchColumnData col) {
        int docCount = col.docCount();
        FixedBitSet absent = col.absent();
        return switch (col.kind()) {
            case ElasticsearchColumnKind.LONG -> {
                BytesRef d = col.data().toBytesRef();
                yield new ElasticsearchLongColumn(columnIndex, docCount, absent, d.bytes, d.offset);
            }
            case ElasticsearchColumnKind.DOUBLE -> {
                BytesRef d = col.data().toBytesRef();
                yield new ElasticsearchDoubleColumn(columnIndex, docCount, absent, d.bytes, d.offset);
            }
            case ElasticsearchColumnKind.BOOL -> new ElasticsearchBoolColumn(
                columnIndex,
                docCount,
                absent,
                boolWords(col.values(), docCount)
            );
            case ElasticsearchColumnKind.STRING -> {
                BytesRef d = col.data().toBytesRef();
                yield new ElasticsearchStringColumn(columnIndex, docCount, absent, d.bytes, d.offset, col.offsets());
            }
            case ElasticsearchColumnKind.BINARY -> {
                BytesRef d = col.data().toBytesRef();
                yield new ElasticsearchBinaryColumn(columnIndex, docCount, absent, d.bytes, d.offset, col.offsets());
            }
            case ElasticsearchColumnKind.ARRAY -> ElasticsearchArrayColumn.fromData(columnIndex, docCount, absent, col);
            case ElasticsearchColumnKind.UNION -> {
                BytesRef d = col.data().toBytesRef();
                yield new ElasticsearchUnionColumn(columnIndex, docCount, absent, col.typeVector(), 0, col.offsets(), d.bytes, d.offset);
            }
            default -> throw new IllegalStateException("Unknown ESCF column kind: " + ElasticsearchColumnKind.name(col.kind()));
        };
    }

    /**
     * Materializes the BOOL value bitset into full-width little-endian words ({@code ceil(docCount / 64)}),
     * zero-filled when {@code values == null} or shorter than {@code docCount} (documents past the last set
     * bit read as {@code false}).
     */
    private static long[] boolWords(FixedBitSet values, int docCount) {
        int words = (docCount + 63) >>> 6;
        long[] out = new long[Math.max(1, words)];
        if (values != null) {
            long[] bits = values.getBits();
            System.arraycopy(bits, 0, out, 0, Math.min(bits.length, out.length));
        }
        return out;
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
            throw new ArithmeticException("Long value " + val + " does not fit in int for column " + columnIndex);
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
        return new IllegalStateException(
            "Column " + columnIndex + " kind=" + ElasticsearchColumnKind.name(kind()) + " has no " + what + " values"
        );
    }
}
