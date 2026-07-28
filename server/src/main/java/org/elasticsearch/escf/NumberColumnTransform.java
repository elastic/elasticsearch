/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.util.function.DoubleToLongFunction;
import java.util.function.LongUnaryOperator;

/**
 * Converts a numeric {@link EscfColumn} (LONG or DOUBLE kind) into an {@link EscfColumnData} of
 * LONG kind whose values are the sortable-long doc-values encoding for a given
 * {@link NumberFieldMapper.NumberType}. The sortable encoding ({@link NumericUtils#floatToSortableInt},
 * {@link NumericUtils#doubleToSortableLong}) is applied by {@code NumberType.addFields}, not the
 * parser; this class reproduces it without boxing or calling {@code NumberType.parse}.
 */
public final class NumberColumnTransform {

    private NumberColumnTransform() {}

    public static EscfColumnData toSortableLongColumn(
        EscfColumn source,
        NumberFieldMapper.NumberType type,
        boolean coerce,
        Recycler<BytesRef> recycler
    ) {
        return switch (source.kind()) {
            case EscfColumnKind.LONG -> fromLong(source, type, recycler);
            case EscfColumnKind.DOUBLE -> fromDouble(source, type, coerce, recycler);
            case EscfColumnKind.ARRAY -> fromArray(source, type, coerce, recycler);
            default -> throw new UnsupportedOperationException(
                "toSortableLongColumn: unsupported ESCF column kind ["
                    + EscfColumnKind.name(source.kind())
                    + "] — only LONG, DOUBLE, and ARRAY are supported"
            );
        };
    }

    private static EscfColumnData fromArray(
        EscfColumn source,
        NumberFieldMapper.NumberType type,
        boolean coerce,
        Recycler<BytesRef> recycler
    ) {
        // Materialize the array structure: offsets + child data. The child is always dense (all
        // elements present — absent rows are represented by an empty offset range, not a child gap).
        EscfColumnData sourceData = source.toColumnData();
        EscfColumnData childData = sourceData.child();
        EscfColumn child = EscfColumn.from(childData);
        EscfColumnData transformedChild = switch (child.kind()) {
            case EscfColumnKind.LONG -> fromLong(child, type, recycler);
            case EscfColumnKind.DOUBLE -> fromDouble(child, type, coerce, recycler);
            default -> throw new UnsupportedOperationException(
                "toSortableLongColumn: ARRAY child kind ["
                    + EscfColumnKind.name(child.kind())
                    + "] is not supported — child must be LONG or DOUBLE"
            );
        };
        return EscfColumnData.ofArray(sourceData.docCount(), sourceData.validity(), sourceData.offsets(), transformedChild);
    }

    private static EscfColumnData fromLong(EscfColumn source, NumberFieldMapper.NumberType type, Recycler<BytesRef> recycler) {
        return switch (type) {
            // Two's-complement longs are already the sortable encoding: zero-copy no-op.
            case LONG -> source.toColumnData();
            case BYTE -> {
                validateLongRange(source, Byte.MIN_VALUE, Byte.MAX_VALUE, "a byte");
                yield source.toColumnData();
            }
            case SHORT -> {
                validateLongRange(source, Short.MIN_VALUE, Short.MAX_VALUE, "a short");
                yield source.toColumnData();
            }
            case INTEGER -> {
                validateLongRange(source, Integer.MIN_VALUE, Integer.MAX_VALUE, "an integer");
                yield source.toColumnData();
            }
            // (float) l and (double) l match the row path: Jackson uses l2f/l2d directly.
            case FLOAT -> copyLong(source, l -> (long) NumericUtils.floatToSortableInt((float) l), recycler);
            case DOUBLE -> copyLong(source, l -> NumericUtils.doubleToSortableLong((double) l), recycler);
            case HALF_FLOAT -> copyLong(source, l -> toValidatedHalfFloat((float) l), recycler);
        };
    }

    private static void validateLongRange(EscfColumn source, long min, long max, String typeName) {
        LongTupleCursor cursor = source.longCursor();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            long l = cursor.longValue();
            if (l < min || l > max) {
                throw new IllegalArgumentException("Value [" + l + "] is out of range for " + typeName);
            }
        }
    }

    private static EscfColumnData copyLong(EscfColumn source, LongUnaryOperator convert, Recycler<BytesRef> recycler) {
        EscfColumnBuilder builder = newLongBuilder(recycler);
        LongTupleCursor cursor = source.longCursor();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            builder.setLong(doc, convert.applyAsLong(cursor.longValue()));
        }
        return builder.finish(source.docCount());
    }

    private static EscfColumnData fromDouble(
        EscfColumn source,
        NumberFieldMapper.NumberType type,
        boolean coerce,
        Recycler<BytesRef> recycler
    ) {
        return switch (type) {
            // sortableDoubleBits is a branch-free bit-op equivalent to doubleToSortableLong.
            case DOUBLE -> copyDoubleBits(source, recycler);
            case FLOAT -> copyDouble(source, NumberColumnTransform::doubleToFloatSortable, recycler);
            case BYTE -> narrowDoubleToInteger(source, Byte.MIN_VALUE, Byte.MAX_VALUE, "a byte", coerce, recycler);
            case SHORT -> narrowDoubleToInteger(source, Short.MIN_VALUE, Short.MAX_VALUE, "a short", coerce, recycler);
            case INTEGER -> narrowDoubleToInteger(source, Integer.MIN_VALUE, Integer.MAX_VALUE, "an integer", coerce, recycler);
            case LONG -> narrowDoubleToInteger(source, Long.MIN_VALUE, Long.MAX_VALUE, "a long", coerce, recycler);
            case HALF_FLOAT -> copyDouble(source, d -> toValidatedHalfFloat((float) d), recycler);
        };
    }

    private static EscfColumnData copyDoubleBits(EscfColumn source, Recycler<BytesRef> recycler) {
        EscfColumnBuilder builder = newLongBuilder(recycler);
        LongTupleCursor cursor = source.longCursor();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            builder.setLong(doc, NumericUtils.sortableDoubleBits(cursor.longValue()));
        }
        return builder.finish(source.docCount());
    }

    private static EscfColumnData copyDouble(EscfColumn source, DoubleToLongFunction convert, Recycler<BytesRef> recycler) {
        EscfColumnBuilder builder = newLongBuilder(recycler);
        LongTupleCursor cursor = source.longCursor();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            double d = Double.longBitsToDouble(cursor.longValue());
            builder.setLong(doc, convert.applyAsLong(d));
        }
        return builder.finish(source.docCount());
    }

    private static long doubleToFloatSortable(double d) {
        float f = (float) d;
        if (Float.isFinite(f) == false) {
            throw new IllegalArgumentException("[float] supports only finite values, but got [" + f + "]");
        }
        return NumericUtils.floatToSortableInt(f);
    }

    /**
     * Narrows a double to an integer type. Out-of-range always throws (coerce-independent);
     * a fractional value throws when {@code coerce=false}.
     */
    private static EscfColumnData narrowDoubleToInteger(
        EscfColumn source,
        double min,
        double max,
        String typeName,
        boolean coerce,
        Recycler<BytesRef> recycler
    ) {
        EscfColumnBuilder builder = newLongBuilder(recycler);
        LongTupleCursor cursor = source.longCursor();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            double d = Double.longBitsToDouble(cursor.longValue());
            if (d < min || d > max) {
                throw new IllegalArgumentException("Value [" + d + "] is out of range for " + typeName);
            }
            if (coerce == false && d % 1 != 0) {
                throw new IllegalArgumentException("Value [" + d + "] has a decimal part");
            }
            builder.setLong(doc, (long) d);
        }
        return builder.finish(source.docCount());
    }

    private static short toValidatedHalfFloat(float f) {
        short s = HalfFloatPoint.halfFloatToSortableShort(f);
        if (Float.isFinite(HalfFloatPoint.sortableShortToHalfFloat(s)) == false) {
            throw new IllegalArgumentException("[half_float] supports only finite values, but got [" + f + "]");
        }
        return s;
    }

    private static EscfColumnBuilder newLongBuilder(Recycler<BytesRef> recycler) {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE, recycler);
        b.lockScalar(EscfColumnKind.LONG);
        return b;
    }
}
