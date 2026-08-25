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
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.util.function.DoubleToLongFunction;
import java.util.function.LongUnaryOperator;

/**
 * Converts a numeric {@link EscfColumn} (LONG, DOUBLE, STRING, or ARRAY) into an
 * {@link EscfColumnData} of LONG kind holding the sortable-long doc-values encoding for a given
 * {@link NumberFieldMapper.NumberType}. STRING values are parsed with indexing-path semantics:
 * integer types try an ASCII fast path then fall back to {@link AbstractXContentParser};
 * float/double go straight to {@link String} parsing. {@code coerce=false} rejects any string.
 */
public final class NumberColumnTransform {

    private NumberColumnTransform() {}

    /**
     * Converts a LONG {@link EscfColumn} whose values are
     * {@link HalfFloatPoint#halfFloatToSortableShort} encoded sortable shorts into a BINARY
     * {@link EscfColumnData} containing the 2-byte {@link HalfFloatPoint} BKD point encoding for
     * each value. Use the result with a {@link org.elasticsearch.escf.LuceneBinaryColumn} to emit
     * the points column for an indexed {@code half_float} field.
     */
    public static EscfColumnData toHalfFloatPointBinaryColumn(EscfColumn source, Recycler<BytesRef> recycler) {
        assert source.kind() == EscfColumnKind.LONG || source.kind() == EscfColumnKind.ARRAY
            : "expected LONG or ARRAY, got " + EscfColumnKind.name(source.kind());
        EscfColumnBuilder builder = newBytesBuilder(recycler);
        final byte[] buf = new byte[Short.BYTES];
        final BytesRef ref = new BytesRef(buf);
        LongTupleCursor cursor = source.longCursor();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            HalfFloatPoint.encodeDimension(HalfFloatPoint.sortableShortToHalfFloat((short) cursor.longValue()), buf, 0);
            builder.setBinary(doc, ref);
        }
        return builder.finish(source.docCount());
    }

    public static EscfColumnData toSortableLongColumn(
        EscfColumn source,
        NumberFieldMapper.NumberType type,
        boolean coerce,
        Recycler<BytesRef> recycler
    ) {
        return toSortableLongColumn(source, type, coerce, recycler, null, false);
    }

    /**
     * @param rejectDroppedValues whether to throw rather than let a source slot produce no output value. The
     *     offsets sidecar needs one ordinal per slot, so a dropped slot has nothing to point at. Callers that
     *     emit a sidecar pass {@code true} to fall the chunk back to the row path instead.
     */
    public static EscfColumnData toSortableLongColumn(
        EscfColumn source,
        NumberFieldMapper.NumberType type,
        boolean coerce,
        Recycler<BytesRef> recycler,
        Long nullReplacement,
        boolean rejectDroppedValues
    ) {
        return switch (source.kind()) {
            case EscfColumnKind.LONG -> fromLong(source, type, recycler);
            case EscfColumnKind.DOUBLE -> fromDouble(source, type, coerce, recycler);
            case EscfColumnKind.STRING -> fromString(source, type, coerce, recycler, nullReplacement, rejectDroppedValues);
            case EscfColumnKind.ARRAY -> fromArray(source, type, coerce, recycler, nullReplacement, rejectDroppedValues);
            default -> throw new UnsupportedOperationException(
                "toSortableLongColumn: unsupported ESCF column kind ["
                    + EscfColumnKind.name(source.kind())
                    + "] — only LONG, DOUBLE, STRING, and ARRAY are supported"
            );
        };
    }

    private static EscfColumnData fromArray(
        EscfColumn source,
        NumberFieldMapper.NumberType type,
        boolean coerce,
        Recycler<BytesRef> recycler,
        Long nullReplacement,
        boolean rejectDroppedValues
    ) {
        // Materialize the array structure: offsets + child data. The child is always dense (all
        // elements present — absent rows are represented by an empty offset range, not a child gap).
        EscfColumnData sourceData = source.toColumnData();
        EscfColumnData childData = sourceData.child();
        EscfColumn child = EscfColumn.from(childData);
        return switch (child.kind()) {
            case EscfColumnKind.STRING -> fromString(source, type, coerce, recycler, nullReplacement, rejectDroppedValues);
            case EscfColumnKind.LONG -> EscfColumnData.ofArray(
                sourceData.docCount(),
                sourceData.validity(),
                sourceData.offsets(),
                fromLong(child, type, recycler)
            );
            case EscfColumnKind.DOUBLE -> EscfColumnData.ofArray(
                sourceData.docCount(),
                sourceData.validity(),
                sourceData.offsets(),
                fromDouble(child, type, coerce, recycler)
            );
            default -> throw new UnsupportedOperationException(
                "toSortableLongColumn: ARRAY child kind ["
                    + EscfColumnKind.name(child.kind())
                    + "] is not supported — child must be LONG, DOUBLE, or STRING"
            );
        };
    }

    private static EscfColumnData fromString(
        EscfColumn source,
        NumberFieldMapper.NumberType type,
        boolean coerce,
        Recycler<BytesRef> recycler,
        Long nullReplacement,
        boolean rejectDroppedValues
    ) {
        AbstractXContentParser.checkCoerceString(coerce, classForType(type));
        EscfColumnBuilder builder = newLongBuilder(recycler);
        try {
            // retainValues=false: each value is parsed inside the loop body, before the cursor advances.
            ObjectTupleCursor<BytesRef> cursor = source.bytesRefCursor(false);
            final long min = integerMinForType(type);
            final long max = integerMaxForType(type);
            final long[] scratch = new long[1];
            for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                BytesRef value = cursor.value();
                if (coerce && value.length == 0) {
                    if (nullReplacement != null) {
                        builder.setLong(doc, nullReplacement);
                    } else if (rejectDroppedValues) {
                        throw new UnsupportedOperationException(
                            "toSortableLongColumn: an empty string with no null_value has no output value, which a positional sidecar "
                                + "cannot represent"
                        );
                    }
                    continue;
                }
                builder.setLong(doc, stringToSortableLong(value, type, min, max, scratch));
            }
            return builder.finish(source.docCount());
        } finally {
            builder.discard();
        }
    }

    /**
     * Parses a UTF-8 {@link BytesRef} into the sortable-long encoding for {@code type}: integer
     * types try the ASCII fast path first, float/double go straight to the string slow path.
     */
    private static long stringToSortableLong(BytesRef ref, NumberFieldMapper.NumberType type, long min, long max, long[] scratch) {
        if (ref.length > AbstractXContentParser.MAX_NUMERIC_STRING_LENGTH) {
            throw new IllegalArgumentException(
                "Numeric value length ["
                    + ref.length
                    + "] exceeds the maximum of ["
                    + AbstractXContentParser.MAX_NUMERIC_STRING_LENGTH
                    + "]"
            );
        }
        return switch (type) {
            case FLOAT, HALF_FLOAT, DOUBLE -> stringSlowPath(ref, type);
            case BYTE, SHORT, INTEGER, LONG -> tryParseAsciiLong(ref, min, max, scratch) ? scratch[0] : stringSlowPath(ref, type);
        };
    }

    /** Lower bound for the integer ASCII fast path; float/double are unused (they take the slow path). */
    private static long integerMinForType(NumberFieldMapper.NumberType type) {
        return switch (type) {
            case BYTE -> Byte.MIN_VALUE;
            case SHORT -> Short.MIN_VALUE;
            case INTEGER -> Integer.MIN_VALUE;
            case LONG, FLOAT, HALF_FLOAT, DOUBLE -> Long.MIN_VALUE;
        };
    }

    /** Upper bound for the integer ASCII fast path; float/double are unused (they take the slow path). */
    private static long integerMaxForType(NumberFieldMapper.NumberType type) {
        return switch (type) {
            case BYTE -> Byte.MAX_VALUE;
            case SHORT -> Short.MAX_VALUE;
            case INTEGER -> Integer.MAX_VALUE;
            case LONG, FLOAT, HALF_FLOAT, DOUBLE -> Long.MAX_VALUE;
        };
    }

    /** Slow-path String fallback; separate to avoid inlining cold code into the hot path. */
    private static long stringSlowPath(BytesRef ref, NumberFieldMapper.NumberType type) {
        String s = ref.utf8ToString();
        return switch (type) {
            case LONG -> AbstractXContentParser.toLong(s, true);
            case INTEGER -> AbstractXContentParser.parseInt(s);
            case SHORT -> AbstractXContentParser.parseShort(s);
            case BYTE -> {
                // BYTE indexes via parser.intValue, so the coerce-rejection class is Integer.
                int intVal = AbstractXContentParser.parseInt(s);
                if (intVal < Byte.MIN_VALUE || intVal > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + intVal + "] is out of range for a byte");
                }
                yield intVal;
            }
            case FLOAT -> doubleToFloatSortable(Float.parseFloat(s));
            case HALF_FLOAT -> toValidatedHalfFloat(Float.parseFloat(s));
            case DOUBLE -> {
                double d = Double.parseDouble(s);
                if (Double.isFinite(d) == false) {
                    throw new IllegalArgumentException("[double] supports only finite values, but got [" + d + "]");
                }
                yield NumericUtils.doubleToSortableLong(d);
            }
        };
    }

    /**
     * Parses a plain ASCII decimal integer from {@code ref} into {@code out[0]} without allocating
     * a {@link String}. Accepts optional {@code '-'} then one or more ASCII digits, nothing else;
     * accumulates negatively to handle {@link Long#MIN_VALUE} without overflow. Returns {@code true}
     * on success; {@code false} if the bytes are not a plain integer or the value is outside
     * {@code [min, max]} — the caller then falls back to {@link String} parsing.
     */
    private static boolean tryParseAsciiLong(BytesRef ref, long min, long max, long[] out) {
        byte[] bytes = ref.bytes;
        int offset = ref.offset;
        int len = ref.length;
        if (len == 0) {
            return false;
        }
        int pos = offset;
        int end = offset + len;
        boolean negative = false;
        if (bytes[pos] == '-') {
            negative = true;
            pos++;
            if (pos == end) {
                return false; // bare '-' is not a number
            }
        }
        long negativeAcc = 0;
        while (pos < end) {
            int digit = bytes[pos] - '0';
            if (digit < 0 || digit > 9) {
                return false; // non-digit byte (e.g. '.', 'e', '+', whitespace)
            }
            if (negativeAcc < Long.MIN_VALUE / 10) {
                return false; // would overflow on next multiply
            }
            negativeAcc = negativeAcc * 10 - digit;
            if (negativeAcc > 0) {
                return false; // wrapped around (only possible for very large magnitudes)
            }
            pos++;
        }
        long value = negative ? negativeAcc : -negativeAcc;
        if (negative == false && negativeAcc == Long.MIN_VALUE) {
            return false; // -Long.MIN_VALUE overflows; narrower types can't represent it anyway
        }
        if (value < min || value > max) {
            return false; // out of target-type range → let String path produce the proper error message
        }
        out[0] = value;
        return true;
    }

    private static Class<? extends Number> classForType(NumberFieldMapper.NumberType type) {
        return switch (type) {
            case LONG -> Long.class;
            case INTEGER -> Integer.class;
            case SHORT -> Short.class;
            case BYTE -> Integer.class; // BYTE indexes via parser.intValue
            case FLOAT, HALF_FLOAT -> Float.class;
            case DOUBLE -> Double.class;
        };
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

    private static EscfColumnBuilder newBytesBuilder(Recycler<BytesRef> recycler) {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE, recycler);
        b.lockScalar(EscfColumnKind.BINARY);
        return b;
    }
}
