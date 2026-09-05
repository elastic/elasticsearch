/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

public class NumberColumnTransformTests extends ESTestCase {

    private static EscfBatch encode(String... jsonDocs) throws IOException {
        List<BytesReference> sources = java.util.Arrays.stream(jsonDocs).map(s -> (BytesReference) new BytesArray(s)).toList();
        return EscfEncoder.encode(sources, XContentType.JSON);
    }

    private static EscfColumn column(EscfBatch batch, String field) {
        for (int i = 0; i < batch.schema().leafCount(); i++) {
            if (batch.schema().getFullPath(i).equals(field)) {
                return batch.column(i);
            }
        }
        throw new AssertionError("field [" + field + "] not found in batch");
    }

    /**
     * Reads all present-row sortable-long values from an {@link EscfColumnData} (must be LONG kind)
     * via a {@link LuceneLongColumn} cursor. Absent rows are recorded as {@link Long#MIN_VALUE} as a
     * sentinel (the actual test never uses out-of-range values that would collide with this).
     */
    private static long[] readValues(EscfColumnData data, int docCount) {
        LuceneLongColumn col = LuceneLongColumn.of(data, "_test", SortedNumericDocValuesField.TYPE, LongColumn.NumericKind.LONG);
        long[] result = new long[docCount];
        java.util.Arrays.fill(result, Long.MIN_VALUE);
        LongTupleCursor cursor = col.tuples();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            result[doc] = cursor.longValue();
        }
        return result;
    }

    public void testLongToLong_noop() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 5}", "{\"f\": 100}", "{\"f\": -3}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.LONG,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 3);
            assertEquals(5L, vals[0]);
            assertEquals(100L, vals[1]);
            assertEquals(-3L, vals[2]);
        }
    }

    public void testLongToLong_sparse() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 7}", "{}", "{\"f\": 42}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.LONG,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 3);
            assertEquals(7L, vals[0]);
            assertEquals(Long.MIN_VALUE, vals[1]); // absent
            assertEquals(42L, vals[2]);
        }
    }

    public void testLongToInteger_validationOnly_inRange() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 0}", "{\"f\": 2147483647}", "{\"f\": -2147483648}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.INTEGER,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 3);
            assertEquals(0L, vals[0]);
            assertEquals((long) Integer.MAX_VALUE, vals[1]);
            assertEquals((long) Integer.MIN_VALUE, vals[2]);
        }
    }

    public void testLongToInteger_outOfRange_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 2147483648}")) {
            EscfColumn src = column(batch, "f");
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.INTEGER, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
            assertTrue(ex.getMessage().contains("2147483648"));
        }
    }

    public void testLongToShort_validationOnly_inRange() throws IOException {
        try (EscfBatch batch = encode("{\"f\": -1}", "{\"f\": 32767}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.SHORT,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 2);
            assertEquals(-1L, vals[0]);
            assertEquals(32767L, vals[1]);
        }
    }

    public void testLongToShort_outOfRange_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 32768}")) {
            EscfColumn src = column(batch, "f");
            expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.SHORT, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
        }
    }

    public void testLongToByte_validationOnly_inRange() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 0}", "{\"f\": 127}", "{\"f\": -128}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.BYTE,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 3);
            assertEquals(0L, vals[0]);
            assertEquals(127L, vals[1]);
            assertEquals(-128L, vals[2]);
        }
    }

    public void testLongToByte_outOfRange_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 300}")) {
            EscfColumn src = column(batch, "f");
            expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.BYTE, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
        }
    }

    public void testLongToFloat_conversion() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 5}", "{\"f\": -100}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.FLOAT,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 2);
            assertEquals((long) NumericUtils.floatToSortableInt(5f), vals[0]);
            assertEquals((long) NumericUtils.floatToSortableInt(-100f), vals[1]);
        }
    }

    public void testLongToDouble_conversion() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 5}", "{\"f\": -100}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.DOUBLE,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 2);
            assertEquals(NumericUtils.doubleToSortableLong(5.0), vals[0]);
            assertEquals(NumericUtils.doubleToSortableLong(-100.0), vals[1]);
        }
    }

    public void testDoubleToDouble_conversion() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 1.5}", "{\"f\": -2.25}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.DOUBLE,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 2);
            assertEquals(NumericUtils.doubleToSortableLong(1.5), vals[0]);
            assertEquals(NumericUtils.doubleToSortableLong(-2.25), vals[1]);
        }
    }

    public void testDoubleToDouble_sparse() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 1.5}", "{}", "{\"f\": 3.0}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.DOUBLE,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 3);
            assertEquals(NumericUtils.doubleToSortableLong(1.5), vals[0]);
            assertEquals(Long.MIN_VALUE, vals[1]); // absent
            assertEquals(NumericUtils.doubleToSortableLong(3.0), vals[2]);
        }
    }

    public void testDoubleToFloat_conversion() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 1.5}", "{\"f\": -2.25}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.FLOAT,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 2);
            assertEquals((long) NumericUtils.floatToSortableInt(1.5f), vals[0]);
            assertEquals((long) NumericUtils.floatToSortableInt(-2.25f), vals[1]);
        }
    }

    public void testDoubleToLong_wholeValue() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 3.0}", "{\"f\": -7.0}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.LONG,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 2);
            assertEquals(3L, vals[0]);
            assertEquals(-7L, vals[1]);
        }
    }

    public void testDoubleToLong_fractional_coerceTrue_truncates() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 3.5}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.LONG,
                true,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 1);
            assertEquals(3L, vals[0]);
        }
    }

    public void testDoubleToLong_fractional_coerceFalse_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 3.5}")) {
            EscfColumn src = column(batch, "f");
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.LONG, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
            assertTrue(ex.getMessage().contains("decimal part") || ex.getMessage().contains("3.5"));
        }
    }

    public void testDoubleToInteger_fractional_coerceFalse_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 1.5}")) {
            EscfColumn src = column(batch, "f");
            expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.INTEGER, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
        }
    }

    public void testDoubleToInteger_outOfRange_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 2.0E10}")) {
            EscfColumn src = column(batch, "f");
            expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.INTEGER, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
        }
    }

    public void testDoubleToInteger_wholeValue() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 5.0}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.INTEGER,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] vals = readValues(out, 1);
            assertEquals(5L, vals[0]);
        }
    }

    public void testDoubleToByte_outOfRange_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 200.0}")) {
            EscfColumn src = column(batch, "f");
            expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.BYTE, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
        }
    }

    /**
     * For LONG-source columns, verifies that the transform's output long equals the production row-path
     * oracle {@code NumberType.toSortableLong(NumberType.parse(originalLong, false))} for each
     * combination of target type and value. This directly validates the float/double sortable-encoding
     * conversions (floatToSortableInt, doubleToSortableLong) as well as the integer no-op paths.
     */
    public void testOracleComparison_longSource() throws IOException {
        long[] values = { 0L, 1L, -1L, 127L, -128L, 32767L, -32768L, Integer.MAX_VALUE, Integer.MIN_VALUE, 1_000_000L, -1_000_000L };
        NumberType[] types = {
            NumberType.LONG,
            NumberType.BYTE,
            NumberType.SHORT,
            NumberType.INTEGER,
            NumberType.FLOAT,
            NumberType.DOUBLE,
            NumberType.HALF_FLOAT };

        for (NumberType type : types) {
            // Filter to values in range for integer types
            long[] inRange = java.util.Arrays.stream(values).filter(l -> isLongInRange(l, type)).toArray();

            String[] docs = java.util.Arrays.stream(inRange).mapToObj(l -> "{\"f\": " + l + "}").toArray(String[]::new);
            try (EscfBatch batch = encode(docs)) {
                EscfColumn src = column(batch, "f");
                EscfColumnData out = NumberColumnTransform.toSortableLongColumn(src, type, false, BytesRefRecycler.NON_RECYCLING_INSTANCE);
                long[] actual = readValues(out, inRange.length);

                for (int i = 0; i < inRange.length; i++) {
                    long l = inRange[i];
                    long expected = type.toSortableLong(type.parse(l, false));
                    assertEquals("LONG→" + type + " value=" + l + ": transform output differs from row-path oracle", expected, actual[i]);
                }
            }
        }
    }

    /**
     * For DOUBLE-source columns (decimal JSON tokens), verifies that the transform's output long equals
     * {@code NumberType.toSortableLong(NumberType.parse(originalDouble, coerce))} for each target type.
     * Covers both the no-decimal-part (coerce-independent) and the double→double bit-op cases.
     */
    public void testOracleComparison_doubleSource() throws IOException {
        double[] values = { 0.0, 1.5, -1.5, 2.25, -2.25, 1.0E10, -1.0E10, 1.0, -1.0, 127.0, -128.0 };
        NumberType[] intTypes = { NumberType.BYTE, NumberType.SHORT, NumberType.INTEGER, NumberType.LONG };

        // Integer targets: use only whole values, coerce=false
        double[] wholeValues = { 0.0, 1.0, -1.0, 127.0, -128.0 };
        for (NumberType type : intTypes) {
            double[] inRange = java.util.Arrays.stream(wholeValues).filter(d -> isDoubleInRange(d, type)).toArray();
            String[] docs = java.util.Arrays.stream(inRange).mapToObj(d -> "{\"f\": " + d + "}").toArray(String[]::new);
            try (EscfBatch batch = encode(docs)) {
                EscfColumn src = column(batch, "f");
                EscfColumnData out = NumberColumnTransform.toSortableLongColumn(src, type, false, BytesRefRecycler.NON_RECYCLING_INSTANCE);
                long[] actual = readValues(out, inRange.length);

                for (int i = 0; i < inRange.length; i++) {
                    double d = inRange[i];
                    long expected = type.toSortableLong(type.parse(d, false));
                    assertEquals("DOUBLE→" + type + " value=" + d + ": transform differs from oracle", expected, actual[i]);
                }
            }
        }

        // Float/double/half_float targets: filter to values in range for each type
        for (NumberType type : new NumberType[] { NumberType.FLOAT, NumberType.DOUBLE, NumberType.HALF_FLOAT }) {
            double[] inRange = java.util.Arrays.stream(values).filter(d -> isDoubleInRange(d, type)).toArray();
            String[] docs = java.util.Arrays.stream(inRange).mapToObj(d -> "{\"f\": " + d + "}").toArray(String[]::new);
            try (EscfBatch batch = encode(docs)) {
                EscfColumn src = column(batch, "f");
                EscfColumnData out = NumberColumnTransform.toSortableLongColumn(src, type, false, BytesRefRecycler.NON_RECYCLING_INSTANCE);
                long[] actual = readValues(out, inRange.length);

                for (int i = 0; i < inRange.length; i++) {
                    double d = inRange[i];
                    long expected = type.toSortableLong(type.parse(d, false));
                    assertEquals("DOUBLE→" + type + " value=" + d + ": transform differs from oracle", expected, actual[i]);
                }
            }
        }
    }

    /**
     * LONG-to-long is a zero-copy no-op: the returned {@link EscfColumnData} must share the source
     * column's backing {@link org.elasticsearch.common.bytes.BytesReference}.
     */
    public void testZeroCopy_longToLong() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 1}", "{\"f\": 2}", "{\"f\": 3}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.LONG,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            // toColumnData() wraps the same BytesReference each time; assert the backing buffer is shared.
            assertSame("LONG-to-long must reuse the source buffer (zero-copy)", src.toColumnData().data(), out.data());
        }
    }

    /**
     * LONG-to-byte/short/int with in-range values: after the range-validation scan, the result must
     * reuse the source buffer — no new column is written.
     */
    public void testZeroCopy_longToNarrowInt_inRange() throws IOException {
        for (NumberType type : new NumberType[] { NumberType.BYTE, NumberType.SHORT, NumberType.INTEGER }) {
            try (EscfBatch batch = encode("{\"f\": 1}", "{\"f\": -1}", "{\"f\": 0}")) {
                EscfColumn src = column(batch, "f");
                EscfColumnData out = NumberColumnTransform.toSortableLongColumn(src, type, false, BytesRefRecycler.NON_RECYCLING_INSTANCE);
                assertSame("LONG-to-" + type + " in-range must reuse the source buffer (zero-copy)", src.toColumnData().data(), out.data());
            }
        }
    }

    /** A double value beyond float range must throw when the field is mapped as float. */
    public void testDoubleToFloat_overflow_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 1.0E300}")) {
            EscfColumn src = column(batch, "f");
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.FLOAT, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
            assertTrue("expected 'finite values' in message but got: " + ex.getMessage(), ex.getMessage().contains("finite values"));
        }
    }

    /**
     * For negative doubles, {@code sortableDoubleBits(rawBits)} must equal
     * {@code doubleToSortableLong(d)} (the branch-free bit-op must handle the sign-flip correctly).
     */
    public void testDoubleToDouble_negativeAndBoundaryValues() throws IOException {
        double[] values = { -0.0, -1.5, Double.MIN_VALUE, -Double.MIN_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, Math.PI, -Math.PI };
        String[] docs = java.util.Arrays.stream(values).mapToObj(d -> "{\"f\": " + d + "}").toArray(String[]::new);
        try (EscfBatch batch = encode(docs)) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.DOUBLE,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] actual = readValues(out, values.length);
            for (int i = 0; i < values.length; i++) {
                long expected = NumericUtils.doubleToSortableLong(values[i]);
                assertEquals("sortableDoubleBits for " + values[i], expected, actual[i]);
            }
        }
    }

    public void testLongToHalfFloat_conversion() throws IOException {
        // Values well within half-float range (max finite half-float is 65504)
        try (EscfBatch batch = encode("{\"f\": 0}", "{\"f\": 1}", "{\"f\": -1}", "{\"f\": 100}", "{\"f\": -32768}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.HALF_FLOAT,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] actual = readValues(out, 5);
            long[] expected = {
                HalfFloatPoint.halfFloatToSortableShort(0f),
                HalfFloatPoint.halfFloatToSortableShort(1f),
                HalfFloatPoint.halfFloatToSortableShort(-1f),
                HalfFloatPoint.halfFloatToSortableShort(100f),
                HalfFloatPoint.halfFloatToSortableShort(-32768f) };
            assertArrayEquals(expected, actual);
        }
    }

    public void testLongToHalfFloat_outOfRange_throws() throws IOException {
        // 100000 > 65504 (max representable half-float), so (float) 100000 round-trips through half-float as Infinity
        try (EscfBatch batch = encode("{\"f\": 100000}")) {
            EscfColumn src = column(batch, "f");
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.HALF_FLOAT, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
            assertTrue("expected 'finite values' in message but got: " + ex.getMessage(), ex.getMessage().contains("finite values"));
        }
    }

    public void testDoubleToHalfFloat_conversion() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 0.0}", "{\"f\": 1.5}", "{\"f\": -2.25}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.HALF_FLOAT,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[] actual = readValues(out, 3);
            long[] expected = {
                HalfFloatPoint.halfFloatToSortableShort(0f),
                HalfFloatPoint.halfFloatToSortableShort(1.5f),
                HalfFloatPoint.halfFloatToSortableShort(-2.25f) };
            assertArrayEquals(expected, actual);
        }
    }

    public void testDoubleToHalfFloat_outOfRange_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\": 1.0E300}")) {
            EscfColumn src = column(batch, "f");
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.HALF_FLOAT, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
            assertTrue("expected 'finite values' in message but got: " + ex.getMessage(), ex.getMessage().contains("finite values"));
        }
    }

    /**
     * Reads per-doc element arrays from a LONG or ARRAY kind {@link EscfColumnData}. Elements are
     * returned in source order. Absent docs appear as {@code null}.
     */
    private static long[][] readArrayValues(EscfColumnData data, int docCount) {
        LuceneLongColumn col = LuceneLongColumn.of(data, "_test", SortedNumericDocValuesField.TYPE, LongColumn.NumericKind.LONG);
        int[] counts = new int[docCount];
        LongTupleCursor c1 = col.tuples();
        for (int doc = c1.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = c1.nextDoc()) {
            counts[doc]++;
        }
        long[][] result = new long[docCount][];
        for (int doc = 0; doc < docCount; doc++) {
            if (counts[doc] > 0) result[doc] = new long[counts[doc]];
        }
        int[] idx = new int[docCount];
        LongTupleCursor c2 = col.tuples();
        for (int doc = c2.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = c2.nextDoc()) {
            result[doc][idx[doc]++] = c2.longValue();
        }
        return result;
    }

    /** Integer-token arrays: LONG child, long target type — no-op transform per element. */
    public void testLongArray_longType() throws IOException {
        try (EscfBatch batch = encode("{\"f\":[1,2,3]}", "{}", "{\"f\":[-9223372036854775808,9223372036854775807]}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.LONG,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[][] vals = readArrayValues(out, 3);
            assertArrayEquals(new long[] { 1L, 2L, 3L }, vals[0]);
            assertNull(vals[1]);
            assertArrayEquals(new long[] { Long.MIN_VALUE, Long.MAX_VALUE }, vals[2]);
        }
    }

    /** Integer-token array with float target: each element goes through {@code floatToSortableInt}. */
    public void testLongArray_floatType() throws IOException {
        try (EscfBatch batch = encode("{\"f\":[5,10,-3]}", "{}", "{\"f\":[0]}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.FLOAT,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[][] vals = readArrayValues(out, 3);
            assertArrayEquals(
                new long[] {
                    NumericUtils.floatToSortableInt(5f),
                    NumericUtils.floatToSortableInt(10f),
                    NumericUtils.floatToSortableInt(-3f) },
                vals[0]
            );
            assertNull(vals[1]);
            assertArrayEquals(new long[] { NumericUtils.floatToSortableInt(0f) }, vals[2]);
        }
    }

    /** Decimal-token arrays: DOUBLE child, double target type — sortableDoubleBits per element. */
    public void testDoubleArray_doubleType() throws IOException {
        try (EscfBatch batch = encode("{\"f\":[1.5,2.25,-3.5]}", "{}", "{\"f\":[0.75]}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.DOUBLE,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[][] vals = readArrayValues(out, 3);
            assertArrayEquals(
                new long[] {
                    NumericUtils.doubleToSortableLong(1.5),
                    NumericUtils.doubleToSortableLong(2.25),
                    NumericUtils.doubleToSortableLong(-3.5) },
                vals[0]
            );
            assertNull(vals[1]);
            assertArrayEquals(new long[] { NumericUtils.doubleToSortableLong(0.75) }, vals[2]);
        }
    }

    /** Decimal-token array with float target: double cast to float, then {@code floatToSortableInt}. */
    public void testDoubleArray_floatType() throws IOException {
        try (EscfBatch batch = encode("{\"f\":[1.5,-2.25]}", "{\"f\":[0.5,2.5]}")) {
            EscfColumn src = column(batch, "f");
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
                src,
                NumberType.FLOAT,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            );
            long[][] vals = readArrayValues(out, 2);
            assertArrayEquals(new long[] { NumericUtils.floatToSortableInt(1.5f), NumericUtils.floatToSortableInt(-2.25f) }, vals[0]);
            assertArrayEquals(new long[] { NumericUtils.floatToSortableInt(0.5f), NumericUtils.floatToSortableInt(2.5f) }, vals[1]);
        }
    }

    /** An out-of-range element inside an array throws the same as the scalar path. */
    public void testLongArray_outOfRange_throws() throws IOException {
        try (EscfBatch batch = encode("{\"f\":[-128,0,128]}")) {
            EscfColumn src = column(batch, "f");
            expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(src, NumberType.BYTE, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
        }
    }

    // =================== STRING column tests ===================

    /**
     * Builds a scalar STRING {@link EscfColumnData} where each non-null entry in {@code values}
     * becomes a present row and each null entry becomes an absent row. The string bytes are stored
     * verbatim as UTF-8.
     */
    private static EscfColumnData stringColumnData(String... values) {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE, BytesRefRecycler.NON_RECYCLING_INSTANCE);
        b.lockScalar(EscfColumnKind.STRING);
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                b.setString(i, new BytesRef(values[i]));
            }
        }
        return b.finish(values.length);
    }

    /**
     * Builds an ARRAY-of-STRING {@link EscfColumnData}. Each non-null entry in {@code rows} is an
     * array of strings (possibly empty); null entries are absent rows.
     */
    private static EscfColumnData stringArrayColumnData(String[]... rows) {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE, BytesRefRecycler.NON_RECYCLING_INSTANCE);
        for (int doc = 0; doc < rows.length; doc++) {
            if (rows[doc] != null) {
                b.beginArray(doc);
                for (String s : rows[doc]) {
                    b.appendString(new BytesRef(s));
                }
                b.endArray();
            }
        }
        return b.finish(rows.length);
    }

    /** Happy path: integer strings parse to the expected sortable-long for each integer type. */
    public void testStringToLong_integerTypes_happyPath() {
        NumberType[] intTypes = { NumberType.LONG, NumberType.INTEGER, NumberType.SHORT, NumberType.BYTE };
        long[] values = { 0L, 1L, -1L, 42L, 127L, -128L };
        for (NumberType type : intTypes) {
            long[] inRange = java.util.Arrays.stream(values).filter(v -> isLongInRange(v, type)).toArray();
            String[] strs = java.util.Arrays.stream(inRange).mapToObj(Long::toString).toArray(String[]::new);
            EscfColumnData src = stringColumnData(strs);
            EscfColumn col = EscfColumn.from(src);
            EscfColumnData out = NumberColumnTransform.toSortableLongColumn(col, type, true, BytesRefRecycler.NON_RECYCLING_INSTANCE);
            long[] actual = readValues(out, inRange.length);
            for (int i = 0; i < inRange.length; i++) {
                assertEquals(type + " string \"" + strs[i] + "\"", inRange[i], actual[i]);
            }
        }
    }

    /** Absent rows in a STRING column propagate to the output as absent rows. */
    public void testStringToLong_absentRows() {
        EscfColumnData src = stringColumnData("10", null, "30");
        EscfColumn col = EscfColumn.from(src);
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            col,
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[] vals = readValues(out, 3);
        assertEquals(10L, vals[0]);
        assertEquals(Long.MIN_VALUE, vals[1]); // absent sentinel
        assertEquals(30L, vals[2]);
    }

    /**
     * ASCII fast-path equivalence: strings that look like plain integers (fast path) and strings
     * that require the fallback (scientific notation, decimal) must produce the same result for LONG.
     */
    public void testStringToLong_fastPathAndFallbackEquivalent() {
        // "1000" → fast path; "1e3" → fallback; both should equal 1000L
        EscfColumnData fast = stringColumnData("1000");
        EscfColumnData slow = stringColumnData("1e3");
        long fastVal = readValues(
            NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(fast),
                NumberType.LONG,
                true,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            ),
            1
        )[0];
        long slowVal = readValues(
            NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(slow),
                NumberType.LONG,
                true,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            ),
            1
        )[0];
        assertEquals("fast-path and fallback must agree for \"1000\" vs \"1e3\"", fastVal, slowVal);
    }

    /** LONG string: decimal with coerce=true truncates (matches AbstractXContentParser.toLong). */
    public void testStringToLong_decimal_coerceTrue_truncates() {
        EscfColumnData src = stringColumnData("1.9");
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        assertEquals(1L, readValues(out, 1)[0]);
    }

    /** LONG string: large BigDecimal values truncate toward zero when coerce=true. */
    public void testStringToLong_bigDecimal_coerceTrue_truncates() {
        EscfColumnData src = stringColumnData("1234567890123456789.9", "-1234567890123456789.9");
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[] vals = readValues(out, 2);
        assertEquals(1234567890123456789L, vals[0]);
        assertEquals(-1234567890123456789L, vals[1]);
    }

    /** INTEGER string: BigDecimal values truncate when coerce=true, matching parser.intValue. */
    public void testStringToInteger_bigDecimal_coerceTrue_truncates() {
        EscfColumnData src = stringColumnData("123.9", "-123.9");
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.INTEGER,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[] vals = readValues(out, 2);
        assertEquals(123L, vals[0]);
        assertEquals(-123L, vals[1]);
    }

    public void testStringToLong_bigIntegerOutOfRange_throws() {
        EscfColumnData src = stringColumnData("9223372036854775808");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(src),
                NumberType.LONG,
                true,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            )
        );
        assertTrue("expected out-of-range message but got: " + ex.getMessage(), ex.getMessage().contains("out of range for a long"));
    }

    public void testStringToInteger_bigIntegerOutOfRange_throws() {
        EscfColumnData src = stringColumnData("2147483648");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(src),
                NumberType.INTEGER,
                true,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            )
        );
        assertTrue("expected out-of-range message but got: " + ex.getMessage(), ex.getMessage().contains("out of range for an integer"));
    }

    /** Empty strings with coerce=true and no null_value become absent values. */
    public void testStringToLong_emptyString_coerceTrue_becomesAbsent() {
        EscfColumnData src = stringColumnData("10", "", "30");
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[] vals = readValues(out, 3);
        assertEquals(10L, vals[0]);
        assertEquals(Long.MIN_VALUE, vals[1]);
        assertEquals(30L, vals[2]);
    }

    /** Empty strings with coerce=true use the mapper null_value when one is configured. */
    public void testStringToLong_emptyString_coerceTrue_usesNullValue() {
        EscfColumnData src = stringColumnData("10", "", "30");
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            99L,
            false
        );
        long[] vals = readValues(out, 3);
        assertEquals(10L, vals[0]);
        assertEquals(99L, vals[1]);
        assertEquals(30L, vals[2]);
    }

    public void testStringToLong_emptyString_coerceFalse_throws() {
        EscfColumnData src = stringColumnData("");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(src),
                NumberType.LONG,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            )
        );
        assertTrue("expected coerce message but got: " + ex.getMessage(), ex.getMessage().contains("Long value passed as String"));
    }

    /** LONG string: decimal with coerce=false throws "has a decimal part". */
    public void testStringToLong_decimal_coerceFalse_throws() {
        EscfColumnData src = stringColumnData("1.9");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(src),
                NumberType.LONG,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            )
        );
        assertTrue("expected coerce message but got: " + ex.getMessage(), ex.getMessage().contains("Long value passed as String"));
    }

    /** Any string with coerce=false throws "X value passed as String" (where X is the type name). */
    public void testStringToAny_coerceFalse_throws() {
        NumberType[] types = NumberType.values();
        for (NumberType type : types) {
            EscfColumnData src = stringColumnData("42");
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(EscfColumn.from(src), type, false, BytesRefRecycler.NON_RECYCLING_INSTANCE)
            );
            assertTrue(
                type + ": expected 'value passed as String' in message but got: " + ex.getMessage(),
                ex.getMessage().contains("value passed as String")
            );
        }
    }

    /**
     * BYTE uses Integer as the coerce-check class (mirrors {@code parser.intValue} in the row path),
     * so the rejection message must say "Integer value passed as String".
     */
    public void testStringToByte_coerceFalse_throwsIntegerMessage() {
        EscfColumnData src = stringColumnData("5");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(src),
                NumberType.BYTE,
                false,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            )
        );
        assertTrue("expected 'Integer value passed as String' but got: " + ex.getMessage(), ex.getMessage().contains("Integer"));
    }

    /** Out-of-range string for BYTE throws. */
    public void testStringToByte_outOfRange_throws() {
        EscfColumnData src = stringColumnData("128");
        expectThrows(
            IllegalArgumentException.class,
            () -> NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(src),
                NumberType.BYTE,
                true,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            )
        );
    }

    /** Out-of-range string for SHORT throws. */
    public void testStringToShort_outOfRange_throws() {
        EscfColumnData src = stringColumnData("70000");
        expectThrows(
            IllegalArgumentException.class,
            () -> NumberColumnTransform.toSortableLongColumn(
                EscfColumn.from(src),
                NumberType.SHORT,
                true,
                BytesRefRecycler.NON_RECYCLING_INSTANCE
            )
        );
    }

    /** Float string produces the sortable-int encoding matching the row-path oracle. */
    public void testStringToFloat_happyPath() {
        EscfColumnData src = stringColumnData("1.5", "-2.25");
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.FLOAT,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[] vals = readValues(out, 2);
        assertEquals((long) NumericUtils.floatToSortableInt(1.5f), vals[0]);
        assertEquals((long) NumericUtils.floatToSortableInt(-2.25f), vals[1]);
    }

    /** Double string produces the sortable-long encoding. */
    public void testStringToDouble_happyPath() {
        EscfColumnData src = stringColumnData("1.5", "-2.25", "0.0");
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.DOUBLE,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[] vals = readValues(out, 3);
        assertEquals(NumericUtils.doubleToSortableLong(1.5), vals[0]);
        assertEquals(NumericUtils.doubleToSortableLong(-2.25), vals[1]);
        assertEquals(NumericUtils.doubleToSortableLong(0.0), vals[2]);
    }

    /** Non-finite float strings throw for FLOAT. */
    public void testStringToFloat_nonFinite_throws() {
        for (String nonFinite : new String[] { "Infinity", "-Infinity", "NaN" }) {
            EscfColumnData src = stringColumnData(nonFinite);
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(
                    EscfColumn.from(src),
                    NumberType.FLOAT,
                    true,
                    BytesRefRecycler.NON_RECYCLING_INSTANCE
                )
            );
            assertTrue("expected 'finite values' in message but got: " + ex.getMessage(), ex.getMessage().contains("finite values"));
        }
    }

    /** Non-finite double strings throw for DOUBLE. */
    public void testStringToDouble_nonFinite_throws() {
        for (String nonFinite : new String[] { "Infinity", "-Infinity", "NaN" }) {
            EscfColumnData src = stringColumnData(nonFinite);
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(
                    EscfColumn.from(src),
                    NumberType.DOUBLE,
                    true,
                    BytesRefRecycler.NON_RECYCLING_INSTANCE
                )
            );
            assertTrue(
                "DOUBLE \"" + nonFinite + "\": expected 'finite values' in message but got: " + ex.getMessage(),
                ex.getMessage().contains("finite values")
            );
        }
    }

    /** Non-finite half_float strings throw for HALF_FLOAT. */
    public void testStringToHalfFloat_nonFinite_throws() {
        for (String nonFinite : new String[] { "Infinity", "-Infinity", "NaN" }) {
            EscfColumnData src = stringColumnData(nonFinite);
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> NumberColumnTransform.toSortableLongColumn(
                    EscfColumn.from(src),
                    NumberType.HALF_FLOAT,
                    true,
                    BytesRefRecycler.NON_RECYCLING_INSTANCE
                )
            );
            assertTrue(
                "HALF_FLOAT \"" + nonFinite + "\": expected 'finite values' in message but got: " + ex.getMessage(),
                ex.getMessage().contains("finite values")
            );
        }
    }

    /**
     * ARRAY-of-STRING: elements are parsed to the correct sortable-long per-element. Absent docs
     * produce null in the output. Element-order and row offsets are preserved.
     */
    public void testStringArray_longType() {
        EscfColumnData src = stringArrayColumnData(new String[] { "1", "2", "3" }, null, new String[] { "-100", "42" });
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[][] vals = readArrayValues(out, 3);
        assertArrayEquals(new long[] { 1L, 2L, 3L }, vals[0]);
        assertNull(vals[1]);
        assertArrayEquals(new long[] { -100L, 42L }, vals[2]);
    }

    /** ARRAY-of-STRING for a float type: each element is parsed and encoded via floatToSortableInt. */
    public void testStringArray_floatType() {
        EscfColumnData src = stringArrayColumnData(new String[] { "1.5", "-2.25" }, new String[] { "0.5" });
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.FLOAT,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[][] vals = readArrayValues(out, 2);
        assertArrayEquals(new long[] { NumericUtils.floatToSortableInt(1.5f), NumericUtils.floatToSortableInt(-2.25f) }, vals[0]);
        assertArrayEquals(new long[] { NumericUtils.floatToSortableInt(0.5f) }, vals[1]);
    }

    /** ARRAY-of-STRING: empty elements are dropped when coerce=true and no null_value is configured. */
    public void testStringArray_emptyString_coerceTrue_compactsOffsets() {
        EscfColumnData src = stringArrayColumnData(new String[] { "1", "", "3" }, new String[] { "", "" }, new String[] { "4" });
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[][] vals = readArrayValues(out, 3);
        assertArrayEquals(new long[] { 1L, 3L }, vals[0]);
        assertNull(vals[1]);
        assertArrayEquals(new long[] { 4L }, vals[2]);
    }

    /** ARRAY-of-STRING: empty elements use null_value when configured. */
    public void testStringArray_emptyString_coerceTrue_usesNullValue() {
        EscfColumnData src = stringArrayColumnData(new String[] { "1", "", "3" }, new String[] { "" });
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            99L,
            false
        );
        long[][] vals = readArrayValues(out, 2);
        assertArrayEquals(new long[] { 1L, 99L, 3L }, vals[0]);
        assertArrayEquals(new long[] { 99L }, vals[1]);
    }

    /** ARRAY-of-STRING: BigDecimal elements truncate per element when coerce=true. */
    public void testStringArray_bigDecimal_coerceTrue_truncates() {
        EscfColumnData src = stringArrayColumnData(new String[] { "1.9", "-2.9" });
        EscfColumnData out = NumberColumnTransform.toSortableLongColumn(
            EscfColumn.from(src),
            NumberType.LONG,
            true,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        long[][] vals = readArrayValues(out, 1);
        assertArrayEquals(new long[] { 1L, -2L }, vals[0]);
    }

    private static boolean isLongInRange(long l, NumberType type) {
        return switch (type) {
            case BYTE -> l >= Byte.MIN_VALUE && l <= Byte.MAX_VALUE;
            case SHORT -> l >= Short.MIN_VALUE && l <= Short.MAX_VALUE;
            case INTEGER -> l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE;
            case LONG, FLOAT, DOUBLE -> true;
            // Half-float max finite value is 65504; use round-trip check for exactness
            case HALF_FLOAT -> Float.isFinite(HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort((float) l)));
        };
    }

    private static boolean isDoubleInRange(double d, NumberType type) {
        return switch (type) {
            case BYTE -> d >= Byte.MIN_VALUE && d <= Byte.MAX_VALUE;
            case SHORT -> d >= Short.MIN_VALUE && d <= Short.MAX_VALUE;
            case INTEGER -> d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE;
            case LONG -> d >= Long.MIN_VALUE && d <= Long.MAX_VALUE;
            case FLOAT, DOUBLE -> true;
            case HALF_FLOAT -> Float.isFinite(HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort((float) d)));
        };
    }
}
