/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.MvEvaluator;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongSubtractExact;

/**
 * Reduce a multivalued field to a single valued field containing the median absolute deviation of the values.
 */
public class MvMedianAbsoluteDeviation extends AbstractMultivalueFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvMedianAbsoluteDeviation",
        MvMedianAbsoluteDeviation::new
    );

    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Converts a multivalued field into a single valued field containing the median absolute deviation."
            + "\n\n"
            + "It is calculated as the median of each data point’s deviation from the median of "
            + "the entire sample. That is, for a random variable `X`, the median absolute "
            + "deviation is `median(|median(X) - X|)`.",
        note = "If the field has an even number of values, "
            + "the medians will be calculated as the average of the middle two values. "
            + "If the value is not a floating point number, the averages are rounded towards 0.",
        examples = @Example(file = "mv_median_absolute_deviation", tag = "example")
    )
    public MvMedianAbsoluteDeviation(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Multivalue expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private MvMedianAbsoluteDeviation(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), t -> t.isNumeric() && isRepresentable(t), sourceText(), null, "numeric");
    }

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case DOUBLE -> new MvMedianAbsoluteDeviationDoubleEvaluator.Factory(fieldEval);
            case INT -> new MvMedianAbsoluteDeviationIntEvaluator.Factory(fieldEval);
            case LONG -> field().dataType() == DataType.UNSIGNED_LONG
                ? new MvMedianAbsoluteDeviationUnsignedLongEvaluator.Factory(fieldEval)
                : new MvMedianAbsoluteDeviationLongEvaluator.Factory(fieldEval);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMedianAbsoluteDeviation(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMedianAbsoluteDeviation::new, field());
    }

    static class Longs {
        public long[] values = new long[2];
        public int count;
    }

    /**
     * Evaluator for integers.
     * <p>
     *     To avoid integer overflows, we're using the {@link Longs} class to store the values.
     * </p>
     */
    @MvEvaluator(extraName = "Int", finish = "finishInts", ascending = "ascending", single = "single")
    static void process(Longs longs, int v) {
        if (longs.values.length < longs.count + 1) {
            longs.values = ArrayUtil.grow(longs.values, longs.count + 1);
        }
        longs.values[longs.count++] = v;
    }

    static int finishInts(Longs longs) {
        try {
            long median = longMedianOf(longs);
            for (int i = 0; i < longs.count; i++) {
                long value = longs.values[i];
                // We know they were ints, so we can calculate differences within a long
                longs.values[i] = value > median ? value - median : median - value;
            }
            return Math.toIntExact(longMedianOf(longs));
        } finally {
            longs.count = 0;
        }
    }

    /**
     * Similar to the code in `finish`, for when the values are in ascending order. The major differences are:
     * - As values are sorted, we don’t need to sort them for the first median calculation.
     * - We take the values directly from the block instead of from the helper object.
     */
    static int ascending(Longs longs, IntBlock values, int firstValue, int count) {
        try {
            if (longs.values.length < count) {
                longs.values = ArrayUtil.grow(longs.values, count);
            }
            longs.count = count;
            int middle = firstValue + count / 2;
            long median = count % 2 == 1 ? values.getInt(middle) : avgWithoutOverflow(values.getInt(middle - 1), values.getInt(middle));
            for (int i = 0; i < count; i++) {
                long value = values.getInt(firstValue + i);
                longs.values[i] = value > median ? value - median : median - value;
            }
            return Math.toIntExact(longMedianOf(longs));
        } finally {
            longs.count = 0;
        }
    }

    static int single(int value) {
        return 0;
    }

    @MvEvaluator(extraName = "Long", finish = "finish", ascending = "ascending", single = "single")
    static void process(Longs longs, long v) {
        if (longs.values.length < longs.count + 1) {
            longs.values = ArrayUtil.grow(longs.values, longs.count + 1);
        }
        longs.values[longs.count++] = v;
    }

    static long finish(Longs longs) {
        try {
            long median = longMedianOf(longs);
            for (int i = 0; i < longs.count; i++) {
                long value = longs.values[i];
                // From here, this array contains unsigned longs
                longs.values[i] = unsignedDifference(value, median);
            }
            return NumericUtils.unsignedLongAsLongExact(unsignedLongMedianOf(longs));
        } finally {
            longs.count = 0;
        }
    }

    /**
     * Similar to the code in `finish`, for when the values are in ascending order. The major differences are:
     * - As values are sorted, we don’t need to sort them for the first median calculation.
     * - We take the values directly from the block instead of from the helper object.
     */
    static long ascending(Longs longs, LongBlock values, int firstValue, int count) {
        try {
            if (longs.values.length < count) {
                longs.values = ArrayUtil.grow(longs.values, count);
            }
            longs.count = count;
            int middle = firstValue + count / 2;
            long median = count % 2 == 1 ? values.getLong(middle) : avgWithoutOverflow(values.getLong(middle - 1), values.getLong(middle));
            for (int i = 0; i < count; i++) {
                long value = values.getLong(firstValue + i);
                // From here, this array contains unsigned longs
                longs.values[i] = unsignedDifference(value, median);
            }
            return NumericUtils.unsignedLongAsLongExact(unsignedLongMedianOf(longs));
        } finally {
            longs.count = 0;
        }
    }

    static long single(long value) {
        return 0L;
    }

    static long longMedianOf(Longs longs) {
        // TODO quickselect
        Arrays.sort(longs.values, 0, longs.count);
        int middle = longs.count / 2;
        return longs.count % 2 == 1 ? longs.values[middle] : avgWithoutOverflow(longs.values[middle - 1], longs.values[middle]);
    }

    static class Doubles {
        public double[] values = new double[2];
        public int count;
    }

    @MvEvaluator(extraName = "Double", finish = "finish", ascending = "ascending", single = "single")
    static void process(Doubles doubles, double v) {
        if (doubles.values.length < doubles.count + 1) {
            doubles.values = ArrayUtil.grow(doubles.values, doubles.count + 1);
        }
        doubles.values[doubles.count++] = v;
    }

    static double finish(Doubles doubles) {
        try {
            double median = doubleMedianOf(doubles);
            for (int i = 0; i < doubles.count; i++) {
                double value = doubles.values[i];
                // Double differences between median and the values may potentially result in +/-Infinity.
                // As we use that value just to sort, the MAD should remain finite.
                doubles.values[i] = value > median ? value - median : median - value;
            }
            return doubleMedianOf(doubles);
        } finally {
            doubles.count = 0;
        }
    }

    /**
     * Similar to the code in `finish`, for when the values are in ascending order. The major differences are:
     * - As values are sorted, we don’t need to sort them for the first median calculation.
     * - We take the values directly from the block instead of from the helper object.
     */
    static double ascending(Doubles doubles, DoubleBlock values, int firstValue, int count) {
        try {
            if (doubles.values.length < count) {
                doubles.values = ArrayUtil.grow(doubles.values, count);
            }
            doubles.count = count;
            int middle = firstValue + count / 2;
            double median = count % 2 == 1 ? values.getDouble(middle) : (values.getDouble(middle - 1) / 2 + values.getDouble(middle) / 2);
            for (int i = 0; i < count; i++) {
                double value = values.getDouble(firstValue + i);
                // Double differences between median and the values may potentially result in +/-Infinity.
                // As we use that value just to sort, the MAD should remain finite.
                doubles.values[i] = value > median ? value - median : median - value;
            }
            return doubleMedianOf(doubles);
        } finally {
            doubles.count = 0;
        }
    }

    static double single(double value) {
        return 0.;
    }

    static double doubleMedianOf(Doubles doubles) {
        // TODO quickselect
        Arrays.sort(doubles.values, 0, doubles.count);
        int middle = doubles.count / 2;
        double median = doubles.count % 2 == 1 ? doubles.values[middle] : (doubles.values[middle - 1] / 2 + doubles.values[middle] / 2);
        return NumericUtils.asFiniteNumber(median);
    }

    @MvEvaluator(
        extraName = "UnsignedLong",
        finish = "finishUnsignedLong",
        ascending = "ascendingUnsignedLong",
        single = "singleUnsignedLong"
    )
    static void processUnsignedLong(Longs longs, long v) {
        process(longs, v);
    }

    static long finishUnsignedLong(Longs longs) {
        try {
            long median = unsignedLongMedianOf(longs);
            for (int i = 0; i < longs.count; i++) {
                long value = longs.values[i];
                longs.values[i] = value > median ? unsignedLongSubtractExact(value, median) : unsignedLongSubtractExact(median, value);
            }
            return unsignedLongMedianOf(longs);
        } finally {
            longs.count = 0;
        }
    }

    /**
     * Similar to the code in `finish`, for when the values are in ascending order. The major differences are:
     * - As values are sorted, we don’t need to sort them for the first median calculation.
     * - We take the values directly from the block instead of from the helper object.
     */
    static long ascendingUnsignedLong(Longs longs, LongBlock values, int firstValue, int count) {
        try {
            if (longs.values.length < count) {
                longs.values = ArrayUtil.grow(longs.values, count);
            }
            longs.count = count;
            int middle = firstValue + count / 2;
            long median;
            if (count % 2 == 1) {
                median = values.getLong(middle);
            } else {
                median = unsignedLongAvgWithoutOverflow(values.getLong(middle - 1), values.getLong(middle));
            }
            for (int i = 0; i < count; i++) {
                long value = values.getLong(firstValue + i);
                longs.values[i] = value > median ? unsignedLongSubtractExact(value, median) : unsignedLongSubtractExact(median, value);
            }
            return unsignedLongMedianOf(longs);
        } finally {
            longs.count = 0;
        }
    }

    static long singleUnsignedLong(long value) {
        return NumericUtils.ZERO_AS_UNSIGNED_LONG;
    }

    static long unsignedLongMedianOf(Longs longs) {
        // TODO quickselect
        Arrays.sort(longs.values, 0, longs.count);
        int middle = longs.count / 2;
        if (longs.count % 2 == 1) {
            return longs.values[middle];
        }
        return unsignedLongAvgWithoutOverflow(longs.values[middle - 1], longs.values[middle]);
    }

    // Utility methods

    /**
     * Average two {@code int}s together without overflow.
     */
    static int avgWithoutOverflow(int a, int b) {
        var value = (a & b) + ((a ^ b) >> 1);
        // This method rounds negative values down instead of towards zero, like (a + b) / 2 would.
        // Here we rectify up if the average is negative and the two values have different parities.
        return value < 0 && ((a & 1) ^ (b & 1)) == 1 ? value + 1 : value;
    }

    /**
     * Average two {@code long}s without any overflow.
     */
    static long avgWithoutOverflow(long a, long b) {
        var value = (a & b) + ((a ^ b) >> 1);
        // This method rounds negative values down instead of towards zero, like (a + b) / 2 would.
        // Here we rectify up if the average is negative and the two values have different parities.
        return value < 0 && ((a & 1) ^ (b & 1)) == 1 ? value + 1 : value;
    }

    /**
     * Average two {@code unsigned long}s without any overflow.
     */
    static long unsignedLongAvgWithoutOverflow(long a, long b) {
        return (a >> 1) + (b >> 1) + (a & b & 1);
    }

    /**
     * Returns the difference between two signed long values as an unsigned long.
     */
    static long unsignedDifference(long a, long b) {
        if (a >= b) {
            if (a < 0 || b >= 0) {
                return NumericUtils.asLongUnsigned(a - b);
            }

            return NumericUtils.unsignedLongSubtractExact(a, b);
        }

        // Same operations, but inverted

        if (b < 0 || a >= 0) {
            return NumericUtils.asLongUnsigned(b - a);
        }

        return NumericUtils.unsignedLongSubtractExact(b, a);
    }
}
