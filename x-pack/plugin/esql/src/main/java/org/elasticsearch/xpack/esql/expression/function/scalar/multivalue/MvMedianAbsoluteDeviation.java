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
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongSubtractExact;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.bigIntegerToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToBigInteger;

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
        description = "Converts a multivalued field into a single valued field containing the median absolute deviation.",
        examples = {
            @Example(file = "math", tag = "mv_median_absolute_deviation"),
            @Example(
                description = "If the field has an even number of values, "
                    + "the medians will be calculated as the average of the middle two values. "
                    + "If the column is not floating point, the average rounds *down*.",
                file = "math",
                tag = "mv_median_absolute_deviation_round_down"
            ) }
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

    @MvEvaluator(extraName = "Int", finish = "finishInts", ascending = "ascending", single = "single")
    static void process(Longs longs, int v) {
        if (longs.values.length < longs.count + 1) {
            longs.values = ArrayUtil.grow(longs.values, longs.count + 1);
        }
        longs.values[longs.count++] = v;
    }

    static int finishInts(Longs longs) {
        long median = longMedianOf(longs.values, longs.count);
        for (int i = 0; i < longs.count; i++) {
            long value = longs.values[i];
            // We know they were ints, so we can calculate differences within a long
            longs.values[i] = value > median ? Math.subtractExact(value, median) : Math.subtractExact(median, value);
        }
        int mad = Math.toIntExact(longMedianOf(longs.values, longs.count));
        longs.count = 0;
        return mad;
    }

    /**
     * If the values are ascending pick the middle value or average the two middle values together.
     */
    static int ascending(Longs longs, IntBlock values, int firstValue, int count) {
        if (longs.values.length < count) {
            longs.values = ArrayUtil.grow(longs.values, count);
        }
        int middle = firstValue + count / 2;
        long median = count % 2 == 1 ? values.getInt(middle) : avgWithoutOverflow(values.getInt(middle - 1), values.getInt(middle));
        for (int i = 0; i < count; i++) {
            long value = values.getInt(firstValue + i);
            longs.values[i] = value > median ? Math.subtractExact(value, median) : Math.subtractExact(median, value);
        }
        return Math.toIntExact(longMedianOf(longs.values, count));
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
        long median = longMedianOf(longs.values, longs.count);
        for (int i = 0; i < longs.count; i++) {
            long value = longs.values[i];
            // From here, this array contains unsigned longs
            longs.values[i] = unsignedDifference(value, median);
        }
        long mad = NumericUtils.asLongUnsigned(unsignedLongMedianOf(longs.values, longs.count));
        longs.count = 0;
        return mad;
    }

    /**
     * If the values are ascending pick the middle value or average the two middle values together.
     */
    static long ascending(Longs longs, LongBlock values, int firstValue, int count) {
        if (longs.values.length < count) {
            longs.values = ArrayUtil.grow(longs.values, count);
        }
        int middle = firstValue + count / 2;
        long median = count % 2 == 1 ? values.getLong(middle) : avgWithoutOverflow(values.getLong(middle - 1), values.getLong(middle));
        for (int i = 0; i < count; i++) {
            long value = values.getLong(firstValue + i);
            // From here, this array contains unsigned longs
            longs.values[i] = unsignedDifference(value, median);
        }
        return NumericUtils.asLongUnsigned(unsignedLongMedianOf(longs.values, count));
    }

    static long single(long value) {
        return 0L;
    }

    static long longMedianOf(long[] values, int count) {
        // TODO quickselect
        Arrays.sort(values, 0, count);
        int middle = count / 2;
        return count % 2 == 1 ? values[middle] : avgWithoutOverflow(values[middle - 1], values[middle]);
    }

    static class Doubles {
        public double[] values = new double[2];
        public int count;
    }

    @MvEvaluator(extraName = "Double", finish = "finish", single = "single")
    static void process(Doubles doubles, double v) {
        if (doubles.values.length < doubles.count + 1) {
            doubles.values = ArrayUtil.grow(doubles.values, doubles.count + 1);
        }
        doubles.values[doubles.count++] = v;
    }

    static double finish(Doubles doubles) {
        double median = doubleMedianOf(doubles.values, doubles.count);
        for (int i = 0; i < doubles.count; i++) {
            double value = doubles.values[i];
            // Double differences between median and the values may potentially result in +/-Infinity.
            // As we use that value just to sort, the MAD should remain finite.
            doubles.values[i] = value > median ? value - median : median - value;
        }
        double mad = doubleMedianOf(doubles.values, doubles.count);
        doubles.count = 0;
        return mad;
    }

    static double ascending(Doubles doubles, DoubleBlock values, int firstValue, int count) {
        if (doubles.values.length < count) {
            doubles.values = ArrayUtil.grow(doubles.values, count);
        }
        int middle = firstValue + count / 2;
        double median = count % 2 == 1 ? values.getDouble(middle) : (values.getDouble(middle - 1) + values.getDouble(middle)) / 2;
        for (int i = 0; i < count; i++) {
            double value = values.getDouble(firstValue + i);
            doubles.values[i] = value > median ? value - median : median - value;
            assert Double.isFinite(doubles.values[i]) : "Overflow on median differences";
        }
        return doubleMedianOf(doubles.values, count);
    }

    static double single(double value) {
        return 0.;
    }

    static double doubleMedianOf(double[] values, int count) {
        // TODO quickselect
        Arrays.sort(values, 0, count);
        int middle = count / 2;
        return count % 2 == 1 ? values[middle] : (values[middle - 1] / 2 + values[middle] / 2);
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
        long median = unsignedLongMedianOf(longs.values, longs.count);
        for (int i = 0; i < longs.count; i++) {
            long value = longs.values[i];
            longs.values[i] = value > median ? unsignedLongSubtractExact(value, median) : unsignedLongSubtractExact(median, value);
        }
        long mad = unsignedLongMedianOf(longs.values, longs.count);
        longs.count = 0;
        return mad;
    }

    /**
     * If the values are ascending pick the middle value or average the two middle values together.
     */
    static long ascendingUnsignedLong(Longs longs, LongBlock values, int firstValue, int count) {
        if (longs.values.length < count) {
            longs.values = ArrayUtil.grow(longs.values, count);
        }
        int middle = firstValue + count / 2;
        long median;
        if (count % 2 == 1) {
            median = values.getLong(middle);
        } else {
            BigInteger a = unsignedLongToBigInteger(values.getLong(middle - 1));
            BigInteger b = unsignedLongToBigInteger(values.getLong(middle));
            median = bigIntegerToUnsignedLong(a.add(b).shiftRight(1));
        }
        for (int i = 0; i < count; i++) {
            long value = values.getLong(firstValue + i);
            longs.values[i] = value > median ? unsignedLongSubtractExact(value, median) : unsignedLongSubtractExact(median, value);
        }
        return unsignedLongMedianOf(longs.values, count);
    }

    static long singleUnsignedLong(long value) {
        return NumericUtils.ZERO_AS_UNSIGNED_LONG;
    }

    static long unsignedLongMedianOf(long[] values, int count) {
        // TODO quickselect
        Arrays.sort(values, 0, count);
        int middle = count / 2;
        if (count % 2 == 1) {
            return values[middle];
        }
        BigInteger a = unsignedLongToBigInteger(values[middle - 1]);
        BigInteger b = unsignedLongToBigInteger(values[middle]);
        return bigIntegerToUnsignedLong(a.add(b).shiftRight(1));
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
