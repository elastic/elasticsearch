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
            doubles.values[i] = value > median ? value - median : median - value;
            assert Double.isFinite(doubles.values[i]) : "Overflow on median differences";
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

    static class Longs {
        public long[] values = new long[2];
        public int count;
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
            longs.values[i] = value > median ? Math.subtractExact(value, median) : Math.subtractExact(median, value);
        }
        long mad = longMedianOf(longs.values, longs.count);
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
            longs.values[i] = value > median ? Math.subtractExact(value, median) : Math.subtractExact(median, value);
        }
        return longMedianOf(longs.values, count);
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

    /**
     * Average two {@code long}s without any overflow.
     */
    static long avgWithoutOverflow(long a, long b) {
        return (a & b) + ((a ^ b) >> 1);
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

    static class Ints {
        public int[] values = new int[2];
        public int count;
    }

    @MvEvaluator(extraName = "Int", finish = "finish", ascending = "ascending", single = "single")
    static void process(Ints ints, int v) {
        if (ints.values.length < ints.count + 1) {
            ints.values = ArrayUtil.grow(ints.values, ints.count + 1);
        }
        ints.values[ints.count++] = v;
    }

    static int finish(Ints ints) {
        int median = intMedianOf(ints.values, ints.count);
        for (int i = 0; i < ints.count; i++) {
            int value = ints.values[i];
            ints.values[i] = value > median ? Math.subtractExact(value, median) : Math.subtractExact(median, value);
        }
        int mad = intMedianOf(ints.values, ints.count);
        ints.count = 0;
        return mad;
    }

    /**
     * If the values are ascending pick the middle value or average the two middle values together.
     */
    static int ascending(Ints ints, IntBlock values, int firstValue, int count) {
        if (ints.values.length < count) {
            ints.values = ArrayUtil.grow(ints.values, count);
        }
        int middle = firstValue + count / 2;
        int median = count % 2 == 1 ? values.getInt(middle) : avgWithoutOverflow(values.getInt(middle - 1), values.getInt(middle));
        for (int i = 0; i < count; i++) {
            int value = values.getInt(firstValue + i);
            ints.values[i] = value > median ? Math.subtractExact(value, median) : Math.subtractExact(median, value);
        }
        return intMedianOf(ints.values, count);
    }

    static int single(int value) {
        return 0;
    }

    static int intMedianOf(int[] values, int count) {
        // TODO quickselect
        Arrays.sort(values, 0, count);
        int middle = count / 2;
        return count % 2 == 1 ? values[middle] : avgWithoutOverflow(values[middle - 1], values[middle]);
    }

    /**
     * Average two {@code int}s together without overflow.
     */
    static int avgWithoutOverflow(int a, int b) {
        return (a & b) + ((a ^ b) >> 1);
    }
}
