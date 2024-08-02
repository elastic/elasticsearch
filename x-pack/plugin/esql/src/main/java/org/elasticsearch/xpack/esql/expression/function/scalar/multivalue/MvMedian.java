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
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.bigIntegerToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToBigInteger;

/**
 * Reduce a multivalued field to a single valued field containing the average value.
 */
public class MvMedian extends AbstractMultivalueFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvMedian", MvMedian::new);

    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Converts a multivalued field into a single valued field containing the median value.",
        examples = {
            @Example(file = "math", tag = "mv_median"),
            @Example(
                description = "If the row has an even number of values for a column, "
                    + "the result will be the average of the middle two entries. If the column is not floating point, "
                    + "the average rounds *down*:",
                file = "math",
                tag = "mv_median_round_down"
            ) }
    )
    public MvMedian(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Multivalue expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private MvMedian(StreamInput in) throws IOException {
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
            case DOUBLE -> new MvMedianDoubleEvaluator.Factory(fieldEval);
            case INT -> new MvMedianIntEvaluator.Factory(fieldEval);
            case LONG -> field().dataType() == DataType.UNSIGNED_LONG
                ? new MvMedianUnsignedLongEvaluator.Factory(fieldEval)
                : new MvMedianLongEvaluator.Factory(fieldEval);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMedian(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMedian::new, field());
    }

    static class Doubles {
        public double[] values = new double[2];
        public int count;
    }

    @MvEvaluator(extraName = "Double", finish = "finish")
    static void process(Doubles doubles, double v) {
        if (doubles.values.length < doubles.count + 1) {
            doubles.values = ArrayUtil.grow(doubles.values, doubles.count + 1);
        }
        doubles.values[doubles.count++] = v;
    }

    static double finish(Doubles doubles) {
        // TODO quickselect
        Arrays.sort(doubles.values, 0, doubles.count);
        int middle = doubles.count / 2;
        double median = doubles.count % 2 == 1 ? doubles.values[middle] : (doubles.values[middle - 1] + doubles.values[middle]) / 2;
        doubles.count = 0;
        return median;
    }

    static double ascending(DoubleBlock values, int firstValue, int count) {
        int middle = firstValue + count / 2;
        if (count % 2 == 1) {
            return values.getDouble(middle);
        }
        return (values.getDouble(middle - 1) + values.getDouble(middle)) / 2;
    }

    static class Longs {
        public long[] values = new long[2];
        public int count;
    }

    @MvEvaluator(extraName = "Long", finish = "finish", ascending = "ascending")
    static void process(Longs longs, long v) {
        if (longs.values.length < longs.count + 1) {
            longs.values = ArrayUtil.grow(longs.values, longs.count + 1);
        }
        longs.values[longs.count++] = v;
    }

    static long finish(Longs longs) {
        // TODO quickselect
        Arrays.sort(longs.values, 0, longs.count);
        int middle = longs.count / 2;
        if (longs.count % 2 == 1) {
            longs.count = 0;
            return longs.values[middle];
        }
        longs.count = 0;
        return avgWithoutOverflow(longs.values[middle - 1], longs.values[middle]);
    }

    /**
     * If the values are ascending pick the middle value or average the two middle values together.
     */
    static long ascending(LongBlock values, int firstValue, int count) {
        int middle = firstValue + count / 2;
        if (count % 2 == 1) {
            return values.getLong(middle);
        }
        return avgWithoutOverflow(values.getLong(middle - 1), values.getLong(middle));
    }

    /**
     * Average two {@code long}s without any overflow.
     */
    static long avgWithoutOverflow(long a, long b) {
        return (a & b) + ((a ^ b) >> 1);
    }

    @MvEvaluator(extraName = "UnsignedLong", finish = "finishUnsignedLong", ascending = "ascendingUnsignedLong")
    static void processUnsignedLong(Longs longs, long v) {
        process(longs, v);
    }

    static long finishUnsignedLong(Longs longs) {
        if (longs.count % 2 == 1) {
            return finish(longs);
        }
        // TODO quickselect
        Arrays.sort(longs.values, 0, longs.count);
        int middle = longs.count / 2;
        longs.count = 0;
        BigInteger a = unsignedLongToBigInteger(longs.values[middle - 1]);
        BigInteger b = unsignedLongToBigInteger(longs.values[middle]);
        return bigIntegerToUnsignedLong(a.add(b).shiftRight(1));
    }

    /**
     * If the values are ascending pick the middle value or average the two middle values together.
     */
    static long ascendingUnsignedLong(LongBlock values, int firstValue, int count) {
        int middle = firstValue + count / 2;
        if (count % 2 == 1) {
            return values.getLong(middle);
        }
        BigInteger a = unsignedLongToBigInteger(values.getLong(middle - 1));
        BigInteger b = unsignedLongToBigInteger(values.getLong(middle));
        return bigIntegerToUnsignedLong(a.add(b).shiftRight(1));
    }

    static class Ints {
        public int[] values = new int[2];
        public int count;
    }

    @MvEvaluator(extraName = "Int", finish = "finish", ascending = "ascending")
    static void process(Ints ints, int v) {
        if (ints.values.length < ints.count + 1) {
            ints.values = ArrayUtil.grow(ints.values, ints.count + 1);
        }
        ints.values[ints.count++] = v;
    }

    static int finish(Ints ints) {
        // TODO quickselect
        Arrays.sort(ints.values, 0, ints.count);
        int middle = ints.count / 2;
        if (ints.count % 2 == 1) {
            ints.count = 0;
            return ints.values[middle];
        }
        ints.count = 0;
        return avgWithoutOverflow(ints.values[middle - 1], ints.values[middle]);
    }

    /**
     * If the values are ascending pick the middle value or average the two middle values together.
     */
    static int ascending(IntBlock values, int firstValue, int count) {
        int middle = firstValue + count / 2;
        if (count % 2 == 1) {
            return values.getInt(middle);
        }
        return avgWithoutOverflow(values.getInt(middle - 1), values.getInt(middle));
    }

    /**
     * Average two {@code int}s together without overflow.
     */
    static int avgWithoutOverflow(int a, int b) {
        return (a & b) + ((a ^ b) >> 1);
    }
}
