/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

public class MvPercentile extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvPercentile",
        MvPercentile::new
    );

    /**
     * 2^52 is the smallest integer where it and all smaller integers can be represented exactly as double
     */
    private static final double MAX_SAFE_LONG_DOUBLE = Double.longBitsToDouble(0x4330000000000000L);
    /**
     * Max double that can be used to calculate averages without overflowing
     */
    private static final double MAX_SAFE_DOUBLE = Double.MAX_VALUE / 4;

    private final Expression field;
    private final Expression percentile;

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "Converts a multivalued field into a single valued field containing "
            + "the value at which a certain percentage of observed values occur."
        /*examples = {
            @Example(file = "math", tag = "mv_median"),
            @Example(
                description = "If the row has an even number of values for a column, "
                    + "the result will be the average of the middle two entries. If the column is not floating point, "
                    + "the average rounds *down*:",
                file = "math",
                tag = "mv_median_round_down"
            ) }*/
    )
    public MvPercentile(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long" },
            description = "Multivalue expression."
        ) Expression field,
        @Param(name = "percentile", type = { "double", "integer", "long" }) Expression percentile
    ) {
        super(source, List.of(field, percentile));
        this.field = field;
        this.percentile = percentile;
    }

    private MvPercentile(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(percentile);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isType(field, dt -> dt.isNumeric() && dt != UNSIGNED_LONG, sourceText(), FIRST, "numeric except unsigned_long").and(
            isType(percentile, dt -> dt.isNumeric() && dt != UNSIGNED_LONG, sourceText(), SECOND, "numeric except unsigned_long")
        );
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    public final Expression field() {
        return field;
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public final ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var fieldEval = toEvaluator.apply(field);
        var percentileEval = toEvaluator.apply(percentile);

        return switch (PlannerUtils.toElementType(field.dataType())) {
            case INT -> switch (PlannerUtils.toElementType(percentile.dataType())) {
                case INT -> new MvPercentileIntegerIntegerEvaluator.Factory(source(), fieldEval, percentileEval);
                case LONG -> new MvPercentileIntegerLongEvaluator.Factory(source(), fieldEval, percentileEval);
                case DOUBLE -> new MvPercentileIntegerDoubleEvaluator.Factory(source(), fieldEval, percentileEval);
                default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
            };
            case LONG -> switch (PlannerUtils.toElementType(percentile.dataType())) {
                case INT -> new MvPercentileLongIntegerEvaluator.Factory(source(), fieldEval, percentileEval);
                case LONG -> new MvPercentileLongLongEvaluator.Factory(source(), fieldEval, percentileEval);
                case DOUBLE -> new MvPercentileLongDoubleEvaluator.Factory(source(), fieldEval, percentileEval);
                default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
            };
            case DOUBLE -> switch (PlannerUtils.toElementType(percentile.dataType())) {
                case INT -> new MvPercentileDoubleIntegerEvaluator.Factory(source(), fieldEval, percentileEval);
                case LONG -> new MvPercentileDoubleLongEvaluator.Factory(source(), fieldEval, percentileEval);
                case DOUBLE -> new MvPercentileDoubleDoubleEvaluator.Factory(source(), fieldEval, percentileEval);
                default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
            };
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvPercentile(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvPercentile::new, field, percentile);
    }

    @Evaluator(extraName = "DoubleInteger")
    static void process(DoubleBlock.Builder builder, int position, DoubleBlock values, int percentile) {
        processDoubles(builder, position, values, percentile);
    }

    @Evaluator(extraName = "DoubleLong")
    static void process(DoubleBlock.Builder builder, int position, DoubleBlock values, long percentile) {
        processDoubles(builder, position, values, percentile);
    }

    @Evaluator(extraName = "DoubleDouble")
    static void process(DoubleBlock.Builder builder, int position, DoubleBlock values, double percentile) {
        processDoubles(builder, position, values, percentile);
    }

    @Evaluator(extraName = "IntegerInteger")
    static void process(IntBlock.Builder builder, int position, IntBlock values, int percentile) {
        processInts(builder, position, values, percentile);
    }

    @Evaluator(extraName = "IntegerLong")
    static void process(IntBlock.Builder builder, int position, IntBlock values, long percentile) {
        processInts(builder, position, values, percentile);
    }

    @Evaluator(extraName = "IntegerDouble")
    static void process(IntBlock.Builder builder, int position, IntBlock values, double percentile) {
        processInts(builder, position, values, percentile);
    }

    @Evaluator(extraName = "LongInteger")
    static void process(LongBlock.Builder builder, int position, LongBlock values, int percentile) {
        processLongs(builder, position, values, percentile);
    }

    @Evaluator(extraName = "LongLong")
    static void process(LongBlock.Builder builder, int position, LongBlock values, long percentile) {
        processLongs(builder, position, values, percentile);
    }

    @Evaluator(extraName = "LongDouble")
    static void process(LongBlock.Builder builder, int position, LongBlock values, double percentile) {
        processLongs(builder, position, values, percentile);
    }

    private static void processDoubles(DoubleBlock.Builder builder, int position, DoubleBlock valuesBlock, double percentile) {
        int valueCount = valuesBlock.getValueCount(position);
        int firstValueIndex = valuesBlock.getFirstValueIndex(position);

        if (valueCount == 0 || percentile < 0 || percentile > 100) {
            builder.appendNull();
            return;
        }

        if (valueCount == 1) {
            builder.appendDouble(valuesBlock.getDouble(firstValueIndex));
            return;
        }

        var p = percentile / 100.0;
        var index = p * (valueCount - 1);
        var lowerIndex = (int) index;
        var upperIndex = lowerIndex + 1;
        var fraction = index - lowerIndex;

        if (valuesBlock.mvSortedAscending()) {
            if (percentile == 0) {
                builder.appendDouble(valuesBlock.getDouble(0));
            } else if (percentile == 100) {
                builder.appendDouble(valuesBlock.getDouble(valueCount - 1));
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                var result = calculateDoublePercentile(fraction, valuesBlock.getDouble(lowerIndex), valuesBlock.getDouble(upperIndex));
                builder.appendDouble(result);
            }
        }

        var values = new double[valueCount];

        for (int i = 0; i < valueCount; i++) {
            values[i] = valuesBlock.getDouble(firstValueIndex + i);
        }

        Arrays.sort(values);

        if (percentile == 0) {
            builder.appendDouble(values[0]);
        } else if (percentile == 100) {
            builder.appendDouble(values[valueCount - 1]);
        } else {
            assert lowerIndex >= 0 && upperIndex < valueCount;
            var result = calculateDoublePercentile(fraction, values[lowerIndex], values[upperIndex]);
            builder.appendDouble(result);
        }
    }

    private static void processInts(IntBlock.Builder builder, int position, IntBlock valuesBlock, double percentile) {
        int valueCount = valuesBlock.getValueCount(position);
        int firstValueIndex = valuesBlock.getFirstValueIndex(position);

        if (valueCount == 0 || percentile < 0 || percentile > 100) {
            builder.appendNull();
            return;
        }

        if (valueCount == 1) {
            builder.appendInt(valuesBlock.getInt(firstValueIndex));
            return;
        }

        var p = percentile / 100.0;
        var index = p * (valueCount - 1);
        var lowerIndex = (int) index;
        var upperIndex = lowerIndex + 1;
        var fraction = index - lowerIndex;

        if (valuesBlock.mvSortedAscending()) {
            if (percentile == 0) {
                builder.appendInt(valuesBlock.getInt(0));
            } else if (percentile == 100) {
                builder.appendInt(valuesBlock.getInt(valueCount - 1));
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                var lowerValue = valuesBlock.getInt(lowerIndex);
                var upperValue = valuesBlock.getInt(upperIndex);
                var difference = (long) upperValue - lowerValue;
                var percentileValue = lowerValue + (int) (fraction * difference);
                builder.appendInt(percentileValue);
            }
        }

        var values = new int[valueCount];

        for (int i = 0; i < valueCount; i++) {
            values[i] = valuesBlock.getInt(firstValueIndex + i);
        }

        Arrays.sort(values);

        if (percentile == 0) {
            builder.appendInt(values[0]);
        } else if (percentile == 100) {
            builder.appendInt(values[valueCount - 1]);
        } else {
            assert lowerIndex >= 0 && upperIndex < valueCount;
            var lowerValue = values[lowerIndex];
            var upperValue = values[upperIndex];
            var difference = (long) upperValue - lowerValue;
            var percentileValue = lowerValue + (int) (fraction * difference);
            builder.appendInt(percentileValue);
        }
    }

    private static void processLongs(LongBlock.Builder builder, int position, LongBlock valuesBlock, double percentile) {
        int valueCount = valuesBlock.getValueCount(position);
        int firstValueIndex = valuesBlock.getFirstValueIndex(position);

        if (valueCount == 0 || percentile < 0 || percentile > 100) {
            builder.appendNull();
            return;
        }

        if (valueCount == 1) {
            builder.appendLong(valuesBlock.getLong(firstValueIndex));
            return;
        }

        var p = percentile / 100.0;
        var index = p * (valueCount - 1);
        var lowerIndex = (int) index;
        var upperIndex = lowerIndex + 1;
        var fraction = index - lowerIndex;

        if (valuesBlock.mvSortedAscending()) {
            if (percentile == 0) {
                builder.appendLong(valuesBlock.getLong(0));
            } else if (percentile == 100) {
                builder.appendLong(valuesBlock.getLong(valueCount - 1));
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                var result = calculateLongPercentile(fraction, valuesBlock.getLong(lowerIndex), valuesBlock.getLong(upperIndex));
                builder.appendLong(result);
            }
        }

        var values = new long[valueCount];

        for (int i = 0; i < valueCount; i++) {
            values[i] = valuesBlock.getLong(firstValueIndex + i);
        }

        Arrays.sort(values);

        if (percentile == 0) {
            builder.appendLong(values[0]);
        } else if (percentile == 100) {
            builder.appendLong(values[valueCount - 1]);
        } else {
            assert lowerIndex >= 0 && upperIndex < valueCount;
            var result = calculateLongPercentile(fraction, values[lowerIndex], values[upperIndex]);
            builder.appendLong(result);
        }
    }

    /**
     * Calculates a percentile for a long avoiding overflows and double precision issues.
     * <p>
     *     To do that, if the values are over the limit of the representable double integers,
     *     it uses instead BigDecimals for the calculations.
     * </p>
     */
    private static long calculateLongPercentile(double fraction, long lowerValue, long upperValue) {
        if (upperValue < MAX_SAFE_LONG_DOUBLE && lowerValue > -MAX_SAFE_LONG_DOUBLE) {
            var difference = upperValue - lowerValue;
            return lowerValue + (long) (fraction * difference);
        }

        return calculateBigDecimalPercentile(fraction, new BigDecimal(lowerValue), new BigDecimal(upperValue)).longValue();
    }

    /**
     * Calculates a percentile for a double avoiding overflows.
     * <p>
     *     To do that, if the values are over a limit, it uses instead BigDecimals for the calculations.
     * </p>
     */
    private static double calculateDoublePercentile(double fraction, double lowerValue, double upperValue) {
        if (upperValue < MAX_SAFE_DOUBLE && lowerValue > -MAX_SAFE_DOUBLE) {
            var difference = upperValue - lowerValue;
            return lowerValue + fraction * difference;
        }

        return calculateBigDecimalPercentile(fraction, new BigDecimal(lowerValue), new BigDecimal(upperValue)).doubleValue();
    }

    private static BigDecimal calculateBigDecimalPercentile(double fraction, BigDecimal lowerValue, BigDecimal upperValue) {
        var difference = upperValue.subtract(lowerValue);
        var fractionBigDecimal = new BigDecimal(fraction);
        return lowerValue.add(fractionBigDecimal.multiply(difference));
    }
}
