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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
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

    private final Expression field;
    private final Expression percentile;

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "Converts a multivalued field into a single valued field containing "
            + "the value at which a certain percentage of observed values occur.",
        examples = @Example(file = "mv_percentile", tag = "example")
    )
    public MvPercentile(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }, description = "Multivalue expression.") Expression field,
        @Param(
            name = "percentile",
            type = { "double", "integer", "long" },
            description = "The percentile to calculate. Must be a number between 0 and 100. "
                + "Numbers out of range will return a null instead."
        ) Expression percentile
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
        return field.foldable() && percentile.foldable();
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
                case INT -> new MvPercentileIntegerIntegerEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new IntSortingScratch()
                );
                case LONG -> new MvPercentileIntegerLongEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new IntSortingScratch()
                );
                case DOUBLE -> new MvPercentileIntegerDoubleEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new IntSortingScratch()
                );
                default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
            };
            case LONG -> switch (PlannerUtils.toElementType(percentile.dataType())) {
                case INT -> new MvPercentileLongIntegerEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new LongSortingScratch()
                );
                case LONG -> new MvPercentileLongLongEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new LongSortingScratch()
                );
                case DOUBLE -> new MvPercentileLongDoubleEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new LongSortingScratch()
                );
                default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
            };
            case DOUBLE -> switch (PlannerUtils.toElementType(percentile.dataType())) {
                case INT -> new MvPercentileDoubleIntegerEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new DoubleSortingScratch()
                );
                case LONG -> new MvPercentileDoubleLongEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new DoubleSortingScratch()
                );
                case DOUBLE -> new MvPercentileDoubleDoubleEvaluator.Factory(
                    source(),
                    fieldEval,
                    percentileEval,
                    (d) -> new DoubleSortingScratch()
                );
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

    static class DoubleSortingScratch {
        private static final double[] EMPTY = new double[0];

        public double[] values = EMPTY;
    }

    static class IntSortingScratch {
        private static final int[] EMPTY = new int[0];

        public int[] values = EMPTY;
    }

    static class LongSortingScratch {
        private static final long[] EMPTY = new long[0];

        public long[] values = EMPTY;
    }

    // Evaluators

    @Evaluator(extraName = "DoubleInteger", warnExceptions = IllegalArgumentException.class)
    static void process(
        DoubleBlock.Builder builder,
        int position,
        DoubleBlock values,
        int percentile,
        @Fixed(includeInToString = false, build = true) DoubleSortingScratch scratch
    ) {
        processDoubles(builder, position, values, percentile, scratch);
    }

    @Evaluator(extraName = "DoubleLong", warnExceptions = IllegalArgumentException.class)
    static void process(
        DoubleBlock.Builder builder,
        int position,
        DoubleBlock values,
        long percentile,
        @Fixed(includeInToString = false, build = true) DoubleSortingScratch scratch
    ) {
        processDoubles(builder, position, values, percentile, scratch);
    }

    @Evaluator(extraName = "DoubleDouble", warnExceptions = IllegalArgumentException.class)
    static void process(
        DoubleBlock.Builder builder,
        int position,
        DoubleBlock values,
        double percentile,
        @Fixed(includeInToString = false, build = true) DoubleSortingScratch scratch
    ) {
        processDoubles(builder, position, values, percentile, scratch);
    }

    @Evaluator(extraName = "IntegerInteger", warnExceptions = IllegalArgumentException.class)
    static void process(
        IntBlock.Builder builder,
        int position,
        IntBlock values,
        int percentile,
        @Fixed(includeInToString = false, build = true) IntSortingScratch scratch
    ) {
        processInts(builder, position, values, percentile, scratch);
    }

    @Evaluator(extraName = "IntegerLong", warnExceptions = IllegalArgumentException.class)
    static void process(
        IntBlock.Builder builder,
        int position,
        IntBlock values,
        long percentile,
        @Fixed(includeInToString = false, build = true) IntSortingScratch scratch
    ) {
        processInts(builder, position, values, percentile, scratch);
    }

    @Evaluator(extraName = "IntegerDouble", warnExceptions = IllegalArgumentException.class)
    static void process(
        IntBlock.Builder builder,
        int position,
        IntBlock values,
        double percentile,
        @Fixed(includeInToString = false, build = true) IntSortingScratch scratch
    ) {
        processInts(builder, position, values, percentile, scratch);
    }

    @Evaluator(extraName = "LongInteger", warnExceptions = IllegalArgumentException.class)
    static void process(
        LongBlock.Builder builder,
        int position,
        LongBlock values,
        int percentile,
        @Fixed(includeInToString = false, build = true) LongSortingScratch scratch
    ) {
        processLongs(builder, position, values, percentile, scratch);
    }

    @Evaluator(extraName = "LongLong", warnExceptions = IllegalArgumentException.class)
    static void process(
        LongBlock.Builder builder,
        int position,
        LongBlock values,
        long percentile,
        @Fixed(includeInToString = false, build = true) LongSortingScratch scratch
    ) {
        processLongs(builder, position, values, percentile, scratch);
    }

    @Evaluator(extraName = "LongDouble", warnExceptions = IllegalArgumentException.class)
    static void process(
        LongBlock.Builder builder,
        int position,
        LongBlock values,
        double percentile,
        @Fixed(includeInToString = false, build = true) LongSortingScratch scratch
    ) {
        processLongs(builder, position, values, percentile, scratch);
    }

    // Per-field-type processors

    private static void processDoubles(
        DoubleBlock.Builder builder,
        int position,
        DoubleBlock valuesBlock,
        double percentile,
        DoubleSortingScratch scratch
    ) {
        int valueCount = valuesBlock.getValueCount(position);
        int firstValueIndex = valuesBlock.getFirstValueIndex(position);

        if (valueCount == 0) {
            builder.appendNull();
            return;
        }

        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("Percentile parameter must be a number between 0 and 100, found [" + percentile + "]");
        }

        builder.appendDouble(calculateDoublePercentile(valuesBlock, firstValueIndex, valueCount, percentile, scratch));
    }

    private static void processInts(
        IntBlock.Builder builder,
        int position,
        IntBlock valuesBlock,
        double percentile,
        IntSortingScratch scratch
    ) {
        int valueCount = valuesBlock.getValueCount(position);
        int firstValueIndex = valuesBlock.getFirstValueIndex(position);

        if (valueCount == 0) {
            builder.appendNull();
            return;
        }

        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("Percentile parameter must be a number between 0 and 100, found [" + percentile + "]");
        }

        builder.appendInt(calculateIntPercentile(valuesBlock, firstValueIndex, valueCount, percentile, scratch));
    }

    private static void processLongs(
        LongBlock.Builder builder,
        int position,
        LongBlock valuesBlock,
        double percentile,
        LongSortingScratch scratch
    ) {
        int valueCount = valuesBlock.getValueCount(position);
        int firstValueIndex = valuesBlock.getFirstValueIndex(position);

        if (valueCount == 0) {
            builder.appendNull();
            return;
        }

        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("Percentile parameter must be a number between 0 and 100, found [" + percentile + "]");
        }

        builder.appendLong(calculateLongPercentile(valuesBlock, firstValueIndex, valueCount, percentile, scratch));
    }

    // Percentile calculators

    private static double calculateDoublePercentile(
        DoubleBlock valuesBlock,
        int firstValueIndex,
        int valueCount,
        double percentile,
        DoubleSortingScratch scratch
    ) {
        if (valueCount == 1) {
            return valuesBlock.getDouble(firstValueIndex);
        }

        var p = percentile / 100.0;
        var index = p * (valueCount - 1);
        var lowerIndex = (int) index;
        var upperIndex = lowerIndex + 1;
        var fraction = index - lowerIndex;

        if (valuesBlock.mvSortedAscending()) {
            if (percentile == 0) {
                return valuesBlock.getDouble(0);
            } else if (percentile == 100) {
                return valuesBlock.getDouble(valueCount - 1);
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                return calculateDoublePercentile(fraction, valuesBlock.getDouble(lowerIndex), valuesBlock.getDouble(upperIndex));
            }
        }

        if (percentile == 0) {
            double min = Double.POSITIVE_INFINITY;
            for (int i = 0; i < valueCount; i++) {
                min = Math.min(min, valuesBlock.getDouble(firstValueIndex + i));
            }
            return min;
        } else if (percentile == 100) {
            double max = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < valueCount; i++) {
                max = Math.max(max, valuesBlock.getDouble(firstValueIndex + i));
            }
            return max;
        }

        if (scratch.values.length < valueCount) {
            scratch.values = new double[ArrayUtil.oversize(valueCount, Double.BYTES)];
        }

        for (int i = 0; i < valueCount; i++) {
            scratch.values[i] = valuesBlock.getDouble(firstValueIndex + i);
        }

        Arrays.sort(scratch.values, 0, valueCount);

        assert lowerIndex >= 0 && upperIndex < valueCount;
        return calculateDoublePercentile(fraction, scratch.values[lowerIndex], scratch.values[upperIndex]);
    }

    private static int calculateIntPercentile(
        IntBlock valuesBlock,
        int firstValueIndex,
        int valueCount,
        double percentile,
        IntSortingScratch scratch
    ) {
        if (valueCount == 1) {
            return valuesBlock.getInt(firstValueIndex);
        }

        var p = percentile / 100.0;
        var index = p * (valueCount - 1);
        var lowerIndex = (int) index;
        var upperIndex = lowerIndex + 1;
        var fraction = index - lowerIndex;

        if (valuesBlock.mvSortedAscending()) {
            if (percentile == 0) {
                return valuesBlock.getInt(0);
            } else if (percentile == 100) {
                return valuesBlock.getInt(valueCount - 1);
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                var lowerValue = valuesBlock.getInt(lowerIndex);
                var upperValue = valuesBlock.getInt(upperIndex);
                var difference = (long) upperValue - lowerValue;
                return lowerValue + (int) (fraction * difference);
            }
        }

        if (percentile == 0) {
            int min = Integer.MAX_VALUE;
            for (int i = 0; i < valueCount; i++) {
                min = Math.min(min, valuesBlock.getInt(firstValueIndex + i));
            }
            return min;
        } else if (percentile == 100) {
            int max = Integer.MIN_VALUE;
            for (int i = 0; i < valueCount; i++) {
                max = Math.max(max, valuesBlock.getInt(firstValueIndex + i));
            }
            return max;
        }

        if (scratch.values.length < valueCount) {
            scratch.values = new int[ArrayUtil.oversize(valueCount, Integer.BYTES)];
        }

        for (int i = 0; i < valueCount; i++) {
            scratch.values[i] = valuesBlock.getInt(firstValueIndex + i);
        }

        Arrays.sort(scratch.values, 0, valueCount);

        assert lowerIndex >= 0 && upperIndex < valueCount;
        var lowerValue = scratch.values[lowerIndex];
        var upperValue = scratch.values[upperIndex];
        var difference = (long) upperValue - lowerValue;
        return lowerValue + (int) (fraction * difference);
    }

    private static long calculateLongPercentile(
        LongBlock valuesBlock,
        int firstValueIndex,
        int valueCount,
        double percentile,
        LongSortingScratch scratch
    ) {
        if (valueCount == 1) {
            return valuesBlock.getLong(firstValueIndex);
        }

        var p = percentile / 100.0;
        var index = p * (valueCount - 1);
        var lowerIndex = (int) index;
        var upperIndex = lowerIndex + 1;
        var fraction = index - lowerIndex;

        if (valuesBlock.mvSortedAscending()) {
            if (percentile == 0) {
                return valuesBlock.getLong(0);
            } else if (percentile == 100) {
                return valuesBlock.getLong(valueCount - 1);
            } else {
                assert lowerIndex >= 0 && upperIndex < valueCount;
                return calculateLongPercentile(fraction, valuesBlock.getLong(lowerIndex), valuesBlock.getLong(upperIndex));
            }
        }

        if (percentile == 0) {
            long min = Long.MAX_VALUE;
            for (int i = 0; i < valueCount; i++) {
                min = Math.min(min, valuesBlock.getLong(firstValueIndex + i));
            }
            return min;
        } else if (percentile == 100) {
            long max = Long.MIN_VALUE;
            for (int i = 0; i < valueCount; i++) {
                max = Math.max(max, valuesBlock.getLong(firstValueIndex + i));
            }
            return max;
        }

        if (scratch.values.length < valueCount) {
            scratch.values = new long[ArrayUtil.oversize(valueCount, Long.BYTES)];
        }

        for (int i = 0; i < valueCount; i++) {
            scratch.values[i] = valuesBlock.getLong(firstValueIndex + i);
        }

        Arrays.sort(scratch.values, 0, valueCount);

        assert lowerIndex >= 0 && upperIndex < valueCount;
        return calculateLongPercentile(fraction, scratch.values[lowerIndex], scratch.values[upperIndex]);
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

        var lowerValueBigDecimal = new BigDecimal(lowerValue);
        var upperValueBigDecimal = new BigDecimal(upperValue);
        var difference = upperValueBigDecimal.subtract(lowerValueBigDecimal);
        var fractionBigDecimal = new BigDecimal(fraction);
        return lowerValueBigDecimal.add(fractionBigDecimal.multiply(difference)).longValue();
    }

    /**
     * Calculates a percentile for a double avoiding overflows.
     * <p>
     *     If the values are too separated (negative + positive), it uses a slightly different approach.
     *     This approach would fail if the values are big but not separated, so it's only used in this case.
     * </p>
     */
    private static double calculateDoublePercentile(double fraction, double lowerValue, double upperValue) {
        if (lowerValue < 0 && upperValue > 0) {
            // Order is required to avoid `upper - lower` overflows
            return (lowerValue + fraction * upperValue) - fraction * lowerValue;
        }

        var difference = upperValue - lowerValue;
        return lowerValue + fraction * difference;
    }
}
