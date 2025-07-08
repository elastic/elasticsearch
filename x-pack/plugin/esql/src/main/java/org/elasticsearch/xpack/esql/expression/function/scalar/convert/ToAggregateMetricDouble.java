/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlock;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

public class ToAggregateMetricDouble extends AbstractConvertFunction {

    private static final Map<DataType, AbstractConvertFunction.BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(AGGREGATE_METRIC_DOUBLE, (source, fieldEval) -> fieldEval),
        Map.entry(DOUBLE, DoubleFactory::new),
        Map.entry(INTEGER, IntFactory::new),
        Map.entry(LONG, LongFactory::new),
        Map.entry(UNSIGNED_LONG, UnsignedLongFactory::new)
    );

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToAggregateMetricDouble",
        ToAggregateMetricDouble::new
    );

    @FunctionInfo(
        returnType = "aggregate_metric_double",
        description = "Encode a numeric to an aggregate_metric_double.",
        examples = {
            @Example(file = "convert", tag = "toAggregateMetricDouble"),
            @Example(description = "The expression also accepts multi-values", file = "convert", tag = "toAggregateMetricDoubleMv") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.COMING) }
    )
    public ToAggregateMetricDouble(
        Source source,
        @Param(
            name = "number",
            type = { "double", "long", "unsigned_long", "integer", "aggregate_metric_double" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToAggregateMetricDouble(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(
            field,
            dt -> dt == DataType.AGGREGATE_METRIC_DOUBLE || dt == DataType.DOUBLE || dt == LONG || dt == INTEGER || dt == UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric or aggregate_metric_double"
        );
    }

    @Override
    public DataType dataType() {
        return AGGREGATE_METRIC_DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToAggregateMetricDouble(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToAggregateMetricDouble::new, field);
    }

    @Override
    protected Map<DataType, AbstractConvertFunction.BuildFactory> factories() {
        return EVALUATORS;
    }

    private static class AggregateMetricDoubleVectorBuilder implements Releasable {
        private final DoubleVector.FixedBuilder valuesBuilder;
        private final BlockFactory blockFactory;

        private AggregateMetricDoubleVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
            this.valuesBuilder = blockFactory.newDoubleVectorFixedBuilder(estimatedSize);
        }

        private void appendValue(double value) {
            valuesBuilder.appendDouble(value);
        }

        private Block build() {
            DoubleBlock doubleBlock = null;
            IntBlock countBlock = null;
            boolean success = false;
            try {
                doubleBlock = valuesBuilder.build().asBlock();
                countBlock = blockFactory.newConstantIntBlockWith(1, doubleBlock.getPositionCount());
                AggregateMetricDoubleBlock aggBlock = new AggregateMetricDoubleBlock(doubleBlock, doubleBlock, doubleBlock, countBlock);
                doubleBlock.incRef();
                doubleBlock.incRef();
                success = true;
                return aggBlock;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(doubleBlock, countBlock);
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(valuesBuilder);
        }
    }

    public static class DoubleFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;

        private final EvalOperator.ExpressionEvaluator.Factory fieldEvaluator;

        public DoubleFactory(Source source, EvalOperator.ExpressionEvaluator.Factory fieldEvaluator) {
            this.fieldEvaluator = fieldEvaluator;
            this.source = source;
        }

        @Override
        public String toString() {
            return "ToAggregateMetricDoubleFromDoubleEvaluator[" + "field=" + fieldEvaluator + "]";
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            final EvalOperator.ExpressionEvaluator eval = fieldEvaluator.get(context);

            return new EvalOperator.ExpressionEvaluator() {
                private Block evalBlock(Block block) {
                    int positionCount = block.getPositionCount();
                    DoubleBlock doubleBlock = (DoubleBlock) block;
                    try (
                        AggregateMetricDoubleBlockBuilder builder = context.blockFactory()
                            .newAggregateMetricDoubleBlockBuilder(positionCount)
                    ) {
                        CompensatedSum compensatedSum = new CompensatedSum();
                        for (int p = 0; p < positionCount; p++) {
                            int valueCount = doubleBlock.getValueCount(p);
                            if (valueCount == 0) {
                                builder.appendNull();
                                continue;
                            }
                            int start = doubleBlock.getFirstValueIndex(p);
                            int end = start + valueCount;
                            if (valueCount == 1) {
                                double current = doubleBlock.getDouble(start);
                                builder.min().appendDouble(current);
                                builder.max().appendDouble(current);
                                builder.sum().appendDouble(current);
                                builder.count().appendInt(valueCount);
                                continue;
                            }
                            double min = Double.POSITIVE_INFINITY;
                            double max = Double.NEGATIVE_INFINITY;
                            for (int i = start; i < end; i++) {
                                double current = doubleBlock.getDouble(i);
                                min = Math.min(min, current);
                                max = Math.max(max, current);
                                compensatedSum.add(current);
                            }
                            builder.min().appendDouble(min);
                            builder.max().appendDouble(max);
                            builder.sum().appendDouble(compensatedSum.value());
                            builder.count().appendInt(valueCount);
                            compensatedSum.reset(0, 0);
                        }
                        return builder.build();
                    }
                }

                private Block evalVector(Vector vector) {
                    int positionCount = vector.getPositionCount();
                    DoubleVector doubleVector = (DoubleVector) vector;
                    try (
                        AggregateMetricDoubleVectorBuilder builder = new AggregateMetricDoubleVectorBuilder(
                            positionCount,
                            context.blockFactory()
                        )
                    ) {
                        for (int p = 0; p < positionCount; p++) {
                            double value = doubleVector.getDouble(p);
                            builder.appendValue(value);
                        }
                        return builder.build();
                    }
                }

                @Override
                public Block eval(Page page) {
                    try (Block block = eval.eval(page)) {
                        Vector vector = block.asVector();
                        return vector == null ? evalBlock(block) : evalVector(vector);
                    }
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(eval);
                }

                @Override
                public String toString() {
                    return "ToAggregateMetricDoubleFromDoubleEvaluator[field=" + eval + "]";
                }
            };
        }
    }

    public static class IntFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;

        private final EvalOperator.ExpressionEvaluator.Factory fieldEvaluator;

        public IntFactory(Source source, EvalOperator.ExpressionEvaluator.Factory fieldEvaluator) {
            this.fieldEvaluator = fieldEvaluator;
            this.source = source;
        }

        @Override
        public String toString() {
            return "ToAggregateMetricDoubleFromIntEvaluator[" + "field=" + fieldEvaluator + "]";
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            final EvalOperator.ExpressionEvaluator eval = fieldEvaluator.get(context);

            return new EvalOperator.ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    try (Block block = eval.eval(page)) {
                        Vector vector = block.asVector();
                        return vector == null ? evalBlock(block) : evalVector(vector);
                    }
                }

                private Block evalBlock(Block block) {
                    int positionCount = block.getPositionCount();
                    IntBlock intBlock = (IntBlock) block;
                    try (
                        AggregateMetricDoubleBlockBuilder builder = context.blockFactory()
                            .newAggregateMetricDoubleBlockBuilder(positionCount)
                    ) {
                        CompensatedSum sum = new CompensatedSum();
                        for (int p = 0; p < positionCount; p++) {
                            int valueCount = intBlock.getValueCount(p);
                            int start = intBlock.getFirstValueIndex(p);
                            int end = start + valueCount;
                            if (valueCount == 0) {
                                builder.appendNull();
                                continue;
                            }
                            if (valueCount == 1) {
                                double current = intBlock.getInt(start);
                                builder.min().appendDouble(current);
                                builder.max().appendDouble(current);
                                builder.sum().appendDouble(current);
                                builder.count().appendInt(valueCount);
                                continue;
                            }
                            double min = Double.POSITIVE_INFINITY;
                            double max = Double.NEGATIVE_INFINITY;
                            for (int i = start; i < end; i++) {
                                double current = intBlock.getInt(i);
                                min = Math.min(min, current);
                                max = Math.max(max, current);
                                sum.add(current);
                            }
                            builder.min().appendDouble(min);
                            builder.max().appendDouble(max);
                            builder.sum().appendDouble(sum.value());
                            builder.count().appendInt(valueCount);
                            sum.reset(0, 0);
                        }
                        return builder.build();
                    }
                }

                private Block evalVector(Vector vector) {
                    int positionCount = vector.getPositionCount();
                    IntVector intVector = (IntVector) vector;
                    try (
                        AggregateMetricDoubleVectorBuilder builder = new AggregateMetricDoubleVectorBuilder(
                            positionCount,
                            context.blockFactory()
                        )
                    ) {
                        for (int p = 0; p < positionCount; p++) {
                            double value = intVector.getInt(p);
                            builder.appendValue(value);
                        }
                        return builder.build();
                    }
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(eval);
                }

                @Override
                public String toString() {
                    return "ToAggregateMetricDoubleFromIntEvaluator[field=" + eval + "]";
                }
            };
        }
    }

    public static class LongFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;

        private final EvalOperator.ExpressionEvaluator.Factory fieldEvaluator;

        public LongFactory(Source source, EvalOperator.ExpressionEvaluator.Factory fieldEvaluator) {
            this.fieldEvaluator = fieldEvaluator;
            this.source = source;
        }

        @Override
        public String toString() {
            return "ToAggregateMetricDoubleFromLongEvaluator[" + "field=" + fieldEvaluator + "]";
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            final EvalOperator.ExpressionEvaluator eval = fieldEvaluator.get(context);

            return new EvalOperator.ExpressionEvaluator() {
                private Block evalBlock(Block block) {
                    int positionCount = block.getPositionCount();
                    LongBlock longBlock = (LongBlock) block;
                    try (
                        AggregateMetricDoubleBlockBuilder builder = context.blockFactory()
                            .newAggregateMetricDoubleBlockBuilder(positionCount)
                    ) {
                        CompensatedSum sum = new CompensatedSum();
                        for (int p = 0; p < positionCount; p++) {
                            int valueCount = longBlock.getValueCount(p);
                            int start = longBlock.getFirstValueIndex(p);
                            int end = start + valueCount;
                            if (valueCount == 0) {
                                builder.appendNull();
                                continue;
                            }
                            if (valueCount == 1) {
                                double current = longBlock.getLong(start);
                                builder.min().appendDouble(current);
                                builder.max().appendDouble(current);
                                builder.sum().appendDouble(current);
                                builder.count().appendInt(valueCount);
                                continue;
                            }
                            double min = Double.POSITIVE_INFINITY;
                            double max = Double.NEGATIVE_INFINITY;
                            for (int i = start; i < end; i++) {
                                double current = longBlock.getLong(i);
                                min = Math.min(min, current);
                                max = Math.max(max, current);
                                sum.add(current);
                            }
                            builder.min().appendDouble(min);
                            builder.max().appendDouble(max);
                            builder.sum().appendDouble(sum.value());
                            builder.count().appendInt(valueCount);
                            sum.reset(0, 0);
                        }
                        return builder.build();
                    }
                }

                private Block evalVector(Vector vector) {
                    int positionCount = vector.getPositionCount();
                    LongVector longVector = (LongVector) vector;
                    try (
                        AggregateMetricDoubleVectorBuilder builder = new AggregateMetricDoubleVectorBuilder(
                            positionCount,
                            context.blockFactory()
                        )
                    ) {
                        for (int p = 0; p < positionCount; p++) {
                            double value = longVector.getLong(p);
                            builder.appendValue(value);
                        }
                        return builder.build();
                    }
                }

                @Override
                public Block eval(Page page) {
                    try (Block block = eval.eval(page)) {
                        Vector vector = block.asVector();
                        return vector == null ? evalBlock(block) : evalVector(vector);
                    }
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(eval);
                }

                @Override
                public String toString() {
                    return "ToAggregateMetricDoubleFromLongEvaluator[field=" + eval + "]";
                }
            };
        }
    }

    public static class UnsignedLongFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;

        private final EvalOperator.ExpressionEvaluator.Factory fieldEvaluator;

        public UnsignedLongFactory(Source source, EvalOperator.ExpressionEvaluator.Factory fieldEvaluator) {
            this.fieldEvaluator = fieldEvaluator;
            this.source = source;
        }

        @Override
        public String toString() {
            return "ToAggregateMetricDoubleFromUnsignedLongEvaluator[" + "field=" + fieldEvaluator + "]";
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            final EvalOperator.ExpressionEvaluator eval = fieldEvaluator.get(context);

            return new EvalOperator.ExpressionEvaluator() {
                private Block evalBlock(Block block) {
                    int positionCount = block.getPositionCount();
                    LongBlock longBlock = (LongBlock) block;
                    try (
                        AggregateMetricDoubleBlockBuilder builder = context.blockFactory()
                            .newAggregateMetricDoubleBlockBuilder(positionCount)
                    ) {
                        CompensatedSum sum = new CompensatedSum();
                        for (int p = 0; p < positionCount; p++) {
                            int valueCount = longBlock.getValueCount(p);
                            int start = longBlock.getFirstValueIndex(p);
                            int end = start + valueCount;
                            if (valueCount == 0) {
                                builder.appendNull();
                                continue;
                            }
                            if (valueCount == 1) {
                                double current = EsqlDataTypeConverter.unsignedLongToDouble(longBlock.getLong(p));
                                builder.min().appendDouble(current);
                                builder.max().appendDouble(current);
                                builder.sum().appendDouble(current);
                                builder.count().appendInt(valueCount);
                                continue;
                            }
                            double min = Double.POSITIVE_INFINITY;
                            double max = Double.NEGATIVE_INFINITY;
                            for (int i = start; i < end; i++) {
                                double current = EsqlDataTypeConverter.unsignedLongToDouble(longBlock.getLong(p));
                                min = Math.min(min, current);
                                max = Math.max(max, current);
                                sum.add(current);
                            }
                            builder.min().appendDouble(min);
                            builder.max().appendDouble(max);
                            builder.sum().appendDouble(sum.value());
                            builder.count().appendInt(valueCount);
                            sum.reset(0, 0);
                        }
                        return builder.build();
                    }
                }

                private Block evalVector(Vector vector) {
                    int positionCount = vector.getPositionCount();
                    LongVector longVector = (LongVector) vector;
                    try (
                        AggregateMetricDoubleVectorBuilder builder = new AggregateMetricDoubleVectorBuilder(
                            positionCount,
                            context.blockFactory()
                        )
                    ) {
                        for (int p = 0; p < positionCount; p++) {
                            double value = EsqlDataTypeConverter.unsignedLongToDouble(longVector.getLong(p));
                            builder.appendValue(value);
                        }
                        return builder.build();
                    }
                }

                @Override
                public Block eval(Page page) {
                    try (Block block = eval.eval(page)) {
                        Vector vector = block.asVector();
                        return vector == null ? evalBlock(block) : evalVector(vector);
                    }
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(eval);
                }

                @Override
                public String toString() {
                    return "ToAggregateMetricDoubleFromUnsignedLongEvaluator[field=" + eval + "]";
                }
            };
        }
    }
}
