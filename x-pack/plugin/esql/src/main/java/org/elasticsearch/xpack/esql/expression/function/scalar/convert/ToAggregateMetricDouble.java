/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;

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
        Map.entry(INTEGER, ((source, fieldEval) -> new DoubleFactory(source, Cast.cast(source, INTEGER, DOUBLE, fieldEval)))),
        Map.entry(LONG, ((source, fieldEval) -> new DoubleFactory(source, Cast.cast(source, LONG, DOUBLE, fieldEval)))),
        Map.entry(UNSIGNED_LONG, ((source, fieldEval) -> new DoubleFactory(source, Cast.cast(source, UNSIGNED_LONG, DOUBLE, fieldEval))))
    );

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToAggregateMetricDouble",
        ToAggregateMetricDouble::new
    );

    @FunctionInfo(returnType = "aggregate_metric_double", description = "Encode a numeric to an aggregate_metric_double.")
    public ToAggregateMetricDouble(
        Source source,
        @Param(
            name = "number",
            type = { "double", "long", "unsigned_long", "integer", "aggregate_metric_double" },
            description = "Input value. The input can be a single-valued column or an expression."
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

    public static class DoubleFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;

        private final EvalOperator.ExpressionEvaluator.Factory fieldEvaluator;

        public DoubleFactory(Source source, EvalOperator.ExpressionEvaluator.Factory fieldEvaluator) {
            this.fieldEvaluator = fieldEvaluator;
            this.source = source;
        }

        @Override
        public String toString() {
            return "ToAggregateMetricDoubleEvaluator[" + "field=" + fieldEvaluator + "]";
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            final EvalOperator.ExpressionEvaluator eval = fieldEvaluator.get(context);

            return new EvalOperator.ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    try (Block block = eval.eval(page)) {
                        int positionCount = block.getPositionCount();
                        DoubleBlock doubleBlock = (DoubleBlock) block;
                        try (
                            AggregateMetricDoubleBlockBuilder result = context.blockFactory()
                                .newAggregateMetricDoubleBlockBuilder(positionCount)
                        ) {
                            CompensatedSum sum = new CompensatedSum();
                            for (int p = 0; p < positionCount; p++) {
                                int valueCount = doubleBlock.getValueCount(p);
                                int start = doubleBlock.getFirstValueIndex(p);
                                int end = start + valueCount;
                                if (valueCount == 0) {
                                    result.appendNull();
                                    continue;
                                }
                                double min = Double.POSITIVE_INFINITY;
                                double max = Double.NEGATIVE_INFINITY;
                                for (int i = start; i < end; i++) {
                                    double current = doubleBlock.getDouble(i);
                                    min = Math.min(min, current);
                                    max = Math.max(max, current);
                                    sum.add(current);
                                }
                                result.min().appendDouble(min);
                                result.max().appendDouble(max);
                                result.sum().appendDouble(sum.value());
                                result.count().appendInt(valueCount);
                                sum.reset(0, 0);
                            }
                            return result.build();
                        }
                    }
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(eval);
                }

                @Override
                public String toString() {
                    return "ToAggregateMetricDoubleEvaluator[field=" + eval + "]";
                }
            };
        }
    }
}
