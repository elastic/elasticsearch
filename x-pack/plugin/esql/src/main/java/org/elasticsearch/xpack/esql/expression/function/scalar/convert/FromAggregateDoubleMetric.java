/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.AggregateDoubleMetricBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;

public class FromAggregateDoubleMetric extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FromAggregateDoubleMetric",
        FromAggregateDoubleMetric::new
    );

    private final Expression field;
    private final Expression subfieldIndex;

    @FunctionInfo(returnType = { "long", "double" }, description = "Convert aggregate double metric to a block of a single subfield.")
    public FromAggregateDoubleMetric(
        Source source,
        @Param(
            name = "aggregate_metric_double",
            type = { "aggregate_metric_double" },
            description = "Aggregate double metric to convert."
        ) Expression field,
        @Param(name = "subfieldIndex", type = "int", description = "Index of subfield") Expression subfieldIndex
    ) {
        super(source, List.of(field, subfieldIndex));
        this.field = field;
        this.subfieldIndex = subfieldIndex;
    }

    private FromAggregateDoubleMetric(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(subfieldIndex);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        if (subfieldIndex.foldable() == false) {
            throw new EsqlIllegalArgumentException("Received a non-foldable value for subfield index");
        }

        var subfield = ((Number) subfieldIndex.fold(FoldContext.small())).intValue();
        if (subfield == AggregateDoubleMetricBlockBuilder.Metric.COUNT.ordinal()) {
            return INTEGER;
        }
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FromAggregateDoubleMetric(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FromAggregateDoubleMetric::new, field, subfieldIndex);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(field, dt -> dt == DataType.AGGREGATE_METRIC_DOUBLE, sourceText(), DEFAULT, "aggregate_metric_double only");
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        return driverContext -> {
            final EvalOperator.ExpressionEvaluator eval = fieldEvaluator.get(driverContext);
            return new EvalOperator.ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    CompositeBlock compositeBlock = (CompositeBlock) eval.eval(page);
                    try {
                        Block block = compositeBlock.getBlock(((Number) subfieldIndex.fold(FoldContext.small())).intValue());
                        block.incRef();
                        return block;
                    } finally {
                        compositeBlock.close();
                    }
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(eval);
                }
            };
        };
    }
}
