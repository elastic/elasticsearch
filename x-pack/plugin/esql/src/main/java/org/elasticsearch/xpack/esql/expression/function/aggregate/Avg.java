/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;

public class Avg extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Avg", Avg::new);
    private final Expression summationMode;

    @FunctionInfo(
        returnType = "double",
        description = "The average of a numeric field.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "avg"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the average "
                    + "over a multivalued column, first use `MV_AVG` to average the multiple values per row, "
                    + "and use the result with the `AVG` function",
                file = "stats",
                tag = "docsStatsAvgNestedExpression"
            ) }
    )
    public Avg(
        Source source,
        @Param(
            name = "number",
            type = { "aggregate_metric_double", "exponential_histogram", "double", "integer", "long" },
            description = "Expression that outputs values to average."
        ) Expression field
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, SummationMode.COMPENSATED_LITERAL);
    }

    public Avg(Source source, Expression field, Expression filter, Expression window, Expression summationMode) {
        super(source, field, filter, window, List.of(summationMode));
        this.summationMode = summationMode;
    }

    public Expression summationMode() {
        return summationMode;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isType(
            field(),
            dt -> (dt.isNumeric() && dt != DataType.UNSIGNED_LONG) || dt == AGGREGATE_METRIC_DOUBLE || dt == EXPONENTIAL_HISTOGRAM,
            sourceText(),
            DEFAULT,
            "aggregate_metric_double, exponential_histogram or numeric except unsigned_long or counter types"
        );
    }

    private Avg(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in),
            readSummationMode(in)
        );
    }

    private static Expression readSummationMode(StreamInput in) throws IOException {
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return parameters.isEmpty() ? SummationMode.COMPENSATED_LITERAL : parameters.getFirst();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected NodeInfo<Avg> info() {
        return NodeInfo.create(this, Avg::new, field(), filter(), window(), summationMode);
    }

    @Override
    public Avg replaceChildren(List<Expression> newChildren) {
        return new Avg(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public Avg withFilter(Expression filter) {
        return new Avg(source(), field(), filter, window(), summationMode);
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();
        if (field.dataType() == AGGREGATE_METRIC_DOUBLE) {
            return new Div(
                s,
                new Sum(s, field, filter(), window(), summationMode).surrogate(),
                new Count(s, field, filter(), window()).surrogate()
            );
        }
        if (field.dataType() == EXPONENTIAL_HISTOGRAM) {
            Sum valuesSum = new Sum(s, field, filter(), window(), summationMode);
            Sum totalCount = new Sum(
                s,
                ExtractHistogramComponent.create(s, field, ExponentialHistogramBlock.Component.COUNT),
                filter(),
                window(),
                summationMode
            );
            return new Div(s, valuesSum, totalCount);
        }
        if (field.foldable()) {
            return new MvAvg(s, field);
        }
        return new Div(s, new Sum(s, field, filter(), window(), summationMode), new Count(s, field, filter(), window()), dataType());
    }
}
