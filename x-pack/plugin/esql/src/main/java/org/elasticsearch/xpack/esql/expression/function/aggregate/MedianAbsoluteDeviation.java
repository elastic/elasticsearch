/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MedianAbsoluteDeviationDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MedianAbsoluteDeviationIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MedianAbsoluteDeviationLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedianAbsoluteDeviation;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;

public class MedianAbsoluteDeviation extends NumericAggregate implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MedianAbsoluteDeviation",
        MedianAbsoluteDeviation::new
    );

    // TODO: Add parameter
    @FunctionInfo(
        returnType = "double",
        description = "Returns the median absolute deviation, a measure of variability. It is a robust "
            + "statistic, meaning that it is useful for describing data that may have outliers, "
            + "or may not be normally distributed. For such data it can be more descriptive "
            + "than standard deviation."
            + "\n\n"
            + "It is calculated as the median of each data point’s deviation from the median of "
            + "the entire sample. That is, for a random variable `X`, the median absolute "
            + "deviation is `median(|median(X) - X|)`.",
        note = "Like <<esql-percentile>>, `MEDIAN_ABSOLUTE_DEVIATION` is <<esql-percentile-approximate,usually approximate>>.",
        appendix = """
            ::::{warning}
            `MEDIAN_ABSOLUTE_DEVIATION` is also {wikipedia}/Nondeterministic_algorithm[non-deterministic].
            This means you can get slightly different results using the same data.
            ::::""",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "median_absolute_deviation", tag = "median-absolute-deviation"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the "
                    + "median absolute deviation of the maximum values of a multivalued column, first "
                    + "use `MV_MAX` to get the maximum value per row, and use the result with the "
                    + "`MEDIAN_ABSOLUTE_DEVIATION` function",
                file = "median_absolute_deviation",
                tag = "docsStatsMADNestedExpression"
            ), }
    )
    public MedianAbsoluteDeviation(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        this(source, field, Literal.TRUE);
    }

    public MedianAbsoluteDeviation(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private MedianAbsoluteDeviation(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<MedianAbsoluteDeviation> info() {
        return NodeInfo.create(this, MedianAbsoluteDeviation::new, field(), filter());
    }

    @Override
    public MedianAbsoluteDeviation replaceChildren(List<Expression> newChildren) {
        return new MedianAbsoluteDeviation(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public MedianAbsoluteDeviation withFilter(Expression filter) {
        return new MedianAbsoluteDeviation(source(), field(), filter);
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier() {
        return new MedianAbsoluteDeviationLongAggregatorFunctionSupplier();
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier() {
        return new MedianAbsoluteDeviationIntAggregatorFunctionSupplier();
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier() {
        return new MedianAbsoluteDeviationDoubleAggregatorFunctionSupplier();
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();

        if (field.foldable()) {
            return new MvMedianAbsoluteDeviation(s, new ToDouble(s, field));
        }

        return null;
    }
}
