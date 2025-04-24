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
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Count extends AggregateFunction implements ToAggregator, SurrogateExpression, HasSampleCorrection {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Count", Count::new);

    private final boolean isSampleCorrected;

    @FunctionInfo(
        returnType = "long",
        description = "Returns the total number (count) of input values.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "count"),
            @Example(description = "To count the number of rows, use `COUNT()` or `COUNT(*)`", file = "docs", tag = "countAll"),
            @Example(
                description = "The expression can use inline functions. This example splits a string into "
                    + "multiple values using the `SPLIT` function and counts the values",
                file = "stats",
                tag = "docsCountWithExpression"
            ),
            @Example(
                description = "To count the number of times an expression returns `TRUE` use "
                    + "a <<esql-where>> command to remove rows that shouldn’t be included",
                file = "stats",
                tag = "count-where"
            ),
            @Example(
                description = "To count the same stream of data based on two different expressions "
                    + "use the pattern `COUNT(<expression> OR NULL)`. This builds on the three-valued logic "
                    + "({wikipedia}/Three-valued_logic[3VL]) of the language: `TRUE OR NULL` is `TRUE`, but `FALSE OR NULL` is `NULL`, "
                    + "plus the way COUNT handles `NULL`s: `COUNT(TRUE)` and `COUNT(FALSE)` are both 1, but `COUNT(NULL)` is 0.",
                file = "stats",
                tag = "count-or-null"
            ) }
    )
    public Count(
        Source source,
        @Param(
            optional = true,
            name = "field",
            type = {
                "aggregate_metric_double",
                "boolean",
                "cartesian_point",
                "date",
                "double",
                "geo_point",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Expression that outputs values to be counted. If omitted, equivalent to `COUNT(*)` (the number of rows)."
        ) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public Count(Source source, Expression field, Expression filter) {
        this(source, field, filter, false);
    }

    private Count(Source source, Expression field, Expression filter, boolean isSampleCorrected) {
        super(source, field, filter, emptyList());
        this.isSampleCorrected = isSampleCorrected;
    }

    private Count(StreamInput in) throws IOException {
        super(in);
        // isSampleCorrected is only used during query optimization to mark
        // whether this function has been processed. Hence there's no need to
        // serialize it.
        this.isSampleCorrected = false;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Count> info() {
        return NodeInfo.create(this, Count::new, field(), filter());
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Count(source(), field(), filter);
    }

    @Override
    public Count replaceChildren(List<Expression> newChildren) {
        return new Count(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return CountAggregatorFunction.supplier();
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt.isCounter() == false, sourceText(), DEFAULT, "any type except counter types");
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();
        if (field.dataType() == DataType.AGGREGATE_METRIC_DOUBLE) {
            return new Sum(s, FromAggregateMetricDouble.withMetric(source(), field, AggregateMetricDoubleBlockBuilder.Metric.COUNT));
        }

        if (field.foldable()) {
            if (field instanceof Literal l) {
                if (l.value() != null && (l.value() instanceof List<?>) == false) {
                    // TODO: Normalize COUNT(*), COUNT(), COUNT("foobar"), COUNT(1) as COUNT(*).
                    // Does not apply to COUNT([1,2,3])
                    // return new Count(s, new Literal(s, StringUtils.WILDCARD, DataType.KEYWORD));
                    return null;
                }
            }

            // COUNT(const) is equivalent to MV_COUNT(const)*COUNT(*) if const is not null; otherwise COUNT(const) == 0.
            return new Mul(
                s,
                new Coalesce(s, new MvCount(s, field), List.of(new Literal(s, 0, DataType.INTEGER))),
                new Count(s, new Literal(s, StringUtils.WILDCARD, DataType.KEYWORD))
            );
        }

        return null;
    }

    @Override
    public boolean isSampleCorrected() {
        return isSampleCorrected;
    }

    @Override
    public Expression sampleCorrection(Expression sampleProbability) {
        return new ToLong(source(), new Div(source(), new Count(source(), field(), filter(), true), sampleProbability));
    }
}
