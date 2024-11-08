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
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Count extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Count", Count::new);

    @FunctionInfo(
        returnType = "long",
        description = "Returns the total number (count) of input values.",
        isAggregation = true,
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
                    + "a <<esql-where>> command to remove rows that shouldn't be included",
                file = "stats",
                tag = "count-where"
            ),
            @Example(
                description = "To count the same stream of data based on two different expressions "
                    + "use the pattern `COUNT(<expression> OR NULL)`",
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
        super(source, field, filter, emptyList());
    }

    private Count(StreamInput in) throws IOException {
        super(in);
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
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        return CountAggregatorFunction.supplier(inputChannels);
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
}
