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
import org.elasticsearch.compute.aggregation.PresentAggregatorFunction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * The function that checks for the presence of a field in the output result.
 * Presence means that the input expression yields any non-null value.
 */
public class Present extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Present", Present::new);

    @FunctionInfo(
        returnType = "integer",
        description = "Returns 1 if the input expression yields any non-null values within the current aggregation context. "
            + "Otherwise it returns 0.",
        type = FunctionType.AGGREGATE,
        examples = {
        // TODO: Add examples
        }
    )
    public Present(
        Source source,
        @Param(
            optional = true,
            name = "field",
            type = {
                "aggregate_metric_double",
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Expression that outputs values to be checked for presence."
        ) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public Present(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private Present(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Present> info() {
        return NodeInfo.create(this, Present::new, field(), filter());
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Present(source(), field(), filter);
    }

    @Override
    public Present replaceChildren(List<Expression> newChildren) {
        return new Present(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return PresentAggregatorFunction.supplier();
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
        var source = source();
        if (field().foldable()) {
            if (field() instanceof Literal l) {
                return new Literal(source, l.value() != null ? 1 : 0, DataType.INTEGER);
            }

            return new Least(
                source,
                new Count(source, Literal.keyword(source, StringUtils.WILDCARD)),
                List.of(new Literal(source, 1, DataType.INTEGER))
            );
        }
        return null;
    }
}
