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
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
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
public class Present extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Present", Present::new);

    @FunctionInfo(
        returnType = "boolean",
        description = "Returns true if the input expression yields any non-null values within the current aggregation context. "
            + "Otherwise it returns false.",
        type = FunctionType.AGGREGATE,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0") },
        examples = {
            @Example(file = "present", tag = "present"),
            @Example(
                description = "To check for the presence inside a group use `PRESENT()` and `BY` clauses",
                file = "present",
                tag = "present-by"
            ),
            @Example(
                description = "To check for the presence and return 1 when it's true and 0 when it's false",
                file = "present",
                tag = "present-as-integer"
            ) }
    )
    public Present(
        Source source,
        @Param(
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
                "geohash",
                "geotile",
                "geohex",
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
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public Present(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
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
        return NodeInfo.create(this, Present::new, field(), filter(), window());
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Present(source(), field(), filter, window());
    }

    @Override
    public Present replaceChildren(List<Expression> newChildren) {
        return new Present(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
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
        return isType(
            field(),
            dt -> dt.isCounter() == false && dt != DataType.DENSE_VECTOR,
            sourceText(),
            DEFAULT,
            "any type except counter types or dense_vector"
        );
    }
}
