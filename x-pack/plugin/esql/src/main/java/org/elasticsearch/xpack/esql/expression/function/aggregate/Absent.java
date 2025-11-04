/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * The function that checks for the absence of a field in the output result.
 * An absence means that the input expression does not yield a non-null value.
 */
public class Absent extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Absent", Absent::new);

    @FunctionInfo(
        returnType = "boolean",
        description = "Returns true if the input expression yields no non-null values within the current aggregation context. "
            + "Otherwise it returns false.",
        type = FunctionType.AGGREGATE,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0") },
        examples = {
            @Example(file = "absent", tag = "absent"),
            @Example(
                description = "To check for the absence inside a group use `ABSENT()` and `BY` clauses",
                file = "absent",
                tag = "absent-by"
            ),
            @Example(
                description = "To check for the absence and return 1 when it's true and 0 when it's false you can use to_integer()",
                file = "absent",
                tag = "absent-as-integer"
            ) }
    )
    public Absent(
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
            description = "Expression that outputs values to be checked for absence."
        ) Expression field
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public Absent(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    private Absent(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Absent> info() {
        return NodeInfo.create(this, Absent::new, field(), filter(), window());
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Absent(source(), field(), filter, window());
    }

    @Override
    public Absent replaceChildren(List<Expression> newChildren) {
        return new Absent(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
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

    @Override
    public Expression surrogate() {
        return new Not(source(), new Present(source(), field(), filter(), window()));
    }
}
