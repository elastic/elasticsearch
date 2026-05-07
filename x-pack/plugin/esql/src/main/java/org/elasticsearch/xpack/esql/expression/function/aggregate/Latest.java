/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.IMPLICIT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * An alias for {@link Last} where the sort field (second parameter) is set to {@code @timestamp}. This is not a time series function.
 */
public class Latest extends AggregateFunction implements OnlySurrogateExpression, TimestampAware {
    public static final String NAME = "Latest";
    private final Expression timestamp;
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Latest.class).binary(Latest::new).name("latest");

    @FunctionInfo(
        type = FunctionType.AGGREGATE,
        returnType = { "long", "integer", "double", "keyword", "ip", "boolean", "date", "date_nanos" },
        description = """
            An alias for [`LAST`](/reference/query-languages/esql/functions-operators/aggregation-functions/last.md) where
            the sort field (the second parameter) is implicit and is set to `@timestamp`.""",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = @Example(file = "stats_earliest_latest", tag = "latest")
    )
    public Latest(
        Source source,
        @Param(
            name = "field",
            type = { "long", "integer", "double", "keyword", "text", "ip", "boolean", "date", "date_nanos" },
            description = "The search field"
        ) Expression field,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, timestamp);
    }

    private Latest(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public Expression surrogate() {
        return new Last(source(), field(), timestamp);
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isType(
            field(),
            dt -> dt == DataType.BOOLEAN
                || dt == DataType.DATETIME
                || dt == DataType.DATE_NANOS
                || DataType.isString(dt)
                || dt == DataType.IP
                || (dt.isNumeric() && dt != DataType.UNSIGNED_LONG),
            sourceText(),
            DEFAULT,
            "boolean",
            "date",
            "ip",
            "string",
            "numeric except unsigned_long or counter types"
        ).and(
            isType(
                timestamp,
                dt -> dt == DataType.INTEGER || dt == DataType.LONG || dt == DataType.DATETIME || dt == DataType.DATE_NANOS,
                sourceText(),
                IMPLICIT,
                "int or long or date_nanos or datetime"
            )
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Latest::new, field(), timestamp);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Latest(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public Latest withFilter(Expression filter) {
        return new Latest(source(), field(), filter, window(), timestamp);
    }

    @Override
    public String toString() {
        return "latest(" + field() + ", " + timestamp() + ")";
    }
}
