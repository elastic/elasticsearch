/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AggregateMetricDoubleNativeSupport;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIFTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isWholeNumber;

public class Sparkline extends AggregateFunction implements AggregateMetricDoubleNativeSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Sparkline",
        Sparkline::new
    );

    @FunctionInfo(
        returnType = { "integer", "long", "double" },
        description = "The values representing the y-axis values of a sparkline graph for a given aggregation over a period of time.",
        type = FunctionType.AGGREGATE,
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        examples = { @Example(file = "stats_sparkline", tag = "sparkline") }
    )
    public Sparkline(
        Source source,
        @Param(
            name = "field",
            type = { "integer", "long", "double" },
            description = "Expression that calculates the y-axis value of the sparkline graph for each datapoint."
        ) Expression field,
        @Param(name = "key", type = { "date" }, description = "Date expression from which to derive buckets.") Expression key,
        @Param(
            name = "buckets",
            type = { "integer" },
            description = "Target number of buckets, or desired bucket size if `from` and `to` parameters are omitted."
        ) Expression buckets,
        @Param(
            name = "from",
            type = { "date", "keyword", "text" },
            description = "Start of the range. Can be a date or a date expressed as a string."
        ) Expression from,
        @Param(
            name = "to",
            type = { "date", "keyword", "text" },
            description = "End of the range. Can be a date or a date expressed as a string."
        ) Expression to
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, key, buckets, from, to);
    }

    public Sparkline(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression key,
        Expression buckets,
        Expression from,
        Expression to
    ) {
        super(source, field, filter, window, List.of(key, buckets, from, to));
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }

        TypeResolution fieldNullCheck = isNotNull(field(), sourceText(), FIRST);
        if (fieldNullCheck.unresolved()) {
            return fieldNullCheck;
        } else if (field() instanceof AggregateFunction == false) {
            return new TypeResolution(
                LoggerMessageFormat.format(
                    null,
                    "first argument of [{}] must be an aggregate function, found value [{}] type [{}]",
                    sourceText(),
                    field().sourceText(),
                    field().dataType().typeName()
                )
            );
        }

        TypeResolution resolution = isType(
            field(),
            dt -> dt == DataType.INTEGER || dt == DataType.LONG || dt == DataType.DOUBLE,
            sourceText(),
            FIRST,
            "integer or long or double"
        ).and(isNotNull(key(), sourceText(), SECOND))
            .and(isType(key(), dt -> dt == DataType.DATETIME, sourceText(), SECOND, "date"))
            .and(isNotNull(buckets(), sourceText(), THIRD))
            .and(isWholeNumber(buckets(), sourceText(), THIRD))
            .and(isFoldable(buckets(), sourceText(), THIRD))
            .and(isNotNull(from(), sourceText(), FOURTH))
            .and(
                isType(
                    from(),
                    dt -> dt == DataType.DATETIME || dt == DataType.KEYWORD || dt == DataType.TEXT,
                    sourceText(),
                    FOURTH,
                    "date or keyword or text"
                )
            )
            .and(isFoldable(from(), sourceText(), FOURTH))
            .and(isNotNull(to(), sourceText(), FIFTH))
            .and(
                isType(
                    to(),
                    dt -> dt == DataType.DATETIME || dt == DataType.KEYWORD || dt == DataType.TEXT,
                    sourceText(),
                    FIFTH,
                    "date or keyword or text"
                )
            )
            .and(isFoldable(to(), sourceText(), FIFTH));

        if (resolution.unresolved()) {
            return resolution;
        } else {
            return TypeResolution.TYPE_RESOLVED;
        }
    }

    private Sparkline(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected NodeInfo<Sparkline> info() {
        return NodeInfo.create(this, Sparkline::new, field(), filter(), window(), key(), buckets(), from(), to());
    }

    public Expression key() {
        return parameters().get(0);
    }

    public Expression buckets() {
        return parameters().get(1);
    }

    public Expression from() {
        return parameters().get(2);
    }

    public Expression to() {
        return parameters().get(3);
    }

    @Override
    public Sparkline replaceChildren(List<Expression> newChildren) {
        return new Sparkline(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4),
            newChildren.get(5),
            newChildren.get(6)
        );
    }

    @Override
    public Sparkline withFilter(Expression filter) {
        return new Sparkline(source(), field(), filter, window(), key(), buckets(), from(), to());
    }
}
