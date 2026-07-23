/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;

/**
 * Constructs a range value from two scalar boundary values.
 * The first argument is the inclusive lower bound; the second is the exclusive upper bound,
 * matching the ES|QL half-open [from, to) convention used for all range types.
 * Currently supports {@code date_range} (from two {@code datetime} values).
 * Future overloads will cover {@code long_range}, {@code integer_range}, {@code ip_range}, etc.
 */
public class ToRange extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToRange", ToRange::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToRange.class).binary(ToRange::new).name("to_range");

    private final Expression from;
    private final Expression to;

    @FunctionInfo(
        returnType = "date_range",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.5.0") },
        briefSummary = "Constructs a range from two boundary values.",
        description = "Constructs a range from two boundary values. "
            + "The first argument is the inclusive lower bound; "
            + "the second is the exclusive upper bound, following the half-open `[from, to)` "
            + "convention used for all range types in ES|QL. "
            + "Currently accepts `datetime` arguments and returns a `date_range`.",
        examples = @Example(file = "date_range", tag = "to_range")
    )
    public ToRange(
        Source source,
        @Param(
            name = "from",
            type = { "date" },
            description = "Inclusive lower bound of the range (`[from, to)`). If `null`, the function returns `null`."
        ) Expression from,
        @Param(
            name = "to",
            type = { "date" },
            description = "Exclusive upper bound of the range (`[from, to)`). If `null`, the function returns `null`."
        ) Expression to
    ) {
        super(source, List.of(from, to));
        this.from = from;
        this.to = to;
    }

    private ToRange(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(from);
        out.writeNamedWriteable(to);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression from() {
        return from;
    }

    public Expression to() {
        return to;
    }

    @Override
    public DataType dataType() {
        // Null literals are polymorphic; use the other argument's type to determine output.
        DataType elementType = from.dataType() == DataType.NULL ? to.dataType() : from.dataType();
        return switch (elementType) {
            case DATETIME -> DataType.DATE_RANGE;
            default -> DataType.NULL;
        };
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution first = isType(from, dt -> dt == DATETIME, sourceText(), FIRST, "date");
        if (first.unresolved()) {
            return first;
        }
        DataType fromType = from.dataType();
        // When from is a null literal its type is NULL; fall back to checking to against all
        // currently supported types rather than matching NULL, which would reject valid inputs.
        if (fromType == DataType.NULL) {
            return isType(to, dt -> dt == DATETIME, sourceText(), SECOND, "date");
        }
        return isType(to, dt -> dt == fromType, sourceText(), SECOND, fromType.esType());
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Evaluator(extraName = "Long", warnExceptions = { IllegalArgumentException.class })
    static LongRangeBlockBuilder.LongRange process(long from, long to) {
        if (from >= to) {
            throw new IllegalArgumentException("'from' [" + from + "] must be less than 'to' [" + to + "]");
        }
        return new LongRangeBlockBuilder.LongRange(from, to);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new ToRangeLongEvaluator.Factory(source(), toEvaluator.apply(from), toEvaluator.apply(to));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToRange(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToRange::new, from, to);
    }
}
