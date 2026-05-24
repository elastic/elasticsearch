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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
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
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;

/**
 * RANGE_WITHIN(value, range) -> boolean
 * Returns true if the first argument is within the second (the range).
 * Supported signatures:
 * <ul>
 *   <li>(date, date_range): point within range</li>
 *   <li>(date_range, date_range): first range within second (first fully contained by second)</li>
 * </ul>
 * (date_range, date) and (date, date) are not supported; they do not match "value within range" semantics.
 */
public class RangeWithin extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RangeWithin",
        RangeWithin::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(RangeWithin.class)
        .binary(RangeWithin::new)
        .name("range_within");

    private final Expression left;
    private final Expression right;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = "Returns true if the first argument is "
            + "[within](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-range-query) "
            + "the second argument. "
            + "Supports (date, date_range) and (date_range, date_range). The second argument must be a date_range.",
        examples = @Example(file = "date_range", tag = "rangeWithin", explanation = "Filter events within a specific date range")
    )
    public RangeWithin(
        Source source,
        @Param(name = "left", type = { "date", "date_range" }, description = "Value to test (point or range).") Expression left,
        @Param(name = "right", type = { "date_range" }, description = "Container range.") Expression right
    ) {
        super(source, List.of(left, right));
        this.left = left;
        this.right = right;
    }

    private RangeWithin(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public Object fold(FoldContext foldContext) {
        if (foldable() == false) {
            return this;
        }

        Object leftValue = left.fold(foldContext);
        Object rightValue = right.fold(foldContext);

        if (leftValue == null || rightValue == null) {
            return null;
        }

        if (left.dataType() == DATE_RANGE) {
            return processRange((LongRangeBlockBuilder.LongRange) leftValue, (LongRangeBlockBuilder.LongRange) rightValue);
        }
        return processPoint((Long) leftValue, (LongRangeBlockBuilder.LongRange) rightValue);
    }

    @Evaluator(extraName = "Point")
    static boolean processPoint(long point, LongRangeBlockBuilder.LongRange range) {
        // Range is [from, to); to is exclusive.
        return point >= range.from() && point < range.to();
    }

    @Evaluator(extraName = "Range")
    static boolean processRange(LongRangeBlockBuilder.LongRange a, LongRangeBlockBuilder.LongRange b) {
        return a.from() >= b.from() && a.to() <= b.to();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution first = isType(left, dt -> dt == DATE_RANGE || dt == DATETIME, sourceText(), FIRST, "date", "date_range");
        TypeResolution second = isType(right, dt -> dt == DATE_RANGE, sourceText(), SECOND, "date_range");
        return first.and(second);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var leftEvaluator = toEvaluator.apply(left);
        var rightEvaluator = toEvaluator.apply(right);
        if (left.dataType() == DATE_RANGE) {
            return new RangeWithinRangeEvaluator.Factory(source(), leftEvaluator, rightEvaluator);
        }
        return new RangeWithinPointEvaluator.Factory(source(), leftEvaluator, rightEvaluator);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RangeWithin(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RangeWithin::new, left, right);
    }
}
