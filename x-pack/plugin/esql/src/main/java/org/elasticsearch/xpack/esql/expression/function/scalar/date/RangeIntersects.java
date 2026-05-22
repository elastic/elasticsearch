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
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;

/**
 * RANGE_INTERSECTS(a, b) -> boolean
 * Returns true if the two arguments overlap. The relation is symmetric: argument order does not matter.
 * Supported signatures:
 * <ul>
 *   <li>(date_range, date_range): the two ranges overlap, i.e. {@code a.from < b.to && b.from < a.to} for half-open ranges</li>
 *   <li>(date, date_range) and (date_range, date): the point is inside the range — equivalent to RANGE_WITHIN's point case</li>
 *   <li>(date, date): degenerate; equivalent to {@code a == b}, lowered to {@link Equals} via {@link SurrogateExpression}</li>
 * </ul>
 */
public class RangeIntersects extends EsqlScalarFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RangeIntersects",
        RangeIntersects::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(RangeIntersects.class)
        .binary(RangeIntersects::new)
        .name("range_intersects");

    private final Expression left;
    private final Expression right;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = "Returns true if the two arguments overlap. The relation is symmetric — argument order does not matter. "
            + "Supports any combination of `date` and `date_range`. "
            + "When both arguments are `date`, this is equivalent to `a == b`.",
        examples = @Example(file = "date_range", tag = "rangeIntersects", explanation = "Find ranges that overlap a target window")
    )
    public RangeIntersects(
        Source source,
        @Param(name = "left", type = { "date", "date_range" }, description = "First value (point or range).") Expression left,
        @Param(name = "right", type = { "date", "date_range" }, description = "Second value (point or range).") Expression right
    ) {
        super(source, List.of(left, right));
        this.left = left;
        this.right = right;
    }

    private RangeIntersects(StreamInput in) throws IOException {
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

        DataType leftType = left.dataType();
        DataType rightType = right.dataType();
        if (leftType == DATE_RANGE && rightType == DATE_RANGE) {
            return processRange((LongRangeBlockBuilder.LongRange) leftValue, (LongRangeBlockBuilder.LongRange) rightValue);
        }
        if (leftType == DATETIME && rightType == DATE_RANGE) {
            return processPoint((Long) leftValue, (LongRangeBlockBuilder.LongRange) rightValue);
        }
        if (leftType == DATE_RANGE && rightType == DATETIME) {
            return processPoint((Long) rightValue, (LongRangeBlockBuilder.LongRange) leftValue);
        }
        // (date, date): degenerate equality
        return ((Long) leftValue).longValue() == ((Long) rightValue).longValue();
    }

    @Evaluator(extraName = "Point")
    static boolean processPoint(long point, LongRangeBlockBuilder.LongRange range) {
        // Range is half-open [from, to); point intersects iff it lies inside.
        return point >= range.from() && point < range.to();
    }

    @Evaluator(extraName = "Range")
    static boolean processRange(LongRangeBlockBuilder.LongRange a, LongRangeBlockBuilder.LongRange b) {
        // Half-open ranges intersect iff each starts before the other ends.
        return a.from() < b.to() && b.from() < a.to();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution first = isType(left, dt -> dt == DATE_RANGE || dt == DATETIME, sourceText(), FIRST, "date", "date_range");
        TypeResolution second = isType(right, dt -> dt == DATE_RANGE || dt == DATETIME, sourceText(), SECOND, "date", "date_range");
        return first.and(second);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        DataType leftType = left.dataType();
        DataType rightType = right.dataType();
        var leftEvaluator = toEvaluator.apply(left);
        var rightEvaluator = toEvaluator.apply(right);
        if (leftType == DATE_RANGE && rightType == DATE_RANGE) {
            return new RangeIntersectsRangeEvaluator.Factory(source(), leftEvaluator, rightEvaluator);
        }
        if (leftType == DATETIME && rightType == DATE_RANGE) {
            return new RangeIntersectsPointEvaluator.Factory(source(), leftEvaluator, rightEvaluator);
        }
        if (leftType == DATE_RANGE && rightType == DATETIME) {
            // Swap so the point comes first; intersection is symmetric.
            return new RangeIntersectsPointEvaluator.Factory(source(), rightEvaluator, leftEvaluator);
        }
        // (date, date) is handled by the surrogate() lowering to Equals; reach here only if the optimizer didn't apply it.
        throw new IllegalStateException("RANGE_INTERSECTS(date, date) should have been replaced by Equals via surrogate()");
    }

    @Override
    public Expression surrogate() {
        // (date, date) collapses to plain equality: two timestamps "intersect" iff they're equal.
        if (left.dataType() == DATETIME && right.dataType() == DATETIME) {
            return new Equals(source(), left, right);
        }
        return null;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RangeIntersects(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RangeIntersects::new, left, right);
    }
}
