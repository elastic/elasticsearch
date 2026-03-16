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
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;

/**
 * RANGE_WITHIN(left, right) -> boolean
 * Returns true if the first value "contains" the second, following Lucene CONTAINS semantics (value within range):
 * <ul>
 *   <li>(date_range, date): range contains the point (point within [from, to])</li>
 *   <li>(date, date_range): range contains the point (point within [from, to])</li>
 *   <li>(date_range, date_range): first range contains the second (second fully within first)</li>
 *   <li>(date, date): equality</li>
 * </ul>
 */
public class RangeWithin extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RangeWithin",
        RangeWithin::new
    );

    private final Expression left;
    private final Expression right;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = "Returns true if the value is within the range (Lucene CONTAINS semantics). "
            + "Supports (date_range, date), (date, date_range), (date_range, date_range), (date, date).",
        appendix = "RANGE_WITHIN supports all four combinations of date and date_range with Lucene CONTAINS semantics. "
            + "Lucene pushdown is not implemented for this function yet.",
        examples = @Example(file = "date_range", tag = "rangeWithin", explanation = "Filter events within a specific date range")
    )
    public RangeWithin(
        Source source,
        @Param(name = "left", type = { "date", "date_nanos", "date_range" }, description = "Container (range or point).") Expression left,
        @Param(
            name = "right",
            type = { "date", "date_nanos", "date_range" },
            description = "Value to test (range or point)."
        ) Expression right
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

        return rangeContains(leftValue, rightValue, left.dataType(), right.dataType());
    }

    /**
     * CONTAINS semantics: left contains right.
     * - (range, date): date in range [from, to]
     * - (date, range): range contains point (point in [from, to])
     * - (range, range): right within left
     * - (date, date): equality
     */
    static boolean rangeContains(Object leftVal, Object rightVal, DataType leftType, DataType rightType) {
        if (leftType == DATE_RANGE && rightType == DATE_RANGE) {
            LongRangeBlockBuilder.LongRange a = (LongRangeBlockBuilder.LongRange) leftVal;
            LongRangeBlockBuilder.LongRange b = (LongRangeBlockBuilder.LongRange) rightVal;
            return b.from() >= a.from() && b.to() <= a.to();
        }
        if (leftType == DATE_RANGE && DataType.isMillisOrNanos(rightType)) {
            LongRangeBlockBuilder.LongRange r = (LongRangeBlockBuilder.LongRange) leftVal;
            long point = (Long) rightVal;
            return point >= r.from() && point <= r.to();
        }
        if (DataType.isMillisOrNanos(leftType) && rightType == DATE_RANGE) {
            long point = (Long) leftVal;
            LongRangeBlockBuilder.LongRange r = (LongRangeBlockBuilder.LongRange) rightVal;
            return point >= r.from() && point <= r.to();
        }
        // (date, date)
        return ((Long) leftVal).longValue() == ((Long) rightVal).longValue();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution first = isType(
            left,
            dt -> dt == DATE_RANGE || DataType.isMillisOrNanos(dt),
            sourceText(),
            FIRST,
            "date",
            "date_nanos",
            "date_range"
        );
        TypeResolution second = isType(
            right,
            dt -> dt == DATE_RANGE || DataType.isMillisOrNanos(dt),
            sourceText(),
            SECOND,
            "date",
            "date_nanos",
            "date_range"
        );
        return first.and(second);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var leftEvaluator = toEvaluator.apply(left);
        var rangeEvaluator = toEvaluator.apply(right);
        return new RangeWithinEvaluator.Factory(source(), left.dataType(), right.dataType(), leftEvaluator, rangeEvaluator);
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
