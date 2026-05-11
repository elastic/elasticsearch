/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;

/**
 * RANGE_CONTAINS(a, b) -> boolean
 * Returns true if the first argument contains the second.
 * Equivalent to {@code RANGE_WITHIN(b, a)} — this function lowers to {@link RangeWithin} via
 * {@link OnlySurrogateExpression#surrogate()} on the coordinator, so it has no evaluator of its own.
 * Supported signatures:
 * <ul>
 *   <li>(date_range, date): range contains point</li>
 *   <li>(date_range, date_range): first range contains second (second fully contained by first)</li>
 * </ul>
 */
public class RangeContains extends EsqlScalarFunction implements OnlySurrogateExpression {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(RangeContains.class)
        .binary(RangeContains::new)
        .name("range_contains");

    private final Expression left;
    private final Expression right;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = """
            Returns true if the first argument
            [contains](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-range-query) the second argument.
            This is the inverse of [RANGE_WITHIN](#esql-range_within); equivalent to `RANGE_WITHIN(b, a)`.
            Supports (date_range, date) and (date_range, date_range). The first argument must be a date_range.""",
        examples = @Example(
            file = "date_range",
            tag = "rangeContains",
            explanation = "Filter ranges that contain a target date or sub-range"
        )
    )
    public RangeContains(
        Source source,
        @Param(name = "left", type = { "date_range" }, description = "Container range.") Expression left,
        @Param(name = "right", type = { "date", "date_range" }, description = "Value to test (point or range).") Expression right
    ) {
        super(source, List.of(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("RangeContains is a surrogate; lowered to RangeWithin before serialization");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("RangeContains is a surrogate; lowered to RangeWithin before serialization");
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
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution first = isType(left, dt -> dt == DATE_RANGE, sourceText(), FIRST, "date_range");
        TypeResolution second = isType(right, dt -> dt == DATE_RANGE || dt == DATETIME, sourceText(), SECOND, "date", "date_range");
        return first.and(second);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("RangeContains should have been replaced by RangeWithin via surrogate()");
    }

    @Override
    public Expression surrogate() {
        // RANGE_CONTAINS(a, b) is equivalent to RANGE_WITHIN(b, a). Lowering preserves the relationship
        // and lets the existing RangeWithin evaluators handle every supported type combination.
        return new RangeWithin(source(), right, left);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RangeContains(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RangeContains::new, left, right);
    }
}
