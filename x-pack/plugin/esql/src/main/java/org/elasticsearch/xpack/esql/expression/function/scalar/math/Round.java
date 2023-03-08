/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Round extends ScalarFunction implements OptionalArgument, Mappable {

    private final Expression field, decimals;

    public Round(Source source, Expression field, Expression decimals) {
        super(source, decimals != null ? Arrays.asList(field, decimals) : Arrays.asList(field));
        this.field = field;
        this.decimals = decimals;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isNumeric(field, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;

        }

        return decimals == null ? TypeResolution.TYPE_RESOLVED : isInteger(decimals, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return field.foldable() && (decimals == null || decimals.foldable());
    }

    @Override
    public Object fold() {
        Object fieldVal = field.fold();
        Object decimalsVal = decimals == null ? null : decimals.fold();
        return process(fieldVal, decimalsVal);
    }

    public static Number process(Object fieldVal, Object decimalsVal) {
        if (fieldVal == null) {
            return null;
        }
        if (decimalsVal == null) {
            decimalsVal = 0;
        }
        return Maths.round((Number) fieldVal, (Number) decimalsVal);
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Round(source(), newChildren.get(0), decimals() == null ? null : newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Round::new, field(), decimals());
    }

    public Expression field() {
        return field;
    }

    public Expression decimals() {
        return decimals;
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> fieldEvaluator = toEvaluator.apply(field());
        // round.decimals() == null means that decimals were not provided (it's an optional parameter of the Round function)
        Supplier<EvalOperator.ExpressionEvaluator> decimalsEvaluatorSupplier = decimals != null ? toEvaluator.apply(decimals) : null;
        if (false == field.dataType().isRational()) {
            return fieldEvaluator;
        }
        return () -> new DecimalRoundExpressionEvaluator(
            fieldEvaluator.get(),
            decimalsEvaluatorSupplier == null ? null : decimalsEvaluatorSupplier.get()
        );
    }

    record DecimalRoundExpressionEvaluator(
        EvalOperator.ExpressionEvaluator fieldEvaluator,
        EvalOperator.ExpressionEvaluator decimalsEvaluator
    ) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            Object decimals = decimalsEvaluator != null ? decimalsEvaluator.computeRow(page, pos) : null;
            return Round.process(fieldEvaluator.computeRow(page, pos), decimals);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, decimals);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Round other = (Round) obj;
        return Objects.equals(other.field, field) && Objects.equals(other.decimals, decimals);
    }
}
