/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryPredicate;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.util.CollectionUtils.combine;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.inCommon;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitOr;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.subtract;

public final class BooleanSimplification extends OptimizerRules.OptimizerExpressionRule<ScalarFunction> {

    public BooleanSimplification() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(ScalarFunction e, LogicalOptimizerContext ctx) {
        if (e instanceof And || e instanceof Or) {
            return simplifyAndOr((BinaryPredicate<?, ?, ?, ?>) e);
        }
        if (e instanceof Not) {
            return simplifyNot((Not) e);
        }

        return e;
    }

    private static Expression simplifyAndOr(BinaryPredicate<?, ?, ?, ?> bc) {
        Expression l = bc.left();
        Expression r = bc.right();

        if (bc instanceof And) {
            if (TRUE.equals(l)) {
                return r;
            }
            if (TRUE.equals(r)) {
                return l;
            }

            if (FALSE.equals(l) || FALSE.equals(r)) {
                return new Literal(bc.source(), Boolean.FALSE, DataType.BOOLEAN);
            }
            if (l.semanticEquals(r)) {
                return l;
            }

            //
            // common factor extraction -> (a || b) && (a || c) => a || (b && c)
            //
            List<Expression> leftSplit = splitOr(l);
            List<Expression> rightSplit = splitOr(r);

            List<Expression> common = inCommon(leftSplit, rightSplit);
            if (common.isEmpty()) {
                return bc;
            }
            List<Expression> lDiff = subtract(leftSplit, common);
            List<Expression> rDiff = subtract(rightSplit, common);
            // (a || b || c || ... ) && (a || b) => (a || b)
            if (lDiff.isEmpty() || rDiff.isEmpty()) {
                return combineOr(common);
            }
            // (a || b || c || ... ) && (a || b || d || ... ) => ((c || ...) && (d || ...)) || a || b
            Expression combineLeft = combineOr(lDiff);
            Expression combineRight = combineOr(rDiff);
            return combineOr(combine(common, new And(combineLeft.source(), combineLeft, combineRight)));
        }

        if (bc instanceof Or) {
            if (TRUE.equals(l) || TRUE.equals(r)) {
                return new Literal(bc.source(), Boolean.TRUE, DataType.BOOLEAN);
            }

            if (FALSE.equals(l)) {
                return r;
            }
            if (FALSE.equals(r)) {
                return l;
            }

            if (l.semanticEquals(r)) {
                return l;
            }

            //
            // common factor extraction -> (a && b) || (a && c) => a && (b || c)
            //
            List<Expression> leftSplit = splitAnd(l);
            List<Expression> rightSplit = splitAnd(r);

            List<Expression> common = inCommon(leftSplit, rightSplit);
            if (common.isEmpty()) {
                return bc;
            }
            List<Expression> lDiff = subtract(leftSplit, common);
            List<Expression> rDiff = subtract(rightSplit, common);
            // (a || b || c || ... ) && (a || b) => (a || b)
            if (lDiff.isEmpty() || rDiff.isEmpty()) {
                return combineAnd(common);
            }
            // (a || b || c || ... ) && (a || b || d || ... ) => ((c || ...) && (d || ...)) || a || b
            Expression combineLeft = combineAnd(lDiff);
            Expression combineRight = combineAnd(rDiff);
            return combineAnd(combine(common, new Or(combineLeft.source(), combineLeft, combineRight)));
        }

        // TODO: eliminate conjunction/disjunction
        return bc;

    }

    @SuppressWarnings("rawtypes")
    private Expression simplifyNot(Not n) {
        Expression c = n.field();

        if (TRUE.semanticEquals(c)) {
            return new Literal(n.source(), Boolean.FALSE, DataType.BOOLEAN);
        }
        if (FALSE.semanticEquals(c)) {
            return new Literal(n.source(), Boolean.TRUE, DataType.BOOLEAN);
        }

        Expression negated = maybeSimplifyNegatable(c);
        if (negated != null) {
            return negated;
        }

        if (c instanceof Not) {
            return ((Not) c).field();
        }

        return n;
    }

    /**
     * @param e
     * @return the negated expression or {@code null} if the parameter is not an instance of {@code Negatable}
     */
    protected Expression maybeSimplifyNegatable(Expression e) {
        return null;
    }

}
