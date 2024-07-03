/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.optimizer;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.SurrogateFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.core.plan.logical.Filter;
import org.elasticsearch.xpack.esql.core.plan.logical.Limit;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.ReflectionUtils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.inCommon;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitOr;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.subtract;
import static org.elasticsearch.xpack.esql.core.util.CollectionUtils.combine;

public final class OptimizerRules {

    /**
     * This rule must always be placed after LiteralsOnTheRight, since it looks at TRUE/FALSE literals' existence
     * on the right hand-side of the {@link Equals}/{@link NotEquals} expressions.
     */
    public static final class BooleanFunctionEqualsElimination extends OptimizerExpressionRule<BinaryComparison> {

        public BooleanFunctionEqualsElimination() {
            super(TransformDirection.UP);
        }

        @Override
        protected Expression rule(BinaryComparison bc) {
            if ((bc instanceof Equals || bc instanceof NotEquals) && bc.left() instanceof Function) {
                // for expression "==" or "!=" TRUE/FALSE, return the expression itself or its negated variant

                if (TRUE.equals(bc.right())) {
                    return bc instanceof Equals ? bc.left() : new Not(bc.left().source(), bc.left());
                }
                if (FALSE.equals(bc.right())) {
                    return bc instanceof Equals ? new Not(bc.left().source(), bc.left()) : bc.left();
                }
            }

            return bc;
        }
    }

    public static class BooleanSimplification extends OptimizerExpressionRule<ScalarFunction> {

        public BooleanSimplification() {
            super(TransformDirection.UP);
        }

        @Override
        public Expression rule(ScalarFunction e) {
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
            if (e instanceof Negatable) {
                return ((Negatable<?>) e).negate();
            }
            return null;
        }
    }

    /**
     * Combine disjunctions on the same field into an In expression.
     * This rule looks for both simple equalities:
     * 1. a == 1 OR a == 2 becomes a IN (1, 2)
     * and combinations of In
     * 2. a == 1 OR a IN (2) becomes a IN (1, 2)
     * 3. a IN (1) OR a IN (2) becomes a IN (1, 2)
     *
     * This rule does NOT check for type compatibility as that phase has been
     * already be verified in the analyzer.
     */
    public static class CombineDisjunctionsToIn extends OptimizerExpressionRule<Or> {
        public CombineDisjunctionsToIn() {
            super(TransformDirection.UP);
        }

        @Override
        protected Expression rule(Or or) {
            Expression e = or;
            // look only at equals and In
            List<Expression> exps = splitOr(e);

            Map<Expression, Set<Expression>> found = new LinkedHashMap<>();
            ZoneId zoneId = null;
            List<Expression> ors = new LinkedList<>();

            for (Expression exp : exps) {
                if (exp instanceof Equals eq) {
                    // consider only equals against foldables
                    if (eq.right().foldable()) {
                        found.computeIfAbsent(eq.left(), k -> new LinkedHashSet<>()).add(eq.right());
                    } else {
                        ors.add(exp);
                    }
                    if (zoneId == null) {
                        zoneId = eq.zoneId();
                    }
                } else if (exp instanceof In in) {
                    found.computeIfAbsent(in.value(), k -> new LinkedHashSet<>()).addAll(in.list());
                    if (zoneId == null) {
                        zoneId = in.zoneId();
                    }
                } else {
                    ors.add(exp);
                }
            }

            if (found.isEmpty() == false) {
                // combine equals alongside the existing ors
                final ZoneId finalZoneId = zoneId;
                found.forEach(
                    (k, v) -> { ors.add(v.size() == 1 ? createEquals(k, v, finalZoneId) : createIn(k, new ArrayList<>(v), finalZoneId)); }
                );

                Expression combineOr = combineOr(ors);
                // check the result semantically since the result might different in order
                // but be actually the same which can trigger a loop
                // e.g. a == 1 OR a == 2 OR null --> null OR a in (1,2) --> literalsOnTheRight --> cycle
                if (e.semanticEquals(combineOr) == false) {
                    e = combineOr;
                }
            }

            return e;
        }

        protected Equals createEquals(Expression k, Set<Expression> v, ZoneId finalZoneId) {
            return new Equals(k.source(), k, v.iterator().next(), finalZoneId);
        }

        protected In createIn(Expression key, List<Expression> values, ZoneId zoneId) {
            return new In(key.source(), key, values, zoneId);
        }
    }

    public static class ReplaceSurrogateFunction extends OptimizerExpressionRule<Expression> {

        public ReplaceSurrogateFunction() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof SurrogateFunction) {
                e = ((SurrogateFunction) e).substitute();
            }
            return e;
        }
    }

    public abstract static class PruneFilters extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            Expression condition = filter.condition().transformUp(BinaryLogic.class, PruneFilters::foldBinaryLogic);

            if (condition instanceof Literal) {
                if (TRUE.equals(condition)) {
                    return filter.child();
                }
                if (FALSE.equals(condition) || Expressions.isNull(condition)) {
                    return skipPlan(filter);
                }
            }

            if (condition.equals(filter.condition()) == false) {
                return new Filter(filter.source(), filter.child(), condition);
            }
            return filter;
        }

        protected abstract LogicalPlan skipPlan(Filter filter);

        private static Expression foldBinaryLogic(BinaryLogic binaryLogic) {
            if (binaryLogic instanceof Or or) {
                boolean nullLeft = Expressions.isNull(or.left());
                boolean nullRight = Expressions.isNull(or.right());
                if (nullLeft && nullRight) {
                    return new Literal(binaryLogic.source(), null, DataType.NULL);
                }
                if (nullLeft) {
                    return or.right();
                }
                if (nullRight) {
                    return or.left();
                }
            }
            if (binaryLogic instanceof And and) {
                if (Expressions.isNull(and.left()) || Expressions.isNull(and.right())) {
                    return new Literal(binaryLogic.source(), null, DataType.NULL);
                }
            }
            return binaryLogic;
        }
    }

    // NB: it is important to start replacing casts from the bottom to properly replace aliases
    public abstract static class PruneCast<C extends Expression> extends Rule<LogicalPlan, LogicalPlan> {

        private final Class<C> castType;

        public PruneCast(Class<C> castType) {
            this.castType = castType;
        }

        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return rule(plan);
        }

        protected final LogicalPlan rule(LogicalPlan plan) {
            // eliminate redundant casts
            return plan.transformExpressionsUp(castType, this::maybePruneCast);
        }

        protected abstract Expression maybePruneCast(C cast);
    }

    public abstract static class SkipQueryOnLimitZero extends OptimizerRule<Limit> {
        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.limit().foldable()) {
                if (Integer.valueOf(0).equals((limit.limit().fold()))) {
                    return skipPlan(limit);
                }
            }
            return limit;
        }

        protected abstract LogicalPlan skipPlan(Limit limit);
    }

    public static class FoldNull extends OptimizerExpressionRule<Expression> {

        public FoldNull() {
            super(TransformDirection.UP);
        }

        @Override
        public Expression rule(Expression e) {
            Expression result = tryReplaceIsNullIsNotNull(e);
            if (result != e) {
                return result;
            } else if (e instanceof In in) {
                if (Expressions.isNull(in.value())) {
                    return Literal.of(in, null);
                }
            } else if (e instanceof Alias == false
                && e.nullable() == Nullability.TRUE
                && Expressions.anyMatch(e.children(), Expressions::isNull)) {
                    return Literal.of(e, null);
                }
            return e;
        }

        protected Expression tryReplaceIsNullIsNotNull(Expression e) {
            if (e instanceof IsNotNull isnn) {
                if (isnn.field().nullable() == Nullability.FALSE) {
                    return new Literal(e.source(), Boolean.TRUE, DataType.BOOLEAN);
                }
            } else if (e instanceof IsNull isn) {
                if (isn.field().nullable() == Nullability.FALSE) {
                    return new Literal(e.source(), Boolean.FALSE, DataType.BOOLEAN);
                }
            }
            return e;
        }
    }

    // a IS NULL AND a IS NOT NULL -> FALSE
    // a IS NULL AND a > 10 -> a IS NULL and FALSE
    // can be extended to handle null conditions where available
    public static class PropagateNullable extends OptimizerExpressionRule<And> {

        public PropagateNullable() {
            super(TransformDirection.DOWN);
        }

        @Override
        public Expression rule(And and) {
            List<Expression> splits = Predicates.splitAnd(and);

            Set<Expression> nullExpressions = new LinkedHashSet<>();
            Set<Expression> notNullExpressions = new LinkedHashSet<>();
            List<Expression> others = new LinkedList<>();

            // first find isNull/isNotNull
            for (Expression ex : splits) {
                if (ex instanceof IsNull isn) {
                    nullExpressions.add(isn.field());
                } else if (ex instanceof IsNotNull isnn) {
                    notNullExpressions.add(isnn.field());
                }
                // the rest
                else {
                    others.add(ex);
                }
            }

            // check for is isNull and isNotNull --> FALSE
            if (Sets.haveNonEmptyIntersection(nullExpressions, notNullExpressions)) {
                return Literal.of(and, Boolean.FALSE);
            }

            // apply nullability across relevant/matching expressions

            // first against all nullable expressions
            // followed by all not-nullable expressions
            boolean modified = replace(nullExpressions, others, splits, this::nullify);
            modified |= replace(notNullExpressions, others, splits, this::nonNullify);
            if (modified) {
                // reconstruct the expression
                return Predicates.combineAnd(splits);
            }
            return and;
        }

        /**
         * Replace the given 'pattern' expressions against the target expression.
         * If a match is found, the matching expression will be replaced by the replacer result
         * or removed if null is returned.
         */
        private static boolean replace(
            Iterable<Expression> pattern,
            List<Expression> target,
            List<Expression> originalExpressions,
            BiFunction<Expression, Expression, Expression> replacer
        ) {
            boolean modified = false;
            for (Expression s : pattern) {
                for (int i = 0; i < target.size(); i++) {
                    Expression t = target.get(i);
                    // identify matching expressions
                    if (t.anyMatch(s::semanticEquals)) {
                        Expression replacement = replacer.apply(t, s);
                        // if the expression has changed, replace it
                        if (replacement != t) {
                            modified = true;
                            target.set(i, replacement);
                            originalExpressions.replaceAll(e -> t.semanticEquals(e) ? replacement : e);
                        }
                    }
                }
            }
            return modified;
        }

        // default implementation nullifies all nullable expressions
        protected Expression nullify(Expression exp, Expression nullExp) {
            return exp.nullable() == Nullability.TRUE ? Literal.of(exp, null) : exp;
        }

        // placeholder for non-null
        protected Expression nonNullify(Expression exp, Expression nonNullExp) {
            return exp;
        }
    }

    public abstract static class OptimizerRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        private final TransformDirection direction;

        public OptimizerRule() {
            this(TransformDirection.DOWN);
        }

        protected OptimizerRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return direction == TransformDirection.DOWN
                ? plan.transformDown(typeToken(), this::rule)
                : plan.transformUp(typeToken(), this::rule);
        }

        protected abstract LogicalPlan rule(SubPlan plan);
    }

    public abstract static class OptimizerExpressionRule<E extends Expression> extends Rule<LogicalPlan, LogicalPlan> {

        private final TransformDirection direction;
        // overriding type token which returns the correct class but does an uncheck cast to LogicalPlan due to its generic bound
        // a proper solution is to wrap the Expression rule into a Plan rule but that would affect the rule declaration
        // so instead this is hacked here
        private final Class<E> expressionTypeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        public OptimizerExpressionRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return direction == TransformDirection.DOWN
                ? plan.transformExpressionsDown(expressionTypeToken, this::rule)
                : plan.transformExpressionsUp(expressionTypeToken, this::rule);
        }

        protected LogicalPlan rule(LogicalPlan plan) {
            return plan;
        }

        protected abstract Expression rule(E e);

        public Class<E> expressionToken() {
            return expressionTypeToken;
        }
    }

    public enum TransformDirection {
        UP,
        DOWN
    }
}
