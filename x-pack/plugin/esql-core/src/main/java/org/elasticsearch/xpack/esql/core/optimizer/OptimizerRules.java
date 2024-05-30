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
import org.elasticsearch.xpack.esql.core.expression.Order;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.SurrogateFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.esql.core.plan.logical.Filter;
import org.elasticsearch.xpack.esql.core.plan.logical.Limit;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.ReflectionUtils;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static java.lang.Math.signum;
import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.inCommon;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitOr;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.subtract;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.ADD;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.DIV;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.MOD;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.MUL;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.SUB;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.util.CollectionUtils.combine;

public final class OptimizerRules {

    public static final class ConstantFolding extends OptimizerExpressionRule<Expression> {

        public ConstantFolding() {
            super(TransformDirection.DOWN);
        }

        @Override
        public Expression rule(Expression e) {
            return e.foldable() ? Literal.of(e) : e;
        }
    }

    /**
     * This rule must always be placed after {@link LiteralsOnTheRight}, since it looks at TRUE/FALSE literals' existence
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
                    return new Literal(bc.source(), Boolean.FALSE, DataTypes.BOOLEAN);
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
                    return new Literal(bc.source(), Boolean.TRUE, DataTypes.BOOLEAN);
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
                return new Literal(n.source(), Boolean.FALSE, DataTypes.BOOLEAN);
            }
            if (FALSE.semanticEquals(c)) {
                return new Literal(n.source(), Boolean.TRUE, DataTypes.BOOLEAN);
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

    public static final class LiteralsOnTheRight extends OptimizerExpressionRule<BinaryOperator<?, ?, ?, ?>> {

        public LiteralsOnTheRight() {
            super(TransformDirection.UP);
        }

        @Override
        public BinaryOperator<?, ?, ?, ?> rule(BinaryOperator<?, ?, ?, ?> be) {
            return be.left() instanceof Literal && (be.right() instanceof Literal) == false ? be.swapLeftAndRight() : be;
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

    // Simplifies arithmetic expressions with BinaryComparisons and fixed point fields, such as: (int + 2) / 3 > 4 => int > 10
    public static final class SimplifyComparisonsArithmetics extends OptimizerExpressionRule<BinaryComparison> {
        BiFunction<DataType, DataType, Boolean> typesCompatible;

        public SimplifyComparisonsArithmetics(BiFunction<DataType, DataType, Boolean> typesCompatible) {
            super(TransformDirection.UP);
            this.typesCompatible = typesCompatible;
        }

        @Override
        protected Expression rule(BinaryComparison bc) {
            // optimize only once the expression has a literal on the right side of the binary comparison
            if (bc.right() instanceof Literal) {
                if (bc.left() instanceof ArithmeticOperation) {
                    return simplifyBinaryComparison(bc);
                }
                if (bc.left() instanceof Neg) {
                    return foldNegation(bc);
                }
            }
            return bc;
        }

        private Expression simplifyBinaryComparison(BinaryComparison comparison) {
            ArithmeticOperation operation = (ArithmeticOperation) comparison.left();
            // Use symbol comp: SQL operations aren't available in this package (as dependencies)
            String opSymbol = operation.symbol();
            // Modulo can't be simplified.
            if (opSymbol.equals(MOD.symbol())) {
                return comparison;
            }
            OperationSimplifier simplification = null;
            if (isMulOrDiv(opSymbol)) {
                simplification = new MulDivSimplifier(comparison);
            } else if (opSymbol.equals(ADD.symbol()) || opSymbol.equals(SUB.symbol())) {
                simplification = new AddSubSimplifier(comparison);
            }

            return (simplification == null || simplification.isUnsafe(typesCompatible)) ? comparison : simplification.apply();
        }

        private static boolean isMulOrDiv(String opSymbol) {
            return opSymbol.equals(MUL.symbol()) || opSymbol.equals(DIV.symbol());
        }

        private static Expression foldNegation(BinaryComparison bc) {
            Literal bcLiteral = (Literal) bc.right();
            Expression literalNeg = tryFolding(new Neg(bcLiteral.source(), bcLiteral));
            return literalNeg == null ? bc : bc.reverse().replaceChildren(asList(((Neg) bc.left()).field(), literalNeg));
        }

        private static Expression tryFolding(Expression expression) {
            if (expression.foldable()) {
                try {
                    expression = new Literal(expression.source(), expression.fold(), expression.dataType());
                } catch (ArithmeticException | DateTimeException e) {
                    // null signals that folding would result in an over-/underflow (such as Long.MAX_VALUE+1); the optimisation is skipped.
                    expression = null;
                }
            }
            return expression;
        }

        private abstract static class OperationSimplifier {
            final BinaryComparison comparison;
            final Literal bcLiteral;
            final ArithmeticOperation operation;
            final Expression opLeft;
            final Expression opRight;
            final Literal opLiteral;

            OperationSimplifier(BinaryComparison comparison) {
                this.comparison = comparison;
                operation = (ArithmeticOperation) comparison.left();
                bcLiteral = (Literal) comparison.right();

                opLeft = operation.left();
                opRight = operation.right();

                if (opLeft instanceof Literal) {
                    opLiteral = (Literal) opLeft;
                } else if (opRight instanceof Literal) {
                    opLiteral = (Literal) opRight;
                } else {
                    opLiteral = null;
                }
            }

            // can it be quickly fast-tracked that the operation can't be reduced?
            final boolean isUnsafe(BiFunction<DataType, DataType, Boolean> typesCompatible) {
                if (opLiteral == null) {
                    // one of the arithm. operands must be a literal, otherwise the operation wouldn't simplify anything
                    return true;
                }

                // Only operations on fixed point literals are supported, since optimizing float point operations can also change the
                // outcome of the filtering:
                // x + 1e18 > 1e18::long will yield different results with a field value in [-2^6, 2^6], optimised vs original;
                // x * (1 + 1e-15d) > 1 : same with a field value of (1 - 1e-15d)
                // so consequently, int fields optimisation requiring FP arithmetic isn't possible either: (x - 1e-15) * (1 + 1e-15) > 1.
                if (opLiteral.dataType().isRational() || bcLiteral.dataType().isRational()) {
                    return true;
                }

                // the Literal will be moved to the right of the comparison, but only if data-compatible with what's there
                if (typesCompatible.apply(bcLiteral.dataType(), opLiteral.dataType()) == false) {
                    return true;
                }

                return isOpUnsafe();
            }

            final Expression apply() {
                // force float point folding for FlP field
                Literal bcl = operation.dataType().isRational()
                    ? Literal.of(bcLiteral, ((Number) bcLiteral.value()).doubleValue())
                    : bcLiteral;

                Expression bcRightExpression = ((BinaryComparisonInversible) operation).binaryComparisonInverse()
                    .create(bcl.source(), bcl, opRight);
                bcRightExpression = tryFolding(bcRightExpression);
                return bcRightExpression != null
                    ? postProcess((BinaryComparison) comparison.replaceChildren(List.of(opLeft, bcRightExpression)))
                    : comparison;
            }

            // operation-specific operations:
            // - fast-tracking of simplification unsafety
            abstract boolean isOpUnsafe();

            // - post optimisation adjustments
            Expression postProcess(BinaryComparison binaryComparison) {
                return binaryComparison;
            }
        }

        private static class AddSubSimplifier extends OperationSimplifier {

            AddSubSimplifier(BinaryComparison comparison) {
                super(comparison);
            }

            @Override
            boolean isOpUnsafe() {
                // no ADD/SUB with floating fields
                if (operation.dataType().isRational()) {
                    return true;
                }

                if (operation.symbol().equals(SUB.symbol()) && opRight instanceof Literal == false) { // such as: 1 - x > -MAX
                    // if next simplification step would fail on overflow anyways, skip the optimisation already
                    return tryFolding(new Sub(EMPTY, opLeft, bcLiteral)) == null;
                }

                return false;
            }
        }

        private static class MulDivSimplifier extends OperationSimplifier {

            private final boolean isDiv; // and not MUL.
            private final int opRightSign; // sign of the right operand in: (left) (op) (right) (comp) (literal)

            MulDivSimplifier(BinaryComparison comparison) {
                super(comparison);
                isDiv = operation.symbol().equals(DIV.symbol());
                opRightSign = sign(opRight);
            }

            @Override
            boolean isOpUnsafe() {
                // Integer divisions are not safe to optimise: x / 5 > 1 <=/=> x > 5 for x in [6, 9]; same for the `==` comp
                if (operation.dataType().isInteger() && isDiv) {
                    return true;
                }

                // If current operation is a multiplication, it's inverse will be a division: safe only if outcome is still integral.
                if (isDiv == false && opLeft.dataType().isInteger()) {
                    long opLiteralValue = ((Number) opLiteral.value()).longValue();
                    return opLiteralValue == 0 || ((Number) bcLiteral.value()).longValue() % opLiteralValue != 0;
                }

                // can't move a 0 in Mul/Div comparisons
                return opRightSign == 0;
            }

            @Override
            Expression postProcess(BinaryComparison binaryComparison) {
                // negative multiplication/division changes the direction of the comparison
                return opRightSign < 0 ? binaryComparison.reverse() : binaryComparison;
            }

            private static int sign(Object obj) {
                int sign = 1;
                if (obj instanceof Number) {
                    sign = (int) signum(((Number) obj).doubleValue());
                } else if (obj instanceof Literal) {
                    sign = sign(((Literal) obj).value());
                } else if (obj instanceof Neg) {
                    sign = -sign(((Neg) obj).field());
                } else if (obj instanceof ArithmeticOperation operation) {
                    if (isMulOrDiv(operation.symbol())) {
                        sign = sign(operation.left()) * sign(operation.right());
                    }
                }
                return sign;
            }
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
                    return new Literal(binaryLogic.source(), null, DataTypes.NULL);
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
                    return new Literal(binaryLogic.source(), null, DataTypes.NULL);
                }
            }
            return binaryLogic;
        }
    }

    public static final class PruneLiteralsInOrderBy extends OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy ob) {
            List<Order> prunedOrders = new ArrayList<>();

            for (Order o : ob.order()) {
                if (o.child().foldable()) {
                    prunedOrders.add(o);
                }
            }

            // everything was eliminated, the order isn't needed anymore
            if (prunedOrders.size() == ob.order().size()) {
                return ob.child();
            }
            if (prunedOrders.size() > 0) {
                List<Order> newOrders = new ArrayList<>(ob.order());
                newOrders.removeAll(prunedOrders);
                return new OrderBy(ob.source(), ob.child(), newOrders);
            }

            return ob;
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
                    return new Literal(e.source(), Boolean.TRUE, DataTypes.BOOLEAN);
                }
            } else if (e instanceof IsNull isn) {
                if (isn.field().nullable() == Nullability.FALSE) {
                    return new Literal(e.source(), Boolean.FALSE, DataTypes.BOOLEAN);
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

    public static final class SetAsOptimized extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            plan.forEachUp(SetAsOptimized::rule);
            return plan;
        }

        private static void rule(LogicalPlan plan) {
            if (plan.optimized() == false) {
                plan.setOptimized();
            }
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
