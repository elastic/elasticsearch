/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.optimizer;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.Functions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.SurrogateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.expression.predicate.regex.StringPattern;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

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
import static org.elasticsearch.xpack.ql.expression.Literal.FALSE;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.inCommon;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitOr;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.subtract;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.ADD;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.DIV;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.MOD;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.MUL;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.SUB;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;


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

    public static final class BooleanSimplification extends OptimizerExpressionRule<ScalarFunction> {

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

        private Expression simplifyAndOr(BinaryPredicate<?, ?, ?, ?> bc) {
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

            if (c instanceof Negatable) {
                return ((Negatable) c).negate();
            }

            if (c instanceof Not) {
                return ((Not) c).field();
            }

            return n;
        }
    }

    public static class BinaryComparisonSimplification extends OptimizerExpressionRule<BinaryComparison> {

        public BinaryComparisonSimplification() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(BinaryComparison bc) {
            Expression l = bc.left();
            Expression r = bc.right();

            // true for equality
            if (bc instanceof Equals || bc instanceof GreaterThanOrEqual || bc instanceof LessThanOrEqual) {
                if (l.nullable() == Nullability.FALSE && r.nullable() == Nullability.FALSE && l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.TRUE, DataTypes.BOOLEAN);
                }
            }
            if (bc instanceof NullEquals) {
                if (l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.TRUE, DataTypes.BOOLEAN);
                }
                if (Expressions.isNull(r)) {
                    return new IsNull(bc.source(), l);
                }
            }

            // false for equality
            if (bc instanceof NotEquals || bc instanceof GreaterThan || bc instanceof LessThan) {
                if (l.nullable() == Nullability.FALSE && r.nullable() == Nullability.FALSE && l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                }
            }

            return bc;
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
     * Propagate Equals to eliminate conjuncted Ranges or BinaryComparisons.
     * When encountering a different Equals, non-containing {@link Range} or {@link BinaryComparison}, the conjunction becomes false.
     * When encountering a containing {@link Range}, {@link BinaryComparison} or {@link NotEquals}, these get eliminated by the equality.
     *
     * Since this rule can eliminate Ranges and BinaryComparisons, it should be applied before {@link CombineBinaryComparisons}.
     *
     * This rule doesn't perform any promotion of {@link BinaryComparison}s, that is handled by
     * {@link CombineBinaryComparisons} on purpose as the resulting Range might be foldable
     * (which is picked by the folding rule on the next run).
     */
    public static final class PropagateEquals extends OptimizerExpressionRule<BinaryLogic> {

        public PropagateEquals() {
            super(TransformDirection.DOWN);
        }

        @Override
        public Expression rule(BinaryLogic e) {
            if (e instanceof And) {
                return propagate((And) e);
            } else if (e instanceof Or) {
                return propagate((Or) e);
            }
            return e;
        }

        // combine conjunction
        private Expression propagate(And and) {
            List<Range> ranges = new ArrayList<>();
            // Only equalities, not-equalities and inequalities with a foldable .right are extracted separately;
            // the others go into the general 'exps'.
            List<BinaryComparison> equals = new ArrayList<>();
            List<NotEquals> notEquals = new ArrayList<>();
            List<BinaryComparison> inequalities = new ArrayList<>();
            List<Expression> exps = new ArrayList<>();

            boolean changed = false;

            for (Expression ex : Predicates.splitAnd(and)) {
                if (ex instanceof Range) {
                    ranges.add((Range) ex);
                } else if (ex instanceof Equals || ex instanceof NullEquals) {
                    BinaryComparison otherEq = (BinaryComparison) ex;
                    // equals on different values evaluate to FALSE
                    if (otherEq.right().foldable()) {
                        for (BinaryComparison eq : equals) {
                            if (otherEq.left().semanticEquals(eq.left())) {
                                    Integer comp = BinaryComparison.compare(eq.right().fold(), otherEq.right().fold());
                                    if (comp != null) {
                                        // var cannot be equal to two different values at the same time
                                        if (comp != 0) {
                                            return new Literal(and.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                                        }
                                    }
                                }
                            }
                        equals.add(otherEq);
                    } else {
                        exps.add(otherEq);
                    }
                } else if (ex instanceof GreaterThan || ex instanceof GreaterThanOrEqual ||
                    ex instanceof LessThan || ex instanceof LessThanOrEqual) {
                    BinaryComparison bc = (BinaryComparison) ex;
                    if (bc.right().foldable()) {
                        inequalities.add(bc);
                    } else {
                        exps.add(ex);
                    }
                } else if (ex instanceof NotEquals) {
                    NotEquals otherNotEq = (NotEquals) ex;
                    if (otherNotEq.right().foldable()) {
                        notEquals.add(otherNotEq);
                    } else {
                        exps.add(ex);
                    }
                } else {
                    exps.add(ex);
                }
            }

            // check
            for (BinaryComparison eq : equals) {
                Object eqValue = eq.right().fold();

                for (Iterator<Range> iterator = ranges.iterator(); iterator.hasNext(); ) {
                    Range range = iterator.next();

                    if (range.value().semanticEquals(eq.left())) {
                        // if equals is outside the interval, evaluate the whole expression to FALSE
                        if (range.lower().foldable()) {
                            Integer compare = BinaryComparison.compare(range.lower().fold(), eqValue);
                            if (compare != null && (
                                 // eq outside the lower boundary
                                 compare > 0 ||
                                 // eq matches the boundary but should not be included
                                 (compare == 0 && range.includeLower() == false))
                            ) {
                                return new Literal(and.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                            }
                        }
                        if (range.upper().foldable()) {
                            Integer compare = BinaryComparison.compare(range.upper().fold(), eqValue);
                            if (compare != null && (
                                 // eq outside the upper boundary
                                 compare < 0 ||
                                 // eq matches the boundary but should not be included
                                 (compare == 0 && range.includeUpper() == false))
                            ) {
                                return new Literal(and.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                            }
                        }

                        // it's in the range and thus, remove it
                        iterator.remove();
                        changed = true;
                    }
                }

                // evaluate all NotEquals against the Equal
                for (Iterator<NotEquals> iter = notEquals.iterator(); iter.hasNext(); ) {
                    NotEquals neq = iter.next();
                    if (eq.left().semanticEquals(neq.left())) {
                        Integer comp = BinaryComparison.compare(eqValue, neq.right().fold());
                        if (comp != null) {
                            if (comp == 0) { // clashing and conflicting: a = 1 AND a != 1
                                return new Literal(and.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                            } else { // clashing and redundant: a = 1 AND a != 2
                                iter.remove();
                                changed = true;
                            }
                        }
                    }
                }

                // evaluate all inequalities against the Equal
                for (Iterator<BinaryComparison> iter = inequalities.iterator(); iter.hasNext(); ) {
                    BinaryComparison bc = iter.next();
                    if (eq.left().semanticEquals(bc.left())) {
                        Integer compare = BinaryComparison.compare(eqValue, bc.right().fold());
                        if (compare != null) {
                            if (bc instanceof LessThan || bc instanceof LessThanOrEqual) { // a = 2 AND a </<= ?
                                if ((compare == 0 && bc instanceof LessThan) || // a = 2 AND a < 2
                                    0 < compare) { // a = 2 AND a </<= 1
                                    return new Literal(and.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                                }
                            } else if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) { // a = 2 AND a >/>= ?
                                if ((compare == 0 && bc instanceof GreaterThan) || // a = 2 AND a > 2
                                    compare < 0) { // a = 2 AND a >/>= 3
                                    return new Literal(and.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                                }
                            }

                            iter.remove();
                            changed = true;
                        }
                    }
                }
            }

            return changed ? Predicates.combineAnd(CollectionUtils.combine(exps, equals, notEquals, inequalities, ranges)) : and;
        }

        // combine disjunction:
        // a = 2 OR a > 3 -> nop; a = 2 OR a > 1 -> a > 1
        // a = 2 OR a < 3 -> a < 3; a = 2 OR a < 1 -> nop
        // a = 2 OR 3 < a < 5 -> nop; a = 2 OR 1 < a < 3 -> 1 < a < 3; a = 2 OR 0 < a < 1 -> nop
        // a = 2 OR a != 2 -> TRUE; a = 2 OR a = 5 -> nop; a = 2 OR a != 5 -> a != 5
        private Expression propagate(Or or) {
            List<Expression> exps = new ArrayList<>();
            List<Equals> equals = new ArrayList<>(); // foldable right term Equals
            List<NotEquals> notEquals = new ArrayList<>(); // foldable right term NotEquals
            List<Range> ranges = new ArrayList<>();
            List<BinaryComparison> inequalities = new ArrayList<>(); // foldable right term (=limit) BinaryComparision

            // split expressions by type
            for (Expression ex : Predicates.splitOr(or)) {
                if (ex instanceof Equals) {
                    Equals eq = (Equals) ex;
                    if (eq.right().foldable()) {
                        equals.add(eq);
                    } else {
                        exps.add(ex);
                    }
                } else if (ex instanceof NotEquals) {
                    NotEquals neq = (NotEquals) ex;
                    if (neq.right().foldable()) {
                        notEquals.add(neq);
                    } else {
                        exps.add(ex);
                    }
                } else if (ex instanceof Range) {
                    ranges.add((Range) ex);
                } else if (ex instanceof BinaryComparison) {
                    BinaryComparison bc = (BinaryComparison) ex;
                    if (bc.right().foldable()) {
                        inequalities.add(bc);
                    } else {
                        exps.add(ex);
                    }
                } else {
                    exps.add(ex);
                }
            }

            boolean updated = false; // has the expression been modified?

            // evaluate the impact of each Equal over the different types of Expressions
            for (Iterator<Equals> iterEq = equals.iterator(); iterEq.hasNext(); ) {
                Equals eq = iterEq.next();
                Object eqValue = eq.right().fold();
                boolean removeEquals = false;

                // Equals OR NotEquals
                for (NotEquals neq : notEquals) {
                    if (eq.left().semanticEquals(neq.left())) { // a = 2 OR a != ? -> ...
                        Integer comp = BinaryComparison.compare(eqValue, neq.right().fold());
                        if (comp != null) {
                            if (comp == 0) { // a = 2 OR a != 2 -> TRUE
                                return TRUE;
                            } else { // a = 2 OR a != 5 -> a != 5
                                removeEquals = true;
                                break;
                            }
                        }
                    }
                }
                if (removeEquals) {
                    iterEq.remove();
                    updated = true;
                    continue;
                }

                // Equals OR Range
                for (int i = 0; i < ranges.size(); i ++) { // might modify list, so use index loop
                    Range range = ranges.get(i);
                    if (eq.left().semanticEquals(range.value())) {
                        Integer lowerComp = range.lower().foldable() ? BinaryComparison.compare(eqValue, range.lower().fold()) : null;
                        Integer upperComp = range.upper().foldable() ? BinaryComparison.compare(eqValue, range.upper().fold()) : null;

                        if (lowerComp != null && lowerComp == 0) {
                            if (range.includeLower() == false) { // a = 2 OR 2 < a < ? -> 2 <= a < ?
                                ranges.set(i, new Range(range.source(), range.value(), range.lower(), true,
                                    range.upper(), range.includeUpper(), range.zoneId()));
                            } // else : a = 2 OR 2 <= a < ? -> 2 <= a < ?
                            removeEquals = true; // update range with lower equality instead or simply superfluous
                            break;
                        } else if (upperComp != null && upperComp == 0) {
                            if (range.includeUpper() == false) { // a = 2 OR ? < a < 2 -> ? < a <= 2
                                ranges.set(i, new Range(range.source(), range.value(), range.lower(), range.includeLower(),
                                    range.upper(), true, range.zoneId()));
                            } // else : a = 2 OR ? < a <= 2 -> ? < a <= 2
                            removeEquals = true; // update range with upper equality instead
                            break;
                        } else if (lowerComp != null && upperComp != null) {
                            if (0 < lowerComp && upperComp < 0) { // a = 2 OR 1 < a < 3
                                removeEquals = true; // equality is superfluous
                                break;
                            }
                        }
                    }
                }
                if (removeEquals) {
                    iterEq.remove();
                    updated = true;
                    continue;
                }

                // Equals OR Inequality
                for (int i = 0; i < inequalities.size(); i ++) {
                    BinaryComparison bc = inequalities.get(i);
                    if (eq.left().semanticEquals(bc.left())) {
                        Integer comp = BinaryComparison.compare(eqValue, bc.right().fold());
                        if (comp != null) {
                            if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) {
                                if (comp < 0) { // a = 1 OR a > 2 -> nop
                                    continue;
                                } else if (comp == 0 && bc instanceof GreaterThan) { // a = 2 OR a > 2 -> a >= 2
                                    inequalities.set(i, new GreaterThanOrEqual(bc.source(), bc.left(), bc.right(), bc.zoneId()));
                                } // else (0 < comp || bc instanceof GreaterThanOrEqual) :
                                // a = 3 OR a > 2 -> a > 2; a = 2 OR a => 2 -> a => 2

                                removeEquals = true; // update range with equality instead or simply superfluous
                                break;
                            } else if (bc instanceof LessThan || bc instanceof LessThanOrEqual) {
                                if (comp > 0) { // a = 2 OR a < 1 -> nop
                                    continue;
                                }
                                if (comp == 0 && bc instanceof LessThan) { // a = 2 OR a < 2 -> a <= 2
                                    inequalities.set(i, new LessThanOrEqual(bc.source(), bc.left(), bc.right(), bc.zoneId()));
                                } // else (comp < 0 || bc instanceof LessThanOrEqual) : a = 2 OR a < 3 -> a < 3; a = 2 OR a <= 2 -> a <= 2
                                removeEquals = true; // update range with equality instead or simply superfluous
                                break;
                            }
                        }
                    }
                }
                if (removeEquals) {
                    iterEq.remove();
                    updated = true;
                }
            }

            return updated ? Predicates.combineOr(CollectionUtils.combine(exps, equals, notEquals, inequalities, ranges)) : or;
        }
    }

    public static final class CombineBinaryComparisons extends OptimizerExpressionRule<BinaryLogic> {

        public CombineBinaryComparisons() {
            super(TransformDirection.DOWN);
        }

        @Override
        public Expression rule(BinaryLogic e) {
            if (e instanceof And) {
                return combine((And) e);
            } else if (e instanceof Or) {
                return combine((Or) e);
            }
            return e;
        }

        // combine conjunction
        private Expression combine(And and) {
            List<Range> ranges = new ArrayList<>();
            List<BinaryComparison> bcs = new ArrayList<>();
            List<Expression> exps = new ArrayList<>();

            boolean changed = false;

            List<Expression> andExps = Predicates.splitAnd(and);
            // Ranges need to show up before BinaryComparisons in list, to allow the latter be optimized away into a Range, if possible.
            // NotEquals need to be last in list, to have a complete set of Ranges (ranges) and BinaryComparisons (bcs) and allow these to
            // optimize the NotEquals away.
            andExps.sort((o1, o2) -> {
                if (o1 instanceof Range && o2 instanceof Range) {
                    return 0; // keep ranges' order
                } else if (o1 instanceof Range || o2 instanceof Range) {
                    return o2 instanceof Range ? 1 : -1; // push Ranges down
                } else if (o1 instanceof NotEquals && o2 instanceof NotEquals) {
                    return 0; // keep NotEquals' order
                } else if (o1 instanceof NotEquals || o2 instanceof NotEquals) {
                    return o1 instanceof NotEquals ? 1 : -1; // push NotEquals up
                } else {
                    return 0; // keep non-Ranges' and non-NotEquals' order
                }
            });
            for (Expression ex : andExps) {
                if (ex instanceof Range) {
                    Range r = (Range) ex;
                    if (findExistingRange(r, ranges, true)) {
                        changed = true;
                    } else {
                        ranges.add(r);
                    }
                } else if (ex instanceof BinaryComparison && (ex instanceof Equals || ex instanceof NotEquals) == false) {
                    BinaryComparison bc = (BinaryComparison) ex;

                    if (bc.right().foldable() && (findConjunctiveComparisonInRange(bc, ranges) || findExistingComparison(bc, bcs, true))) {
                        changed = true;
                    } else {
                        bcs.add(bc);
                    }
                } else if (ex instanceof NotEquals) {
                    NotEquals neq = (NotEquals) ex;
                    if (neq.right().foldable() && notEqualsIsRemovableFromConjunction(neq, ranges, bcs)) {
                        // the non-equality can simply be dropped: either superfluous or has been merged with an updated range/inequality
                        changed = true;
                    } else { // not foldable OR not overlapping
                        exps.add(ex);
                    }
                } else {
                    exps.add(ex);
                }
            }

            // finally try combining any left BinaryComparisons into possible Ranges
            // this could be a different rule but it's clearer here wrt the order of comparisons

            for (int i = 0, step = 1; i < bcs.size() - 1; i += step, step = 1) {
                BinaryComparison main = bcs.get(i);

                for (int j = i + 1; j < bcs.size(); j++) {
                    BinaryComparison other = bcs.get(j);

                    if (main.left().semanticEquals(other.left())) {
                        // >/>= AND </<=
                        if ((main instanceof GreaterThan || main instanceof GreaterThanOrEqual)
                            && (other instanceof LessThan || other instanceof LessThanOrEqual)) {
                            bcs.remove(j);
                            bcs.remove(i);

                            ranges.add(new Range(and.source(), main.left(),
                                main.right(), main instanceof GreaterThanOrEqual,
                                other.right(), other instanceof LessThanOrEqual, main.zoneId()));

                            changed = true;
                            step = 0;
                            break;
                        }
                        // </<= AND >/>=
                        else if ((other instanceof GreaterThan || other instanceof GreaterThanOrEqual)
                            && (main instanceof LessThan || main instanceof LessThanOrEqual)) {
                            bcs.remove(j);
                            bcs.remove(i);

                            ranges.add(new Range(and.source(), main.left(),
                                other.right(), other instanceof GreaterThanOrEqual,
                                main.right(), main instanceof LessThanOrEqual, main.zoneId()));

                            changed = true;
                            step = 0;
                            break;
                        }
                    }
                }
            }


            return changed ? Predicates.combineAnd(CollectionUtils.combine(exps, bcs, ranges)) : and;
        }

        // combine disjunction
        private Expression combine(Or or) {
            List<BinaryComparison> bcs = new ArrayList<>();
            List<Range> ranges = new ArrayList<>();
            List<Expression> exps = new ArrayList<>();

            boolean changed = false;

            for (Expression ex : Predicates.splitOr(or)) {
                if (ex instanceof Range) {
                    Range r = (Range) ex;
                    if (findExistingRange(r, ranges, false)) {
                        changed = true;
                    } else {
                        ranges.add(r);
                    }
                } else if (ex instanceof BinaryComparison) {
                    BinaryComparison bc = (BinaryComparison) ex;
                    if (bc.right().foldable() && findExistingComparison(bc, bcs, false)) {
                        changed = true;
                    } else {
                        bcs.add(bc);
                    }
                } else {
                    exps.add(ex);
                }
            }

            return changed ? Predicates.combineOr(CollectionUtils.combine(exps, bcs, ranges)) : or;
        }

        private static boolean findExistingRange(Range main, List<Range> ranges, boolean conjunctive) {
            if (main.lower().foldable() == false && main.upper().foldable() == false) {
                return false;
            }
            // NB: the loop modifies the list (hence why the int is used)
            for (int i = 0; i < ranges.size(); i++) {
                Range other = ranges.get(i);

                if (main.value().semanticEquals(other.value())) {

                    // make sure the comparison was done
                    boolean compared = false;

                    boolean lower = false;
                    boolean upper = false;
                    // boundary equality (useful to differentiate whether a range is included or not)
                    // and thus whether it should be preserved or ignored
                    boolean lowerEq = false;
                    boolean upperEq = false;

                    // evaluate lower
                    if (main.lower().foldable() && other.lower().foldable()) {
                        compared = true;

                        Integer comp = BinaryComparison.compare(main.lower().fold(), other.lower().fold());
                        // values are comparable
                        if (comp != null) {
                            // boundary equality
                            lowerEq = comp == 0 && main.includeLower() == other.includeLower();
                            // AND
                            if (conjunctive) {
                                // (2 < a < 3) AND (1 < a < 3) -> (2 < a < 3)
                                lower = comp > 0 ||
                                // (2 < a < 3) AND (2 <= a < 3) -> (2 < a < 3)
                                        (comp == 0 && main.includeLower() == false && other.includeLower());
                            }
                            // OR
                            else {
                                // (1 < a < 3) OR (2 < a < 3) -> (1 < a < 3)
                                lower = comp < 0 ||
                                // (2 <= a < 3) OR (2 < a < 3) -> (2 <= a < 3)
                                        (comp == 0 && main.includeLower() && other.includeLower() == false) || lowerEq;
                            }
                        }
                    }
                    // evaluate upper
                    if (main.upper().foldable() && other.upper().foldable()) {
                        compared = true;

                        Integer comp = BinaryComparison.compare(main.upper().fold(), other.upper().fold());
                        // values are comparable
                        if (comp != null) {
                            // boundary equality
                            upperEq = comp == 0 && main.includeUpper() == other.includeUpper();

                            // AND
                            if (conjunctive) {
                                // (1 < a < 2) AND (1 < a < 3) -> (1 < a < 2)
                                upper = comp < 0 ||
                                // (1 < a < 2) AND (1 < a <= 2) -> (1 < a < 2)
                                        (comp == 0 && main.includeUpper() == false && other.includeUpper());
                            }
                            // OR
                            else {
                                // (1 < a < 3) OR (1 < a < 2) -> (1 < a < 3)
                                upper = comp > 0 ||
                                // (1 < a <= 3) OR (1 < a < 3) -> (2 < a < 3)
                                        (comp == 0 && main.includeUpper() && other.includeUpper() == false) || upperEq;
                            }
                        }
                    }

                    // AND - at least one of lower or upper
                    if (conjunctive) {
                        // can tighten range
                        if (lower || upper) {
                            ranges.set(i,
                                    new Range(main.source(), main.value(),
                                            lower ? main.lower() : other.lower(),
                                            lower ? main.includeLower() : other.includeLower(),
                                            upper ? main.upper() : other.upper(),
                                            upper ? main.includeUpper() : other.includeUpper(),
                                            main.zoneId()));
                        }

                        // range was comparable
                        return compared;
                    }
                    // OR - needs both upper and lower to loosen range
                    else {
                        // can loosen range
                        if (lower && upper) {
                            ranges.set(i,
                                    new Range(main.source(), main.value(),
                                            main.lower(),
                                            main.includeLower(),
                                            main.upper(),
                                            main.includeUpper(),
                                            main.zoneId()));
                            return true;
                        }

                        // if the range in included, no need to add it
                        return compared && (((lower && lowerEq == false) || (upper && upperEq == false)) == false);
                    }
                }
            }
            return false;
        }

        private boolean findConjunctiveComparisonInRange(BinaryComparison main, List<Range> ranges) {
            Object value = main.right().fold();

            // NB: the loop modifies the list (hence why the int is used)
            for (int i = 0; i < ranges.size(); i++) {
                Range other = ranges.get(i);

                if (main.left().semanticEquals(other.value())) {

                    if (main instanceof GreaterThan || main instanceof GreaterThanOrEqual) {
                        if (other.lower().foldable()) {
                            Integer comp = BinaryComparison.compare(value, other.lower().fold());
                            if (comp != null) {
                                // 2 < a AND (2 <= a < 3) -> 2 < a < 3
                                boolean lowerEq = comp == 0 && other.includeLower() && main instanceof GreaterThan;
                                // 2 < a AND (1 < a < 3) -> 2 < a < 3
                                boolean lower = comp > 0 || lowerEq;

                                if (lower) {
                                    ranges.set(i,
                                            new Range(other.source(), other.value(),
                                                    main.right(), lowerEq ? false : main instanceof GreaterThanOrEqual,
                                                    other.upper(), other.includeUpper(), other.zoneId()));
                                }

                                // found a match
                                return true;
                            }
                        }
                    } else if (main instanceof LessThan || main instanceof LessThanOrEqual) {
                        if (other.upper().foldable()) {
                            Integer comp = BinaryComparison.compare(value, other.upper().fold());
                            if (comp != null) {
                                // a < 2 AND (1 < a <= 2) -> 1 < a < 2
                                boolean upperEq = comp == 0 && other.includeUpper() && main instanceof LessThan;
                                // a < 2 AND (1 < a < 3) -> 1 < a < 2
                                boolean upper = comp < 0 || upperEq;

                                if (upper) {
                                    ranges.set(i, new Range(other.source(), other.value(),
                                            other.lower(), other.includeLower(),
                                            main.right(), upperEq ? false : main instanceof LessThanOrEqual, other.zoneId()));
                                }

                                // found a match
                                return true;
                            }
                        }
                    }

                    return false;
                }
            }
            return false;
        }

        /**
         * Find commonalities between the given comparison in the given list.
         * The method can be applied both for conjunctive (AND) or disjunctive purposes (OR).
         */
        private static boolean findExistingComparison(BinaryComparison main, List<BinaryComparison> bcs, boolean conjunctive) {
            Object value = main.right().fold();

            // NB: the loop modifies the list (hence why the int is used)
            for (int i = 0; i < bcs.size(); i++) {
                BinaryComparison other = bcs.get(i);
                // skip if cannot evaluate
                if (other.right().foldable() == false) {
                    continue;
                }
                // if bc is a higher/lower value or gte vs gt, use it instead
                if ((other instanceof GreaterThan || other instanceof GreaterThanOrEqual) &&
                    (main instanceof GreaterThan || main instanceof GreaterThanOrEqual)) {

                    if (main.left().semanticEquals(other.left())) {
                        Integer compare = BinaryComparison.compare(value, other.right().fold());

                        if (compare != null) {
                            // AND
                            if ((conjunctive &&
                            // a > 3 AND a > 2 -> a > 3
                                    (compare > 0 ||
                                    // a > 2 AND a >= 2 -> a > 2
                                            (compare == 0 && main instanceof GreaterThan && other instanceof GreaterThanOrEqual)))
                                    ||
                                    // OR
                                    (conjunctive == false &&
                                    // a > 2 OR a > 3 -> a > 2
                                            (compare < 0 ||
                                            // a >= 2 OR a > 2 -> a >= 2
                                  (compare == 0 && main instanceof GreaterThanOrEqual && other instanceof GreaterThan)))) {
                                bcs.remove(i);
                                bcs.add(i, main);
                            }
                            // found a match
                            return true;
                        }

                        return false;
                    }
                }
                // if bc is a lower/higher value or lte vs lt, use it instead
                else if ((other instanceof LessThan || other instanceof LessThanOrEqual) &&
                        (main instanceof LessThan || main instanceof LessThanOrEqual)) {

                            if (main.left().semanticEquals(other.left())) {
                                Integer compare = BinaryComparison.compare(value, other.right().fold());

                                if (compare != null) {
                                    // AND
                                    if ((conjunctive &&
                                    // a < 2 AND a < 3 -> a < 2
                                    (compare < 0 ||
                                    // a < 2 AND a <= 2 -> a < 2
                                  (compare == 0 && main instanceof LessThan && other instanceof LessThanOrEqual)))
                                ||
                                    // OR
                                    (conjunctive == false &&
                                    // a < 2 OR a < 3 -> a < 3
                                    (compare > 0 ||
                                    // a <= 2 OR a < 2 -> a <= 2
                                    (compare == 0 && main instanceof LessThanOrEqual && other instanceof LessThan)))) {
                                        bcs.remove(i);
                                        bcs.add(i, main);

                                    }
                                    // found a match
                                    return true;
                                }

                                return false;
                            }
                        }
            }

            return false;
        }

        private static boolean notEqualsIsRemovableFromConjunction(NotEquals notEquals, List<Range> ranges, List<BinaryComparison> bcs) {
            Object neqVal = notEquals.right().fold();
            Integer comp;

            // check on "condition-overlapping" ranges:
            // a != 2 AND 3 < a < 5 -> 3 < a < 5; a != 2 AND 0 < a < 1 -> 0 < a < 1 (discard NotEquals)
            // a != 2 AND 2 <= a < 3 -> 2 < a < 3; a != 3 AND 2 < a <= 3 -> 2 < a < 3 (discard NotEquals, plus update Range)
            // a != 2 AND 1 < a < 3 -> nop (do nothing)
            for (int i = 0; i < ranges.size(); i ++) {
                Range range = ranges.get(i);

                if (notEquals.left().semanticEquals(range.value())) {
                    comp = range.lower().foldable() ? BinaryComparison.compare(neqVal, range.lower().fold()) : null;
                    if (comp != null) {
                        if (comp <= 0) {
                            if (comp == 0 && range.includeLower()) { // a != 2 AND 2 <= a < ? -> 2 < a < ?
                                ranges.set(i, new Range(range.source(), range.value(), range.lower(), false, range.upper(),
                                    range.includeUpper(), range.zoneId()));
                            }
                            // else: !.includeLower() : a != 2 AND 2 < a < 3 -> 2 < a < 3; or:
                            // else: comp < 0 : a != 2 AND 3 < a < ? ->  3 < a < ?

                            return true;
                        } else { // comp > 0 : a != 4 AND 2 < a < ? : can only remove NotEquals if outside the range
                            comp = range.upper().foldable() ? BinaryComparison.compare(neqVal, range.upper().fold()) : null;
                            if (comp != null && comp >= 0) {
                                if (comp == 0 && range.includeUpper()) { // a != 4 AND 2 < a <= 4 -> 2 < a < 4
                                    ranges.set(i, new Range(range.source(), range.value(), range.lower(), range.includeLower(),
                                        range.upper(), false, range.zoneId()));
                                }
                                // else: !.includeUpper() : a != 4 AND 2 < a < 4 -> 2 < a < 4
                                // else: comp > 0 : a != 4 AND 2 < a < 3 -> 2 < a < 3

                                return true;
                            }
                            // else: comp < 0 : a != 4 AND 2 < a < 5 -> nop; or:
                            // else: comp == null : upper bound not comparable -> nop
                        }
                    } // else: comp == null : lower bound not comparable: evaluate upper bound, in case non-equality value is ">="

                    comp = range.upper().foldable() ? BinaryComparison.compare(neqVal, range.upper().fold()) : null;
                    if (comp != null && comp >= 0) {
                        if (comp == 0 && range.includeUpper()) { // a != 3 AND ?? < a <= 3 -> ?? < a < 3
                            ranges.set(i, new Range(range.source(), range.value(), range.lower(), range.includeLower(), range.upper(),
                                false, range.zoneId()));
                        }
                        // else: !.includeUpper() : a != 3 AND ?? < a < 3 -> ?? < a < 3
                        // else: comp > 0 : a != 3 and ?? < a < 2 -> ?? < a < 2

                        return true;
                    }
                    // else: comp < 0 : a != 3 AND ?? < a < 4 -> nop, as a decision can't be drawn; or:
                    // else: comp == null : a != 3 AND ?? < a < ?? -> nop
                }
            }

            // check on "condition-overlapping" inequalities:
            // a != 2 AND a > 3 -> a > 3 (discard NotEquals)
            // a != 2 AND a >= 2 -> a > 2 (discard NotEquals plus update inequality)
            // a != 2 AND a > 1 -> nop (do nothing)
            //
            // a != 2 AND a < 3 -> nop
            // a != 2 AND a <= 2 -> a < 2
            // a != 2 AND a < 1 -> a < 1
            for (int i = 0; i < bcs.size(); i ++) {
                BinaryComparison bc = bcs.get(i);

                if (notEquals.left().semanticEquals(bc.left())) {
                    if (bc instanceof LessThan || bc instanceof LessThanOrEqual) {
                        comp = bc.right().foldable() ? BinaryComparison.compare(neqVal, bc.right().fold()) : null;
                        if (comp != null) {
                            if (comp >= 0) {
                                if (comp == 0 && bc instanceof LessThanOrEqual) { // a != 2 AND a <= 2 -> a < 2
                                    bcs.set(i, new LessThan(bc.source(), bc.left(), bc.right(), bc.zoneId()));
                                } // else : comp > 0 (a != 2 AND a </<= 1 -> a </<= 1), or == 0 && bc i.of "<" (a != 2 AND a < 2 -> a < 2)
                                return true;
                            } // else: comp < 0 : a != 2 AND a </<= 3 -> nop
                        } // else: non-comparable, nop
                    } else if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) {
                        comp = bc.right().foldable() ? BinaryComparison.compare(neqVal, bc.right().fold()) : null;
                        if (comp != null) {
                            if (comp <= 0) {
                                if (comp == 0 && bc instanceof GreaterThanOrEqual) { // a != 2 AND a >= 2 -> a > 2
                                    bcs.set(i, new GreaterThan(bc.source(), bc.left(), bc.right(), bc.zoneId()));
                                } // else: comp < 0 (a != 2 AND a >/>= 3 -> a >/>= 3), or == 0 && bc i.of ">" (a != 2 AND a > 2 -> a > 2)
                                return true;
                            } // else: comp > 0 : a != 2 AND a >/>= 1 -> nop
                        } // else: non-comparable, nop
                    } // else: other non-relevant type
                }
            }

            return false;
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
                if (exp instanceof Equals) {
                    Equals eq = (Equals) exp;
                    // consider only equals against foldables
                    if (eq.right().foldable()) {
                        found.computeIfAbsent(eq.left(), k -> new LinkedHashSet<>()).add(eq.right());
                    } else {
                        ors.add(exp);
                    }
                    if (zoneId == null) {
                        zoneId = eq.zoneId();
                    }
                } else if (exp instanceof In) {
                    In in = (In) exp;
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
                    (k, v) -> {
                        ors.add(
                            v.size() == 1
                                ? new Equals(k.source(), k, v.iterator().next(), finalZoneId)
                                : createIn(k, new ArrayList<>(v), finalZoneId)
                        );
                    }
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

        protected In createIn(Expression key, List<Expression> values, ZoneId zoneId) {
            return new In(key.source(), key, values, zoneId);
        }
    }

    public static class PushDownAndCombineFilters extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            LogicalPlan plan = filter;
            LogicalPlan child = filter.child();
            Expression condition = filter.condition();

            if (child instanceof Filter) {
                Filter f = (Filter) child;
                plan = f.with(new And(f.source(), f.condition(), condition));
            }
            // as it stands, all other unary plans should allow filters to be pushed down
            else if (child instanceof UnaryPlan) {
                UnaryPlan unary = (UnaryPlan) child;
                // in case of aggregates, worry about filters that contain aggregations
                if (unary instanceof Aggregate && condition.anyMatch(Functions::isAggregate)) {
                    List<Expression> conjunctions = new ArrayList<>(splitAnd(condition));
                    List<Expression> inPlace = new ArrayList<>();
                    // extract all conjunctions containing aggregates
                    for (Iterator<Expression> iterator = conjunctions.iterator(); iterator.hasNext();) {
                        Expression conjunction = iterator.next();
                        if (conjunction.anyMatch(Functions::isAggregate)) {
                            inPlace.add(conjunction);
                            iterator.remove();
                        }
                    }
                    // if at least one expression can be pushed down, update the tree
                    if (conjunctions.size() > 0) {
                        child = unary.replaceChild(filter.with(unary.child(), Predicates.combineAnd(conjunctions)));
                        plan = filter.with(child, Predicates.combineAnd(inPlace));
                    }
                } else {
                    // push down filter
                    plan = unary.replaceChild(filter.with(unary.child(), condition));
                }
            }

            return plan;
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
            if (opSymbol == MOD.symbol()) {
                return comparison;
            }
            OperationSimplifier simplification = null;
            if (isMulOrDiv(opSymbol)) {
                simplification = new MulDivSimplifier(comparison);
            } else if (opSymbol == ADD.symbol() || opSymbol == SUB.symbol()) {
                simplification = new AddSubSimplifier(comparison);
            }

            return (simplification == null || simplification.isUnsafe(typesCompatible)) ? comparison : simplification.apply();
        }

        private static boolean isMulOrDiv(String opSymbol) {
            return opSymbol == MUL.symbol() || opSymbol == DIV.symbol();
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
                //    x + 1e18 > 1e18::long will yield different results with a field value in [-2^6, 2^6], optimised vs original;
                //    x * (1 + 1e-15d) > 1 : same with a field value of (1 - 1e-15d)
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
                    ? postProcess((BinaryComparison) comparison.replaceChildren(asList(opLeft, bcRightExpression)))
                    : comparison;
            }

            // operation-specific operations:
            //  - fast-tracking of simplification unsafety
            abstract boolean isOpUnsafe();

            //  - post optimisation adjustments
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

                if (operation.symbol() == SUB.symbol() && opRight instanceof Literal == false) { // such as: 1 - x > -MAX
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
                isDiv = operation.symbol() == DIV.symbol();
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
                } else if (obj instanceof ArithmeticOperation) {
                    ArithmeticOperation operation = (ArithmeticOperation) obj;
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
            if (binaryLogic instanceof Or) {
                Or or = (Or) binaryLogic;
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
            if (binaryLogic instanceof And) {
                And and = (And) binaryLogic;
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

        @Override
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

    public static class ReplaceRegexMatch extends OptimizerExpressionRule<RegexMatch<?>> {

        public ReplaceRegexMatch() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(RegexMatch<?> regexMatch) {
            Expression e = regexMatch;
            StringPattern pattern = regexMatch.pattern();
            if (pattern.matchesAll()) {
                e = new IsNotNull(e.source(), regexMatch.field());
            } else {
                String match = pattern.exactMatch();
                if (match != null) {
                    Literal literal = new Literal(regexMatch.source(), match, DataTypes.KEYWORD);
                    e = regexToEquals(regexMatch, literal);
                }
            }
            return e;
        }

        protected Expression regexToEquals(RegexMatch<?> regexMatch, Literal literal) {
            return new Equals(regexMatch.source(), regexMatch.field(), literal);
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
        protected Expression rule(And and) {
            List<Expression> splits = Predicates.splitAnd(and);

            Set<Expression> nullExpressions = new LinkedHashSet<>();
            Set<Expression> notNullExpressions = new LinkedHashSet<>();
            List<Expression> others = new LinkedList<>();

            // first find isNull/isNotNull
            for (Expression ex : splits) {
                if (ex instanceof IsNull) {
                    nullExpressions.add(((IsNull) ex).field());
                } else if (ex instanceof IsNotNull) {
                    notNullExpressions.add(((IsNotNull) ex).field());
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
            return exp.nullable() == Nullability.TRUE ? new Literal(exp.source(), null, DataTypes.NULL) : exp;
        }

        // placeholder for non-null
        protected Expression nonNullify(Expression exp, Expression nonNullExp) {
            return exp;
        }
    }

    public static final class SetAsOptimized extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            plan.forEachUp(this::rule);
            return plan;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan.optimized() == false) {
                plan.setOptimized();
            }
            return plan;
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
            return direction == TransformDirection.DOWN ?
                plan.transformDown(typeToken(), this::rule) : plan.transformUp(typeToken(), this::rule);
        }

        @Override
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

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan;
        }

        protected abstract Expression rule(E e);

        public Class<E> expressionToken() {
            return expressionTypeToken;
        }
    }

    public enum TransformDirection {
        UP, DOWN
    }
}
