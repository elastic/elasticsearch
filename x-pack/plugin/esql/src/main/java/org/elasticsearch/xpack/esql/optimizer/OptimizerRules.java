/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.plan.QueryPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.RegexExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitOr;

class OptimizerRules {

    private OptimizerRules() {}

    static class DependencyConsistency<P extends QueryPlan<P>> {

        void checkPlan(P p, Failures failures) {
            AttributeSet refs = references(p);
            AttributeSet input = p.inputSet();
            AttributeSet generated = generates(p);
            AttributeSet missing = refs.subtract(input).subtract(generated);
            if (missing.size() > 0) {
                failures.add(fail(p, "Plan [{}] optimized incorrectly due to missing references {}", p.nodeString(), missing));
            }
        }

        protected AttributeSet references(P p) {
            return p.references();
        }

        protected AttributeSet generates(P p) {
            return AttributeSet.EMPTY;
        }
    }

    static class LogicalPlanDependencyCheck extends DependencyConsistency<LogicalPlan> {
        @Override
        protected AttributeSet references(LogicalPlan plan) {
            if (plan instanceof Enrich enrich) {
                // The enrichFields are NamedExpressions, so we compute their references as well when just calling enrich.references().
                // But they are not actually referring to attributes from the input plan - only the match field does.
                return enrich.matchField().references();
            }
            return super.references(plan);
        }

        @Override
        protected AttributeSet generates(LogicalPlan logicalPlan) {
            // source-like operators
            if (logicalPlan instanceof EsRelation
                || logicalPlan instanceof LocalRelation
                || logicalPlan instanceof Row
                || logicalPlan instanceof Aggregate) {
                return logicalPlan.outputSet();
            }
            if (logicalPlan instanceof Eval eval) {
                return new AttributeSet(Expressions.asAttributes(eval.fields()));
            }
            if (logicalPlan instanceof RegexExtract extract) {
                return new AttributeSet(extract.extractedFields());
            }
            if (logicalPlan instanceof MvExpand mvExpand) {
                return new AttributeSet(mvExpand.expanded());
            }
            if (logicalPlan instanceof Enrich enrich) {
                return new AttributeSet(Expressions.asAttributes(enrich.enrichFields()));
            }

            return AttributeSet.EMPTY;
        }
    }

    static class PhysicalPlanDependencyCheck extends DependencyConsistency<PhysicalPlan> {
        @Override
        protected AttributeSet generates(PhysicalPlan physicalPlan) {
            // source-like operators
            if (physicalPlan instanceof EsSourceExec
                || physicalPlan instanceof EsStatsQueryExec
                || physicalPlan instanceof EsQueryExec
                || physicalPlan instanceof LocalSourceExec
                || physicalPlan instanceof RowExec
                || physicalPlan instanceof ExchangeExec
                || physicalPlan instanceof ExchangeSourceExec
                || physicalPlan instanceof AggregateExec
                || physicalPlan instanceof ShowExec) {
                return physicalPlan.outputSet();
            }

            if (physicalPlan instanceof FieldExtractExec fieldExtractExec) {
                return new AttributeSet(fieldExtractExec.attributesToExtract());
            }
            if (physicalPlan instanceof EvalExec eval) {
                return new AttributeSet(Expressions.asAttributes(eval.fields()));
            }
            if (physicalPlan instanceof RegexExtractExec extract) {
                return new AttributeSet(extract.extractedFields());
            }
            if (physicalPlan instanceof MvExpandExec mvExpand) {
                return new AttributeSet(mvExpand.expanded());
            }
            if (physicalPlan instanceof EnrichExec enrich) {
                return new AttributeSet(Expressions.asAttributes(enrich.enrichFields()));
            }

            return AttributeSet.EMPTY;
        }

        @Override
        protected AttributeSet references(PhysicalPlan plan) {
            if (plan instanceof AggregateExec aggregate) {
                if (aggregate.getMode() == AggregateExec.Mode.FINAL) {
                    // lousy hack - need to generate the intermediate aggs yet the intermediateAggs method keep creating new IDs on each
                    // call
                    // in practice, the final aggregate should clearly declare the expected properties not hold on the original ones
                    // as they no longer apply
                    return aggregate.inputSet();
                }
            }
            return plan.references();
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
    public static class CombineDisjunctionsToIn extends org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.OptimizerExpressionRule<
        Or> {
        CombineDisjunctionsToIn() {
            super(org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.TransformDirection.UP);
        }

        protected In createIn(Expression key, List<Expression> values, ZoneId zoneId) {
            return new In(key.source(), key, values);
        }

        protected Equals createEquals(Expression k, Set<Expression> v, ZoneId finalZoneId) {
            return new Equals(k.source(), k, v.iterator().next(), finalZoneId);
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

                // TODO: this makes a QL `or`, not an ESQL `or`
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
    }

    /**
     * This rule must always be placed after {@link org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.LiteralsOnTheRight}
     * since it looks at TRUE/FALSE literals' existence on the right hand-side of the {@link Equals}/{@link NotEquals} expressions.
     */
    public static final class BooleanFunctionEqualsElimination extends
        org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.OptimizerExpressionRule<BinaryComparison> {

        BooleanFunctionEqualsElimination() {
            super(org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected Expression rule(BinaryComparison bc) {
            if ((bc instanceof Equals || bc instanceof NotEquals) && bc.left() instanceof Function) {
                // for expression "==" or "!=" TRUE/FALSE, return the expression itself or its negated variant

                // TODO: Replace use of QL Not with ESQL Not
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

    /**
     * Propagate Equals to eliminate conjuncted Ranges or BinaryComparisons.
     * When encountering a different Equals, non-containing {@link Range} or {@link BinaryComparison}, the conjunction becomes false.
     * When encountering a containing {@link Range}, {@link BinaryComparison} or {@link NotEquals}, these get eliminated by the equality.
     *
     * Since this rule can eliminate Ranges and BinaryComparisons, it should be applied before
     * {@link CombineBinaryComparisons}.
     *
     * This rule doesn't perform any promotion of {@link BinaryComparison}s, that is handled by
     * {@link CombineBinaryComparisons} on purpose as the resulting Range might
     * be foldable (which is picked by the folding rule on the next run).
     */
    public static final class PropagateEquals extends org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.OptimizerExpressionRule<
        BinaryLogic> {

        PropagateEquals() {
            super(org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.TransformDirection.DOWN);
        }

        public Expression rule(BinaryLogic e) {
            if (e instanceof And) {
                return propagate((And) e);
            } else if (e instanceof Or) {
                return propagate((Or) e);
            }
            return e;
        }

        // combine conjunction
        private static Expression propagate(And and) {
            List<Range> ranges = new ArrayList<>();
            // Only equalities, not-equalities and inequalities with a foldable .right are extracted separately;
            // the others go into the general 'exps'.
            // TODO: In 105217, this should change to EsqlBinaryComparison, but it doesn't exist in this branch yet
            List<BinaryComparison> equals = new ArrayList<>();
            List<NotEquals> notEquals = new ArrayList<>();
            List<BinaryComparison> inequalities = new ArrayList<>();
            List<Expression> exps = new ArrayList<>();

            boolean changed = false;

            for (Expression ex : Predicates.splitAnd(and)) {
                if (ex instanceof Range) {
                    ranges.add((Range) ex);
                } else if (ex instanceof Equals otherEq) {
                    // equals on different values evaluate to FALSE
                    // ignore date/time fields as equality comparison might actually be a range check
                    if (otherEq.right().foldable() && DataTypes.isDateTime(otherEq.left().dataType()) == false) {
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
                } else if (ex instanceof GreaterThan
                    || ex instanceof GreaterThanOrEqual
                    || ex instanceof LessThan
                    || ex instanceof LessThanOrEqual) {
                        BinaryComparison bc = (BinaryComparison) ex;
                        if (bc.right().foldable()) {
                            inequalities.add(bc);
                        } else {
                            exps.add(ex);
                        }
                    } else if (ex instanceof NotEquals otherNotEq) {
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

                for (Iterator<Range> iterator = ranges.iterator(); iterator.hasNext();) {
                    Range range = iterator.next();

                    if (range.value().semanticEquals(eq.left())) {
                        // if equals is outside the interval, evaluate the whole expression to FALSE
                        if (range.lower().foldable()) {
                            Integer compare = BinaryComparison.compare(range.lower().fold(), eqValue);
                            if (compare != null && (
                            // eq outside the lower boundary
                            compare > 0 ||
                            // eq matches the boundary but should not be included
                                (compare == 0 && range.includeLower() == false))) {
                                return new Literal(and.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                            }
                        }
                        if (range.upper().foldable()) {
                            Integer compare = BinaryComparison.compare(range.upper().fold(), eqValue);
                            if (compare != null && (
                            // eq outside the upper boundary
                            compare < 0 ||
                            // eq matches the boundary but should not be included
                                (compare == 0 && range.includeUpper() == false))) {
                                return new Literal(and.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                            }
                        }

                        // it's in the range and thus, remove it
                        iterator.remove();
                        changed = true;
                    }
                }

                // evaluate all NotEquals against the Equal
                for (Iterator<NotEquals> iter = notEquals.iterator(); iter.hasNext();) {
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
                for (Iterator<BinaryComparison> iter = inequalities.iterator(); iter.hasNext();) {
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
        private static Expression propagate(Or or) {
            List<Expression> exps = new ArrayList<>();
            List<Equals> equals = new ArrayList<>(); // foldable right term Equals
            List<NotEquals> notEquals = new ArrayList<>(); // foldable right term NotEquals
            List<Range> ranges = new ArrayList<>();
            List<BinaryComparison> inequalities = new ArrayList<>(); // foldable right term (=limit) BinaryComparision

            // split expressions by type
            for (Expression ex : Predicates.splitOr(or)) {
                if (ex instanceof Equals eq) {
                    if (eq.right().foldable()) {
                        equals.add(eq);
                    } else {
                        exps.add(ex);
                    }
                } else if (ex instanceof NotEquals neq) {
                    if (neq.right().foldable()) {
                        notEquals.add(neq);
                    } else {
                        exps.add(ex);
                    }
                } else if (ex instanceof Range) {
                    ranges.add((Range) ex);
                } else if (ex instanceof BinaryComparison bc) {
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
            for (Iterator<Equals> iterEq = equals.iterator(); iterEq.hasNext();) {
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
                for (int i = 0; i < ranges.size(); i++) { // might modify list, so use index loop
                    Range range = ranges.get(i);
                    if (eq.left().semanticEquals(range.value())) {
                        Integer lowerComp = range.lower().foldable() ? BinaryComparison.compare(eqValue, range.lower().fold()) : null;
                        Integer upperComp = range.upper().foldable() ? BinaryComparison.compare(eqValue, range.upper().fold()) : null;

                        if (lowerComp != null && lowerComp == 0) {
                            if (range.includeLower() == false) { // a = 2 OR 2 < a < ? -> 2 <= a < ?
                                ranges.set(
                                    i,
                                    new Range(
                                        range.source(),
                                        range.value(),
                                        range.lower(),
                                        true,
                                        range.upper(),
                                        range.includeUpper(),
                                        range.zoneId()
                                    )
                                );
                            } // else : a = 2 OR 2 <= a < ? -> 2 <= a < ?
                            removeEquals = true; // update range with lower equality instead or simply superfluous
                            break;
                        } else if (upperComp != null && upperComp == 0) {
                            if (range.includeUpper() == false) { // a = 2 OR ? < a < 2 -> ? < a <= 2
                                ranges.set(
                                    i,
                                    new Range(
                                        range.source(),
                                        range.value(),
                                        range.lower(),
                                        range.includeLower(),
                                        range.upper(),
                                        true,
                                        range.zoneId()
                                    )
                                );
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
                for (int i = 0; i < inequalities.size(); i++) {
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

    public static class BinaryComparisonSimplification extends
        org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.OptimizerExpressionRule<BinaryComparison> {

        BinaryComparisonSimplification() {
            super(org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.TransformDirection.DOWN);
        }

        @Override
        public Expression rule(BinaryComparison bc) {
            Expression l = bc.left();
            Expression r = bc.right();

            // true for equality
            if (bc instanceof Equals || bc instanceof GreaterThanOrEqual || bc instanceof LessThanOrEqual) {
                if (l.nullable() == Nullability.FALSE && r.nullable() == Nullability.FALSE && l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.TRUE, DataTypes.BOOLEAN);
                }
            }

            // Null Equals logic removed here, as ESQL doesn't have a null equals

            // false for equality
            if (bc instanceof NotEquals || bc instanceof GreaterThan || bc instanceof LessThan) {
                if (l.nullable() == Nullability.FALSE && r.nullable() == Nullability.FALSE && l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                }
            }

            return bc;
        }
    }

    public static final class CombineBinaryComparisons extends
        org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.OptimizerExpressionRule<BinaryLogic> {

        CombineBinaryComparisons() {
            super(org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.TransformDirection.DOWN);
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
        private static Expression combine(And and) {
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
                if (ex instanceof Range r) {
                    if (findExistingRange(r, ranges, true)) {
                        changed = true;
                    } else {
                        ranges.add(r);
                    }
                } else if (ex instanceof BinaryComparison bc && (ex instanceof Equals || ex instanceof NotEquals) == false) {

                    if (bc.right().foldable() && (findConjunctiveComparisonInRange(bc, ranges) || findExistingComparison(bc, bcs, true))) {
                        changed = true;
                    } else {
                        bcs.add(bc);
                    }
                } else if (ex instanceof NotEquals neq) {
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

                            ranges.add(
                                new Range(
                                    and.source(),
                                    main.left(),
                                    main.right(),
                                    main instanceof GreaterThanOrEqual,
                                    other.right(),
                                    other instanceof LessThanOrEqual,
                                    main.zoneId()
                                )
                            );

                            changed = true;
                            step = 0;
                            break;
                        }
                        // </<= AND >/>=
                        else if ((other instanceof GreaterThan || other instanceof GreaterThanOrEqual)
                            && (main instanceof LessThan || main instanceof LessThanOrEqual)) {
                                bcs.remove(j);
                                bcs.remove(i);

                                ranges.add(
                                    new Range(
                                        and.source(),
                                        main.left(),
                                        other.right(),
                                        other instanceof GreaterThanOrEqual,
                                        main.right(),
                                        main instanceof LessThanOrEqual,
                                        main.zoneId()
                                    )
                                );

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
        private static Expression combine(Or or) {
            List<BinaryComparison> bcs = new ArrayList<>();
            List<Range> ranges = new ArrayList<>();
            List<Expression> exps = new ArrayList<>();

            boolean changed = false;

            for (Expression ex : Predicates.splitOr(or)) {
                if (ex instanceof Range r) {
                    if (findExistingRange(r, ranges, false)) {
                        changed = true;
                    } else {
                        ranges.add(r);
                    }
                } else if (ex instanceof BinaryComparison bc) {
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
                            ranges.set(
                                i,
                                new Range(
                                    main.source(),
                                    main.value(),
                                    lower ? main.lower() : other.lower(),
                                    lower ? main.includeLower() : other.includeLower(),
                                    upper ? main.upper() : other.upper(),
                                    upper ? main.includeUpper() : other.includeUpper(),
                                    main.zoneId()
                                )
                            );
                        }

                        // range was comparable
                        return compared;
                    }
                    // OR - needs both upper and lower to loosen range
                    else {
                        // can loosen range
                        if (lower && upper) {
                            ranges.set(
                                i,
                                new Range(
                                    main.source(),
                                    main.value(),
                                    main.lower(),
                                    main.includeLower(),
                                    main.upper(),
                                    main.includeUpper(),
                                    main.zoneId()
                                )
                            );
                            return true;
                        }

                        // if the range in included, no need to add it
                        return compared && (((lower && lowerEq == false) || (upper && upperEq == false)) == false);
                    }
                }
            }
            return false;
        }

        private static boolean findConjunctiveComparisonInRange(BinaryComparison main, List<Range> ranges) {
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
                                    ranges.set(
                                        i,
                                        new Range(
                                            other.source(),
                                            other.value(),
                                            main.right(),
                                            lowerEq ? false : main instanceof GreaterThanOrEqual,
                                            other.upper(),
                                            other.includeUpper(),
                                            other.zoneId()
                                        )
                                    );
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
                                    ranges.set(
                                        i,
                                        new Range(
                                            other.source(),
                                            other.value(),
                                            other.lower(),
                                            other.includeLower(),
                                            main.right(),
                                            upperEq ? false : main instanceof LessThanOrEqual,
                                            other.zoneId()
                                        )
                                    );
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
                if ((other instanceof GreaterThan || other instanceof GreaterThanOrEqual)
                    && (main instanceof GreaterThan || main instanceof GreaterThanOrEqual)) {

                    if (main.left().semanticEquals(other.left())) {
                        Integer compare = BinaryComparison.compare(value, other.right().fold());

                        if (compare != null) {
                            // AND
                            if ((conjunctive &&
                            // a > 3 AND a > 2 -> a > 3
                                (compare > 0 ||
                                // a > 2 AND a >= 2 -> a > 2
                                    (compare == 0 && main instanceof GreaterThan && other instanceof GreaterThanOrEqual))) ||
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
                else if ((other instanceof LessThan || other instanceof LessThanOrEqual)
                    && (main instanceof LessThan || main instanceof LessThanOrEqual)) {

                        if (main.left().semanticEquals(other.left())) {
                            Integer compare = BinaryComparison.compare(value, other.right().fold());

                            if (compare != null) {
                                // AND
                                if ((conjunctive &&
                                // a < 2 AND a < 3 -> a < 2
                                    (compare < 0 ||
                                // a < 2 AND a <= 2 -> a < 2
                                        (compare == 0 && main instanceof LessThan && other instanceof LessThanOrEqual))) ||
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
            for (int i = 0; i < ranges.size(); i++) {
                Range range = ranges.get(i);

                if (notEquals.left().semanticEquals(range.value())) {
                    comp = range.lower().foldable() ? BinaryComparison.compare(neqVal, range.lower().fold()) : null;
                    if (comp != null) {
                        if (comp <= 0) {
                            if (comp == 0 && range.includeLower()) { // a != 2 AND 2 <= a < ? -> 2 < a < ?
                                ranges.set(
                                    i,
                                    new Range(
                                        range.source(),
                                        range.value(),
                                        range.lower(),
                                        false,
                                        range.upper(),
                                        range.includeUpper(),
                                        range.zoneId()
                                    )
                                );
                            }
                            // else: !.includeLower() : a != 2 AND 2 < a < 3 -> 2 < a < 3; or:
                            // else: comp < 0 : a != 2 AND 3 < a < ? -> 3 < a < ?

                            return true;
                        } else { // comp > 0 : a != 4 AND 2 < a < ? : can only remove NotEquals if outside the range
                            comp = range.upper().foldable() ? BinaryComparison.compare(neqVal, range.upper().fold()) : null;
                            if (comp != null && comp >= 0) {
                                if (comp == 0 && range.includeUpper()) { // a != 4 AND 2 < a <= 4 -> 2 < a < 4
                                    ranges.set(
                                        i,
                                        new Range(
                                            range.source(),
                                            range.value(),
                                            range.lower(),
                                            range.includeLower(),
                                            range.upper(),
                                            false,
                                            range.zoneId()
                                        )
                                    );
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
                            ranges.set(
                                i,
                                new Range(
                                    range.source(),
                                    range.value(),
                                    range.lower(),
                                    range.includeLower(),
                                    range.upper(),
                                    false,
                                    range.zoneId()
                                )
                            );
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
            for (int i = 0; i < bcs.size(); i++) {
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
}
