/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;

/**
 * Propagate Equals to eliminate conjuncted Ranges or BinaryComparisons.
 * When encountering a different Equals, non-containing {@link Range} or {@link BinaryComparison}, the conjunction becomes false.
 * When encountering a containing {@link Range}, {@link BinaryComparison} or {@link NotEquals}, these get eliminated by the equality.
 */
public final class PropagateEquals extends OptimizerRules.OptimizerExpressionRule<BinaryLogic> {

    public PropagateEquals() {
        super(OptimizerRules.TransformDirection.DOWN);
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
                if (otherEq.right().foldable() && DataType.isDateTime(otherEq.left().dataType()) == false) {
                    for (BinaryComparison eq : equals) {
                        if (otherEq.left().semanticEquals(eq.left())) {
                            Integer comp = BinaryComparison.compare(eq.right().fold(), otherEq.right().fold());
                            if (comp != null) {
                                // var cannot be equal to two different values at the same time
                                if (comp != 0) {
                                    return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
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
                            return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
                        }
                    }
                    if (range.upper().foldable()) {
                        Integer compare = BinaryComparison.compare(range.upper().fold(), eqValue);
                        if (compare != null && (
                        // eq outside the upper boundary
                        compare < 0 ||
                        // eq matches the boundary but should not be included
                            (compare == 0 && range.includeUpper() == false))) {
                            return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
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
                            return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
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
                                return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
                            }
                        } else if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) { // a = 2 AND a >/>= ?
                            if ((compare == 0 && bc instanceof GreaterThan) || // a = 2 AND a > 2
                                compare < 0) { // a = 2 AND a >/>= 3
                                return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
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
