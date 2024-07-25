/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Range;

import java.util.ArrayList;
import java.util.List;

public final class CombineBinaryComparisons extends OptimizerRules.OptimizerExpressionRule<BinaryLogic> {

    public CombineBinaryComparisons() {
        super(OptimizerRules.TransformDirection.DOWN);

    }

    @Override
    public Expression rule(BinaryLogic e) {
        if (e instanceof And and) {
            return combine(and);
        } else if (e instanceof Or or) {
            return combine(or);
        }
        return e;
    }

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
            if (main.right().foldable() == false || main.left() instanceof StDistance) {
                continue;
            }
            for (int j = i + 1; j < bcs.size(); j++) {
                BinaryComparison other = bcs.get(j);
                if (other.right().foldable() == false || other.left() instanceof StDistance) {
                    continue;
                }
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
    private Expression combine(Or or) {
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
