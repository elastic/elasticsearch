/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.util.ArrayList;
import java.util.List;

public final class CombineBinaryComparisons extends OptimizerRules.OptimizerExpressionRule<BinaryLogic> {

    public CombineBinaryComparisons() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public Expression rule(BinaryLogic e, LogicalOptimizerContext ctx) {
        if (e instanceof And and) {
            return combine(ctx.foldCtx(), and);
        } else if (e instanceof Or or) {
            return combine(ctx.foldCtx(), or);
        }
        return e;
    }

    // combine conjunction
    private static Expression combine(FoldContext ctx, And and) {
        List<BinaryComparison> bcs = new ArrayList<>();
        List<Expression> exps = new ArrayList<>();
        boolean changed = false;
        List<Expression> andExps = Predicates.splitAnd(and);

        andExps.sort((o1, o2) -> {
            if (o1 instanceof NotEquals && o2 instanceof NotEquals) {
                return 0; // keep NotEquals' order
            } else if (o1 instanceof NotEquals || o2 instanceof NotEquals) {
                return o1 instanceof NotEquals ? 1 : -1; // push NotEquals up
            } else {
                return 0; // keep non-Ranges' and non-NotEquals' order
            }
        });
        for (Expression ex : andExps) {
            if (ex instanceof BinaryComparison bc && (ex instanceof Equals || ex instanceof NotEquals) == false) {
                if (bc.right().foldable() && (findExistingComparison(ctx, bc, bcs, true))) {
                    changed = true;
                } else {
                    bcs.add(bc);
                }
            } else if (ex instanceof NotEquals neq) {
                if (neq.right().foldable() && notEqualsIsRemovableFromConjunction(ctx, neq, bcs)) {
                    // the non-equality can simply be dropped: either superfluous or has been merged with an updated range/inequality
                    changed = true;
                } else { // not foldable OR not overlapping
                    exps.add(ex);
                }
            } else {
                exps.add(ex);
            }
        }
        return changed ? Predicates.combineAnd(CollectionUtils.combine(exps, bcs)) : and;
    }

    // combine disjunction
    private static Expression combine(FoldContext ctx, Or or) {
        List<BinaryComparison> bcs = new ArrayList<>();
        List<Expression> exps = new ArrayList<>();
        boolean changed = false;
        for (Expression ex : Predicates.splitOr(or)) {
            if (ex instanceof BinaryComparison bc) {
                if (bc.right().foldable() && findExistingComparison(ctx, bc, bcs, false)) {
                    changed = true;
                } else {
                    bcs.add(bc);
                }
            } else {
                exps.add(ex);
            }
        }
        return changed ? Predicates.combineOr(CollectionUtils.combine(exps, bcs)) : or;
    }

    /**
     * Find commonalities between the given comparison in the given list.
     * The method can be applied both for conjunctive (AND) or disjunctive purposes (OR).
     */
    private static boolean findExistingComparison(FoldContext ctx, BinaryComparison main, List<BinaryComparison> bcs, boolean conjunctive) {
        Object value = main.right().fold(ctx);
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
                    Integer compare = BinaryComparison.compare(value, other.right().fold(ctx));
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
                        Integer compare = BinaryComparison.compare(value, other.right().fold(ctx));
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

    private static boolean notEqualsIsRemovableFromConjunction(FoldContext ctx, NotEquals notEquals, List<BinaryComparison> bcs) {
        Object neqVal = notEquals.right().fold(ctx);
        Integer comp;

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
                    comp = bc.right().foldable() ? BinaryComparison.compare(neqVal, bc.right().fold(ctx)) : null;
                    if (comp != null) {
                        if (comp >= 0) {
                            if (comp == 0 && bc instanceof LessThanOrEqual) { // a != 2 AND a <= 2 -> a < 2
                                bcs.set(i, new LessThan(bc.source(), bc.left(), bc.right(), bc.zoneId()));
                            } // else : comp > 0 (a != 2 AND a </<= 1 -> a </<= 1), or == 0 && bc i.of "<" (a != 2 AND a < 2 -> a < 2)
                            return true;
                        } // else: comp < 0 : a != 2 AND a </<= 3 -> nop
                    } // else: non-comparable, nop
                } else if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) {
                    comp = bc.right().foldable() ? BinaryComparison.compare(neqVal, bc.right().fold(ctx)) : null;
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
