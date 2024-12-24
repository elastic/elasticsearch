/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

/**
 * Remove unused columns created in the plan, in fields inside eval or aggregations inside stats.
 */
public final class PruneColumns extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        // track used references
        var used = plan.outputSet();
        // while going top-to-bottom (upstream)
        var pl = plan.transformDown(p -> {
            // Note: It is NOT required to do anything special for binary plans like JOINs. It is perfectly fine that transformDown descends
            // first into the left side, adding all kinds of attributes to the `used` set, and then descends into the right side - even
            // though the `used` set will contain stuff only used in the left hand side. That's because any attribute that is used in the
            // left hand side must have been created in the left side as well. Even field attributes belonging to the same index fields will
            // have different name ids in the left and right hand sides - as in the extreme example
            // `FROM lookup_idx | LOOKUP JOIN lookup_idx ON key_field`.

            // skip nodes that simply pass the input through
            if (p instanceof Limit) {
                return p;
            }

            // remember used
            boolean recheck;
            // analyze the unused items against dedicated 'producer' nodes such as Eval and Aggregate
            // perform a loop to retry checking if the current node is completely eliminated
            do {
                recheck = false;
                if (p instanceof Aggregate aggregate) {
                    var remaining = removeUnused(aggregate.aggregates(), used);

                    if (remaining != null) {
                        if (remaining.isEmpty()) {
                            // We still need to have a plan that produces 1 row per group.
                            if (aggregate.groupings().isEmpty()) {
                                p = new LocalRelation(
                                    aggregate.source(),
                                    List.of(new EmptyAttribute(aggregate.source())),
                                    LocalSupplier.of(
                                        new Block[] { BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, null, 1) }
                                    )
                                );
                            } else {
                                // Aggs cannot produce pages with 0 columns, so retain one grouping.
                                remaining = List.of(Expressions.attribute(aggregate.groupings().get(0)));
                                p = new Aggregate(
                                    aggregate.source(),
                                    aggregate.child(),
                                    aggregate.aggregateType(),
                                    aggregate.groupings(),
                                    remaining
                                );
                            }
                        } else {
                            p = new Aggregate(
                                aggregate.source(),
                                aggregate.child(),
                                aggregate.aggregateType(),
                                aggregate.groupings(),
                                remaining
                            );
                        }
                    }
                } else if (p instanceof Eval eval) {
                    var remaining = removeUnused(eval.fields(), used);
                    // no fields, no eval
                    if (remaining != null) {
                        if (remaining.isEmpty()) {
                            p = eval.child();
                            recheck = true;
                        } else {
                            p = new Eval(eval.source(), eval.child(), remaining);
                        }
                    }
                } else if (p instanceof EsRelation esRelation && esRelation.indexMode() == IndexMode.LOOKUP) {
                    // Normally, pruning EsRelation has no effect because InsertFieldExtraction only extracts the required fields, anyway.
                    // The field extraction for LOOKUP JOIN works differently, however - we extract all fields (other than the join key)
                    // that the EsRelation has.
                    var remaining = removeUnused(esRelation.output(), used);
                    // TODO: LookupFromIndexOperator cannot handle 0 lookup fields, yet. That means 1 field in total (key field + lookup).
                    // https://github.com/elastic/elasticsearch/issues/118778
                    if (remaining != null && remaining.size() > 1) {
                        p = new EsRelation(esRelation.source(), esRelation.index(), remaining, esRelation.indexMode(), esRelation.frozen());
                    }
                }
            } while (recheck);

            used.addAll(p.references());

            // preserve the state before going to the next node
            return p;
        });

        return pl;
    }

    /**
     * Prunes attributes from the list not found in the given set.
     * Returns null if no changed occurred.
     */
    private static <N extends NamedExpression> List<N> removeUnused(List<N> named, AttributeSet used) {
        var clone = new ArrayList<>(named);
        var it = clone.listIterator(clone.size());

        // due to Eval, go in reverse
        while (it.hasPrevious()) {
            N prev = it.previous();
            if (used.contains(prev.toAttribute()) == false) {
                it.remove();
            } else {
                used.addAll(prev.references());
            }
        }
        return clone.size() != named.size() ? clone : null;
    }
}
