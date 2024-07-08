/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Remove unused columns created in the plan, in fields inside eval or aggregations inside stats.
 */
public final class PruneColumns extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        var used = new AttributeSet();
        // don't remove Evals without any Project/Aggregate (which might not occur as the last node in the plan)
        var seenProjection = new Holder<>(Boolean.FALSE);

        // start top-to-bottom
        // and track used references
        var pl = plan.transformDown(p -> {
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
                    var remaining = seenProjection.get() ? removeUnused(aggregate.aggregates(), used) : null;

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

                    seenProjection.set(Boolean.TRUE);
                } else if (p instanceof Eval eval) {
                    var remaining = seenProjection.get() ? removeUnused(eval.fields(), used) : null;
                    // no fields, no eval
                    if (remaining != null) {
                        if (remaining.isEmpty()) {
                            p = eval.child();
                            recheck = true;
                        } else {
                            p = new Eval(eval.source(), eval.child(), remaining);
                        }
                    }
                } else if (p instanceof Project) {
                    seenProjection.set(Boolean.TRUE);
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
     * Returns null if no changes occurred.
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
