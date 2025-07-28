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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
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
        var used = plan.outputSet().asBuilder();
        // track inlinestats' own aggregation output (right-hand side of the join) so that any other plan on the left-hand side of the
        // inline join won't have its columns pruned due to the lack of "visibility" into the right hand side output/Attributes
        var inlineJoinRightOutput = new ArrayList<Attribute>();
        Holder<Boolean> forkPresent = new Holder<>(false);

        // while going top-to-bottom (upstream)
        var pl = plan.transformDown(p -> {
            // Note: It is NOT required to do anything special for binary plans like JOINs, except INLINESTATS. It is perfectly fine that
            // transformDown descends first into the left side, adding all kinds of attributes to the `used` set, and then descends into
            // the right side - even though the `used` set will contain stuff only used in the left hand side. That's because any attribute
            // that is used in the left hand side must have been created in the left side as well. Even field attributes belonging to the
            // same index fields will have different name ids in the left and right hand sides - as in the extreme example
            // `FROM lookup_idx | LOOKUP JOIN lookup_idx ON key_field`.

            // skip nodes that simply pass the input through
            if (p instanceof Limit) {
                return p;
            }

            if (p instanceof Fork) {
                forkPresent.set(true);
            }
            // pruning columns for Fork branches can have the side effect of having misaligned outputs
            if (forkPresent.get()) {
                return p;
            }

            // TODO: INLINESTATS unit testing for tracking this set
            if (p instanceof InlineJoin ij) {
                inlineJoinRightOutput.addAll(ij.right().outputSet());
            }

            // remember used
            boolean recheck;
            // analyze the unused items against dedicated 'producer' nodes such as Eval and Aggregate
            // perform a loop to retry checking if the current node is completely eliminated
            do {
                recheck = false;
                if (p instanceof Aggregate aggregate) {
                    // TODO: INLINESTATS https://github.com/elastic/elasticsearch/pull/128917#discussion_r2175162099
                    var remaining = removeUnused(aggregate.aggregates(), used, inlineJoinRightOutput);

                    if (remaining != null) {
                        if (remaining.isEmpty()) {
                            // We still need to have a plan that produces 1 row per group.
                            if (aggregate.groupings().isEmpty()) {
                                p = new LocalRelation(
                                    aggregate.source(),
                                    List.of(Expressions.attribute(aggregate.aggregates().getFirst())),
                                    LocalSupplier.of(
                                        new Block[] { BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, null, 1) }
                                    )
                                );
                            } else {
                                // Aggs cannot produce pages with 0 columns, so retain one grouping.
                                Attribute attribute = Expressions.attribute(aggregate.groupings().getFirst());
                                NamedExpression firstAggregate = aggregate.aggregates().getFirst();
                                remaining = List.of(
                                    new Alias(firstAggregate.source(), firstAggregate.name(), attribute, firstAggregate.id())
                                );
                                p = aggregate.with(aggregate.groupings(), remaining);
                            }
                        } else {
                            p = aggregate.with(aggregate.groupings(), remaining);
                        }
                    }
                } else if (p instanceof InlineJoin ij) {// TODO: InlineStats - add unit tests for this IJ removal
                    var remaining = removeUnused(ij.right().output(), used, inlineJoinRightOutput);
                    if (remaining != null) {
                        if (remaining.isEmpty()) {
                            // remove the InlineJoin altogether
                            p = ij.left();
                            recheck = true;
                        }
                        // TODO: InlineStats - prune ONLY the unused output columns from it? In other words, don't perform more aggs
                        // if they will not be used anyway
                    }
                } else if (p instanceof Eval eval) {
                    var remaining = removeUnused(eval.fields(), used, inlineJoinRightOutput);
                    // no fields, no eval
                    if (remaining != null) {
                        if (remaining.isEmpty()) {
                            p = eval.child();
                            recheck = true;
                        } else {
                            p = new Eval(eval.source(), eval.child(), remaining);
                        }
                    }
                } else if (p instanceof EsRelation esr && esr.indexMode() == IndexMode.LOOKUP) {
                    // Normally, pruning EsRelation has no effect because InsertFieldExtraction only extracts the required fields, anyway.
                    // However, InsertFieldExtraction can't be currently used in LOOKUP JOIN right index,
                    // it works differently as we extract all fields (other than the join key) that the EsRelation has.
                    var remaining = removeUnused(esr.output(), used, inlineJoinRightOutput);
                    if (remaining != null) {
                        p = new EsRelation(esr.source(), esr.indexPattern(), esr.indexMode(), esr.indexNameWithModes(), remaining);
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
    private static <N extends NamedExpression> List<N> removeUnused(List<N> named, AttributeSet.Builder used, List<Attribute> exceptions) {
        var clone = new ArrayList<>(named);
        var it = clone.listIterator(clone.size());

        // due to Eval, go in reverse
        while (it.hasPrevious()) {
            N prev = it.previous();
            var attr = prev.toAttribute();
            if (used.contains(attr) == false && exceptions.contains(attr) == false) {
                it.remove();
            } else {
                used.addAll(prev.references());
            }
        }
        return clone.size() != named.size() ? clone : null;
    }
}
