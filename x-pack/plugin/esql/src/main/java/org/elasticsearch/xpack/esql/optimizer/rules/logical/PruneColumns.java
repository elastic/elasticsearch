/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
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
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneEmptyPlans.skipPlan;

/**
 * Remove unused columns created in the plan, in fields inside eval or aggregations inside stats.
 */
public final class PruneColumns extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return pruneColumns(plan, plan.outputSet().asBuilder(), false);
    }

    private static LogicalPlan pruneColumns(LogicalPlan plan, AttributeSet.Builder used, boolean inlineJoin) {
        Holder<Boolean> forkPresent = new Holder<>(false);
        // while going top-to-bottom (upstream)
        return plan.transformDown(p -> {
            // Note: It is NOT required to do anything special for binary plans like JOINs, except INLINE STATS. It is perfectly fine that
            // transformDown descends first into the left side, adding all kinds of attributes to the `used` set, and then descends into
            // the right side - even though the `used` set will contain stuff only used in the left hand side. That's because any attribute
            // that is used in the left hand side must have been created in the left side as well. Even field attributes belonging to the
            // same index fields will have different name ids in the left and right hand sides - as in the extreme example
            // `FROM lookup_idx | LOOKUP JOIN lookup_idx ON key_field`.

            if (forkPresent.get()) {
                return p;
            }

            // TODO: revisit with every new command
            // skip nodes that simply pass the input through and use no references
            if (p instanceof Limit || p instanceof Sample) {
                return p;
            }

            var recheck = new Holder<Boolean>();
            // analyze the unused items against dedicated 'producer' nodes such as Eval and Aggregate
            // perform a loop to retry checking if the current node is completely eliminated
            do {
                recheck.set(false);
                p = switch (p) {
                    case Aggregate agg -> pruneColumnsInAggregate(agg, used, inlineJoin);
                    case InlineJoin inj -> pruneColumnsInInlineJoin(inj, used, recheck);
                    case Eval eval -> pruneColumnsInEval(eval, used, recheck);
                    case Project project -> pruneColumnsInProject(project, used, recheck);
                    case EsRelation esr -> pruneColumnsInEsRelation(esr, used);
                    case Fork fork -> {
                        forkPresent.set(true);
                        yield pruneColumnsInFork(fork, used);
                    }
                    default -> p;
                };
            } while (recheck.get());

            used.addAll(p.references());

            // preserve the state before going to the next node
            return p;
        });
    }

    private static LogicalPlan pruneColumnsInAggregate(Aggregate aggregate, AttributeSet.Builder used, boolean inlineJoin) {
        LogicalPlan p = aggregate;
        var remaining = pruneUnusedAndAddReferences(aggregate.aggregates(), used);

        if (remaining == null) {
            return p;
        }
        if (remaining.isEmpty()) {
            if (inlineJoin) {
                // all aggregates are pruned, delegate to child plan
                p = aggregate.child();
            } else if (aggregate.groupings().isEmpty()) {
                // We still need to have a plan that produces 1 row per group.
                p = new LocalRelation(
                    aggregate.source(),
                    List.of(Expressions.attribute(aggregate.aggregates().getFirst())),
                    LocalSupplier.of(new Page(BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, null, 1)))
                );
            } else {
                // Aggs cannot produce pages with 0 columns, so retain one grouping.
                Attribute attribute = Expressions.attribute(aggregate.groupings().getFirst());
                NamedExpression firstAggregate = aggregate.aggregates().getFirst();
                remaining = List.of(new Alias(firstAggregate.source(), firstAggregate.name(), attribute, attribute.id()));
                p = aggregate.with(aggregate.groupings(), remaining);
            }
        } else {
            if (inlineJoin) {
                // An InlineJoin right-hand side aggregation output had everything pruned, except for (some of the) groupings, which are
                // already part of the IJ output (from the left-hand side): the agg can just be dropped entirely.
                if (aggregate.groupings().containsAll(remaining)) {
                    p = aggregate.child();
                }
                // TODO: deal with prunning partial groupings in InlineJoin right side
            } else { // not an INLINEJOIN or there are actually aggregates to compute
                p = aggregate.with(aggregate.groupings(), remaining);
            }
        }
        return p;
    }

    private static LogicalPlan pruneColumnsInInlineJoin(InlineJoin ij, AttributeSet.Builder used, Holder<Boolean> recheck) {
        LogicalPlan p = ij;
        used.addAll(ij.references());
        var right = pruneColumns(ij.right(), used, true);

        if (right.outputSet().subtract(ij.references()).isEmpty() || isLocalEmptyRelation(right)) {
            // ij.references() are the join keys and if the output of the inline join doesn't contain anything else except the join keys,
            // then the inline join doesn't add any new columns. Since it preserves rows, it doesn't do anything and can be pruned.
            p = pruneRightSideAndProject(ij);
            recheck.set(true);
        }

        return p;
    }

    /*
     * InlineJoin updates the order of the output, so even if the right side is dropped, the groups need to be pulled to the end.
     * So we keep just the left side of the join (i.e. drop the right agg), but place a Project on top to keep the correct columns order.
     */
    private static LogicalPlan pruneRightSideAndProject(InlineJoin ij) {
        List<Attribute> newOutput = new ArrayList<>(ij.output());
        AttributeSet leftOutputSet = ij.left().outputSet();
        newOutput.removeIf(attr -> leftOutputSet.contains(attr) == false);
        return new Project(ij.source(), ij.left(), newOutput);
    }

    private static LogicalPlan pruneColumnsInEval(Eval eval, AttributeSet.Builder used, Holder<Boolean> recheck) {
        LogicalPlan p = eval;

        var remaining = pruneUnusedAndAddReferences(eval.fields(), used);
        // no fields, no eval
        if (remaining != null) {
            if (remaining.isEmpty()) {
                p = eval.child();
                recheck.set(true);
            } else {
                p = new Eval(eval.source(), eval.child(), remaining);
            }
        }

        return p;
    }

    private static LogicalPlan pruneColumnsInProject(Project project, AttributeSet.Builder used, Holder<Boolean> recheck) {
        LogicalPlan p = project;

        var remaining = pruneUnusedAndAddReferences(project.projections(), used);
        if (remaining != null) {
            p = new Project(project.source(), project.child(), remaining);
            recheck.set(true);
        }

        return p;
    }

    private static LogicalPlan pruneColumnsInEsRelation(EsRelation esr, AttributeSet.Builder used) {
        LogicalPlan p = esr;

        if (esr.indexMode() == IndexMode.LOOKUP) {
            // Normally, pruning EsRelation has no effect because InsertFieldExtraction only extracts the required fields, anyway.
            // However, InsertFieldExtraction can't be currently used in LOOKUP JOIN right index,
            // it works differently as we extract all fields (other than the join key) that the EsRelation has.
            var remaining = pruneUnusedAndAddReferences(esr.output(), used);
            if (remaining != null) {
                p = esr.withAttributes(remaining);
            }
        }

        return p;
    }

    // TODO: see ResolveUnmapped#patchFork comment
    private static LogicalPlan pruneColumnsInFork(Fork fork, AttributeSet.Builder used) {

        // exit early for UnionAll
        if (fork instanceof UnionAll) {
            return fork;
        }

        // prune the output attributes of fork based on usage from the rest of the plan
        boolean forkOutputChanged = false;
        AttributeSet.Builder builder = AttributeSet.builder();
        // if any of the fork outputs are used, keep them
        // otherwise, prune them based on the rest of the plan's usage
        Set<String> names = new HashSet<>(used.build().names());
        for (var attr : fork.output()) {
            // we should also ensure to keep any synthetic attributes around as those could still be used for internal processing
            if (attr.synthetic() || names.contains(attr.name())) {
                builder.add(attr);
            } else {
                forkOutputChanged = true;
            }
        }
        var prunedForkAttrs = forkOutputChanged ? builder.build().stream().toList() : fork.output();
        // now that we have the pruned fork output attributes, we can proceed to apply pruning all children plan
        var forkOutputNames = prunedForkAttrs.stream().map(NamedExpression::name).collect(Collectors.toSet());
        boolean subPlanChanged = false;
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (var subPlan : fork.children()) {
            var usedAttrs = AttributeSet.builder();
            LogicalPlan newSubPlan;
            // if it's a local relation, just update the output attributes
            // and return early
            if (subPlan instanceof LocalRelation localRelation) {
                var outputAttrs = localRelation.output().stream().filter(x -> forkOutputNames.contains(x.name())).toList();
                newSubPlan = new LocalRelation(localRelation.source(), outputAttrs, localRelation.supplier());
            } else {
                // otherwise, we first prune the projections of the top-level Project of each subplan
                Holder<Boolean> projectVisited = new Holder<>(false);
                newSubPlan = subPlan.transformDown(Project.class, p -> {
                    if (projectVisited.get()) {
                        return p;
                    }
                    projectVisited.set(true);
                    // filter projections based on fork output attributes
                    var prunedAttrs = p.projections().stream().filter(x -> forkOutputNames.contains(x.name())).toList();
                    p = new Project(p.source(), p.child(), prunedAttrs);
                    // add all output attributes to used set
                    usedAttrs.addAll(p.output());
                    return p;
                });
                newSubPlan = pruneColumns(newSubPlan, usedAttrs, false);
            }
            if (false == newSubPlan.equals(subPlan)) {
                subPlanChanged = true;
            }
            newChildren.add(newSubPlan);
        }
        if (subPlanChanged || forkOutputChanged) {
            fork = fork.replaceSubPlansAndOutput(newChildren, prunedForkAttrs);
        }
        return fork;
    }

    private static LogicalPlan emptyLocalRelation(UnaryPlan plan) {
        // create an empty local relation with no attributes
        return skipPlan(plan);
    }

    private static boolean isLocalEmptyRelation(LogicalPlan plan) {
        return plan instanceof LocalRelation local && local.hasEmptySupplier();
    }

    /**
     * Prunes attributes from the `named` list that are not found in the given set (builder).
     * Returns null if no pruning occurred.
     * As a side effect, the references of the kept attributes are added to the input set (builder) -- irrespective of the return value.
     */
    private static <N extends NamedExpression> List<N> pruneUnusedAndAddReferences(List<N> named, AttributeSet.Builder used) {
        var clone = new ArrayList<>(named);

        for (var it = clone.listIterator(clone.size()); it.hasPrevious();) {
            N prev = it.previous();
            var attr = prev.toAttribute();
            if (used.contains(attr)) {
                used.addAll(prev.references());
            } else {
                it.remove();
            }
        }

        return clone.size() != named.size() ? clone : null;
    }
}
