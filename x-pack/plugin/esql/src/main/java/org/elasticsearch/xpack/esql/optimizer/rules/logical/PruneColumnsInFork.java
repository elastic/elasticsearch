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
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.isLocalEmptyRelation;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneEmptyPlans.skipPlan;

/**
 * Remove unused columns created in the plan, in fields inside eval or aggregations inside stats.
 */
public final class PruneColumnsInFork extends OptimizerRules.OptimizerRule<Fork> {

    @Override
    protected LogicalPlan rule(Fork plan) {
        if (plan instanceof UnionAll) {
            return plan;
        }
        boolean changed = false;
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (var child : plan.children()) {
            var newChild = pruneSubPlan(child, plan.output().stream().map(NamedExpression::name).collect(Collectors.toSet()), false);
            newChildren.add(newChild);
            if (false == newChild.equals(child)) {
                changed = true;
            }
        }
        if (changed) {
            return new Fork(plan.source(), newChildren, plan.output());
        } else {
            return plan;
        }
    }

    private static LogicalPlan pruneSubPlan(LogicalPlan plan, Set<String> outputNames, boolean inlineJoin) {
        Holder<Boolean> topLevelProject = new Holder<>(false);
        Holder<Boolean> earlyExit = new Holder<>(false);
        return plan.transformDown(p -> {
            if (p instanceof Limit || p instanceof Sample) {
                return p;
            }
            if (p instanceof Project) {
                if (topLevelProject.get()) {
                    earlyExit.set(true);
                } else {
                    topLevelProject.set(true);
                }
            }
            if (earlyExit.get()) {
                return p;
            }
            var recheck = new Holder<Boolean>();
            do {
                recheck.set(false);
                p = switch (p) {
                    case Aggregate agg -> pruneColumnsInAggregate(agg, outputNames, inlineJoin);
                    case InlineJoin inj -> pruneColumnsInInlineJoinRight(inj, outputNames, recheck);
                    case Eval eval -> pruneColumnsInEval(eval, outputNames, recheck);
                    case Project project -> topLevelProject.get() || inlineJoin ? pruneColumnsInProject(project, outputNames) : project;
                    case EsRelation esr -> pruneColumnsInEsRelation(esr, outputNames);
                    default -> p;
                };
            } while (recheck.get());

            outputNames.addAll(p.references().stream().map(NamedExpression::name).collect(Collectors.toSet()));

            // preserve the state before going to the next node
            return p;
        });
    }

    private static LogicalPlan pruneColumnsInAggregate(Aggregate aggregate, Set<String> used, boolean inlineJoin) {
        LogicalPlan p = aggregate;

        var remaining = pruneUnusedAndAddReferences(aggregate.aggregates(), used);

        if (remaining == null) {
            return p;
        }

        if (remaining.isEmpty()) {
            if (inlineJoin) {
                p = skipPlan(aggregate);
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
            // not expecting high groups cardinality, nested loops in lists should be fine, no need for a HashSet
            if (inlineJoin && aggregate.groupings().containsAll(remaining)) {
                // An InlineJoin right-hand side aggregation output had everything pruned, except for (some of the) groupings, which are
                // already part of the IJ output (from the left-hand side): the agg can just be dropped entirely.
                p = skipPlan(aggregate);
            } else { // not an INLINEJOIN or there are actually aggregates to compute
                p = aggregate.with(aggregate.groupings(), remaining);
            }
        }

        return p;
    }

    private static LogicalPlan pruneColumnsInInlineJoinRight(InlineJoin ij, Set<String> used, Holder<Boolean> recheck) {
        LogicalPlan p = ij;

        used.addAll(ij.references().stream().map(NamedExpression::name).collect(Collectors.toSet()));
        var right = pruneSubPlan(ij.right(), used, true);
        if (right.output().isEmpty() || isLocalEmptyRelation(right)) {
            // InlineJoin updates the order of the output, so even if the computation is dropped, the groups need to be pulled to the end.
            // So we keep just the left side of the join (i.e. drop the computations), but place a Project on top to keep the right order.
            List<Attribute> newOutput = new ArrayList<>(ij.output());
            AttributeSet leftOutputSet = ij.left().outputSet();
            newOutput.removeIf(attr -> leftOutputSet.contains(attr) == false);
            p = new Project(ij.source(), ij.left(), newOutput);
            recheck.set(true);
        } else if (right != ij.right()) {
            // if the right side has been updated, replace it
            p = ij.replaceRight(right);
        }

        return p;
    }

    private static LogicalPlan pruneColumnsInEval(Eval eval, Set<String> used, Holder<Boolean> recheck) {
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

    // Note: only run when the Project is a descendent of an InlineJoin.
    private static LogicalPlan pruneColumnsInProject(Project project, Set<String> used) {
        LogicalPlan p = project;

        var remaining = pruneUnusedAndAddReferences(project.projections(), used);
        if (remaining != null) {
            p = remaining.isEmpty() ? skipPlan(project) : new Project(project.source(), project.child(), remaining);
        }

        return p;
    }

    private static LogicalPlan pruneColumnsInEsRelation(EsRelation esr, Set<String> used) {
        LogicalPlan p = esr;

        if (esr.indexMode() == IndexMode.LOOKUP) {
            // Normally, pruning EsRelation has no effect because InsertFieldExtraction only extracts the required fields, anyway.
            // However, InsertFieldExtraction can't be currently used in LOOKUP JOIN right index,
            // it works differently as we extract all fields (other than the join key) that the EsRelation has.
            var remaining = pruneUnusedAndAddReferences(esr.output(), used);
            if (remaining != null) {
                p = new EsRelation(esr.source(), esr.indexPattern(), esr.indexMode(), esr.indexNameWithModes(), remaining);
            }
        }

        return p;
    }

    private static <N extends NamedExpression> List<N> pruneUnusedAndAddReferences(List<N> named, Set<String> used) {
        var clone = new ArrayList<>(named);
        for (var it = clone.listIterator(clone.size()); it.hasPrevious();) {
            N prev = it.previous();
            var attr = prev.toAttribute();
            if (false == used.contains(attr.name())) {
                it.remove();
            } else {
                used.addAll(prev.references().stream().map(NamedExpression::name).toList());
            }
        }
        return clone.size() != named.size() ? clone : null;
    }
}
