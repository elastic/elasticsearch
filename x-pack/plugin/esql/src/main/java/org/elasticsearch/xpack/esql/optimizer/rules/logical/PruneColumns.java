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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
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
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
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
        return pruneColumns(plan, plan.outputSet().asBuilder(), false);
    }

    private static LogicalPlan pruneColumns(LogicalPlan plan, AttributeSet.Builder used, boolean inlineJoin) {
        Holder<Boolean> forkPresent = new Holder<>(false);

        // while going top-to-bottom (upstream)
        return plan.transformDown(p -> {
            // Note: It is NOT required to do anything special for binary plans like JOINs, except INLINESTATS. It is perfectly fine that
            // transformDown descends first into the left side, adding all kinds of attributes to the `used` set, and then descends into
            // the right side - even though the `used` set will contain stuff only used in the left hand side. That's because any attribute
            // that is used in the left hand side must have been created in the left side as well. Even field attributes belonging to the
            // same index fields will have different name ids in the left and right hand sides - as in the extreme example
            // `FROM lookup_idx | LOOKUP JOIN lookup_idx ON key_field`.

            // TODO: revisit with every new command
            // skip nodes that simply pass the input through and use no references
            if (p instanceof Limit || p instanceof Sample) {
                return p;
            }

            if (p instanceof Fork) {
                forkPresent.set(true);
            }
            // pruning columns for Fork branches can have the side effect of having misaligned outputs
            if (forkPresent.get()) {
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
                    case Project project -> inlineJoin ? pruneColumnsInProject(project, used) : p;
                    case EsRelation esr -> pruneColumnsInEsRelation(esr, used);
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
                p = emptyLocalRelation(aggregate);
            } else if (aggregate.groupings().isEmpty()) {
                // We still need to have a plan that produces 1 row per group.
                p = new LocalRelation(
                    aggregate.source(),
                    List.of(Expressions.attribute(aggregate.aggregates().getFirst())),
                    LocalSupplier.of(new Block[] { BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, null, 1) })
                );
            } else {
                // Aggs cannot produce pages with 0 columns, so retain one grouping.
                Attribute attribute = Expressions.attribute(aggregate.groupings().getFirst());
                NamedExpression firstAggregate = aggregate.aggregates().getFirst();
                remaining = List.of(new Alias(firstAggregate.source(), firstAggregate.name(), attribute, firstAggregate.id()));
                p = aggregate.with(aggregate.groupings(), remaining);
            }
        } else {
            // not expecting high groups cardinality, nested loops in lists should be fine, no need for a HashSet
            if (inlineJoin && aggregate.groupings().containsAll(remaining)) {
                // It's an INLINEJOIN and all remaining attributes are groupings, which are already part of the IJ output (from the
                // left-hand side).
                // TODO: INLINESTATS: revisit condition when adding support for INLINESTATS filters
                if (aggregate.child() instanceof StubRelation stub) {
                    var message = "Aggregate groups references ["
                        + remaining
                        + "] not in child's (StubRelation) output: ["
                        + stub.outputSet()
                        + "]";
                    assert stub.outputSet().containsAll(Expressions.asAttributes(remaining)) : message;

                    p = emptyLocalRelation(aggregate);
                } else {
                    // There are no aggregates to compute, just output the groupings; these are already in the IJ output, so only
                    // restrict the output to what remained.
                    p = new Project(aggregate.source(), aggregate.child(), remaining);
                }
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
        if (right.output().isEmpty()) {
            p = ij.left();
            recheck.set(true);
        } else if (right != ij.right()) {
            // if the right side has been updated, replace it
            p = ij.replaceRight(right);
        }

        return p;
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

    private static LogicalPlan pruneColumnsInProject(Project project, AttributeSet.Builder used) {
        LogicalPlan p = project;

        var remaining = pruneUnusedAndAddReferences(project.projections(), used);
        if (remaining != null) {
            p = remaining.isEmpty() || remaining.stream().allMatch(FieldAttribute.class::isInstance)
                ? emptyLocalRelation(project)
                : new Project(project.source(), project.child(), remaining);
        } else if (project.output().stream().allMatch(FieldAttribute.class::isInstance)) {
            // Use empty relation as a marker for a subsequent pass, in case the project is only outputting field attributes (which are
            // already part of the INLINEJOIN left-hand side output).
            p = emptyLocalRelation(project);
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
                p = new EsRelation(esr.source(), esr.indexPattern(), esr.indexMode(), esr.indexNameWithModes(), remaining);
            }
        }

        return p;
    }

    private static LogicalPlan emptyLocalRelation(LogicalPlan plan) {
        // create an empty local relation with no attributes
        return new LocalRelation(plan.source(), List.of(), EmptyLocalSupplier.EMPTY);
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
