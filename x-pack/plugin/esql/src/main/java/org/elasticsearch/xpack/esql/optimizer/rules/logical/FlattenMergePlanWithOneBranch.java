/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

/**
 * Removes a plain {@link MergePlan} after analyzer and optimizer rules have reduced it to one branch.
 * <p>
 * Analysis keeps that wrapper so {@code Fork.checkForUnseparatedFork} can treat a subquery, view, or LET scope as a merge boundary. Once
 * verification has run, this rule splices the surviving child into the parent, remapping {@link MergePlan#output()} onto the child's
 * attributes so parent references stay valid.
 * <p>
 * The one hard stopper is a VIEW-identity {@link BranchPlan} while a request filter applies to view outputs: {@code Mapper} still needs
 * that node to mark view-branch fragments so the raw DSL is not pushed under the view's STATS/EVAL. A one-child {@code FORK} is unwrapped
 * the same way as a one-child {@code UnionAll}; the surviving branch already carries {@code Eval[forkN AS _fork]}.
 */
public class FlattenMergePlanWithOneBranch extends OptimizerRules.ParameterizedOptimizerRule<MergePlan, LogicalOptimizerContext> {

    public FlattenMergePlanWithOneBranch() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(MergePlan merge, LogicalOptimizerContext context) {
        if (merge.children().size() != 1) {
            return merge;
        }
        if (mustKeepViewBoundary(merge, context)) {
            return merge;
        }
        return flattenUnionAllWithOneChild(merge);
    }

    private static boolean mustKeepViewBoundary(MergePlan merge, LogicalOptimizerContext context) {
        return context.requestFilterAppliesToViewOutputs() && merge instanceof BranchPlan branchPlan && branchPlan.isViewBranch(0);
    }

    /**
     * Project each merge attribute as an {@link Alias} of the child attribute at the same position, keeping the merge {@code NameId}.
     */
    static LogicalPlan flattenUnionAllWithOneChild(MergePlan merge) {
        List<Attribute> mergeOutput = merge.output();
        if (mergeOutput.isEmpty()) {
            return merge;
        }

        LogicalPlan child = merge.children().getFirst();
        List<Attribute> childOutput = child.output();
        if (mergeOutput.size() != childOutput.size()) {
            return merge;
        }

        List<NamedExpression> projections = new ArrayList<>(mergeOutput.size());
        for (int i = 0; i < mergeOutput.size(); i++) {
            Attribute mergeAttr = mergeOutput.get(i);
            Attribute childAttr = childOutput.get(i);
            if (mergeAttr.name().equals(childAttr.name()) == false) {
                return merge;
            }
            projections.add(new Alias(merge.source(), mergeAttr.name(), childAttr, mergeAttr.id(), mergeAttr.synthetic()));
        }
        return new Project(merge.source(), child, projections);
    }
}
