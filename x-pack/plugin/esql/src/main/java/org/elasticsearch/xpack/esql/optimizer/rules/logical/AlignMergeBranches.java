/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergeBranchAlignment;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;

/**
 * Materializes the alignment of every {@link MergePlan}'s branches with the merge's output (see {@link MergeBranchAlignment}): a
 * branch that lacks one of the merge's columns has it filled with nulls, and a {@link org.elasticsearch.xpack.esql.plan.logical.Project}
 * puts its columns in the merge's order.
 * <p>
 * The merge's output is what the rest of the plan reads, so the analyzer only needs to keep that correct. Aligning here, at the very
 * start of optimization, means every later optimizer rule and the physical merge see branches that line up by position. Branches
 * that already line up are left alone, so the rule is idempotent. Merges are aligned bottom-up, so a single pass also aligns nested
 * merges: aligning a branch never changes its output, so an outer merge is unaffected by the alignment of an inner one.
 */
public final class AlignMergeBranches extends OptimizerRules.OptimizerRule<MergePlan> {

    public AlignMergeBranches() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(MergePlan merge) {
        return MergeBranchAlignment.alignBranches(merge);
    }
}
