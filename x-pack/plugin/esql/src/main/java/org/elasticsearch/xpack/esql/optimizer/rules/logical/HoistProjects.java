/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.rule.Rule;

/**
 * Moves every {@link Project} up past the streaming nodes above it in a single bottom-up pass. The top-down rules each move a
 * {@code Project} one node per batch iteration, so a long chain of {@code LOOKUP JOIN} blocks would otherwise need one
 * iteration per block and can exceed the batch's rule execution limit.
 * <p>
 * Each case must stay identical to the {@code Project} step of that node's own rule; only the bottom-up direction differs.
 */
public final class HoistProjects extends OptimizerRules.OptimizerRule<LogicalPlan> implements OptimizerRules.LocalAware<LogicalPlan> {

    private final PushDownJoinPastProject pushDownJoinPastProject = new PushDownJoinPastProject();
    private final PushDownEval pushDownEval = new PushDownEval();
    private final CombineProjections combineProjections;

    public HoistProjects() {
        this(false);
    }

    private HoistProjects(boolean local) {
        super(OptimizerRules.TransformDirection.UP);
        this.combineProjections = new CombineProjections(local);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return switch (plan) {
            case Join join when join.left() instanceof Project -> pushDownJoinPastProject.rule(join);
            case Project project when project.child() instanceof Project -> combineProjections.rule(project);
            case Eval eval when eval.child() instanceof Project -> pushDownEval.rule(eval);
            case Limit limit when limit.child() instanceof Project project -> project.replaceChild(limit.replaceChild(project.child()));
            case Filter filter when filter.child() instanceof Project -> PushDownUtils.pushDownPastProject(filter);
            case RegexExtract regex when regex.child() instanceof Project -> PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(regex);
            case Enrich enrich when enrich.child() instanceof Project -> PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(enrich);
            case CompoundOutputEval<?> coe when coe.child() instanceof Project -> PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(
                coe
            );
            case InferencePlan<?> inference when inference.child() instanceof Project -> PushDownUtils
                .pushGeneratingPlanPastProjectAndOrderBy(inference);
            default -> plan;
        };
    }

    @Override
    public Rule<LogicalPlan, LogicalPlan> local() {
        return new HoistProjects(true);
    }
}
