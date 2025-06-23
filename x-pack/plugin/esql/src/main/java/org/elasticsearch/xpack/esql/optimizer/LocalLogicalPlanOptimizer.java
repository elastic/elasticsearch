/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateEmptyRelation;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStatsFilteredAggWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStringCasingWithInsensitiveRegexMatch;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferIsNotNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferNonNullAggConstraint;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.LocalPropagateEmptyRelation;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceFieldWithConstantOrNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceTopNWithLimitAndSort;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.cleanup;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.operators;

/**
 * This class is part of the planner. Data node level logical optimizations.  At this point we have access to
 * {@link org.elasticsearch.xpack.esql.stats.SearchStats} which provides access to metadata about the index.
 *
 * <p>NB: This class also reapplies all the rules from {@link LogicalPlanOptimizer#operators()} and {@link LogicalPlanOptimizer#cleanup()}
 */
public class LocalLogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LocalLogicalOptimizerContext> {

    private static final List<Batch<LogicalPlan>> RULES = arrayAsArrayList(
        new Batch<>(
            "Local rewrite",
            Limiter.ONCE,
            new ReplaceTopNWithLimitAndSort(),
            new ReplaceFieldWithConstantOrNull(),
            new InferIsNotNull(),
            new InferNonNullAggConstraint()
        ),
        localOperators(),
        cleanup()
    );

    public LocalLogicalPlanOptimizer(LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        super(localLogicalOptimizerContext);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        return RULES;
    }

    @SuppressWarnings("unchecked")
    private static Batch<LogicalPlan> localOperators() {
        var operators = operators();
        var rules = operators().rules();
        List<Rule<?, LogicalPlan>> newRules = new ArrayList<>(rules.length);

        // apply updates to existing rules that have different applicability locally
        for (var r : rules) {
            switch (r) {
                case PropagateEmptyRelation ignoredPropagate -> newRules.add(new LocalPropagateEmptyRelation());
                // skip it: once a fragment contains an Agg, this can no longer be pruned, which the rule can do
                case ReplaceStatsFilteredAggWithEval ignoredReplace -> {
                }
                default -> newRules.add(r);
            }
        }

        // add rule that should only apply locally
        newRules.add(new ReplaceStringCasingWithInsensitiveRegexMatch());

        return operators.with(newRules.toArray(Rule[]::new));
    }

    public LogicalPlan localOptimize(LogicalPlan plan) {
        return execute(plan);
    }
}
