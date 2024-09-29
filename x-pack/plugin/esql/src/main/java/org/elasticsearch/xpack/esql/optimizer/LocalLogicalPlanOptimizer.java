/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateEmptyRelation;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferIsNotNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferNonNullAggConstraint;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.LocalPropagateEmptyRelation;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceMissingFieldWithNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceTopNWithLimitAndSort;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.cleanup;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.operators;

/**
 * This class is part of the planner. Data node level logical optimizations.  At this point we have access to
 * {@link org.elasticsearch.xpack.esql.stats.SearchStats} which provides access to metadata about the index.
 *
 * <p>NB: This class also reapplies all the rules from {@link LogicalPlanOptimizer#operators()} and {@link LogicalPlanOptimizer#cleanup()}
 */
public class LocalLogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LocalLogicalOptimizerContext> {

    public LocalLogicalPlanOptimizer(LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        super(localLogicalOptimizerContext);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        var local = new Batch<>(
            "Local rewrite",
            Limiter.ONCE,
            new ReplaceTopNWithLimitAndSort(),
            new ReplaceMissingFieldWithNull(),
            new InferIsNotNull(),
            new InferNonNullAggConstraint()
        );

        var rules = new ArrayList<Batch<LogicalPlan>>();
        rules.add(local);
        // TODO: if the local rules haven't touched the tree, the rest of the rules can be skipped
        rules.addAll(asList(operators(), cleanup()));
        replaceRules(rules);
        return rules;
    }

    private List<Batch<LogicalPlan>> replaceRules(List<Batch<LogicalPlan>> listOfRules) {
        for (Batch<LogicalPlan> batch : listOfRules) {
            var rules = batch.rules();
            for (int i = 0; i < rules.length; i++) {
                if (rules[i] instanceof PropagateEmptyRelation) {
                    rules[i] = new LocalPropagateEmptyRelation();
                }
            }
        }
        return listOfRules;
    }

    public LogicalPlan localOptimize(LogicalPlan plan) {
        return execute(plan);
    }
}
