/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.rules;

public class LocalLogicalPlanOptimizer extends RuleExecutor<LogicalPlan> {
    @Override
    protected List<Batch<LogicalPlan>> batches() {
        // var local = new Batch<>("Local rewrite", new AddExplicitProject());

        var rules = new ArrayList<Batch<LogicalPlan>>();
        // rules.add(local);
        rules.addAll(rules());
        return rules;
    }

    public LogicalPlan localOptimize(LogicalPlan plan) {
        return execute(plan);
    }
}
