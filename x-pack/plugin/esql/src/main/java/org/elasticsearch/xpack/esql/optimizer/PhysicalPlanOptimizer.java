/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * This class is part of the planner. Performs global (coordinator) optimization of the physical plan. Local (data-node) optimizations
 * occur later by operating just on a plan {@link FragmentExec} (subplan).
 */
public class PhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, PhysicalOptimizerContext> {
    private static final Iterable<RuleExecutor.Batch<PhysicalPlan>> rules = initializeRules(true);

    private final PhysicalVerifier verifier = PhysicalVerifier.INSTANCE;

    public PhysicalPlanOptimizer(PhysicalOptimizerContext context) {
        super(context);
    }

    public PhysicalPlan optimize(PhysicalPlan plan) {
        return verify(execute(plan));
    }

    PhysicalPlan verify(PhysicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    static List<RuleExecutor.Batch<PhysicalPlan>> initializeRules(boolean isOptimizedForEsSource) {
        var boundary = new Batch<>("Plan Boundary", Limiter.ONCE, new ProjectAwayColumns());
        return asList(boundary);
    }

    @Override
    protected Iterable<RuleExecutor.Batch<PhysicalPlan>> batches() {
        return rules;
    }
}
