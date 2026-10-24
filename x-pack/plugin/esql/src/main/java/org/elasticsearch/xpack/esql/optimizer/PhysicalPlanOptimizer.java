/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.InsertPartialWindowAggregates;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ReplaceSampledStatsBySampleAndStats;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is part of the planner. Performs global (coordinator) optimization of the physical plan. Local (data-node) optimizations
 * occur later by operating just on a plan {@link FragmentExec} (subplan).
 */
public class PhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, PhysicalOptimizerContext> {

    private final PhysicalVerifier verifier = PhysicalVerifier.INSTANCE;

    private final AtomicBoolean approximationApplied = new AtomicBoolean();

    public PhysicalPlanOptimizer(PhysicalOptimizerContext context) {
        super(context);
    }

    public boolean approximationApplied() {
        return approximationApplied.get();
    }

    public PhysicalPlan optimize(PhysicalPlan plan) {
        return verify(execute(plan), plan.output());
    }

    PhysicalPlan verify(PhysicalPlan optimizedPlan, List<Attribute> expectedOutputAttributes) {
        Failures failures = verifier.verify(optimizedPlan, expectedOutputAttributes);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        return optimizedPlan;
    }

    @Override
    protected List<RuleExecutor.Batch<PhysicalPlan>> batches() {
        return List.of(
            new Batch<>(
                "Plan Boundary",
                Limiter.ONCE,
                new ProjectAwayColumns(),
                new ReplaceSampledStatsBySampleAndStats(() -> approximationApplied.set(true)),
                new InsertPartialWindowAggregates()
            )
        );
    }
}
