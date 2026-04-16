/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;

import java.util.List;

/**
 * Physical plan optimizer for the lookup node. Mirrors {@link LocalPhysicalPlanOptimizer} but with a
 * reduced rule set appropriate for lookup plans (rooted at ParameterizedQueryExec, not EsSourceExec).
 */
public class LookupPhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, LocalPhysicalOptimizerContext> {

    private static final Logger log = LogManager.getLogger(LookupPhysicalPlanOptimizer.class);

    private static final List<Batch<PhysicalPlan>> RULES = List.of(
        new Batch<>("Push to source", new ReplaceSourceAttributes(), new PushFiltersToSource()),
        new Batch<>("Field extraction", Limiter.ONCE, new InsertFieldExtraction())
    );

    private final PhysicalVerifier verifier = PhysicalVerifier.LOCAL_INSTANCE;

    public LookupPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context) {
        super(context);
    }

    @Override
    protected List<Batch<PhysicalPlan>> batches() {
        return RULES;
    }

    public PhysicalPlan optimize(PhysicalPlan plan) {
        PhysicalPlan optimized = execute(plan);
        return verify(optimized, plan.output());
    }

    private PhysicalPlan verify(PhysicalPlan optimizedPlan, List<Attribute> expectedOutputAttributes) {
        Failures failures = verifier.verify(optimizedPlan, expectedOutputAttributes);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        log.debug("Lookup Physical plan:\n{}", optimizedPlan);
        return optimizedPlan;
    }
}
