/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.optimizer.OptimizerRules.LogicalPlanDependencyCheck;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public final class LogicalVerifier {

    private static final LogicalPlanDependencyCheck DEPENDENCY_CHECK = new LogicalPlanDependencyCheck();
    public static final LogicalVerifier INSTANCE = new LogicalVerifier();

    private LogicalVerifier() {}

    /** Verifies the optimized logical plan. */
    public Failures verify(LogicalPlan plan) {
        Failures failures = new Failures();
        Failures dependencyFailures = new Failures();

        plan.forEachUp(p -> {
            DEPENDENCY_CHECK.checkPlan(p, dependencyFailures);

            if (failures.hasFailures() == false) {
                p.forEachExpression(ex -> {
                    if (ex instanceof Validatable v) {
                        v.validate(failures);
                    }
                });
            }
        });

        if (dependencyFailures.hasFailures()) {
            throw new IllegalStateException(dependencyFailures.toString());
        }

        return failures;
    }
}
