/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public final class LogicalVerifier {

    public static final LogicalVerifier INSTANCE = new LogicalVerifier();

    private LogicalVerifier() {}

    /** Verifies the optimized logical plan. */
    public Failures verify(LogicalPlan plan) {
        Failures failures = new Failures();
        Failures dependencyFailures = new Failures();

        // AwaitsFix https://github.com/elastic/elasticsearch/issues/118531
        var enriches = plan.collectFirstChildren(Enrich.class::isInstance);
        if (enriches.isEmpty() == false && ((Enrich) enriches.get(0)).mode() == Enrich.Mode.REMOTE) {
            return failures;
        }

        plan.forEachUp(p -> {
            PlanConsistencyChecker.checkPlan(p, dependencyFailures);

            if (failures.hasFailures() == false) {
                if (p instanceof PostOptimizationVerificationAware pova) {
                    pova.postOptimizationVerification(failures);
                }
                p.forEachExpression(ex -> {
                    if (ex instanceof PostOptimizationVerificationAware va) {
                        va.postOptimizationVerification(failures);
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
