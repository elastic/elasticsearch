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

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.datatypeEquals;

public final class LogicalVerifier {

    public static final LogicalVerifier INSTANCE = new LogicalVerifier();

    private LogicalVerifier() {}

    /** Verifies the optimized logical plan. */
    public Failures verify(LogicalPlan planAfter, boolean skipRemoteEnrichVerification, LogicalPlan planBefore) {
        Failures failures = new Failures();
        Failures dependencyFailures = new Failures();

        if (skipRemoteEnrichVerification) {
            // AwaitsFix https://github.com/elastic/elasticsearch/issues/118531
            var enriches = planAfter.collectFirstChildren(Enrich.class::isInstance);
            if (enriches.isEmpty() == false && ((Enrich) enriches.get(0)).mode() == Enrich.Mode.REMOTE) {
                return failures;
            }
        }

        planAfter.forEachUp(p -> {
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

        if (datatypeEquals(planBefore.output(), planAfter.output()) == false) {
            failures.add(
                fail(planAfter, "Layout has changed from [{}] to [{}]. ", planBefore.output().toString(), planAfter.output().toString())
            );
        }

        if (dependencyFailures.hasFailures()) {
            throw new IllegalStateException(dependencyFailures.toString());
        }

        return failures;
    }
}
