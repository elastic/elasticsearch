/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public final class LogicalVerifier extends PostOptimizationPhasePlanVerifier<LogicalPlan> {
    public static final LogicalVerifier LOCAL_INSTANCE = new LogicalVerifier(true);
    public static final LogicalVerifier INSTANCE = new LogicalVerifier(false);

    private LogicalVerifier(boolean isLocal) {
        super(isLocal);
    }

    @Override
    public void checkPlanConsistency(LogicalPlan optimizedPlan, Failures failures, Failures depFailures) {
        List<BiConsumer<LogicalPlan, Failures>> checkers = new ArrayList<>();

        optimizedPlan.forEachUp(p -> {
            PlanConsistencyChecker.checkPlan(p, depFailures);

            if (failures.hasFailures() == false) {
                if (p instanceof PostOptimizationVerificationAware pova
                    && (pova instanceof PostOptimizationVerificationAware.CoordinatorOnly && isLocal) == false) {
                    pova.postOptimizationVerification(failures);
                }
                if (p instanceof PostOptimizationPlanVerificationAware popva) {
                    checkers.add(popva.postOptimizationPlanVerification());
                }
                p.forEachExpression(ex -> {
                    if (ex instanceof PostOptimizationVerificationAware va
                        && (va instanceof PostOptimizationVerificationAware.CoordinatorOnly && isLocal) == false) {
                        va.postOptimizationVerification(failures);
                    }
                    if (ex instanceof PostOptimizationPlanVerificationAware vpa) {
                        vpa.postOptimizationPlanVerification().accept(p, failures);
                    }
                });
            }
        });

        optimizedPlan.forEachUp(p -> checkers.forEach(checker -> checker.accept(p, failures)));
    }
}
