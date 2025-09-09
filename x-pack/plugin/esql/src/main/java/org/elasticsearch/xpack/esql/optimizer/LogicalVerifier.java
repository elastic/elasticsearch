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
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public final class LogicalVerifier extends PostOptimizationPhasePlanVerifier<LogicalPlan> {

    private LogicalVerifier(boolean isLocal) {
        super(isLocal);
    }

    public static LogicalVerifier getLocalVerifier() {
        return new LogicalVerifier(true);
    }

    public static LogicalVerifier getGeneralVerifier() {
        return new LogicalVerifier(false);
    }

    @Override
    boolean hasRemoteEnrich(LogicalPlan optimizedPlan) {
        var enriches = optimizedPlan.collectFirstChildren(Enrich.class::isInstance);
        return enriches.isEmpty() == false && ((Enrich) enriches.get(0)).mode() == Enrich.Mode.REMOTE;
    }

    @Override
    void checkPlanConsistency(LogicalPlan optimizedPlan, Failures failures, Failures depFailures) {
        List<BiConsumer<LogicalPlan, Failures>> checkers = new ArrayList<>();

        optimizedPlan.forEachUp(p -> {
            PlanConsistencyChecker.checkPlan(p, depFailures);

            if (failures.hasFailures() == false) {
                if (p instanceof PostOptimizationVerificationAware pova) {
                    pova.postOptimizationVerification(failures);
                }
                if (p instanceof PostOptimizationPlanVerificationAware popva) {
                    checkers.add(popva.postOptimizationPlanVerification());
                }
                p.forEachExpression(ex -> {
                    if (ex instanceof PostOptimizationVerificationAware va) {
                        va.postOptimizationVerification(failures);
                    }
                });
            }
        });

        optimizedPlan.forEachUp(p -> checkers.forEach(checker -> checker.accept(p, failures)));
    }
}
