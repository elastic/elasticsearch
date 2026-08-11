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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public final class LogicalVerifier extends PostOptimizationPhasePlanVerifier<LogicalPlan> {
    public static final LogicalVerifier LOCAL_INSTANCE = new LogicalVerifier(true);
    public static final LogicalVerifier INSTANCE = new LogicalVerifier(false);

    private LogicalVerifier(boolean isLocal) {
        super(isLocal);
    }

    /**
     * Verifies the optimized coordinator plan, additionally applying the limits that are defined for a query as a whole rather than
     * for a single node.
     */
    public Failures verify(LogicalPlan optimizedPlan, List<Attribute> expectedOutputAttributes, QueryPragmas pragmas) {
        assert isLocal == false : "query-wide limits apply to the coordinator plan only";
        Failures failures = verify(optimizedPlan, expectedOutputAttributes);
        checkMaxUnionAllBranches(optimizedPlan, pragmas, failures);
        return failures;
    }

    /**
     * Rejects a query whose {@link UnionAll}s add up to more branches than the {@code max_query_branches} pragma allows.
     * <p>
     * This check lives here rather than in {@link #checkPlanConsistency} for two reasons: that method receives one node at a time
     * through {@link org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware}, which cannot see the whole
     * plan; and it is called directly by tests on plans that were never optimized, which should not be subject to query-wide limits.
     * It only runs on an otherwise failure-free plan, so the branch count never distracts from a more fundamental problem.
     */
    private static void checkMaxUnionAllBranches(LogicalPlan optimizedPlan, QueryPragmas pragmas, Failures failures) {
        if (failures.hasFailures() == false) {
            UnionAll.checkTotalBranchCount(optimizedPlan, pragmas.maxQueryBranches(), failures);
        }
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
