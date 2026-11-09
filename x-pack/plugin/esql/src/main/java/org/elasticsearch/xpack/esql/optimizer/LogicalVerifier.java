/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public final class LogicalVerifier extends PostOptimizationPhasePlanVerifier<LogicalPlan> {
    public static final LogicalVerifier LOCAL_INSTANCE = new LogicalVerifier(true);
    public static final LogicalVerifier INSTANCE = new LogicalVerifier(false);

    private LogicalVerifier(boolean isLocal) {
        super(isLocal);
    }

    /**
     * Verifies the optimized coordinator plan, additionally applying the limits that are defined for each independently executed query
     * rather than for a single node.
     */
    public Failures verify(
        LogicalPlan optimizedPlan,
        List<Attribute> expectedOutputAttributes,
        QueryPragmas pragmas,
        EsqlFlags flags,
        TransportVersion minimumVersion
    ) {
        assert isLocal == false : "query-wide limits apply to the coordinator plan only";
        Failures failures = verify(optimizedPlan, expectedOutputAttributes);
        // These limits need complete main-query and IN-subquery plans, so they live here rather than in {@link #checkPlanConsistency}.
        UnionAll.checkNestedSubqueryLimits(
            optimizedPlan,
            pragmas.maxBranchCount(flags.maxBranchCount()),
            pragmas.maxBranchLevel(flags.maxBranchLevel()),
            pragmas.maxBranchCountLimitSource(EsqlFlags.ESQL_MAX_BRANCH_COUNT.getKey()),
            pragmas.maxBranchLevelLimitSource(EsqlFlags.ESQL_MAX_BRANCH_LEVEL.getKey()),
            failures
        );
        checkKnnRuntimeSearchSupported(optimizedPlan, failures, minimumVersion);
        return failures;
    }

    /**
     * KNN function on runtime dense_vector expressions is enabled by default since {@link Knn#ESQL_KNN_RUNTIME_FIELD}.
     * Older nodes may be running a version which does not support runtime KNN at all or a version which disables it by default.
     * <p>
     * Such a scenario may lead to partial results or errors down the line. Instead, fail fast with a 4xx here when any participating
     * node, including a CCS remote, predates the release.
     * Indexed-field KNN is pushed down as a {@code KnnQuery} that older nodes handle fine, so only the runtime path is gated.
     * <p>
     * This runs on the optimized coordinator plan rather than during analysis because whether a {@code Knn} is a runtime search is
     * not settled until push-down completes: in a query such as {@code FROM colors, (FROM hosts) | WHERE KNN(color_vector, ...)} the
     * field is unresolved against the merged {@code UnionAll} output at analysis time - {@link Knn#isRuntimeSearch()} would report a
     * spurious {@code true} - and only becomes an index-backed {@link org.elasticsearch.xpack.esql.core.expression.FieldAttribute}
     * once {@code PushDownFilterAndLimitIntoUnionAll} relocates the filter into the branch that maps it.
     */
    private static void checkKnnRuntimeSearchSupported(LogicalPlan plan, Failures failures, TransportVersion minimumVersion) {
        if (minimumVersion.supports(Knn.ESQL_KNN_RUNTIME_FIELD)) {
            return;
        }
        plan.forEachExpressionDown(Knn.class, knn -> {
            if (knn.isRuntimeSearch()) {
                failures.add(
                    fail(
                        knn,
                        "KNN over a non-index-mapped field or expression is not supported on every participating node; "
                            + "rolling upgrade in progress, or a remote cluster is on an older version"
                    )
                );
            }
        });
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
