/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalVerifier;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.function.Function;

/**
 * Builds the node-reduction and shard plans after a data node has received the distributed plan.
 */
public final class ReductionPlanner {

    /**
     * Builds a reduction plan without request-specific remote-fetch state. This entry point also supports plan snapshot tests.
     */
    public static ReductionPlan plan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        ExchangeSinkExec originalPlan,
        boolean runNodeLevelReduction,
        boolean reduceNodeLateMaterialization,
        PlanTimeProfile planTimeProfile
    ) {
        return plan(
            plannerSettings,
            flags,
            configuration,
            foldCtx,
            originalPlan,
            runNodeLevelReduction,
            reduceNodeLateMaterialization,
            null,
            planTimeProfile
        );
    }

    static ReductionPlan plan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        ExchangeSinkExec originalPlan,
        boolean runNodeLevelReduction,
        boolean reduceNodeLateMaterialization,
        @Nullable RemoteFetchReductionPlanner.RemoteFetchContext remoteFetchContext,
        PlanTimeProfile planTimeProfile
    ) {
        long startTime = planTimeProfile == null ? 0 : System.nanoTime();
        PhysicalPlan source = new ExchangeSourceExec(originalPlan.source(), originalPlan.output(), originalPlan.isIntermediateAgg());
        ReductionPlan passThroughReduction = new ReductionPlan(originalPlan.replaceChild(source), originalPlan);
        if (remoteFetchContext == null && reduceNodeLateMaterialization == false && runNodeLevelReduction == false) {
            return passThroughReduction;
        }

        Function<PhysicalPlan, ReductionPlan> placePlanBetweenExchanges = p -> new ReductionPlan(
            originalPlan.replaceChild(p.replaceChildren(List.of(source))),
            originalPlan
        );
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory = stats -> new LocalPhysicalOptimizerContext(
            plannerSettings,
            flags,
            configuration,
            foldCtx,
            stats
        );

        // The default plan is just the exchange source piped directly into the exchange sink.
        ReductionPlan reductionPlan = switch (PlannerUtils.reductionPlan(originalPlan)) {
            case PlannerUtils.TopNReduction topN -> planTopNReduction(
                contextFactory,
                originalPlan,
                topN,
                placePlanBetweenExchanges,
                passThroughReduction,
                runNodeLevelReduction,
                reduceNodeLateMaterialization,
                remoteFetchContext
            );
            // Not a TopN - must be an agg or a limit
            case PlannerUtils.ReducedPlan rp when runNodeLevelReduction -> placePlanBetweenExchanges.apply(rp.plan());
            default -> passThroughReduction;
        };
        if (planTimeProfile != null) {
            planTimeProfile.addReductionPlanNanos(System.nanoTime() - startTime);
        }

        /*
         * The handle attribute in the sink schema means the coordinator already committed both sides of the exchange to the
         * remote-fetch schema. All other reductions forward the original doc-based columns, which the coordinator can no
         * longer consume, so fail here instead of surfacing an obscure schema mismatch at exchange time.
         */
        if (originalPlan.output().stream().anyMatch(RemoteFetchHandle::isAttribute)) {
            boolean producesHandle = reductionPlan.nodeReducePlan()
                .anyMatch(
                    p -> p instanceof EvalExec eval && eval.fields().stream().anyMatch(a -> a.child() instanceof RemoteFetchHandleFunction)
                );
            if (producesHandle == false) {
                throw new IllegalStateException(
                    "coordinator planned remote-fetch TopN but the node reduction could not be rebuilt for plan [" + originalPlan + "]"
                );
            }
        }

        // TODO: How we generate intermediate attributes prevents us from cleanly checking dependencies here. We should always be
        // able to perform this check.
        if (Assertions.ENABLED == false
            || (reductionPlan.dataNodePlan().child() instanceof FragmentExec fragment
                && skipConsistencyCheckAfterReductionPlanning(fragment.fragment()))) {
            return reductionPlan;
        }

        PhysicalVerifier.LOCAL_INSTANCE.verify(reductionPlan.nodeReducePlan(), originalPlan.output());
        ExchangeSourceExec reductionSource = (ExchangeSourceExec) reductionPlan.nodeReducePlan().collectLeaves().getFirst();
        // The data driver's output is sent to the reduction driver, so the outputs must match up.
        PhysicalVerifier.LOCAL_INSTANCE.verify(reductionPlan.dataNodePlan(), reductionSource.output());

        return reductionPlan;
    }

    private static ReductionPlan planTopNReduction(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan,
        PlannerUtils.TopNReduction topN,
        Function<PhysicalPlan, ReductionPlan> placePlanBetweenExchanges,
        ReductionPlan passThroughReduction,
        boolean runNodeLevelReduction,
        boolean reduceNodeLateMaterialization,
        @Nullable RemoteFetchReductionPlanner.RemoteFetchContext remoteFetchContext
    ) {
        if (remoteFetchContext != null) {
            var remoteFetchReduction = RemoteFetchReductionPlanner.planReduceDriverTopN(contextFactory, originalPlan, remoteFetchContext);
            if (remoteFetchReduction.isPresent()) {
                return remoteFetchReduction.get();
            }
        }
        if (reduceNodeLateMaterialization) {
            /*
             * In the case of TopN, the source output type is replaced since we're pulling the FieldExtractExec to the reduction node,
             * so essentially we are splitting the TopNExec into two parts, similar to other aggregations, but unlike other aggregations,
             * we also need the original plan, since we add the project in the reduction node.
             */
            var lateMaterializationReduction = LateMaterializationPlanner.planReduceDriverTopN(contextFactory, originalPlan);
            if (lateMaterializationReduction.isPresent()) {
                return lateMaterializationReduction.get();
            }
        }
        if (runNodeLevelReduction) {
            return placePlanBetweenExchanges.apply(topN.plan());
        }
        return passThroughReduction;
    }

    private static boolean skipConsistencyCheckAfterReductionPlanning(LogicalPlan fragment) {
        // FragmentExec.output() doesn't take into account intermediate attributes of aggs, and time series aggs
        // have some peculiarities due to implicit dimensions. We should clean this up and add a proper check here.
        return fragment instanceof Aggregate
            // MetricsInfo/TsInfo do not serialize their output attributes (they are generated automatically and do not depend on the
            // input). After de-serializing the data node plan, the output attributes have different NameIds than the ExchangeSink of
            // the data node plan.
            || fragment instanceof MetricsInfo
            || fragment instanceof TsInfo;
    }

    private ReductionPlanner() {}
}
