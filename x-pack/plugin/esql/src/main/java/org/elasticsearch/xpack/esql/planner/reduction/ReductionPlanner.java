/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.reduction;

import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalVerifier;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceFieldWithConstantOrNull;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/** Builds the node-reduction and shard plans after a data node receives the distributed plan. */
public final class ReductionPlanner {

    /** Builds the node-reduction and shard-data plans for a data-node request. */
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
        long startTime = planTimeProfile == null ? 0 : System.nanoTime();
        PhysicalPlan source = new ExchangeSourceExec(originalPlan.source(), originalPlan.output(), originalPlan.isIntermediateAgg());
        ReductionPlan passThroughReduction = new ReductionPlan(originalPlan.replaceChild(source), originalPlan);
        if (reduceNodeLateMaterialization == false && runNodeLevelReduction == false) {
            return passThroughReduction;
        }

        Function<PhysicalPlan, ReductionPlan> placePlanBetweenExchanges = plan -> new ReductionPlan(
            originalPlan.replaceChild(plan.replaceChildren(List.of(source))),
            originalPlan
        );
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory = stats -> new LocalPhysicalOptimizerContext(
            plannerSettings,
            flags,
            configuration,
            foldCtx,
            stats
        );

        ReductionPlan reductionPlan = switch (PlannerUtils.reductionPlan(originalPlan)) {
            case PlannerUtils.TopNReduction topN -> planTopNReduction(
                contextFactory,
                originalPlan,
                topN,
                placePlanBetweenExchanges,
                passThroughReduction,
                runNodeLevelReduction,
                reduceNodeLateMaterialization
            );
            case PlannerUtils.ReducedPlan reducedPlan -> runNodeLevelReduction
                ? placePlanBetweenExchanges.apply(reducedPlan.plan())
                : passThroughReduction;
            case PlannerUtils.SimplePlanReduction.NO_REDUCTION -> passThroughReduction;
        };
        if (planTimeProfile != null) {
            planTimeProfile.addReductionPlanNanos(System.nanoTime() - startTime);
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
        boolean reduceNodeLateMaterialization
    ) {
        if (reduceNodeLateMaterialization) {
            Optional<ReductionPlan> lateMaterializationReduction = planDeferredTopNFields(contextFactory, originalPlan);
            if (lateMaterializationReduction.isPresent()) {
                return lateMaterializationReduction.get();
            }
        }
        if (runNodeLevelReduction) {
            return placePlanBetweenExchanges.apply(topN.plan());
        }
        return passThroughReduction;
    }

    private static Optional<ReductionPlan> planDeferredTopNFields(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        TopNPlanningContext planningContext = analyzeTopN(contextFactory, originalPlan.child()).orElse(null);
        if (planningContext == null) {
            return Optional.empty();
        }
        FragmentExec fragmentExec = planningContext.fragmentExec();
        TopN topN = planningContext.topN();
        List<Attribute> expectedDataOutput = planningContext.expectedDataOutput();
        var updatedFragment = new Project(Source.EMPTY, planningContext.withAddedDocToRelation(), expectedDataOutput);
        FragmentExec updatedFragmentExec = fragmentExec.withFragment(updatedFragment);
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(updatedFragmentExec);

        PhysicalPlan reductionPlan = planningContext.physicalPlan(fragmentExec.fragment()).transformDown(TopNExec.class, topNExec -> {
            PhysicalPlan exchangeExec = new ExchangeSourceExec(topN.source(), expectedDataOutput, false);
            boolean fragmentIsSorted = updatedFragment.child() instanceof TopN;
            return fragmentIsSorted ? topNExec.replaceChild(exchangeExec).withSortedInput() : topNExec.replaceChild(exchangeExec);
        });
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), reductionPlan);
        return Optional.of(new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan));
    }

    private record TopNPlanningContext(
        FragmentExec fragmentExec,
        Project topLevelProject,
        TopN topN,
        Attribute doc,
        LogicalPlan withAddedDocToRelation,
        List<Attribute> expectedDataOutput,
        LocalPhysicalOptimizerContext optimizerContext
    ) {
        private TopNPlanningContext {
            expectedDataOutput = List.copyOf(expectedDataOutput);
        }

        private PhysicalPlan physicalPlan(LogicalPlan plan) {
            return toPhysicalPlanForReductionSchema(plan, optimizerContext);
        }
    }

    private static Optional<TopNPlanningContext> analyzeTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        PhysicalPlan exchangeChild
    ) {
        FragmentExec fragmentExec = exchangeChild instanceof FragmentExec fe ? fe : null;
        if (fragmentExec == null) {
            return Optional.empty();
        }
        Project topLevelProject = fragmentExec.fragment() instanceof Project project ? project : null;
        if (topLevelProject == null) {
            return Optional.empty();
        }
        TopN topN = topLevelProject.child() instanceof TopN candidate ? candidate : null;
        if (topN == null) {
            return Optional.empty();
        }

        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_TOP_N_REPLACEMENT);
        List<Attribute> physicalPlanOutput = toPhysicalPlanForReductionSchema(topN, context).output();
        Attribute doc = physicalPlanOutput.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (doc == null) {
            return Optional.empty();
        }

        LogicalPlan withAddedDocToRelation = topN.transformUp(EsRelation.class, relation -> {
            if (relation.indexMode() == IndexMode.LOOKUP) {
                return relation;
            }
            return relation.withAttributes(CollectionUtils.prependToCopy(doc, relation.output()));
        });
        if (withAddedDocToRelation.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return Optional.empty();
        }

        AttributeSet orderRefsSet = AttributeSet.of(topN.order().stream().flatMap(order -> order.references().stream()).toList());
        List<Attribute> expectedDataOutput = new ArrayList<>();
        for (Attribute attr : physicalPlanOutput) {
            if (topLevelProject.outputSet().contains(attr) || orderRefsSet.contains(attr) || EsQueryExec.isDocAttribute(attr)) {
                expectedDataOutput.add(attr);
            }
        }
        return Optional.of(
            new TopNPlanningContext(fragmentExec, topLevelProject, topN, doc, withAddedDocToRelation, expectedDataOutput, context)
        );
    }

    /**
     * A stripped-down version of {@link org.elasticsearch.xpack.esql.planner.PlannerUtils#localPlan}. It does only the work required to
     * make the output between the data and node-reduce drivers explicit.
     */
    private static PhysicalPlan toPhysicalPlanForReductionSchema(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        var logicalContext = new LocalLogicalOptimizerContext(context.configuration(), context.foldCtx(), context.searchStats());
        LogicalPlan optimized = new ReplaceFieldWithConstantOrNull().apply(plan, logicalContext);
        return new InsertFieldExtraction().apply(new ReplaceSourceAttributes().apply(LocalMapper.INSTANCE.map(optimized)), context);
    }

    private static final SearchStats SEARCH_STATS_TOP_N_REPLACEMENT = new SearchStats.UnsupportedSearchStats() {
        @Override
        public boolean exists(FieldAttribute.FieldName field) {
            return true;
        }

        @Override
        public boolean isIndexed(FieldAttribute.FieldName field) {
            return false;
        }

        @Override
        public Object min(FieldAttribute.FieldName field) {
            return null;
        }

        @Override
        public Object max(FieldAttribute.FieldName field) {
            return null;
        }
    };

    private static boolean skipConsistencyCheckAfterReductionPlanning(LogicalPlan fragment) {
        return fragment instanceof Aggregate || fragment instanceof MetricsInfo || fragment instanceof TsInfo;
    }

    private ReductionPlanner() {}
}
