/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.reduction;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.scalar.FetchHandleFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalVerifier;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceFieldWithConstantOrNull;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.FetchSource;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.FieldExtractionSpec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.FetchHandle;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

/**
 * Builds the node-reduction and shard plans after a data node has received the distributed plan.
 */
public final class ReductionPlanner {

    /**
     * Combined physical plan after any reduction-dependent distributed rewrite, together with execution lifecycle requirements
     * implied by the rewritten plan.
     */
    public record DistributedReductionPlan(PhysicalPlan plan) {
        /**
         * Whether contracts embedded in this plan require search contexts to survive the initial data-node compute.
         */
        public boolean retainSearchContexts() {
            return plan.anyMatch(FetchBoundaryExec.class::isInstance);
        }
    }

    /**
     * Applies reduction-dependent rewrites while the coordinator and data-node sides still share one physical plan.
     */
    public static DistributedReductionPlan planDistributed(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        PhysicalPlan resolvedPlan,
        Map<String, OriginalIndices> clusterToConcreteIndices,
        TransportVersion minimumTransportVersion
    ) {
        boolean hasConcreteIndices = clusterToConcreteIndices.values().stream().anyMatch(indices -> indices.indices().length > 0);
        if (configuration.pragmas().fetchTopN()
            && hasConcreteIndices
            && clusterToConcreteIndices.size() == 1
            && clusterToConcreteIndices.containsKey(LOCAL_CLUSTER_GROUP_KEY)
            && minimumTransportVersion.supports(FetchBoundaryExec.ESQL_FETCH_BOUNDARY)) {
            var rewrittenPlan = planDistributedTopN(
                stats -> new LocalPhysicalOptimizerContext(plannerSettings, flags, configuration, foldContext, stats),
                resolvedPlan,
                minimumTransportVersion
            );
            if (rewrittenPlan.isPresent()) {
                return new DistributedReductionPlan(rewrittenPlan.get());
            }
        }
        return new DistributedReductionPlan(resolvedPlan);
    }

    /**
     * Builds a reduction plan without request-specific fetch state. This entry point also supports plan snapshot tests.
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
            null,
            planTimeProfile
        );
    }

    /**
     * Builds request-specific node-reduction and shard plans, consuming any fetch boundary carried by the physical plan.
     */
    public static ReductionPlan plan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        ExchangeSinkExec originalPlan,
        boolean runNodeLevelReduction,
        boolean reduceNodeLateMaterialization,
        String localNodeId,
        String retainedSessionId,
        PlanTimeProfile planTimeProfile
    ) {
        List<FetchBoundaryExec> fetchBoundaries = originalPlan.collect(FetchBoundaryExec.class);
        if (fetchBoundaries.size() > 1) {
            throw new IllegalStateException("expected at most one fetch boundary but found [" + fetchBoundaries.size() + "]");
        }
        FetchBoundaryExec fetchBoundary = fetchBoundaries.isEmpty() ? null : fetchBoundaries.getFirst();
        if (fetchBoundary != null && originalPlan.child() != fetchBoundary) {
            throw new IllegalStateException("fetch boundary must be the direct child of the data-node exchange sink");
        }
        if (fetchBoundary != null && (localNodeId == null || retainedSessionId == null)) {
            throw new IllegalStateException("fetch boundary requires local node and retained session identifiers");
        }

        long startTime = planTimeProfile == null ? 0 : System.nanoTime();
        PhysicalPlan source = new ExchangeSourceExec(originalPlan.source(), originalPlan.output(), originalPlan.isIntermediateAgg());
        ReductionPlan passThroughReduction = new ReductionPlan(originalPlan.replaceChild(source), originalPlan);
        if (fetchBoundary == null && reduceNodeLateMaterialization == false && runNodeLevelReduction == false) {
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

        PlannerUtils.PlanReduction planReduction = PlannerUtils.reductionPlan(originalPlan);
        if (fetchBoundary != null && planReduction instanceof PlannerUtils.TopNReduction == false) {
            throw new IllegalStateException("fetch boundary does not describe a supported reduction");
        }

        // The default plan is just the exchange source piped directly into the exchange sink.
        ReductionPlan reductionPlan = switch (planReduction) {
            case PlannerUtils.TopNReduction topN -> planTopNReduction(
                contextFactory,
                originalPlan,
                topN,
                placePlanBetweenExchanges,
                passThroughReduction,
                runNodeLevelReduction,
                reduceNodeLateMaterialization,
                fetchBoundary,
                localNodeId,
                retainedSessionId
            );
            // Not a TopN - must be an agg or a limit
            case PlannerUtils.ReducedPlan rp -> runNodeLevelReduction ? placePlanBetweenExchanges.apply(rp.plan()) : passThroughReduction;
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
        FetchBoundaryExec fetchBoundary,
        String localNodeId,
        String retainedSessionId
    ) {
        if (fetchBoundary != null) {
            return planFetchTopN(contextFactory, originalPlan, fetchBoundary, localNodeId, retainedSessionId);
        }
        if (reduceNodeLateMaterialization) {
            /*
             * In the case of TopN, the source output type is replaced since we're pulling the FieldExtractExec to the reduction node,
             * so essentially we are splitting the TopNExec into two parts, similar to other aggregations, but unlike other aggregations,
             * we also need the original plan, since we add the project in the reduction node.
             */
            var lateMaterializationReduction = planDeferredTopNFields(contextFactory, originalPlan);
            if (lateMaterializationReduction.isPresent()) {
                return lateMaterializationReduction.get();
            }
        }
        if (runNodeLevelReduction) {
            return placePlanBetweenExchanges.apply(topN.plan());
        }
        return passThroughReduction;
    }

    /**
     * Rewrites the distributed TopN so only its eager columns and an opaque fetch handle cross the exchange.
     * Deferred columns are fetched after the coordinator selects the winning rows.
     * <pre>{@code
     * Before:
     * coordinator: [optional ProjectExec] -> TopNExec -> ExchangeExec[all columns]
     * data:        FragmentExec[Project -> TopN]
     *
     * After:
     * coordinator: ProjectExec -> FetchExec -> TopNExec -> ExchangeExec[handle, eager columns]
     * fetch plan:  FragmentExec[FetchSource[deferred columns]]
     * data:        FetchBoundaryExec -> FragmentExec[Project[doc, eager columns] -> TopN]
     * }</pre>
     */
    private static Optional<PhysicalPlan> planDistributedTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        PhysicalPlan distributedPlan,
        TransportVersion minimumTransportVersion
    ) {
        List<Attribute> originalOutput = distributedPlan.output();
        var replacedTopN = new Holder<Boolean>(false);
        PhysicalPlan rewrittenPlan = distributedPlan.transformDown(TopNExec.class, topNExec -> {
            if (replacedTopN.get() || (topNExec.child() instanceof ExchangeExec) == false) {
                return topNExec;
            }
            ExchangeExec exchange = (ExchangeExec) topNExec.child();
            TopNPlanningContext planningContext = analyzeTopN(contextFactory, exchange.child(), ExistingDocPolicy.PREPEND_IF_MISSING)
                .orElse(null);
            if (planningContext == null) {
                return topNExec;
            }
            FragmentExec fragmentExec = planningContext.fragmentExec();
            Project topLevelProject = planningContext.topLevelProject();
            TopN topN = planningContext.topN();
            List<Attribute> expectedDataOutput = planningContext.expectedDataOutput();

            List<Attribute> exchangeOutput = new ArrayList<>();
            Attribute handle = fetchHandleAttribute(topN.source());
            exchangeOutput.add(handle);
            for (Attribute attr : expectedDataOutput) {
                if (EsQueryExec.isDocAttribute(attr) == false) {
                    exchangeOutput.add(attr);
                }
            }

            AttributeSet exchangeOutputSet = AttributeSet.of(exchangeOutput);
            List<Attribute> attributesToFetch = new ArrayList<>();
            List<FieldExtractionSpec> extractionSpecs = new ArrayList<>();
            for (Attribute attr : topLevelProject.output()) {
                if (exchangeOutputSet.contains(attr) == false) {
                    FieldExtractionSpec extractionSpec = FieldExtractionSpec.plan(
                        attr,
                        planningContext.optimizerContext().configuration().pragmas().fieldExtractPreference()
                    ).orElse(null);
                    if (extractionSpec == null || extractionSpec.supports(minimumTransportVersion) == false) {
                        return topNExec;
                    }
                    attributesToFetch.add(attr);
                    extractionSpecs.add(extractionSpec);
                }
            }
            if (attributesToFetch.isEmpty()) {
                return topNExec;
            }

            FragmentExec updatedFragmentExec = fragmentExec.withFragment(
                new Project(Source.EMPTY, planningContext.withAddedDocToRelation(), expectedDataOutput)
            );
            FetchBoundaryExec fetchBoundary = new FetchBoundaryExec(exchange.source(), updatedFragmentExec, handle, exchangeOutput);
            ExchangeExec updatedExchange = new ExchangeExec(exchange.source(), exchangeOutput, exchange.inBetweenAggs(), fetchBoundary);
            FragmentExec fetchPlan = new FragmentExec(new FetchSource(Source.EMPTY, attributesToFetch));
            replacedTopN.set(true);
            TopNExec updatedTopN = topNExec.replaceChild(updatedExchange);
            return new FetchExec(topNExec.source(), updatedTopN, handle, attributesToFetch, extractionSpecs, attributesToFetch, fetchPlan);
        });
        if (replacedTopN.get() == false) {
            return Optional.empty();
        }
        if (rewrittenPlan.output().equals(originalOutput) == false) {
            rewrittenPlan = new ProjectExec(distributedPlan.source(), rewrittenPlan, originalOutput);
        }
        return Optional.of(rewrittenPlan);
    }

    /**
     * Consumes the fetch boundary on a data node and builds the node-reduce and shard-data sides of its handoff.
     * <pre>{@code
     * node reduce: ExchangeSinkExec[handle, eager columns]
     *                  -> ProjectExec -> EvalExec[handle] -> TopNExec -> ExchangeSourceExec[doc, eager columns]
     * shard data:  ExchangeSinkExec[doc, eager columns] -> FragmentExec[Project[doc, eager columns] -> TopN]
     * }</pre>
     */
    private static ReductionPlan planFetchTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan,
        FetchBoundaryExec fetchBoundary,
        String localNodeId,
        String retainedSessionId
    ) {
        if (originalPlan.output().equals(fetchBoundary.handoffOutput()) == false) {
            throw new IllegalStateException(
                "fetch boundary handoff output "
                    + fetchBoundary.handoffOutput()
                    + " does not match exchange output "
                    + originalPlan.output()
            );
        }
        TopNPlanningContext planningContext = analyzeTopN(contextFactory, fetchBoundary.child(), ExistingDocPolicy.PREPEND_IF_MISSING)
            .orElseThrow(() -> new IllegalStateException("fetch boundary does not contain a supported TopN fragment"));
        FragmentExec fragmentExec = planningContext.fragmentExec();
        TopN topN = planningContext.topN();
        Attribute doc = planningContext.doc();
        List<Attribute> expectedDataOutput = planningContext.expectedDataOutput();

        FragmentExec updatedFragmentExec = fragmentExec.withFragment(
            new Project(Source.EMPTY, planningContext.withAddedDocToRelation(), expectedDataOutput)
        );
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(updatedFragmentExec);

        boolean fragmentIsSorted = updatedFragmentExec.fragment() instanceof Project p && p.child() instanceof TopN;
        assert fragmentIsSorted : "expected Project -> TopN fragment shape";
        PhysicalPlan reductionPlan = planningContext.physicalPlan(fragmentExec.fragment()).transformDown(TopNExec.class, t -> {
            PhysicalPlan exchangeExec = new ExchangeSourceExec(topN.source(), expectedDataOutput, false);
            return fragmentIsSorted ? t.replaceChild(exchangeExec).withSortedInput() : t.replaceChild(exchangeExec);
        });
        Alias handleAlias = new Alias(
            Source.EMPTY,
            fetchBoundary.handleAttribute().name(),
            new FetchHandleFunction(Source.EMPTY, doc, localNodeId, retainedSessionId),
            fetchBoundary.handleAttribute().id(),
            true
        );
        PhysicalPlan withHandle = new EvalExec(Source.EMPTY, reductionPlan, List.of(handleAlias));
        PhysicalPlan projected = new ProjectExec(Source.EMPTY, withHandle, fetchBoundary.handoffOutput());
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), projected);
        return new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan);
    }

    private static Optional<ReductionPlan> planDeferredTopNFields(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        TopNPlanningContext planningContext = analyzeTopN(contextFactory, originalPlan.child(), ExistingDocPolicy.ALWAYS_PREPEND).orElse(
            null
        );
        if (planningContext == null) {
            return Optional.empty();
        }
        FragmentExec fragmentExec = planningContext.fragmentExec();
        TopN topN = planningContext.topN();
        List<Attribute> expectedDataOutput = planningContext.expectedDataOutput();
        var updatedFragment = new Project(Source.EMPTY, planningContext.withAddedDocToRelation(), expectedDataOutput);
        FragmentExec updatedFragmentExec = fragmentExec.withFragment(updatedFragment);
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(updatedFragmentExec);

        PhysicalPlan reductionPlan = planningContext.physicalPlan(fragmentExec.fragment()).transformDown(TopNExec.class, t -> {
            PhysicalPlan exchangeExec = new ExchangeSourceExec(topN.source(), expectedDataOutput, false);
            boolean fragmentIsSorted = updatedFragment.child() instanceof TopN;
            return fragmentIsSorted ? t.replaceChild(exchangeExec).withSortedInput() : t.replaceChild(exchangeExec);
        });
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), reductionPlan);
        return Optional.of(new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan));
    }

    private enum ExistingDocPolicy {
        ALWAYS_PREPEND,
        PREPEND_IF_MISSING
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
        PhysicalPlan exchangeChild,
        ExistingDocPolicy existingDocPolicy
    ) {
        FragmentExec fragmentExec = exchangeChild instanceof FragmentExec fe ? fe : null;
        if (fragmentExec == null) {
            return Optional.empty();
        }
        Project topLevelProject = fragmentExec.fragment() instanceof Project p ? p : null;
        if (topLevelProject == null) {
            return Optional.empty();
        }
        TopN topN = topLevelProject.child() instanceof TopN tn ? tn : null;
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
            if (existingDocPolicy == ExistingDocPolicy.PREPEND_IF_MISSING && relation.outputSet().contains(doc)) {
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

    private static PhysicalPlan toPhysicalPlanForReductionSchema(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        var logicalContext = new LocalLogicalOptimizerContext(context.configuration(), context.foldCtx(), context.searchStats());
        LogicalPlan optimized = new ReplaceFieldWithConstantOrNull().apply(plan, logicalContext);
        return new InsertFieldExtraction().apply(new ReplaceSourceAttributes().apply(LocalMapper.INSTANCE.map(optimized)), context);
    }

    private static Attribute fetchHandleAttribute(Source source) {
        return new ReferenceAttribute(source, null, FetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD, Nullability.FALSE, null, true);
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
