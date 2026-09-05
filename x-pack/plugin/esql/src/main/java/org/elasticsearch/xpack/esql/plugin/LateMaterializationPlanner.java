/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSource;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
* Modify a {@link Project} that follows a {@link TopN} such that it tries to minimize field extraction on the data driver.
*
* Consider the following query:
* <pre>
* FROM index | WHERE x > 10 | SORT foo | LIMIT 10 | KEEP bar
* </pre>
* If we can delay materializing {@code bar} until the node-reduce driver has finished its own TopN, we can reduce the amount of data we
* read from the index.
*
* The basic strategy here is to "cut off" the operation right after the last top n, and perform all the removed operations on the
* node reduce drivier, so the data drivers top n operations "feed into" the node reduce one. Ideally, we would just take the top-most
* {@link TopNExec}, but unfortunately that doesn't quite work: the top n might be pushed down to the source in {@link PushTopNToSource},
* which might change the output attributes (the filter might also be pushed down, so no {@code x} will be output). To solve this, we add a
* {@link Project} to ensure that the output schema of the data-side plan remains consistent with the expectations of the reduce-side
* plan (note that while performing the reduce-side plan we have no way of knowing if a pushdown is possible or not, since we don't have
* access to the source's capabilities).
*
* So for the aforementioned query, we would go from (roughly) this plan:
* <pre>
*  Project [bar]
*  └── TopN [foo, limit=10] (this will output _doc, foo, and x)
*      └── Filter [x > 10]
*          └── EsRelation [index]
*  </pre>
*  Into this:
*  <pre>
*  Project [_doc, foo, x]
*  └── TopN [foo, limit=10]
*      └── Filter [x > 10]
*          └── EsRelation [index]
*  </pre>
*  If there's a pushdown, the <i>final</i> plan would be:
*  <pre>
*  Project [_doc, foo]
*  └── EsQuery [index with some TopN pushdown]
*  </pre>
*  Note the above does not project the {@code x} field anymore (this was an enhancement made by #137920)
*/
class LateMaterializationPlanner {
    /**
     * Gates late materialization on the node-reduce driver for {@link TopNBy} and {@link LimitBy} queries.
     * Enabled automatically in snapshot builds; override in release with
     * {@code -Des.esql_node_late_materialization_limit_by_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_LATE_MATERIALIZATION_LIMIT_BY_FEATURE_FLAG = new FeatureFlag(
        "esql_node_late_materialization_limit_by"
    );

    public static Optional<ReductionPlan> planReduceDriverTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        SetupContext ctx = buildSetupContext(contextFactory, originalPlan);
        if (ctx == null || !(ctx.pipelineBreaker instanceof TopN topN)) {
            return Optional.empty();
        }

        AttributeSet orderRefsSet = AttributeSet.of(topN.order().stream().flatMap(o -> o.references().stream()).toList());
        // Get the output from the physical plan below the TopN, and filter it to only the attributes needed for the final output (either
        // because they are in the top-level Project's output, or because they are needed for ordering)
        List<Attribute> expectedDataOutput = new ArrayList<>();
        for (Attribute a : ctx.physicalPlanOutput) {
            if (ctx.topLevelProject.outputSet().contains(a) || orderRefsSet.contains(a) || EsQueryExec.isDocAttribute(a)) {
                expectedDataOutput.add(a);
            }
        }

        // The TopN reduction plan should not be further optimized locally on the node reduce driver, since we took great pains to
        // preplan in advance, including all the necessary field extractions!
        return Optional.of(assembleReductionPlan(ctx, originalPlan, expectedDataOutput, plan -> plan.transformDown(TopNExec.class, t -> {
            PhysicalPlan exchangeExec = new ExchangeSourceExec(topN.source(), expectedDataOutput, false /* isIntermediateAgg */);
            // If the fragment is already sorted, tell the node-reduce TopN that its input will be sorted already
            boolean fragmentIsSorted = ctx.withAddedDocToRelation instanceof TopN;
            return fragmentIsSorted ? t.replaceChild(exchangeExec).withSortedInput() : t.replaceChild(exchangeExec);
        })));
    }

    /**
     * Consumes the coordinator-planned remote-fetch boundary and builds the data and node-reduce plans around its schema contract.
     */
    public static Optional<ReductionPlan> planRemoteFetchTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan,
        String localNodeId,
        String retainedSessionId
    ) {
        if (originalPlan.child() instanceof RemoteFetchBoundaryExec == false) {
            return Optional.empty();
        }
        RemoteFetchBoundaryExec boundary = (RemoteFetchBoundaryExec) originalPlan.child();
        if (boundary.child() instanceof FragmentExec == false) {
            throw invalidRemoteFetchBoundary("expected direct Fragment child", boundary);
        }
        FragmentExec fragmentExec = (FragmentExec) boundary.child();
        if (fragmentExec.fragment() instanceof Project == false) {
            throw invalidRemoteFetchBoundary("expected Fragment(Project) shape", boundary);
        }
        Project project = (Project) fragmentExec.fragment();
        if (project.child() instanceof TopN == false) {
            throw invalidRemoteFetchBoundary("expected Fragment(Project -> TopN) shape", boundary);
        }
        TopN topN = (TopN) project.child();
        if (topN.child().anyMatch(PipelineBreaker.class::isInstance)) {
            throw invalidRemoteFetchBoundary("nested pipeline breaker below TopN", boundary);
        }
        if (originalPlan.output().equals(boundary.handoffOutput()) == false) {
            throw invalidRemoteFetchBoundary(
                "handoff output " + boundary.handoffOutput() + " does not match exchange output " + originalPlan.output(),
                boundary
            );
        }

        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(fragmentExec);
        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_LATE_MATERIALIZATION_REPLACEMENT);
        PhysicalPlan reductionPlan = PlannerUtils.toPhysicalPlanForReductionSchema(fragmentExec.fragment(), context)
            .transformDown(
                TopNExec.class,
                exec -> exec.replaceChild(new ExchangeSourceExec(topN.source(), boundary.dataOutput(), false)).withSortedInput()
            );
        Alias handleAlias = new Alias(
            Source.EMPTY,
            boundary.handleAttribute().name(),
            new RemoteFetchHandleFunction(Source.EMPTY, boundary.documentAttribute(), localNodeId, retainedSessionId),
            boundary.handleAttribute().id(),
            true
        );
        PhysicalPlan withHandle = new EvalExec(Source.EMPTY, reductionPlan, List.of(handleAlias));
        PhysicalPlan projected = new ProjectExec(Source.EMPTY, withHandle, boundary.handoffOutput());
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(fragmentExec.estimatedRowSize(), projected);
        return Optional.of(new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan));
    }

    private static IllegalStateException invalidRemoteFetchBoundary(String reason, RemoteFetchBoundaryExec boundary) {
        return new IllegalStateException("invalid remote-fetch boundary: " + reason + " [" + boundary.nodeName() + "]");
    }

    /**
     * Analogous to {@link #planReduceDriverTopN}, but for {@link TopNBy}.
     *
     * <p>For a query like:
     * <pre>
     * FROM index | WHERE x > 10 | SORT foo | LIMIT 10 BY grp | KEEP bar
     * </pre>
     * we defer reading {@code bar} until after the node-reduce driver has finished its own {@link TopNByExec}, so that
     * {@code bar} is only fetched for the surviving rows (at most {@code 10 * distinct(grp)} rows rather than all rows).
     * The grouping fields are included in the data-side output so the reduce-side {@link TopNByExec} can partition correctly.
     */
    public static Optional<ReductionPlan> planReduceDriverTopNBy(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        SetupContext ctx = buildSetupContext(contextFactory, originalPlan);
        if (ctx == null || !(ctx.pipelineBreaker instanceof TopNBy topNBy)) {
            return Optional.empty();
        }

        AttributeSet orderRefsSet = AttributeSet.of(topNBy.order().stream().flatMap(o -> o.references().stream()).toList());
        AttributeSet groupingRefsSet = AttributeSet.of(topNBy.groupings().stream().flatMap(g -> g.references().stream()).toList());
        List<Attribute> expectedDataOutput = new ArrayList<>();
        for (Attribute a : ctx.physicalPlanOutput) {
            if (ctx.topLevelProject.outputSet().contains(a)
                || orderRefsSet.contains(a)
                || groupingRefsSet.contains(a)
                || EsQueryExec.isDocAttribute(a)) {
                expectedDataOutput.add(a);
            }
        }

        return Optional.of(assembleReductionPlan(ctx, originalPlan, expectedDataOutput, plan -> plan.transformDown(TopNByExec.class, t -> {
            PhysicalPlan exchangeExec = new ExchangeSourceExec(topNBy.source(), expectedDataOutput, false);
            // The reduce driver feeds an exchange that the coordinator's own TopNByExec consumes; sorted output is not required.
            return t.replaceChild(exchangeExec).withNonSortedOutput();
        })));
    }

    /**
     * Analogous to {@link #planReduceDriverTopNBy}, but for {@link LimitBy} (no sort key).
     *
     * <p>For a query like:
     * <pre>
     * FROM index | LIMIT 10 BY grp | KEEP bar
     * </pre>
     * we defer reading {@code bar} until after the node-reduce driver has finished its own {@link LimitByExec}, so that
     * {@code bar} is only fetched for the surviving rows (at most {@code 10 * distinct(grp)} rows rather than all rows sent by
     * all shards).
     */
    public static Optional<ReductionPlan> planReduceDriverLimitBy(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        SetupContext ctx = buildSetupContext(contextFactory, originalPlan);
        if (ctx == null || !(ctx.pipelineBreaker instanceof LimitBy limitBy)) {
            return Optional.empty();
        }

        AttributeSet groupingRefsSet = AttributeSet.of(limitBy.groupings().stream().flatMap(g -> g.references().stream()).toList());
        List<Attribute> expectedDataOutput = new ArrayList<>();
        for (Attribute a : ctx.physicalPlanOutput) {
            if (ctx.topLevelProject.outputSet().contains(a) || groupingRefsSet.contains(a) || EsQueryExec.isDocAttribute(a)) {
                expectedDataOutput.add(a);
            }
        }

        return Optional.of(
            assembleReductionPlan(
                ctx,
                originalPlan,
                expectedDataOutput,
                plan -> plan.transformDown(
                    LimitByExec.class,
                    t -> t.replaceChild(new ExchangeSourceExec(limitBy.source(), expectedDataOutput, false))
                )
            )
        );
    }

    /**
     * Extracts the common setup shared by all three {@code planReduceDriver*} methods: fragment and project extraction,
     * {@code _doc} attribute discovery, {@link EsRelation} patching, and the defensive doc-survives check.
     * Returns {@code null} if any prerequisite is missing (callers must also check the {@code pipelineBreaker} type).
     */
    private static SetupContext buildSetupContext(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        if (!(originalPlan.child() instanceof FragmentExec fragmentExec)) {
            return null;
        }
        if (!(fragmentExec.fragment() instanceof Project topLevelProject)) {
            return null;
        }

        LogicalPlan pipelineBreaker = topLevelProject.child();
        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_LATE_MATERIALIZATION_REPLACEMENT);

        List<Attribute> physicalPlanOutput = PlannerUtils.toPhysicalPlanForReductionSchema(pipelineBreaker, context).output();
        Attribute doc = physicalPlanOutput.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (doc == null) {
            return null;
        }

        LogicalPlan withAddedDocToRelation = pipelineBreaker.transformUp(EsRelation.class, r -> {
            if (r.indexMode() == IndexMode.LOOKUP) {
                return r;
            }
            return r.withAttributes(CollectionUtils.prependToCopy(doc, r.output()));
        });
        // Defensive check: if any intermediate project removed the doc field, abort this optimization.
        if (withAddedDocToRelation.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return null;
        }

        return new SetupContext(fragmentExec, topLevelProject, pipelineBreaker, context, physicalPlanOutput, withAddedDocToRelation);
    }

    /**
     * Builds the final {@link ReductionPlan} from the common context, the filtered {@code expectedDataOutput},
     * and a caller-supplied function that wires the specific exec node (e.g. {@link TopNExec}) to the exchange source.
     */
    private static ReductionPlan assembleReductionPlan(
        SetupContext ctx,
        ExchangeSinkExec originalPlan,
        List<Attribute> expectedDataOutput,
        Function<PhysicalPlan, PhysicalPlan> reductionPlanTransformer
    ) {
        var updatedFragment = new Project(Source.EMPTY, ctx.withAddedDocToRelation, expectedDataOutput);
        FragmentExec updatedFragmentExec = ctx.fragmentExec.withFragment(updatedFragment);
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(updatedFragmentExec);

        PhysicalPlan reductionPlan = reductionPlanTransformer.apply(
            PlannerUtils.toPhysicalPlanForReductionSchema(ctx.fragmentExec.fragment(), ctx.context)
        );
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), reductionPlan);
        return new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan);
    }

    private record SetupContext(
        FragmentExec fragmentExec,
        Project topLevelProject,
        LogicalPlan pipelineBreaker,
        LocalPhysicalOptimizerContext context,
        List<Attribute> physicalPlanOutput,
        LogicalPlan withAddedDocToRelation
    ) {}

    private LateMaterializationPlanner() { /* static class */ }

    // We don't have real search stats during the reduce planning phase, so we assume all fields exist and have no other meaningful stats.
    // The local data optimizer will use the real statistics.
    private static final SearchStats SEARCH_STATS_LATE_MATERIALIZATION_REPLACEMENT = new SearchStats.UnsupportedSearchStats() {
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
}
