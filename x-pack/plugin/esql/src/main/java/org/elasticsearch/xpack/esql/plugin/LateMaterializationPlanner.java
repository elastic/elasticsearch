/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch.Fetchability;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceFieldWithConstantOrNull;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
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
*  Project [_doc, foo]
*  └── TopN [foo, limit=10]
*      └── Filter [x > 10]
*          └── EsRelation [index]
*  </pre>
*  If there's a pushdown, the <i>final</i> plan would be:
*  <pre>
*  Project [_doc, foo]
*  └── EsQuery [index with some TopN pushdown]
*  </pre>
*  Neither plan projects {@code x}: it is a field of the main relation, so the node-reduce driver can read it from {@code _doc}.
*
*  <p>{@link #expectedDataOutput} decides which attributes cross the exchange. An attribute the node-reduce driver can load from
*  {@code _doc} (see {@link #attributesReloadableFromDoc}) is dropped. The rest crosses: the sort and grouping keys of the pipeline
*  breaker, {@code _score}, {@code EVAL} results, {@code LOOKUP JOIN} right-hand fields. The top-level {@link Project} alone cannot
*  make that decision: without a narrowing {@code KEEP}, as in every {@code FORK} branch, it lists the whole relation.
*/
class LateMaterializationPlanner {
    public static Optional<ReductionPlan> planReduceDriverTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        SetupContext ctx = buildSetupContext(contextFactory, originalPlan);
        if (ctx == null || !(ctx.pipelineBreaker instanceof TopN topN)) {
            return Optional.empty();
        }

        AttributeSet orderRefsSet = AttributeSet.of(topN.order().stream().flatMap(o -> o.references().stream()).toList());
        List<Attribute> expectedDataOutput = expectedDataOutput(ctx, orderRefsSet);

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
        List<Attribute> expectedDataOutput = expectedDataOutput(ctx, orderRefsSet.combine(groupingRefsSet));

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
        List<Attribute> expectedDataOutput = expectedDataOutput(ctx, groupingRefsSet);

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

        List<Attribute> physicalPlanOutput = toNonOptimizedPhysicalDataPlan(pipelineBreaker, context).output();
        Attribute doc = physicalPlanOutput.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (doc == null) {
            return null;
        }

        LogicalPlan withAddedDocToRelation = pipelineBreaker.transformUp(EsRelation.class, r -> {
            if (r.indexMode() == IndexMode.LOOKUP) {
                return r;
            }
            if (r.output().stream().anyMatch(EsQueryExec::isDocAttribute)) {
                // ReplaceSourceAttributes made this _doc the doc of the source, and a source outputs only one
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

        // The exchange source goes under the pipeline breaker before the field extractions are inserted. InsertFieldExtraction then
        // sees a plan whose only leaf outputs expectedDataOutput, so it extracts every other field the plan reads above the
        // pipeline breaker, from the _doc the exchange carries.
        PhysicalPlan mappedPlan = toMappedDataPlan(ctx.fragmentExec.fragment(), ctx.context);
        PhysicalPlan reductionPlan = new InsertFieldExtraction().apply(reductionPlanTransformer.apply(mappedPlan), ctx.context);
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), reductionPlan);
        return new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan);
    }

    /**
     * The subset of {@code ctx.physicalPlanOutput} that crosses the exchange from the data drivers to the node-reduce driver.
     * It leaves out what the node-reduce driver can load from the index itself (see {@link #attributesReloadableFromDoc}), so the
     * data drivers do not load it for every row and every slice.
     *
     * @param mustCrossExchange the attributes the reduce-side pipeline breaker reads: its sort and grouping keys. The reduce-side
     *                          field extraction sits above the pipeline breaker, so it cannot provide them.
     */
    private static List<Attribute> expectedDataOutput(SetupContext ctx, AttributeSet mustCrossExchange) {
        AttributeSet reloadable = attributesReloadableFromDoc(ctx.pipelineBreaker);
        // The result is the projection of a Project over ctx.withAddedDocToRelation, so it can only name attributes that logical
        // plan outputs. physicalPlanOutput has more: for a TSDB index ReplaceSourceAttributes creates a FieldAttribute per
        // EsQueryExec.TIME_SERIES_SOURCE_FIELDS in the EsQueryExec. No EsRelation outputs them, and the data node creates its own
        // pair, with other name ids, when it maps the fragment.
        AttributeSet producibleByFragment = ctx.withAddedDocToRelation.outputSet();
        assert producibleByFragment.containsAll(mustCrossExchange)
            : "pipeline breaker keys are not producible by the data-side fragment: " + mustCrossExchange.subtract(producibleByFragment);
        AttributeSet projected = ctx.topLevelProject.outputSet();
        // Keep the order of physicalPlanOutput: it is the exchange layout on both sides.
        List<Attribute> expectedDataOutput = new ArrayList<>(ctx.physicalPlanOutput.size());
        for (Attribute a : ctx.physicalPlanOutput) {
            if (producibleByFragment.contains(a) == false) {
                continue;
            }
            // The top-level Project is the only consumer above the pipeline breaker. An attribute it does not output and the
            // pipeline breaker does not read has no consumer on the reduce side.
            if (EsQueryExec.isDocAttribute(a)
                || mustCrossExchange.contains(a)
                || (projected.contains(a) && reloadable.contains(a) == false)) {
                expectedDataOutput.add(a);
            }
        }
        return expectedDataOutput;
    }

    /**
     * The attributes the node-reduce driver can load from the index, given the {@code _doc} of a row. The data drivers would load
     * them for every row that reaches their own pipeline breaker, once per slice. The node-reduce driver loads them only for the
     * rows that survive.
     *
     * <p>An attribute qualifies when a main {@link EsRelation} under the pipeline breaker outputs it and {@link Fetchability}
     * accepts it. A {@code LOOKUP JOIN} right-hand side outputs {@link FieldAttribute}s of the lookup index, which the
     * {@code _doc} of the main index cannot load, so relations in {@link IndexMode#LOOKUP} do not count. {@code EVAL} results,
     * expression sort keys and {@code MV_EXPAND} targets are not the output of a relation at all.
     */
    private static AttributeSet attributesReloadableFromDoc(LogicalPlan pipelineBreaker) {
        AttributeSet.Builder reloadable = AttributeSet.builder();
        pipelineBreaker.forEachDown(EsRelation.class, relation -> {
            if (relation.indexMode() == IndexMode.LOOKUP) {
                return;
            }
            for (Attribute a : relation.output()) {
                // toMappedDataPlan replaces a NULL-typed field with a constant under the pipeline breaker, where the exchange source
                // goes, so its value has to cross the exchange.
                if (Fetchability.isFetchable(a) && a.dataType() != DataType.NULL) {
                    reloadable.add(a);
                }
            }
        });
        return reloadable.build();
    }

    private record SetupContext(
        FragmentExec fragmentExec,
        Project topLevelProject,
        LogicalPlan pipelineBreaker,
        LocalPhysicalOptimizerContext context,
        List<Attribute> physicalPlanOutput,
        LogicalPlan withAddedDocToRelation
    ) {}

    /**
     * A stripped-down version of {@link org.elasticsearch.xpack.esql.planner.PlannerUtils#localPlan}, doing just the bare minimum to
     * translate the logical plan to a physical one. This is needed here since we need to solidify the expected output between the data
     * drivers and node-reduce one.
     */
    private static PhysicalPlan toNonOptimizedPhysicalDataPlan(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        return new InsertFieldExtraction().apply(toMappedDataPlan(plan, context), context);
    }

    /**
     * {@link #toNonOptimizedPhysicalDataPlan} without the field extractions, for {@link #assembleReductionPlan}, which replaces the
     * plan under the pipeline breaker before it inserts them.
     */
    private static PhysicalPlan toMappedDataPlan(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        var logicalContext = new LocalLogicalOptimizerContext(
            context.configuration(),
            context.foldCtx(),
            context.searchStats(),
            context.flags()
        );
        // Replace NULL-typed fields (from UNMAPPED_FIELDS="NULLIFY") with constant nulls in the *data* node using
        // ReplaceFieldWithConstantOrNull, so that InsertFieldExtraction in the *node-reduce* driver won't try to load them from the index.
        // TODO: Do this in InsertFieldExtraction (See #146068) in the node-reduce driver instead.
        LogicalPlan optimized = new ReplaceFieldWithConstantOrNull().apply(plan, logicalContext);
        return new ReplaceSourceAttributes().apply(LocalMapper.INSTANCE.map(optimized));
    }

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
