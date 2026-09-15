/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MissingEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
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
import java.util.Set;
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
*  Note that neither plan projects {@code x}: it is a plain field of the main relation, so the node-reduce driver can read it back
*  from {@code _doc} if it ever needs it (the pushdown-aware variant of this was an enhancement made by #137920).
*
*  <p>Which attributes have to survive the cut is decided by {@link #attributesReloadableFromDoc}: anything the node-reduce driver
*  can re-read from the index given {@code _doc} is dropped, everything else (sort/grouping keys of the pipeline breaker,
*  {@code _score}, {@code EVAL} results, {@code LOOKUP JOIN} right-hand fields, ...) is kept. Using the top-level {@link Project}
*  as a proxy for "what must cross the exchange" would be wrong: for plans without a narrowing {@code KEEP} - most notably every
*  {@code FORK} branch - that {@link Project} is the whole relation and prunes nothing.
*/
class LateMaterializationPlanner {
    /**
     * Metadata attributes with a verified block loader, so the node-reduce driver can re-read them from {@code _doc}. This is an
     * allow-list rather than an {@code instanceof MetadataAttribute} test on purpose: {@code _score} has no block loader at all, and
     * the loaders of the remaining metadata attributes ({@code _version}, {@code _tsid}, {@code _tier}, {@code _slice}, ...) have not
     * been verified in this context.
     */
    private static final Set<String> RELOADABLE_METADATA_ATTRIBUTES = Set.of(
        MetadataAttribute.INDEX,
        IdFieldMapper.NAME,
        SourceFieldMapper.NAME
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

        // The order below matters. We first map the fragment, then splice the exchange source under the pipeline breaker, and only
        // then insert the field extractions. Inserting them first (as we used to) would derive the reduce-side extract list from a
        // plan in which everything that expectedDataOutput just pruned was still available *below* the pipeline breaker; splicing
        // the exchange in afterwards would drop those producers and leave the reduce plan referencing attributes nothing produces.
        // Doing it in this order is safe because after the splice the only leaf is the ExchangeSourceExec (which carries _doc), and
        // the plan contains no FieldExtractExec yet.
        PhysicalPlan mappedPlan = PlannerUtils.toMappedPlanForReductionSchema(ctx.fragmentExec.fragment(), ctx.context);
        PhysicalPlan reductionPlan = new InsertFieldExtraction().apply(reductionPlanTransformer.apply(mappedPlan), ctx.context);
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), reductionPlan);
        return new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan);
    }

    /**
     * The subset of {@code ctx.physicalPlanOutput} that has to cross the exchange from the data drivers to the node-reduce driver.
     * Everything the node-reduce driver can read back from the index on its own (see {@link #attributesReloadableFromDoc}) is left
     * out, so the data drivers do not pay for loading it once per slice.
     *
     * @param mustCrossExchange attributes the reduce-side pipeline breaker needs as input - its sort and/or grouping keys. They sit
     *                          <i>below</i> the reduce-side field extraction, so re-reading them there is not an option: it would
     *                          make {@link InsertFieldExtraction} push an extract under the pipeline breaker, which is correct but
     *                          defeats the whole point of late materialization.
     */
    private static List<Attribute> expectedDataOutput(SetupContext ctx, AttributeSet mustCrossExchange) {
        AttributeSet reloadable = attributesReloadableFromDoc(ctx.pipelineBreaker);
        // The result becomes the projection of a Project over ctx.withAddedDocToRelation, so it may only name attributes that
        // logical plan can produce. physicalPlanOutput is wider than that: for a TSDB index ReplaceSourceAttributes synthesizes a
        // fresh FieldAttribute per EsQueryExec.TIME_SERIES_SOURCE_FIELDS (_ts_slice_index, _ts_future_max_timestamp) that exists only
        // below the physical leaf. Those are not the output of any EsRelation, so attributesReloadableFromDoc never sees them and the
        // "not reloadable => must cross" default below would wrongly keep them; the data node mints its own pair with different name
        // ids when it maps the fragment, leaving the Project referencing attributes nothing produces.
        AttributeSet producibleByFragment = ctx.withAddedDocToRelation.outputSet();
        assert producibleByFragment.containsAll(mustCrossExchange)
            : "pipeline breaker keys are not producible by the data-side fragment: " + mustCrossExchange.subtract(producibleByFragment);
        // Preserve the iteration order of physicalPlanOutput: it is the exchange layout on both sides.
        List<Attribute> expectedDataOutput = new ArrayList<>(ctx.physicalPlanOutput.size());
        for (Attribute a : ctx.physicalPlanOutput) {
            if (producibleByFragment.contains(a) == false) {
                continue;
            }
            // Nothing sits between the pipeline breaker and the top-level Project, so an attribute that is neither the breaker's own
            // input nor part of that Project has no consumer above the exchange at all - shipping it is pure waste. For the shapes
            // this class exists to fix the Project covers the whole relation, so the term is vacuously true there and the
            // reloadability test below is what does the pruning.
            if (EsQueryExec.isDocAttribute(a)
                || mustCrossExchange.contains(a)
                || (ctx.topLevelProject.outputSet().contains(a) && reloadable.contains(a) == false)) {
                expectedDataOutput.add(a);
            }
        }
        return expectedDataOutput;
    }

    /**
     * The attributes that the node-reduce driver can load from the index itself, given only the {@code _doc} of a surviving row.
     * Sending those across the exchange is pure waste: the data drivers would load them for every row that reaches their own
     * pipeline breaker (and, since #143133 partitions by segment, once per slice), while the reduce driver only needs them for the
     * rows that survive.
     *
     * <p>Reloadability is decided by <i>provenance</i>, not by attribute class. A {@code LOOKUP JOIN} right-hand side contributes
     * {@link FieldAttribute}s too, but they belong to the lookup index and cannot be read from the main index's {@code _doc}, so
     * relations in {@link IndexMode#LOOKUP} are skipped. Everything that is not an output of a main {@link EsRelation} - reference
     * attributes from {@code EVAL}/{@code MV_EXPAND}/expression sort keys, nullified fields, {@code _score} - is simply never added.
     */
    private static AttributeSet attributesReloadableFromDoc(LogicalPlan pipelineBreaker) {
        AttributeSet.Builder reloadable = AttributeSet.builder();
        pipelineBreaker.forEachDown(EsRelation.class, relation -> {
            if (relation.indexMode() == IndexMode.LOOKUP) {
                return;
            }
            for (Attribute a : relation.output()) {
                if (isReloadableFromDoc(a)) {
                    reloadable.add(a);
                }
            }
        });
        return reloadable.build();
    }

    private static boolean isReloadableFromDoc(Attribute a) {
        boolean reloadable;
        if (EsQueryExec.isDocAttribute(a)) {
            // _doc is what everything else is reloaded from, so it can never itself be dropped. Belt and braces rather than
            // load-bearing: expectedDataOutput short-circuits on _doc, and the relations below the pipeline breaker carry none yet
            // (withAddedDocToRelation prepends it only after this has run). It stays because _doc is a plain FieldAttribute, so
            // without it the branch below would call it reloadable.
            reloadable = false;
        } else if (a.getClass() == FieldAttribute.class) {
            // An exact class check, not instanceof: the FieldAttribute subclasses (TimeSeriesMetadataAttribute, UnsupportedAttribute)
            // load through paths we have not verified here.
            FieldAttribute fa = (FieldAttribute) a;
            // TODO: unmapped and nullified fields could be reloadable too, but they depend on the unmapped-field machinery; see #146068.
            reloadable = fa.isPotentiallyUnmapped() == false
                && fa.field() instanceof MissingEsField == false
                && fa.dataType() != DataType.NULL;
        } else {
            reloadable = a instanceof MetadataAttribute ma && RELOADABLE_METADATA_ATTRIBUTES.contains(ma.name());
        }
        assert reloadable == false || MetadataAttribute.isScoreAttribute(a) == false
            : "_score is produced by the Lucene source operator and has no block loader; it must always cross the exchange";
        return reloadable;
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
