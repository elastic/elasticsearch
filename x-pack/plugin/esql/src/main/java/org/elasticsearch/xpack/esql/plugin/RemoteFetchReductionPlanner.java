/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceFieldWithConstantOrNull;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RemoteFetchSource;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Plans remote-fetch rewrites around single-step data-node reductions. The initial supported shape is TopN:
 * defer fetchable fields until after the coordinator has selected the global TopN rows, while still running
 * data-node and node-reduce TopN on eager sort columns.
 * <p>
 * TODO: Keep this TopN-only until the path settles, then evaluate other doc-set reducers one at a time.
 * <ul>
 *     <li>{@code LIMIT}: likely the simplest follow-up; the reducer needs only retained row handles.</li>
 *     <li>{@code LIMIT BY}: needs grouping keys eagerly and can fetch non-key output fields after winner selection.</li>
 *     <li>{@code TopNBy}: needs grouping keys and sort keys eagerly, then fetches the remaining winner fields.</li>
 *     <li>Non-starters: plain {@code ORDER BY} does not reduce the doc set, and aggregate-style breakers
 *     like {@code STATS}, {@code METRICS}, and {@code TS} produce summary rows instead of original rows.</li>
 *     <li>{@code FORK}/{@code FUSE}: possible later, but require a branch-aware fetch design.</li>
 * </ul>
 */
final class RemoteFetchReductionPlanner {
    record CoordinatorPlan(PhysicalPlan coordinatorPlan, ExchangeSinkExec dataNodePlan) {}

    /**
     * Request-specific state required to plan a remote-fetch reduction; present only when the request opted into remote-fetch TopN.
     */
    record RemoteFetchContext(String localNodeId, String retainedSessionId) {
        RemoteFetchContext {
            Objects.requireNonNull(localNodeId, "localNodeId");
            Objects.requireNonNull(retainedSessionId, "retainedSessionId");
        }
    }

    // Reduce planning only needs field-extraction shape: treat every field as present, but non-indexed, so
    // extraction remains explicit instead of being optimized into Lucene pushdown.
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

    private record TopNPlanningContext(
        FragmentExec fragmentExec,
        Project topLevelProject,
        TopN topN,
        Attribute doc,
        LogicalPlan withAddedDocToRelation,
        List<Attribute> expectedDataOutput,
        LocalPhysicalOptimizerContext optimizerContext
    ) {}

    /**
     * Rewrites the first coordinator TopN over the data exchange so that only its eager columns and an opaque fetch handle cross the
     * exchange. Deferred columns are appended after the global TopN has selected its winners.
     * <pre>
     * coordinator: Project -> TopN -> ExchangeSource[all columns]
     * data:        ExchangeSink[all columns] -> Fragment[Project -> TopN]
     *
     * coordinator: Project -> RemoteFetch -> TopN -> ExchangeSource[handle, eager columns]
     *                                  \-> Fragment[RemoteFetchSource[deferred columns]]
     * data:        ExchangeSink[handle, eager columns] -> Fragment[Project[doc, eager columns] -> TopN]
     * </pre>
     */
    static Optional<CoordinatorPlan> planCoordinatorTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalDataPlan,
        PhysicalPlan coordinatorPlan
    ) {
        TopNPlanningContext planningContext = topNPlanningContext(contextFactory, originalDataPlan).orElse(null);
        if (planningContext == null) {
            return Optional.empty();
        }
        FragmentExec fragmentExec = planningContext.fragmentExec();
        Project topLevelProject = planningContext.topLevelProject();
        List<Attribute> expectedDataOutput = planningContext.expectedDataOutput();

        List<Attribute> exchangeOutput = new ArrayList<>();
        Attribute handle = handleAttribute();
        exchangeOutput.add(handle);
        for (Attribute attr : expectedDataOutput) {
            if (EsQueryExec.isDocAttribute(attr) == false) {
                exchangeOutput.add(attr);
            }
        }

        AttributeSet exchangeOutputSet = AttributeSet.of(exchangeOutput);
        List<Attribute> attributesToFetch = new ArrayList<>();
        for (Attribute attr : topLevelProject.output()) {
            if (exchangeOutputSet.contains(attr) == false) {
                if (isFetchable(attr) == false) {
                    return Optional.empty();
                }
                attributesToFetch.add(attr);
            }
        }
        if (attributesToFetch.isEmpty()) {
            return Optional.empty();
        }

        FragmentExec updatedFragmentExec = fragmentExec.withFragment(
            new Project(Source.EMPTY, planningContext.withAddedDocToRelation(), expectedDataOutput)
        );
        ExchangeSinkExec updatedDataPlan = new ExchangeSinkExec(
            originalDataPlan.source(),
            exchangeOutput,
            originalDataPlan.isIntermediateAgg(),
            updatedFragmentExec
        );
        FragmentExec fetchPlan = new FragmentExec(new RemoteFetchSource(Source.EMPTY, attributesToFetch));

        var replacedTopN = new Holder<Boolean>(false);
        PhysicalPlan updatedCoordinatorPlan = coordinatorPlan.transformDown(TopNExec.class, t -> {
            if (replacedTopN.get() == false
                && t.child() instanceof ExchangeSourceExec source
                && source.output().equals(originalDataPlan.output())) {
                replacedTopN.set(true);
                ExchangeSourceExec updatedSource = new ExchangeSourceExec(source.source(), exchangeOutput, source.isIntermediateAgg());
                TopNExec updatedTopN = t.replaceChild(updatedSource);
                // The fetched fields and the appended output are identical until fetch plans perform pushdown work
                // that derives new columns.
                return new RemoteFetchExec(t.source(), updatedTopN, handle, attributesToFetch, attributesToFetch, fetchPlan);
            }
            return t;
        });
        if (replacedTopN.get() == false) {
            return Optional.empty();
        }
        return Optional.of(new CoordinatorPlan(updatedCoordinatorPlan, updatedDataPlan));
    }

    /**
     * Rewrites the node-reduce TopN to consume the shard plan's doc attribute and eager columns, then encodes each winning doc as the
     * opaque handle expected by the coordinator exchange.
     * <pre>
     * shard data:  ExchangeSink[doc, eager columns] -> Fragment[Project -> TopN]
     * node reduce: ExchangeSink[handle, eager columns]
     *                  -> Project -> Eval[handle = remote_fetch_handle(doc)] -> TopN -> ExchangeSource[doc, eager columns]
     * </pre>
     */
    static Optional<ReductionPlan> planReduceDriverTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan,
        RemoteFetchContext remoteFetchContext
    ) {
        Attribute handle = remoteFetchHandleAttribute(originalPlan.output()).orElse(null);
        if (handle == null) {
            return Optional.empty();
        }
        TopNPlanningContext planningContext = topNPlanningContext(contextFactory, originalPlan).orElse(null);
        if (planningContext == null) {
            return Optional.empty();
        }
        FragmentExec fragmentExec = planningContext.fragmentExec();
        TopN topN = planningContext.topN();
        Attribute doc = planningContext.doc();
        LocalPhysicalOptimizerContext context = planningContext.optimizerContext();
        List<Attribute> expectedDataOutput = planningContext.expectedDataOutput();
        if (expectedDataOutput.stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return Optional.empty();
        }

        FragmentExec updatedFragmentExec = fragmentExec.withFragment(
            new Project(Source.EMPTY, planningContext.withAddedDocToRelation(), expectedDataOutput)
        );
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(updatedFragmentExec);

        // As long as the shard fragment remains Project -> TopN, each data driver emits its pages already sorted. Re-evaluate
        // this check when new fetch plan shapes (e.g. LIMIT) stop guaranteeing sorted shard output.
        boolean fragmentIsSorted = updatedFragmentExec.fragment() instanceof Project p && p.child() instanceof TopN;
        PhysicalPlan reductionPlan = toPhysicalPlanForReductionSchema(fragmentExec.fragment(), context).transformDown(TopNExec.class, t -> {
            PhysicalPlan exchangeExec = new ExchangeSourceExec(topN.source(), expectedDataOutput, false);
            return fragmentIsSorted ? t.replaceChild(exchangeExec).withSortedInput() : t.replaceChild(exchangeExec);
        });
        Alias handleAlias = new Alias(
            Source.EMPTY,
            handle.name(),
            new RemoteFetchHandleFunction(Source.EMPTY, doc, remoteFetchContext.localNodeId(), remoteFetchContext.retainedSessionId()),
            handle.id(),
            true
        );
        PhysicalPlan withHandle = new EvalExec(Source.EMPTY, reductionPlan, List.of(handleAlias));
        PhysicalPlan projected = new ProjectExec(Source.EMPTY, withHandle, originalPlan.output());
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), projected);
        return Optional.of(new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan));
    }

    private static Attribute handleAttribute() {
        return new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            true
        );
    }

    private static Optional<Attribute> remoteFetchHandleAttribute(List<Attribute> attributes) {
        return attributes.stream().filter(RemoteFetchHandle::isAttribute).findFirst();
    }

    private static boolean isFetchable(Attribute attr) {
        if (attr instanceof FieldAttribute fieldAttribute) {
            /*
             * The remote-fetch request currently carries only the field name and data type. Potentially-unmapped and union fields also
             * need the specialized loader or per-index conversion stored in their EsField. Use normal field extraction until the remote
             * request can preserve those semantics.
             */
            return (fieldAttribute.field() instanceof PotentiallyUnmappedKeywordEsField) == false
                && (fieldAttribute.field() instanceof UnionTypeEsField) == false;
        }
        return attr instanceof MetadataAttribute || attr instanceof TemporalityAttribute;
    }

    private static Optional<TopNPlanningContext> topNPlanningContext(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        FragmentExec fragmentExec = originalPlan.child() instanceof FragmentExec fe ? fe : null;
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

        LogicalPlan withAddedDocToRelation = topN.transformUp(EsRelation.class, r -> {
            if (r.indexMode() == IndexMode.LOOKUP) {
                return r;
            }
            if (r.outputSet().contains(doc)) {
                return r;
            }
            return r.withAttributes(CollectionUtils.prependToCopy(doc, r.output()));
        });
        if (withAddedDocToRelation.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return Optional.empty();
        }

        AttributeSet orderRefsSet = AttributeSet.of(topN.order().stream().flatMap(o -> o.references().stream()).toList());
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
     * Builds the local physical plan shape used to derive and preserve the data-driver/node-reduce handoff schema.
     * This intentionally skips the full local optimizer, but still applies the passes required for executable field extraction.
     */
    private static PhysicalPlan toPhysicalPlanForReductionSchema(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        var logicalContext = new LocalLogicalOptimizerContext(context.configuration(), context.foldCtx(), context.searchStats());
        LogicalPlan optimized = new ReplaceFieldWithConstantOrNull().apply(plan, logicalContext);
        return new InsertFieldExtraction().apply(new ReplaceSourceAttributes().apply(LocalMapper.INSTANCE.map(optimized)), context);
    }

    private RemoteFetchReductionPlanner() {}
}
