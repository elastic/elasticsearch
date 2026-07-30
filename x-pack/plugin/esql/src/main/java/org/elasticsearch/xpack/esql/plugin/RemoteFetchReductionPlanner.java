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
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
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
class RemoteFetchReductionPlanner {
    static final String HANDLE_ATTRIBUTE_NAME = "_remote_fetch_handle";

    record CoordinatorPlan(PhysicalPlan coordinatorPlan, ExchangeSinkExec dataNodePlan) {}

    static Optional<CoordinatorPlan> planCoordinatorTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalDataPlan,
        PhysicalPlan coordinatorPlan
    ) {
        FragmentExec fragmentExec = originalDataPlan.child() instanceof FragmentExec fe ? fe : null;
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
        List<Attribute> physicalPlanOutput = toNonOptimizedPhysicalDataPlan(topN, context).output();
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

        List<Attribute> exchangeOutput = new ArrayList<>();
        Attribute handle = handleAttribute(topN.source());
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

        FragmentExec updatedFragmentExec = fragmentExec.withFragment(new Project(Source.EMPTY, withAddedDocToRelation, expectedDataOutput));
        ExchangeSinkExec updatedDataPlan = new ExchangeSinkExec(
            originalDataPlan.source(),
            exchangeOutput,
            originalDataPlan.isIntermediateAgg(),
            updatedFragmentExec
        );
        FragmentExec fetchPlan = new FragmentExec(new RemoteFetchSource(Source.EMPTY, attributesToFetch));

        var replacedTopN = new org.elasticsearch.xpack.esql.core.util.Holder<Boolean>(false);
        PhysicalPlan updatedCoordinatorPlan = coordinatorPlan.transformDown(PhysicalPlan.class, p -> {
            if ((p instanceof TopNExec) == false || replacedTopN.get()) {
                return p;
            }
            TopNExec t = (TopNExec) p;
            if ((t.child() instanceof ExchangeSourceExec) == false) {
                return p;
            }
            ExchangeSourceExec source = (ExchangeSourceExec) t.child();
            if (source.output().equals(originalDataPlan.output()) == false) {
                return p;
            }
            replacedTopN.set(true);
            ExchangeSourceExec updatedSource = new ExchangeSourceExec(source.source(), exchangeOutput, source.isIntermediateAgg());
            TopNExec updatedTopN = t.replaceChild(updatedSource);
            return new RemoteFetchExec(t.source(), updatedTopN, handle, attributesToFetch, attributesToFetch, fetchPlan);
        });
        if (replacedTopN.get() == false) {
            return Optional.empty();
        }
        return Optional.of(new CoordinatorPlan(updatedCoordinatorPlan, updatedDataPlan));
    }

    static Optional<ReductionPlan> planReduceDriverTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan,
        String localNodeId,
        String retainedSessionId
    ) {
        Attribute handle = remoteFetchHandleAttribute(originalPlan.output()).orElse(null);
        if (handle == null) {
            return Optional.empty();
        }
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
        List<Attribute> physicalPlanOutput = toNonOptimizedPhysicalDataPlan(topN, context).output();
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
        if (expectedDataOutput.stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return Optional.empty();
        }

        FragmentExec updatedFragmentExec = fragmentExec.withFragment(new Project(Source.EMPTY, withAddedDocToRelation, expectedDataOutput));
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(updatedFragmentExec);

        PhysicalPlan reductionPlan = toNonOptimizedPhysicalDataPlan(fragmentExec.fragment(), context).transformDown(TopNExec.class, t -> {
            PhysicalPlan exchangeExec = new ExchangeSourceExec(topN.source(), expectedDataOutput, false);
            boolean fragmentIsSorted = updatedFragmentExec.fragment() instanceof Project p && p.child() instanceof TopN;
            return fragmentIsSorted ? t.replaceChild(exchangeExec).withSortedInput() : t.replaceChild(exchangeExec);
        });
        Alias handleAlias = new Alias(
            Source.EMPTY,
            handle.name(),
            new RemoteFetchHandleFunction(Source.EMPTY, doc, localNodeId, retainedSessionId),
            handle.id(),
            true
        );
        PhysicalPlan withHandle = new EvalExec(Source.EMPTY, reductionPlan, List.of(handleAlias));
        PhysicalPlan projected = new ProjectExec(Source.EMPTY, withHandle, originalPlan.output());
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), projected);
        return Optional.of(new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan));
    }

    static boolean needsRetainedSearchContexts(PhysicalPlan plan) {
        return plan.anyMatch(p -> p.output().stream().anyMatch(RemoteFetchReductionPlanner::isRemoteFetchHandleAttribute));
    }

    private static Attribute handleAttribute(Source source) {
        return new ReferenceAttribute(source, null, HANDLE_ATTRIBUTE_NAME, DataType.KEYWORD, Nullability.FALSE, null, true);
    }

    private static Optional<Attribute> remoteFetchHandleAttribute(List<Attribute> attributes) {
        return attributes.stream().filter(RemoteFetchReductionPlanner::isRemoteFetchHandleAttribute).findFirst();
    }

    private static boolean isRemoteFetchHandleAttribute(Attribute attr) {
        return attr.synthetic() && attr.name().equals(HANDLE_ATTRIBUTE_NAME) && attr.dataType() == DataType.KEYWORD;
    }

    private static boolean isFetchable(Attribute attr) {
        return attr instanceof FieldAttribute || attr instanceof MetadataAttribute || attr instanceof TemporalityAttribute;
    }

    private static PhysicalPlan toNonOptimizedPhysicalDataPlan(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        var logicalContext = new LocalLogicalOptimizerContext(context.configuration(), context.foldCtx(), context.searchStats());
        LogicalPlan optimized = new ReplaceFieldWithConstantOrNull().apply(plan, logicalContext);
        return new InsertFieldExtraction().apply(new ReplaceSourceAttributes().apply(LocalMapper.INSTANCE.map(optimized)), context);
    }

    private RemoteFetchReductionPlanner() {}

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
}
