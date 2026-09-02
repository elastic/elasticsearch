/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RemoteFetchSource;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.FuseScoreEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MetricsInfoExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.TsInfoExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.RemoteFetchHandle;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.elasticsearch.transport.RemoteClusterAware.isRemoteIndexName;

/**
 * Plans coordinator-driven remote fetch while the coordinator and data-node sides still share one physical plan.
 */
public final class PlanRemoteFetch extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        if (context.configuration().pragmas().remoteFetchTopN() == false
            || context.configuration().pragmas().nodeLevelReduction() == false
            || context.configuration().pragmas().fieldExtractPreference() != MappedFieldType.FieldExtractPreference.NONE
            || context.minimumVersion().supports(RemoteFetchBoundaryExec.ESQL_REMOTE_FETCH_TOPN_REDUCTION) == false) {
            return plan;
        }

        List<TopNExec> distributedTopNs = plan.collect(TopNExec.class)
            .stream()
            .filter(topN -> topN.child() instanceof ExchangeExec)
            .toList();
        if (distributedTopNs.size() != 1) {
            return plan;
        }
        TopNExec coordinatorTopN = distributedTopNs.getFirst();
        if (hasOtherPipelineBreaker(plan, coordinatorTopN)) {
            return plan;
        }
        ExchangeExec exchange = (ExchangeExec) coordinatorTopN.child();
        PlanningContext planning = planningContext(exchange.child(), context).orElse(null);
        if (planning == null) {
            return plan;
        }

        List<Attribute> eagerAttributes = planning.dataOutput()
            .stream()
            .filter(attribute -> EsQueryExec.isDocAttribute(attribute) == false)
            .toList();
        Attribute handle = handleAttribute();
        List<Attribute> handoffOutput = outputWith(handle, eagerAttributes);
        AttributeSet handoffOutputSet = AttributeSet.of(handoffOutput);
        List<Attribute> attributesToFetch = new ArrayList<>();
        for (Attribute attribute : planning.topLevelProject().output()) {
            if (handoffOutputSet.contains(attribute) == false) {
                if (isFetchable(attribute) == false) {
                    return plan;
                }
                attributesToFetch.add(attribute);
            }
        }
        if (attributesToFetch.isEmpty()) {
            return plan;
        }

        FragmentExec updatedFragment = planning.fragmentExec()
            .withFragment(new Project(Source.EMPTY, planning.withAddedDocToRelation(), planning.dataOutput()));
        RemoteFetchBoundaryExec boundary = new RemoteFetchBoundaryExec(
            exchange.source(),
            updatedFragment,
            planning.documentAttribute(),
            handle,
            eagerAttributes
        );
        ExchangeExec updatedExchange = new ExchangeExec(exchange.source(), boundary.handoffOutput(), exchange.inBetweenAggs(), boundary);
        FragmentExec fetchPlan = new FragmentExec(new RemoteFetchSource(Source.EMPTY, attributesToFetch));
        RemoteFetchExec remoteFetch = new RemoteFetchExec(
            coordinatorTopN.source(),
            coordinatorTopN.replaceChild(updatedExchange),
            handle,
            attributesToFetch,
            attributesToFetch,
            fetchPlan
        );

        List<Attribute> originalOutput = plan.output();
        PhysicalPlan rewritten = plan.transformDown(TopNExec.class, candidate -> candidate == coordinatorTopN ? remoteFetch : candidate);
        if (rewritten.output().equals(originalOutput) == false) {
            rewritten = new ProjectExec(plan.source(), rewritten, originalOutput);
        }
        return EstimatesRowSize.estimateRowSize(0, rewritten);
    }

    private static Optional<PlanningContext> planningContext(PhysicalPlan exchangeChild, PhysicalOptimizerContext optimizerContext) {
        if ((exchangeChild instanceof FragmentExec fragmentExec)
            && fragmentExec.fragment() instanceof Project topLevelProject
            && topLevelProject.child() instanceof TopN topN) {
            if (topN.child().anyMatch(PipelineBreaker.class::isInstance)
                || hasProjectedEvalBeforeTopN(topN, topLevelProject)
                || isSingleLocalRelation(topN) == false) {
                return Optional.empty();
            }
            LocalPhysicalOptimizerContext localContext = new LocalPhysicalOptimizerContext(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                optimizerContext.configuration(),
                FoldContext.small(),
                SEARCH_STATS_TOP_N_REPLACEMENT
            );
            List<Attribute> physicalOutput = PlannerUtils.toPhysicalPlanForReductionSchema(topN, localContext).output();
            Attribute doc = physicalOutput.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
            if (doc == null) {
                return Optional.empty();
            }
            var withAddedDoc = topN.transformUp(EsRelation.class, relation -> {
                if (relation.indexMode() == IndexMode.LOOKUP || relation.outputSet().contains(doc)) {
                    return relation;
                }
                return relation.withAttributes(CollectionUtils.prependToCopy(doc, relation.output()));
            });
            if (withAddedDoc.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
                return Optional.empty();
            }

            AttributeSet orderReferences = AttributeSet.of(topN.order().stream().flatMap(order -> order.references().stream()).toList());
            List<Attribute> dataOutput = new ArrayList<>();
            for (Attribute attribute : physicalOutput) {
                if (topLevelProject.outputSet().contains(attribute)
                    || orderReferences.contains(attribute)
                    || EsQueryExec.isDocAttribute(attribute)) {
                    dataOutput.add(attribute);
                }
            }
            if (dataOutput.getFirst().equals(doc) == false) {
                dataOutput.remove(doc);
                dataOutput.addFirst(doc);
            }
            return Optional.of(new PlanningContext(fragmentExec, topLevelProject, doc, withAddedDoc, List.copyOf(dataOutput)));
        }
        return Optional.empty();
    }

    private static boolean hasProjectedEvalBeforeTopN(TopN topN, Project topLevelProject) {
        // Optimizer-synthetic sort expressions are part of an eligible TopN. User-authored values computed before it must stay eager.
        return topN.child()
            .collect(Eval.class)
            .stream()
            .flatMap(eval -> eval.generatedAttributes().stream())
            .anyMatch(attribute -> attribute.synthetic() == false && topLevelProject.outputSet().contains(attribute));
    }

    private static boolean isSingleLocalRelation(TopN topN) {
        List<EsRelation> relations = topN.collect(EsRelation.class);
        if (relations.size() != 1) {
            return false;
        }
        EsRelation relation = relations.getFirst();
        if (relation.concreteIndices().isEmpty()) {
            return Arrays.stream(relation.indexPattern().split(",")).map(String::trim).noneMatch(PlanRemoteFetch::isRemoteIndexExpression);
        }
        return relation.concreteIndices().size() == 1
            && relation.concreteIndices().containsKey(LOCAL_CLUSTER_GROUP_KEY)
            && relation.concreteIndices().get(LOCAL_CLUSTER_GROUP_KEY).isEmpty() == false;
    }

    private static boolean hasOtherPipelineBreaker(PhysicalPlan plan, TopNExec coordinatorTopN) {
        // Physical plans have no PipelineBreaker marker. Keep this list aligned with Mapper's logical PipelineBreaker cases.
        return plan.anyMatch(
            candidate -> candidate != coordinatorTopN
                && (candidate instanceof AggregateExec
                    || candidate instanceof FuseScoreEvalExec
                    || candidate instanceof LimitExec
                    || candidate instanceof LimitByExec
                    || candidate instanceof MetricsInfoExec
                    || candidate instanceof TopNExec
                    || candidate instanceof TopNByExec
                    || candidate instanceof TsInfoExec)
        );
    }

    private static boolean isRemoteIndexExpression(String indexExpression) {
        return indexExpression.startsWith("-") ? isRemoteIndexName(indexExpression.substring(1)) : isRemoteIndexName(indexExpression);
    }

    private static boolean isFetchable(Attribute attribute) {
        if (isDirectFetchType(attribute.dataType()) == false) {
            return false;
        }
        if (attribute instanceof FieldAttribute fieldAttribute) {
            // The fetch request preserves only name and type. These are the normal mapped-field implementations with direct loaders;
            // every other subclass carries specialized extraction or conversion semantics that cannot be reconstructed remotely.
            Class<? extends EsField> fieldClass = fieldAttribute.field().getClass();
            return fieldClass == EsField.class
                || fieldClass == KeywordEsField.class
                || fieldClass == TextEsField.class
                || fieldClass == DateEsField.class;
        }
        return attribute.getClass() == MetadataAttribute.class && MetadataAttribute.SCORE.equals(attribute.name()) == false;
    }

    private static boolean isDirectFetchType(DataType dataType) {
        return switch (dataType) {
            case BOOLEAN, LONG, INTEGER, UNSIGNED_LONG, DOUBLE, KEYWORD, TEXT, DATETIME, DATE_NANOS, IP, VERSION -> true;
            case NULL, SOURCE, UNSUPPORTED, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, SHORT, BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT,
                OBJECT, DATE_PERIOD, TIME_DURATION, GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE, GEOHASH, GEOTILE, GEOHEX,
                DOC_DATA_TYPE, TSID_DATA_TYPE, PARTIAL_AGG, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, HISTOGRAM,
                DENSE_VECTOR, FLATTENED, DATE_RANGE, DOUBLE_RANGE -> false;
        };
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

    private static List<Attribute> outputWith(Attribute first, List<Attribute> rest) {
        List<Attribute> output = new ArrayList<>(rest.size() + 1);
        output.add(first);
        output.addAll(rest);
        return output;
    }

    private record PlanningContext(
        FragmentExec fragmentExec,
        Project topLevelProject,
        Attribute documentAttribute,
        LogicalPlan withAddedDocToRelation,
        List<Attribute> dataOutput
    ) {}

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
