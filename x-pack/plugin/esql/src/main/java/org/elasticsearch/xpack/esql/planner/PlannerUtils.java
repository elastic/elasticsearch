/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;
import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.core.util.Queries.Clause.FILTER;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

public class PlannerUtils {

    public static Tuple<PhysicalPlan, PhysicalPlan> breakPlanBetweenCoordinatorAndDataNode(PhysicalPlan plan, Configuration config) {
        var dataNodePlan = new Holder<PhysicalPlan>();

        // split the given plan when encountering the exchange
        PhysicalPlan coordinatorPlan = plan.transformUp(ExchangeExec.class, e -> {
            // remember the datanode subplan and wire it to a sink
            var subplan = e.child();
            dataNodePlan.set(new ExchangeSinkExec(e.source(), e.output(), e.inBetweenAggs(), subplan));

            return new ExchangeSourceExec(e.source(), e.output(), e.inBetweenAggs());
        });
        return new Tuple<>(coordinatorPlan, dataNodePlan.get());
    }

    public static PhysicalPlan reductionPlan(PhysicalPlan plan) {
        // find the logical fragment
        var fragments = plan.collectFirstChildren(p -> p instanceof FragmentExec);
        if (fragments.isEmpty()) {
            return null;
        }
        final FragmentExec fragment = (FragmentExec) fragments.get(0);

        final var pipelineBreakers = fragment.fragment().collectFirstChildren(Mapper::isPipelineBreaker);
        if (pipelineBreakers.isEmpty()) {
            return null;
        }
        final var pipelineBreaker = pipelineBreakers.get(0);
        final LocalMapper mapper = new LocalMapper();
        PhysicalPlan reducePlan = mapper.map(pipelineBreaker);
        if (reducePlan instanceof AggregateExec agg) {
            reducePlan = agg.withMode(AggregatorMode.INTERMEDIATE);
        }
        return EstimatesRowSize.estimateRowSize(fragment.estimatedRowSize(), reducePlan);
    }

    /**
     * Returns a set of concrete indices after resolving the original indices specified in the FROM command.
     */
    public static Set<String> planConcreteIndices(PhysicalPlan plan) {
        if (plan == null) {
            return Set.of();
        }
        var indices = new LinkedHashSet<String>();
        forEachRelation(plan, relation -> indices.addAll(relation.concreteIndices()));
        return indices;
    }

    /**
     * Returns the original indices specified in the FROM command of the query. We need the original query to resolve alias filters.
     */
    public static String[] planOriginalIndices(PhysicalPlan plan) {
        if (plan == null) {
            return Strings.EMPTY_ARRAY;
        }
        var indices = new LinkedHashSet<String>();
        forEachRelation(plan, relation -> indices.addAll(asList(Strings.commaDelimitedListToStringArray(relation.indexPattern()))));
        return indices.toArray(String[]::new);
    }

    private static void forEachRelation(PhysicalPlan plan, Consumer<EsRelation> action) {
        plan.forEachDown(FragmentExec.class, f -> f.fragment().forEachDown(EsRelation.class, r -> {
            if (r.indexMode() != IndexMode.LOOKUP) {
                action.accept(r);
            }
        }));
    }

    public static PhysicalPlan localPlan(
        List<SearchExecutionContext> searchContexts,
        Configuration configuration,
        FoldContext foldCtx,
        PhysicalPlan plan
    ) {
        return localPlan(configuration, foldCtx, plan, SearchContextStats.from(searchContexts));
    }

    public static PhysicalPlan localPlan(Configuration configuration, FoldContext foldCtx, PhysicalPlan plan, SearchStats searchStats) {
        final var logicalOptimizer = new LocalLogicalPlanOptimizer(new LocalLogicalOptimizerContext(configuration, foldCtx, searchStats));
        var physicalOptimizer = new LocalPhysicalPlanOptimizer(new LocalPhysicalOptimizerContext(configuration, foldCtx, searchStats));

        return localPlan(plan, logicalOptimizer, physicalOptimizer);
    }

    public static PhysicalPlan localPlan(
        PhysicalPlan plan,
        LocalLogicalPlanOptimizer logicalOptimizer,
        LocalPhysicalPlanOptimizer physicalOptimizer
    ) {
        final LocalMapper localMapper = new LocalMapper();
        var isCoordPlan = new Holder<>(Boolean.TRUE);

        var localPhysicalPlan = plan.transformUp(FragmentExec.class, f -> {
            isCoordPlan.set(Boolean.FALSE);
            var optimizedFragment = logicalOptimizer.localOptimize(f.fragment());
            var physicalFragment = localMapper.map(optimizedFragment);
            var filter = f.esFilter();
            if (filter != null) {
                physicalFragment = physicalFragment.transformUp(
                    EsSourceExec.class,
                    query -> new EsSourceExec(
                        Source.EMPTY,
                        query.indexPattern(),
                        query.indexMode(),
                        query.indexNameWithModes(),
                        query.output(),
                        filter
                    )
                );
            }
            var localOptimized = physicalOptimizer.localOptimize(physicalFragment);
            return EstimatesRowSize.estimateRowSize(f.estimatedRowSize(), localOptimized);
        });
        return isCoordPlan.get() ? plan : localPhysicalPlan;
    }

    /**
     * Extracts a filter that can be used to skip unmatched shards on the coordinator.
     */
    public static QueryBuilder canMatchFilter(PhysicalPlan plan) {
        return detectFilter(plan, CoordinatorRewriteContext.SUPPORTED_FIELDS::contains);
    }

    /**
     * Note that since this filter does not have access to SearchStats, it cannot detect if the field is a text field with a delegate.
     * We currently only use this filter for the @timestamp field, which is always a date field. Any tests that wish to use this should
     * take care to not use it with TEXT fields.
     */
    static QueryBuilder detectFilter(PhysicalPlan plan, Predicate<String> fieldName) {
        // first position is the REST filter, the second the query filter
        final List<QueryBuilder> requestFilters = new ArrayList<>();
        plan.forEachDown(FragmentExec.class, fe -> {
            requestFilters.add(fe.esFilter());
            // detect filter inside the query
            fe.fragment().forEachUp(Filter.class, f -> {
                // the only filter that can be pushed down is that on top of the relation
                // reuses the logic from LocalPhysicalPlanOptimizer#PushFiltersToSource
                // but get executed on the logical plan
                List<Expression> matches = new ArrayList<>();
                if (f.child() instanceof EsRelation) {
                    var conjunctions = Predicates.splitAnd(f.condition());
                    // look only at expressions that contain literals and the target field
                    for (var exp : conjunctions) {
                        var refsBuilder = AttributeSet.builder().addAll(exp.references());
                        // remove literals or attributes that match by name
                        boolean matchesField = refsBuilder.removeIf(e -> fieldName.test(e.name()));
                        // the expression only contains the target reference
                        // and the expression is pushable (functions can be fully translated)
                        if (matchesField
                            && refsBuilder.isEmpty()
                            && translatable(exp, LucenePushdownPredicates.DEFAULT).finish() == TranslationAware.FinishedTranslatable.YES) {
                            matches.add(exp);
                        }
                    }
                }
                if (matches.isEmpty() == false) {
                    requestFilters.add(
                        TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, Predicates.combineAnd(matches)).toQueryBuilder()
                    );
                }
            });
        });

        return Queries.combine(FILTER, requestFilters);
    }

    /**
     * Map QL's {@link DataType} to the compute engine's {@link ElementType}, for sortable types only.
     * This specifically excludes spatial data types, which are not themselves sortable.
     */
    public static ElementType toSortableElementType(DataType dataType) {
        if (DataType.isSpatial(dataType)) {
            return ElementType.UNKNOWN;
        }
        return toElementType(dataType);
    }

    /**
     * Map QL's {@link DataType} to the compute engine's {@link ElementType}.
     */
    public static ElementType toElementType(DataType dataType) {
        return toElementType(dataType, NONE);
    }

    /**
     * Map QL's {@link DataType} to the compute engine's {@link ElementType}.
     * Under some situations, the same data type might be extracted into a different element type.
     * For example, spatial types can be extracted into doc-values under specific conditions, otherwise they extract as BytesRef.
     */
    public static ElementType toElementType(DataType dataType, MappedFieldType.FieldExtractPreference fieldExtractPreference) {

        return switch (dataType) {
            case LONG, DATETIME, DATE_NANOS, UNSIGNED_LONG, COUNTER_LONG -> ElementType.LONG;
            case INTEGER, COUNTER_INTEGER -> ElementType.INT;
            case DOUBLE, COUNTER_DOUBLE -> ElementType.DOUBLE;
            // unsupported fields are passed through as a BytesRef
            case KEYWORD, TEXT, IP, SOURCE, VERSION, UNSUPPORTED -> ElementType.BYTES_REF;
            case NULL -> ElementType.NULL;
            case BOOLEAN -> ElementType.BOOLEAN;
            case DOC_DATA_TYPE -> ElementType.DOC;
            case TSID_DATA_TYPE -> ElementType.BYTES_REF;
            case GEO_POINT, CARTESIAN_POINT -> fieldExtractPreference == DOC_VALUES ? ElementType.LONG : ElementType.BYTES_REF;
            case GEO_SHAPE, CARTESIAN_SHAPE -> fieldExtractPreference == EXTRACT_SPATIAL_BOUNDS ? ElementType.INT : ElementType.BYTES_REF;
            case PARTIAL_AGG -> ElementType.COMPOSITE;
            case AGGREGATE_METRIC_DOUBLE -> ElementType.AGGREGATE_METRIC_DOUBLE;
            case DENSE_VECTOR -> ElementType.FLOAT;
            case SHORT, BYTE, DATE_PERIOD, TIME_DURATION, OBJECT, FLOAT, HALF_FLOAT, SCALED_FLOAT -> throw EsqlIllegalArgumentException
                .illegalDataType(dataType);
        };
    }

    /**
     * A non-breaking block factory used to create small pages during the planning
     * TODO: Remove this
     */
    @Deprecated(forRemoval = true)
    public static final BlockFactory NON_BREAKING_BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop-esql-breaker"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    public static boolean usesScoring(QueryPlan<?> plan) {
        return plan.output().stream().anyMatch(attr -> attr instanceof MetadataAttribute ma && ma.name().equals(MetadataAttribute.SCORE));
    }
}
