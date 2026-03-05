/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamWrapperQueryBuilder;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.MetricsInfoExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.core.util.Queries.Clause.FILTER;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

public class PlannerUtils {
    private static final Logger LOGGER = LogManager.getLogger(PlannerUtils.class);

    /**
     * When the plan contains children like {@code MergeExec} resulted from the planning of commands such as FORK,
     * we need to break the plan into sub plans and a main coordinator plan.
     * The result pages from each sub plan will be funneled to the main coordinator plan.
     * To achieve this, we wire each sub plan with a {@code ExchangeSinkExec} and add a {@code ExchangeSourceExec}
     * to the main coordinator plan.
     * There is an additional split of each sub plan into a data node plan and coordinator plan.
     * This split is not done here, but as part of {@code PlannerUtils#breakPlanBetweenCoordinatorAndDataNode}.
     */
    public static Tuple<List<PhysicalPlan>, PhysicalPlan> breakPlanIntoSubPlansAndMainPlan(PhysicalPlan plan) {
        var subplans = new Holder<List<PhysicalPlan>>();
        PhysicalPlan mainPlan = plan.transformUp(MergeExec.class, me -> {
            subplans.set(
                me.children()
                    .stream()
                    .map(child -> (PhysicalPlan) new ExchangeSinkExec(child.source(), child.output(), false, child))
                    .toList()
            );
            return new ExchangeSourceExec(me.source(), me.output(), false);
        });

        return new Tuple<>(subplans.get(), mainPlan);
    }

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

    public sealed interface PlanReduction {}

    public enum SimplePlanReduction implements PlanReduction {
        NO_REDUCTION,
    }

    /** The plan here is used as a fallback if the reduce driver cannot be planned in a way that avoids field extraction after TopN. */
    public record TopNReduction(PhysicalPlan plan) implements PlanReduction {}

    public record ReducedPlan(PhysicalPlan plan) implements PlanReduction {}

    public static PlanReduction reductionPlan(PhysicalPlan plan) {
        // find the logical fragment
        var fragments = plan.collectFirstChildren(p -> p instanceof FragmentExec);
        if (fragments.isEmpty()) {
            return SimplePlanReduction.NO_REDUCTION;
        }
        final FragmentExec fragment = (FragmentExec) fragments.getFirst();

        // Though FORK is technically a pipeline breaker, it should never show up here.
        // See also: https://github.com/elastic/elasticsearch/pull/131945/files#r2235572935
        final var pipelineBreakers = fragment.fragment().collectFirstChildren(p -> p instanceof PipelineBreaker);
        if (pipelineBreakers.isEmpty()) {
            return SimplePlanReduction.NO_REDUCTION;
        }
        final LogicalPlan pipelineBreaker = pipelineBreakers.getFirst();
        int estimatedRowSize = fragment.estimatedRowSize();
        return switch (LocalMapper.INSTANCE.map(pipelineBreaker)) {
            case TopNExec topN -> new TopNReduction(EstimatesRowSize.estimateRowSize(estimatedRowSize, topN));
            case AggregateExec aggExec -> getPhysicalPlanReduction(estimatedRowSize, aggExec.withMode(AggregatorMode.INTERMEDIATE));
            case MetricsInfoExec metricsInfoExec -> getPhysicalPlanReduction(
                estimatedRowSize,
                new MetricsInfoExec(
                    metricsInfoExec.source(),
                    metricsInfoExec.child(),
                    metricsInfoExec.outputAttrs(),
                    plan.output(),
                    MetricsInfoExec.Mode.INTERMEDIATE
                )
            );
            case PhysicalPlan p -> getPhysicalPlanReduction(estimatedRowSize, p);
        };
    }

    private static ReducedPlan getPhysicalPlanReduction(int estimatedRowSize, PhysicalPlan plan) {
        return new ReducedPlan(EstimatesRowSize.estimateRowSize(estimatedRowSize, plan));
    }

    public static boolean requiresSortedTimeSeriesSource(PhysicalPlan plan) {
        return plan.anyMatch(e -> {
            if (e instanceof FragmentExec f) {
                return f.fragment().anyMatch(l -> l instanceof EsRelation r && r.indexMode() == IndexMode.TIME_SERIES);
            }
            return false;
        });
    }

    public static void forEachRelation(PhysicalPlan plan, Consumer<EsRelation> action) {
        plan.forEachDown(FragmentExec.class, f -> f.fragment().forEachDown(EsRelation.class, r -> {
            if (r.indexMode() != IndexMode.LOOKUP) {
                action.accept(r);
            }
        }));
    }

    public static PhysicalPlan localPlan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        List<SearchExecutionContext> searchContexts,
        Configuration configuration,
        FoldContext foldCtx,
        PhysicalPlan plan,
        PlanTimeProfile planTimeProfile
    ) {
        return localPlan(plannerSettings, flags, configuration, foldCtx, plan, SearchContextStats.from(searchContexts), planTimeProfile);
    }

    public static PhysicalPlan localPlan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        PhysicalPlan plan,
        SearchStats searchStats,
        PlanTimeProfile planTimeProfile
    ) {
        final var logicalOptimizer = new LocalLogicalPlanOptimizer(new LocalLogicalOptimizerContext(configuration, foldCtx, searchStats));
        var physicalOptimizer = new LocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(plannerSettings, flags, configuration, foldCtx, searchStats)
        );

        return localPlan(plan, logicalOptimizer, physicalOptimizer, planTimeProfile);
    }

    public static PhysicalPlan integrateEsFilterIntoFragment(PhysicalPlan plan, @Nullable QueryBuilder esFilter) {
        return esFilter == null ? plan : plan.transformUp(FragmentExec.class, f -> {
            var fragmentFilter = f.esFilter();
            // TODO: have an ESFilter and push down to EsQueryExec / EsSource
            // This is an ugly hack to push the filter parameter to Lucene
            // TODO: filter integration testing
            var filter = fragmentFilter != null ? boolQuery().filter(fragmentFilter).must(esFilter) : esFilter;
            LOGGER.debug("Fold filter {} to EsQueryExec", filter);
            return f.withFilter(filter);
        });
    }

    public static PhysicalPlan localPlan(
        PhysicalPlan plan,
        LocalLogicalPlanOptimizer logicalOptimizer,
        LocalPhysicalPlanOptimizer physicalOptimizer,
        PlanTimeProfile planTimeProfile
    ) {
        // TODO add a test assertion for the consistency checker (after https://github.com/elastic/elasticsearch/issues/141654, see
        // https://github.com/elastic/elasticsearch/pull/141082/changes#r2745334028);
        var isCoordPlan = new Holder<>(Boolean.TRUE);
        Set<PhysicalPlan> lookupJoinExecRightChildren = plan.collect(LookupJoinExec.class::isInstance)
            .stream()
            .map(x -> ((LookupJoinExec) x).right())
            .collect(Collectors.toSet());

        PhysicalPlan localPhysicalPlan = plan.transformUp(FragmentExec.class, f -> {
            if (lookupJoinExecRightChildren.contains(f)) {
                // Do not optimize the right child of a lookup join exec
                // The data node does not have the right stats to perform the optimization because the stats are on the lookup node
                // Also we only ship logical plans across the network, so the plan needs to remain logical
                return f;
            }
            isCoordPlan.set(Boolean.FALSE);

            // Logical optimization
            boolean profilingEnabled = planTimeProfile != null;
            long logicalStartNanos = profilingEnabled ? System.nanoTime() : 0;
            LogicalPlan optimizedFragment = logicalOptimizer.localOptimize(f.fragment());
            PhysicalPlan physicalFragment = LocalMapper.INSTANCE.map(optimizedFragment);
            if (profilingEnabled) {
                planTimeProfile.addLogicalOptimizationPlanTime(System.nanoTime() - logicalStartNanos);
            }
            QueryBuilder filter = f.esFilter();
            if (filter != null) {
                physicalFragment = physicalFragment.transformUp(
                    EsSourceExec.class,
                    query -> new EsSourceExec(Source.EMPTY, query.indexPattern(), query.indexMode(), query.output(), filter)
                );
            }

            // Physical optimization
            long physicalStartNanos = profilingEnabled ? System.nanoTime() : 0;
            var localOptimized = physicalOptimizer.localOptimize(physicalFragment);
            if (profilingEnabled) {
                planTimeProfile.addPhysicalOptimizationPlanTime(System.nanoTime() - physicalStartNanos);
            }

            return EstimatesRowSize.estimateRowSize(f.estimatedRowSize(), localOptimized);
        });

        PhysicalPlan resultPlan = isCoordPlan.get() ? plan : localPhysicalPlan;

        return resultPlan;
    }

    /**
     * Extracts a filter that can be used to skip unmatched shards on the coordinator.
     */
    public static QueryBuilder canMatchFilter(
        EsqlFlags flags,
        Configuration configuration,
        TransportVersion minTransportVersion,
        PhysicalPlan plan
    ) {
        return detectFilter(flags, configuration, minTransportVersion, plan, CoordinatorRewriteContext.SUPPORTED_FIELDS::contains);
    }

    /**
     * Note that since this filter does not have access to SearchStats, it cannot detect if the field is a text field with a delegate.
     * We currently only use this filter for the @timestamp field, which is always a date field. Any tests that wish to use this should
     * take care to not use it with TEXT fields.
     */
    static QueryBuilder detectFilter(
        EsqlFlags flags,
        Configuration configuration,
        TransportVersion minTransportVersion,
        PhysicalPlan plan,
        Predicate<String> fieldName
    ) {
        // first position is the REST filter, the second the query filter
        final List<QueryBuilder> requestFilters = new ArrayList<>();
        final LucenePushdownPredicates ctx = LucenePushdownPredicates.forCanMatch(minTransportVersion, flags);
        plan.forEachDown(FragmentExec.class, fe -> {
            if (fe.esFilter() != null && fe.esFilter().supportsVersion(minTransportVersion)) {
                requestFilters.add(fe.esFilter());
            }
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
                            && translatable(exp, ctx).finish() == TranslationAware.FinishedTranslatable.YES) {
                            matches.add(exp);
                        }
                    }
                }
                if (matches.isEmpty() == false) {
                    Query qlQuery = TRANSLATOR_HANDLER.asQuery(ctx, Predicates.combineAnd(matches));
                    QueryBuilder builder = qlQuery.toQueryBuilder();
                    if (qlQuery.containsPlan()) {
                        builder = new PlanStreamWrapperQueryBuilder(configuration, builder);
                    }
                    requestFilters.add(builder);
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
        if (DataType.isSpatialOrGrid(dataType)) {
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
            case LONG, DATETIME, DATE_NANOS, UNSIGNED_LONG, COUNTER_LONG, GEOHASH, GEOTILE, GEOHEX -> ElementType.LONG;
            case INTEGER, COUNTER_INTEGER -> ElementType.INT;
            case DOUBLE, COUNTER_DOUBLE -> ElementType.DOUBLE;
            // unsupported fields are passed through as a BytesRef
            case KEYWORD, TEXT, IP, SOURCE, VERSION, HISTOGRAM, UNSUPPORTED -> ElementType.BYTES_REF;
            case NULL -> ElementType.NULL;
            case BOOLEAN -> ElementType.BOOLEAN;
            case DOC_DATA_TYPE -> ElementType.DOC;
            case TSID_DATA_TYPE -> ElementType.BYTES_REF;
            case GEO_POINT, CARTESIAN_POINT -> fieldExtractPreference == DOC_VALUES ? ElementType.LONG : ElementType.BYTES_REF;
            case GEO_SHAPE, CARTESIAN_SHAPE -> fieldExtractPreference == EXTRACT_SPATIAL_BOUNDS ? ElementType.INT : ElementType.BYTES_REF;
            case AGGREGATE_METRIC_DOUBLE -> ElementType.AGGREGATE_METRIC_DOUBLE;
            case EXPONENTIAL_HISTOGRAM -> ElementType.EXPONENTIAL_HISTOGRAM;
            case TDIGEST -> ElementType.TDIGEST;
            case DENSE_VECTOR -> ElementType.FLOAT;
            case DATE_RANGE -> ElementType.LONG_RANGE;
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

    /**
     * Checks that the input rows of the plan have been reduced by LIMIT.
     * In the case where non-unary plans are used, such as {@code Fork} or {@code UnionAll},
     * we check that the rows from each branch are reduced by LIMIT.
     */
    public static boolean hasLimitedInput(LogicalPlan plan) {
        while (true) {
            switch (plan) {
                case Limit ignored -> {
                    return true;
                }
                case UnaryPlan unaryPlan -> plan = unaryPlan.child();
                case LookupJoin lookupJoin -> plan = lookupJoin.left();
                case Row ignored -> {
                    return true;
                }
                case LeafPlan ignored -> {
                    return false;
                }
                default -> {
                    return plan.children().stream().allMatch(PlannerUtils::hasLimitedInput);
                }
            }
        }
    }
}
