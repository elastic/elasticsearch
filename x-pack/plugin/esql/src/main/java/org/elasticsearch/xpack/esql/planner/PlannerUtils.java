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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;
import static org.elasticsearch.xpack.esql.core.util.Queries.Clause.FILTER;
import static org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer.PushFiltersToSource.canPushToSource;
import static org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer.TRANSLATOR_HANDLER;

public class PlannerUtils {

    public static Tuple<PhysicalPlan, PhysicalPlan> breakPlanBetweenCoordinatorAndDataNode(PhysicalPlan plan, EsqlConfiguration config) {
        var dataNodePlan = new Holder<PhysicalPlan>();

        // split the given plan when encountering the exchange
        PhysicalPlan coordinatorPlan = plan.transformUp(ExchangeExec.class, e -> {
            // remember the datanode subplan and wire it to a sink
            var subplan = e.child();
            dataNodePlan.set(new ExchangeSinkExec(e.source(), e.output(), e.isInBetweenAggs(), subplan));

            return new ExchangeSourceExec(e.source(), e.output(), e.isInBetweenAggs());
        });
        return new Tuple<>(coordinatorPlan, dataNodePlan.get());
    }

    public static PhysicalPlan dataNodeReductionPlan(LogicalPlan plan, PhysicalPlan unused) {
        var pipelineBreakers = plan.collectFirstChildren(Mapper::isPipelineBreaker);

        if (pipelineBreakers.isEmpty() == false) {
            UnaryPlan pipelineBreaker = (UnaryPlan) pipelineBreakers.get(0);
            if (pipelineBreaker instanceof TopN) {
                Mapper mapper = new Mapper(true);
                var physicalPlan = EstimatesRowSize.estimateRowSize(0, mapper.map(plan));
                return physicalPlan.collectFirstChildren(TopNExec.class::isInstance).get(0);
            } else if (pipelineBreaker instanceof Limit limit) {
                return new LimitExec(limit.source(), unused, limit.limit());
            } else if (pipelineBreaker instanceof OrderBy order) {
                return new OrderExec(order.source(), unused, order.order());
            } else if (pipelineBreaker instanceof Aggregate) {
                Mapper mapper = new Mapper(true);
                var physicalPlan = EstimatesRowSize.estimateRowSize(0, mapper.map(plan));
                var aggregate = (AggregateExec) physicalPlan.collectFirstChildren(AggregateExec.class::isInstance).get(0);
                return aggregate.withMode(AggregateExec.Mode.PARTIAL);
            } else {
                throw new EsqlIllegalArgumentException("unsupported unary physical plan node [" + pipelineBreaker.nodeName() + "]");
            }
        }
        return null;
    }

    /**
     * Returns a set of concrete indices after resolving the original indices specified in the FROM command.
     */
    public static Set<String> planConcreteIndices(PhysicalPlan plan) {
        if (plan == null) {
            return Set.of();
        }
        var indices = new LinkedHashSet<String>();
        plan.forEachUp(FragmentExec.class, f -> f.fragment().forEachUp(EsRelation.class, r -> indices.addAll(r.index().concreteIndices())));
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
        plan.forEachUp(
            FragmentExec.class,
            f -> f.fragment()
                .forEachUp(EsRelation.class, r -> indices.addAll(asList(Strings.commaDelimitedListToStringArray(r.index().name()))))
        );
        return indices.toArray(String[]::new);
    }

    public static PhysicalPlan localPlan(List<SearchExecutionContext> searchContexts, EsqlConfiguration configuration, PhysicalPlan plan) {
        return localPlan(configuration, plan, new SearchStats(searchContexts));
    }

    public static PhysicalPlan localPlan(EsqlConfiguration configuration, PhysicalPlan plan, SearchStats searchStats) {
        final var logicalOptimizer = new LocalLogicalPlanOptimizer(new LocalLogicalOptimizerContext(configuration, searchStats));
        var physicalOptimizer = new LocalPhysicalPlanOptimizer(new LocalPhysicalOptimizerContext(configuration, searchStats));

        return localPlan(plan, logicalOptimizer, physicalOptimizer);
    }

    public static PhysicalPlan localPlan(
        PhysicalPlan plan,
        LocalLogicalPlanOptimizer logicalOptimizer,
        LocalPhysicalPlanOptimizer physicalOptimizer
    ) {
        final Mapper mapper = new Mapper(true);
        var isCoordPlan = new Holder<>(Boolean.TRUE);

        var localPhysicalPlan = plan.transformUp(FragmentExec.class, f -> {
            isCoordPlan.set(Boolean.FALSE);
            var optimizedFragment = logicalOptimizer.localOptimize(f.fragment());
            var physicalFragment = mapper.map(optimizedFragment);
            var filter = f.esFilter();
            if (filter != null) {
                physicalFragment = physicalFragment.transformUp(
                    EsSourceExec.class,
                    query -> new EsSourceExec(Source.EMPTY, query.index(), query.output(), filter, query.indexMode())
                );
            }
            var localOptimized = physicalOptimizer.localOptimize(physicalFragment);
            return EstimatesRowSize.estimateRowSize(f.estimatedRowSize(), localOptimized);
        });
        return isCoordPlan.get() ? plan : localPhysicalPlan;
    }

    /**
     * Extracts the ES query provided by the filter parameter
     * @param plan
     * @param hasIdenticalDelegate a lambda that given a field attribute sayis if it has
     *                             a synthetic source delegate with the exact same value
     * @return
     */
    public static QueryBuilder requestFilter(PhysicalPlan plan, Predicate<FieldAttribute> hasIdenticalDelegate) {
        return detectFilter(plan, "@timestamp", hasIdenticalDelegate);
    }

    static QueryBuilder detectFilter(PhysicalPlan plan, String fieldName, Predicate<FieldAttribute> hasIdenticalDelegate) {
        // first position is the REST filter, the second the query filter
        var requestFilter = new QueryBuilder[] { null, null };

        plan.forEachDown(FragmentExec.class, fe -> {
            requestFilter[0] = fe.esFilter();
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
                        var refs = new AttributeSet(exp.references());
                        // remove literals or attributes that match by name
                        boolean matchesField = refs.removeIf(e -> fieldName.equals(e.name()));
                        // the expression only contains the target reference
                        // and the expression is pushable (functions can be fully translated)
                        if (matchesField && refs.isEmpty() && canPushToSource(exp, hasIdenticalDelegate)) {
                            matches.add(exp);
                        }
                    }
                }
                if (matches.size() > 0) {
                    requestFilter[1] = TRANSLATOR_HANDLER.asQuery(Predicates.combineAnd(matches)).asBuilder();
                }
            });
        });

        return Queries.combine(FILTER, asList(requestFilter));
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
            case LONG, DATETIME, UNSIGNED_LONG, COUNTER_LONG -> ElementType.LONG;
            case INTEGER, COUNTER_INTEGER -> ElementType.INT;
            case DOUBLE, COUNTER_DOUBLE -> ElementType.DOUBLE;
            // unsupported fields are passed through as a BytesRef
            case KEYWORD, TEXT, IP, SOURCE, VERSION, UNSUPPORTED -> ElementType.BYTES_REF;
            case NULL -> ElementType.NULL;
            case BOOLEAN -> ElementType.BOOLEAN;
            case DOC_DATA_TYPE -> ElementType.DOC;
            case TSID_DATA_TYPE -> ElementType.BYTES_REF;
            case GEO_POINT, CARTESIAN_POINT -> fieldExtractPreference == DOC_VALUES ? ElementType.LONG : ElementType.BYTES_REF;
            case GEO_SHAPE, CARTESIAN_SHAPE -> ElementType.BYTES_REF;
            case PARTIAL_AGG -> ElementType.COMPOSITE;
            case SHORT, BYTE, DATE_PERIOD, TIME_DURATION, OBJECT, NESTED, FLOAT, HALF_FLOAT, SCALED_FLOAT ->
                throw EsqlIllegalArgumentException.illegalDataType(dataType);
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

    /**
     * Returns DOC_VALUES if the given boolean is set.
     */
    public static MappedFieldType.FieldExtractPreference extractPreference(boolean hasPreference) {
        return hasPreference ? DOC_VALUES : NONE;
    }
}
