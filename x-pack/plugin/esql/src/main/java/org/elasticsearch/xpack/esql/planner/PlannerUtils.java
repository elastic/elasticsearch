/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.Queries;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer.PushFiltersToSource.canPushToSource;
import static org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer.TRANSLATOR_HANDLER;
import static org.elasticsearch.xpack.ql.util.Queries.Clause.FILTER;

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

    public static PhysicalPlan localPlan(List<SearchContext> searchContexts, EsqlConfiguration configuration, PhysicalPlan plan) {
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
                    query -> new EsSourceExec(Source.EMPTY, query.index(), query.output(), filter)
                );
            }
            var localOptimized = physicalOptimizer.localOptimize(physicalFragment);
            return EstimatesRowSize.estimateRowSize(f.estimatedRowSize(), localOptimized);
        });
        return isCoordPlan.get() ? plan : localPhysicalPlan;
    }

    /**
     * Extracts the ES query provided by the filter parameter
     */
    public static QueryBuilder requestFilter(PhysicalPlan plan) {
        return detectFilter(plan, "@timestamp");
    }

    static QueryBuilder detectFilter(PhysicalPlan plan, String fieldName) {
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
                        if (matchesField && refs.isEmpty() && canPushToSource(exp)) {
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
     * This specifically excludes GEO_POINT and CARTESIAN_POINT, which are backed by DataType.LONG
     * but are not themselves sortable (the long can be sorted, but the sort order is not usually useful).
     */
    public static ElementType toSortableElementType(DataType dataType) {
        if (dataType == EsqlDataTypes.GEO_POINT || dataType == EsqlDataTypes.CARTESIAN_POINT) {
            return ElementType.UNKNOWN;
        }
        return toElementType(dataType);
    }

    /**
     * Map QL's {@link DataType} to the compute engine's {@link ElementType}.
     */
    public static ElementType toElementType(DataType dataType) {
        if (dataType == DataTypes.LONG || dataType == DataTypes.DATETIME || dataType == DataTypes.UNSIGNED_LONG) {
            return ElementType.LONG;
        }
        if (dataType == DataTypes.INTEGER) {
            return ElementType.INT;
        }
        if (dataType == DataTypes.DOUBLE) {
            return ElementType.DOUBLE;
        }
        // unsupported fields are passed through as a BytesRef
        if (dataType == DataTypes.KEYWORD
            || dataType == DataTypes.TEXT
            || dataType == DataTypes.IP
            || dataType == DataTypes.SOURCE
            || dataType == DataTypes.VERSION
            || dataType == DataTypes.UNSUPPORTED) {
            return ElementType.BYTES_REF;
        }
        if (dataType == DataTypes.NULL) {
            return ElementType.NULL;
        }
        if (dataType == DataTypes.BOOLEAN) {
            return ElementType.BOOLEAN;
        }
        if (dataType == EsQueryExec.DOC_DATA_TYPE) {
            return ElementType.DOC;
        }
        if (dataType == EsqlDataTypes.GEO_POINT) {
            return ElementType.LONG;
        }
        if (dataType == EsqlDataTypes.CARTESIAN_POINT) {
            return ElementType.LONG;
        }
        throw EsqlIllegalArgumentException.illegalDataType(dataType);
    }
}
