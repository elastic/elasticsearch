/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.DateUtils;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class PlannerUtils {

    private static final Mapper mapper = new Mapper(true);

    public static Tuple<PhysicalPlan, PhysicalPlan> breakPlanBetweenCoordinatorAndDataNode(PhysicalPlan plan) {
        var dataNodePlan = new Holder<PhysicalPlan>();

        // split the given plan when encountering the exchange
        PhysicalPlan coordinatorPlan = plan.transformUp(ExchangeExec.class, e -> {
            // remember the datanode subplan and wire it to a sink
            var subplan = e.child();
            dataNodePlan.set(new ExchangeSinkExec(e.source(), subplan));

            // ugly hack to get the layout
            var dummyConfig = new EsqlConfiguration(DateUtils.UTC, StringUtils.EMPTY, StringUtils.EMPTY, QueryPragmas.EMPTY, 1000);
            var planContainingTheLayout = localPlan(List.of(), dummyConfig, subplan);
            // replace the subnode with an exchange source
            return new ExchangeSourceExec(e.source(), e.output(), planContainingTheLayout);
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
                .forEachUp(EsRelation.class, r -> indices.addAll(Arrays.asList(Strings.commaDelimitedListToStringArray(r.index().name()))))
        );
        return indices.toArray(String[]::new);
    }

    public static PhysicalPlan localPlan(List<SearchContext> searchContexts, EsqlConfiguration configuration, PhysicalPlan plan) {
        var isCoordPlan = new Holder<>(Boolean.TRUE);

        var localPhysicalPlan = plan.transformUp(FragmentExec.class, f -> {
            isCoordPlan.set(Boolean.FALSE);
            var optimizedFragment = new LocalLogicalPlanOptimizer().localOptimize(f.fragment());
            var physicalFragment = mapper.map(optimizedFragment);
            var filter = f.esFilter();
            if (filter != null) {
                physicalFragment = physicalFragment.transformUp(
                    EsSourceExec.class,
                    query -> new EsSourceExec(Source.EMPTY, query.index(), query.output(), filter)
                );
            }
            return physicalFragment;
        });
        return isCoordPlan.get()
            ? plan
            : new LocalPhysicalPlanOptimizer(new LocalPhysicalOptimizerContext(configuration)).localOptimize(localPhysicalPlan);
    }

    /**
     * Extracts the ES query provided by the filter parameter
     */
    public static QueryBuilder requestFilter(PhysicalPlan plan) {
        var filter = new Holder<QueryBuilder>(null);
        plan.forEachDown(FragmentExec.class, es -> filter.set(es.esFilter()));
        return filter.get();
    }
}
