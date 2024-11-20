/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType.COUNT;

/**
 * Looks for the case where certain stats exist right before the query and thus can be pushed down.
 */
public class PushStatsToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<AggregateExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext context) {
        PhysicalPlan plan = aggregateExec;
        if (aggregateExec.child() instanceof EsQueryExec queryExec) {
            var tuple = pushableStats(aggregateExec, context);

            // for the moment support pushing count just for one field
            List<EsStatsQueryExec.Stat> stats = tuple.v2();
            if (stats.size() > 1) {
                return aggregateExec;
            }

            // TODO: handle case where some aggs cannot be pushed down by breaking the aggs into two sources (regular + stats) + union
            // use the stats since the attributes are larger in size (due to seen)
            if (tuple.v2().size() == aggregateExec.aggregates().size()) {
                plan = new EsStatsQueryExec(
                    aggregateExec.source(),
                    queryExec.index(),
                    queryExec.query(),
                    queryExec.limit(),
                    tuple.v1(),
                    tuple.v2()
                );
            }
        }
        return plan;
    }

    private Tuple<List<Attribute>, List<EsStatsQueryExec.Stat>> pushableStats(
        AggregateExec aggregate,
        LocalPhysicalOptimizerContext context
    ) {
        AttributeMap<EsStatsQueryExec.Stat> stats = new AttributeMap<>();
        Tuple<List<Attribute>, List<EsStatsQueryExec.Stat>> tuple = new Tuple<>(new ArrayList<>(), new ArrayList<>());

        if (aggregate.groupings().isEmpty()) {
            for (NamedExpression agg : aggregate.aggregates()) {
                var attribute = agg.toAttribute();
                EsStatsQueryExec.Stat stat = stats.computeIfAbsent(attribute, a -> {
                    if (agg instanceof Alias as) {
                        Expression child = as.child();
                        if (child instanceof Count count) {
                            var target = count.field();
                            String fieldName = null;
                            QueryBuilder query = null;
                            // TODO: add count over field (has to be field attribute)
                            if (target.foldable()) {
                                fieldName = StringUtils.WILDCARD;
                            }
                            // check if regular field
                            else {
                                if (target instanceof FieldAttribute fa) {
                                    var fName = fa.name();
                                    if (context.searchStats().isSingleValue(fName)) {
                                        fieldName = fa.name();
                                        query = QueryBuilders.existsQuery(fieldName);
                                    }
                                }
                            }
                            if (fieldName != null) {
                                return new EsStatsQueryExec.Stat(fieldName, COUNT, query);
                            }
                        }
                    }
                    return null;
                });
                if (stat != null) {
                    List<Attribute> intermediateAttributes = AbstractPhysicalOperationProviders.intermediateAttributes(
                        singletonList(agg),
                        emptyList()
                    );
                    // TODO: the attributes have been recreated here; they will have wrong name ids, and the dependency check will
                    // probably fail when we fix https://github.com/elastic/elasticsearch/issues/105436.
                    // We may need to refactor AbstractPhysicalOperationProviders.intermediateAttributes so it doesn't return just
                    // a list of attributes, but a mapping from the logical to the physical attributes.
                    tuple.v1().addAll(intermediateAttributes);
                    tuple.v2().add(stat);
                }
            }
        }

        return tuple;
    }
}
