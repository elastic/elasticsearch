/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Looks for the case where certain stats exist right before the query and thus can be pushed down.
 */
public class ReplaceSampledStatsByExactStats extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    SampledAggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(SampledAggregateExec sampledAggregateExec, LocalPhysicalOptimizerContext context) {
        if (sampledAggregateExec.child() instanceof EvalExec evalExec
            && evalExec.expressions().size() == 1
            && evalExec.expressions().getFirst() instanceof Alias alias
            && alias.name().equals("$bucket_id")
            && evalExec.child() instanceof EsQueryExec queryExec) {

            var tuple = PushStatsToSource.pushableStats(sampledAggregateExec.groupings(), sampledAggregateExec.originalAggregates(), context);

            // for the moment support pushing count just for one field
            List<EsStatsQueryExec.Stat> stats = tuple.v2();
            if (stats.size() != 1 || stats.size() != sampledAggregateExec.originalAggregates().size()) {
                return sampledAggregateExec;
            }

            List<Alias> nullBuckets = sampledAggregateExec.outputSet()
                .stream()
                .filter(attr -> attr.name().contains("$bucket$"))
                .map(attr -> new Alias(Source.EMPTY, attr.name(), Literal.NULL, attr.id()))
                .collect(Collectors.toList());
            List<NamedExpression> aggregates = sampledAggregateExec.aggregates()
                .stream()
                .filter(attr -> attr.name().contains("$bucket$") == false)
                .collect(Collectors.toList());
            List<Attribute> intermediateAttributes = sampledAggregateExec.intermediateAttributes()
                .stream()
                .filter(attr -> attr.name().contains("$bucket$") == false)
                .collect(Collectors.toList());

            PhysicalPlan plan = new AggregateExec(
                sampledAggregateExec.source(),
                queryExec,
                sampledAggregateExec.groupings(),
                aggregates,
                sampledAggregateExec.getMode(),
                intermediateAttributes,
                sampledAggregateExec.estimatedRowSize()
            );

            return new EvalExec(Source.EMPTY, plan, nullBuckets);
        } else {
            return sampledAggregateExec;
        }
    }
}
