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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;

// FIXME(gal, NOCOMMIT) document
public class PushCountQueryAndTagsToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        // FIXME(gal, NOCOMMIT) Temp documentation, for later
        if (
        // Ensures we are only grouping by one field (2 aggregates: count + group by field).
        aggregateExec.aggregates().size() == 2
            && aggregateExec.aggregates().get(0) instanceof Alias alias
            && aggregateExec.child() instanceof EvalExec evalExec
            && alias.child() instanceof Count count
            // Ensures the eval exec is a filterless count, since we don't support filters yet.
            && count.hasFilter() == false
            && count.field() instanceof Literal // Ensures count(*), or count(1), or equivalent.
            && evalExec.child() instanceof EsQueryExec queryExec
            && queryExec.queryBuilderAndTags().size() > 1 // Ensures there are query and tags to push down.
        ) {
            EsStatsQueryExec statsQueryExec = new EsStatsQueryExec(
                queryExec.source(),
                queryExec.indexPattern(),
                null, // query
                queryExec.limit(),
                queryExec.attrs(),
                List.of(new EsStatsQueryExec.ByStat(aggregateExec, queryExec.queryBuilderAndTags()))
            );
            // Wrap with FilterExec to remove empty buckets (keep buckets where count > 0)
            // FIXME(gal, NOCOMMIT) Hacky
            // FIXME(gal, NOCOMMIT) document better
            Attribute outputAttr = statsQueryExec.output().get(1);
            var zero = new Literal(Source.EMPTY, 0L, DataType.LONG);
            return new FilterExec(Source.EMPTY, statsQueryExec, new GreaterThan(Source.EMPTY, outputAttr, zero));
        }
        return aggregateExec;
    }
}
