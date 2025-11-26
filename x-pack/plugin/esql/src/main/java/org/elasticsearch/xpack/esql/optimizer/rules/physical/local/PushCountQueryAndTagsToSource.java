/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;

/**
 * Pushes count aggregations on top of query and tags to source.
 * Will transform:
 * <pre>
 *  Aggregate (count(*) by x)
 *  └── Eval (x = round_to)
 *      └── Query [query + tags]
 *  </pre>
 *  into:
 *  <pre>
 *  Filter (count > 0)
 *  └── StatsQuery [count with query + tags]
 *  </pre>
 *  Where the filter is needed since the original Aggregate would not produce buckets with count = 0.
 */
public class PushCountQueryAndTagsToSource extends PhysicalOptimizerRules.OptimizerRule<AggregateExec> {
    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec) {
        if (
        // Ensures we are only grouping by one field (2 aggregates: count + group by field).
        aggregateExec.aggregates().size() == 2
            && aggregateExec.aggregates().getFirst() instanceof Alias alias
            && alias.child() instanceof Count count
            && count.hasFilter() == false // TODO We don't support filters at the moment (but we definitely should!).
            && count.field() instanceof Literal // Ensures count(*) or equivalent.
            && aggregateExec.child() instanceof EvalExec evalExec
            && evalExec.child() instanceof EsQueryExec queryExec
            && queryExec.queryBuilderAndTags().size() > 1 // Ensures there are query and tags to push down.
            && queryExec.queryBuilderAndTags().stream().allMatch(PushCountQueryAndTagsToSource::isSingleFilterQuery)) {
            EsStatsQueryExec statsQueryExec = new EsStatsQueryExec(
                queryExec.source(),
                queryExec.indexPattern(),
                null, // query
                queryExec.limit(),
                aggregateExec.output(),
                new EsStatsQueryExec.ByStat(queryExec.queryBuilderAndTags())
            );
            // Wrap with FilterExec to remove empty buckets (keep buckets where count > 0). This was automatically handled by the
            // AggregateExec, but since we removed it, we need to do it manually.
            Attribute countAttr = statsQueryExec.output().get(1);
            return new FilterExec(Source.EMPTY, statsQueryExec, new GreaterThan(Source.EMPTY, countAttr, ZERO));
        }
        return aggregateExec;
    }

    private static boolean isSingleFilterQuery(EsQueryExec.QueryBuilderAndTags queryBuilderAndTags) {
        return switch (queryBuilderAndTags.query()) {
            case SingleValueQuery.Builder unused -> true;
            case BoolQueryBuilder bq -> bq.filter().size() + bq.must().size() + bq.should().size() + bq.mustNot().size() <= 1;
            default -> false;
        };
    }

    private static final Literal ZERO = new Literal(Source.EMPTY, 0L, DataType.LONG);
}
