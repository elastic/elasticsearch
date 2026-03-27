/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
 *
 *  If there's more than one query on the query builder, and both filters <b>are on the same field</b>, this rule will also attempt to merge
 *  them before pushing. This can happen due to an external ESFilter (i.e., outside the ES|QL query), or simply to a previously pushed down
 *  WHERE clause.
 *  If the queries cannot be merged, the rule will not be applied.
 */
public class PushCountQueryAndTagsToSource extends PhysicalOptimizerRules.OptimizerRule<AggregateExec> {
    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec) {
        if (
        // Ensures we are only grouping by one field (2 aggregates: count + group by field).
        aggregateExec.aggregates().size() == 2
            && aggregateExec.aggregates().getFirst() instanceof Alias alias
            && alias.child() instanceof Count count
            && count.hasFilter() == false // We don't support pushing down counts where the filter is *on the count itself*.
            && count.field() instanceof Literal // Ensures count(*) or equivalent.
            && aggregateExec.child() instanceof EvalExec evalExec
            && evalExec.child() instanceof EsQueryExec queryExec
            && queryExec.queryBuilderAndTags().size() > 1 // Ensures there are query and tags to push down.
        ) {
            var withFilter = tryMerge(queryExec.queryBuilderAndTags());
            if (withFilter.isEmpty() || withFilter.stream().allMatch(PushCountQueryAndTagsToSource::shouldPush) == false) {
                return aggregateExec;
            }
            EsStatsQueryExec statsQueryExec = new EsStatsQueryExec(
                queryExec.source(),
                queryExec.indexPattern(),
                null, // query
                queryExec.limit(),
                aggregateExec.output(),
                new EsStatsQueryExec.ByStat(withFilter)
            );
            // Wrap with FilterExec to remove empty buckets (keep buckets where count > 0). This was automatically handled by the
            // AggregateExec, but since we removed it, we need to do it manually.
            Attribute countAttr = statsQueryExec.output().get(1);
            return new FilterExec(Source.EMPTY, statsQueryExec, new GreaterThan(Source.EMPTY, countAttr, ZERO));
        }
        return aggregateExec;
    }

    /** We only push down single and simple queries, since otherwise we risk overloading Lucene with a complex query. */
    private static boolean shouldPush(EsQueryExec.QueryBuilderAndTags queryBuilderAndTags) {
        return switch (queryBuilderAndTags.query()) {
            case SingleValueQuery.Builder unused -> true;
            case RangeQueryBuilder unused -> true;
            case TermQueryBuilder unused -> true;
            case ExistsQueryBuilder unused -> true;
            case BoolQueryBuilder bq -> bq.filter().size() + bq.must().size() + bq.should().size() + bq.mustNot().size() <= 1;
            default -> false;
        };
    }

    private static List<EsQueryExec.QueryBuilderAndTags> tryMerge(List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags) {
        return queryBuilderAndTags.stream().flatMap(e -> tryMergeBoolQuery(e).stream()).flatMap(e -> trySimplifyRange(e).stream()).toList();
    }

    /**
     * If the query specifies an impossible range (e.g., from > to), returns {@link Optional#empty()}; if from == to and both ends are
     * inclusive, returns a {@link TermQueryBuilder}; otherwise returns the original range.
     */
    private static Optional<EsQueryExec.QueryBuilderAndTags> trySimplifyRange(EsQueryExec.QueryBuilderAndTags qbt) {
        if (qbt.query() instanceof RangeQueryBuilder rqb && rqb.from() != null && rqb.to() != null) {
            int comparison = compare(rqb.from(), rqb.to());
            if (comparison > 0) {
                // from > to, can remove the query entry.
                return Optional.empty();
            } else if (comparison == 0) {
                // from == to should be kept only if both ends are inclusive, and then we can replace the range with a term query.
                return rqb.includeLower() && rqb.includeUpper()
                    ? Optional.of(qbt.withQuery(new TermQueryBuilder(rqb.fieldName(), rqb.from())))
                    : Optional.empty();
            }
            return Optional.of(qbt); // valid range, keep as is.

        }
        return Optional.of(qbt);
    }

    /**
     * Attempts to merge a {@link BoolQueryBuilder} filters. Returns {@link Optional#empty()} if the query can be dropped. If the filters
     * cannot be merged, returns the original query.
     */
    private static Optional<EsQueryExec.QueryBuilderAndTags> tryMergeBoolQuery(EsQueryExec.QueryBuilderAndTags qbt) {
        if (qbt.query() instanceof BoolQueryBuilder bqb && bqb.filter().size() == 2) {
            QueryBuilder filter1 = bqb.filter().get(0);
            var range1 = tryExtractSingleRangeQuery(filter1);
            QueryBuilder filter2 = bqb.filter().get(1);
            if (range1 != null
                && filter2 instanceof BoolQueryBuilder internalQuery
                && internalQuery.mustNot().size() == 1
                && internalQuery.mustNot().get(0) instanceof ExistsQueryBuilder) {
                // Simple "must not exist" cases can be dropped since they never match anything when combined with a range.
                return Optional.empty();
            }
            var range2 = tryExtractSingleRangeQuery(filter2);
            return Optional.of(mergeRanges(qbt, range1, range2));
        }
        return Optional.of(qbt);
    }

    private static EsQueryExec.QueryBuilderAndTags mergeRanges(
        EsQueryExec.QueryBuilderAndTags original,
        RangeQueryBuilder range1,
        RangeQueryBuilder range2
    ) {
        return range1 == null || range2 == null ? original : merge(range1, range2).map(original::withQuery).orElse(original);
    }

    /** Returns {@link Optional#empty()} if the queries cannot be merged. */
    private static Optional<QueryBuilder> merge(RangeQueryBuilder range1, RangeQueryBuilder range2) {
        if (range1.fieldName().equals(range2.fieldName()) == false) {
            return Optional.empty();
        }
        if (Objects.equals(nonDefaultTimezone(range1.timeZone()), nonDefaultTimezone(range2.timeZone())) == false) {
            return Optional.empty();
        }

        RangeQueryBuilder merged = new RangeQueryBuilder(range1.fieldName());
        setTighterBound(merged, range1.from(), range2.from(), range1.includeLower(), range2.includeLower(), BoundType.FROM);
        setTighterBound(merged, range1.to(), range2.to(), range1.includeUpper(), range2.includeUpper(), BoundType.TO);

        String timeZone = range1.timeZone();
        if (timeZone != null) {
            merged.timeZone(timeZone);
        }

        if (range1.format() != null && range2.format() != null && range1.format().equals(range2.format()) == false) {
            return Optional.empty();
        }
        var format = range1.format() != null ? range1.format() : range2.format();
        if (format != null) {
            merged.format(format);
        }

        merged.boost(Math.max(range1.boost(), range2.boost()));
        return Optional.of(merged);
    }

    private static String nonDefaultTimezone(String s) {
        return s == null || s.equals(DateUtils.UTC.getId()) ? null : s;
    }

    /** Returns {@code null} if no single range query could be extracted. */
    private static @Nullable RangeQueryBuilder tryExtractSingleRangeQuery(QueryBuilder qb) {
        return switch (qb) {
            case RangeQueryBuilder rqb -> rqb;
            case SingleValueQuery.Builder single -> tryExtractSingleRangeQuery(single.next());
            case BoolQueryBuilder bqb when bqb.filter().size() == 1 -> tryExtractSingleRangeQuery(bqb.filter().getFirst());
            default -> null;
        };
    }

    enum BoundType {
        FROM,
        TO
    }

    // Given two bounds, sets the tighter one on the range.
    private static void setTighterBound(
        RangeQueryBuilder range,
        Object bound1,
        Object bound2,
        boolean include1,
        boolean include2,
        BoundType boundType
    ) {
        if (bound1 == null || bound2 == null) {
            if (bound1 != null) {
                setRange(range, bound1, include1, boundType);
            }
            if (bound2 != null) {
                setRange(range, bound2, include2, boundType);
            }
            return;
        }

        int compare = compare(bound1, bound2);
        boolean useFirst = switch (boundType) {
            case FROM -> compare > 0;
            case TO -> compare < 0;
        };
        Object value = useFirst ? bound1 : bound2;
        boolean include = useFirst ? include1 : include2;
        if (compare == 0) {
            include = include1 && include2;
        }

        setRange(range, value, include, boundType);
    }

    @SuppressWarnings("unchecked")
    private static int compare(Object o1, Object o2) {
        return ((Comparable<Object>) o1).compareTo(o2);
    }

    private static void setRange(RangeQueryBuilder range, Object val, boolean include, BoundType boundType) {
        switch (boundType) {
            case FROM -> range.from(val, include);
            case TO -> range.to(val, include);
        }
    }

    private static final Literal ZERO = new Literal(Source.EMPTY, 0L, DataType.LONG);
}
