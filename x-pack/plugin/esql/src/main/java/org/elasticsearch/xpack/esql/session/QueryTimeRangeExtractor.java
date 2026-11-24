/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFunctionComparisonRewriter;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Finds the range of {@code @timestamp} that a query restricts an index pattern to, from the parsed (not yet analyzed) plan.
 * <p>
 * A request {@code filter} is passed to field caps as its index filter, so that indices with no shard that can match are left
 * out of index resolution. A {@code WHERE} on {@code @timestamp} in the query selects the same rows, but only takes effect once
 * the plan is analyzed and optimized, which is after field caps loaded and merged the mappings of every index of the pattern.
 * On patterns with many indices and wide mappings that is where most of the time goes. The range extracted here lets index
 * resolution skip those indices just like the request filter does.
 * <p>
 * Skipping an index must not change the rows of the result, so the extraction is conservative on two sides:
 * <ul>
 *     <li>Only filters whose every row comes straight from the relation count: between the relation and the filter the plan may
 *     only hold commands that neither depend on other rows nor redefine {@code @timestamp}, see
 *     {@link PartitionFilterHintExtractor#forEachGuardedRelation}.</li>
 *     <li>The range is only ever wider than the condition. Unsupported parts of a conjunction are ignored, an unsupported part
 *     of a disjunction drops the range, and both ends are inclusive at millisecond resolution.</li>
 * </ul>
 * Like the request filter, a range that prunes an index also removes the fields that only that index maps from the resolved
 * mapping, and with that from the columns of a query that does not name its columns.
 */
final class QueryTimeRangeExtractor {

    private static final String TIMESTAMP = MetadataAttribute.TIMESTAMP_FIELD;
    private static final String TRANGE = "trange";
    private static final String DATE_TRUNC = "date_trunc";

    private QueryTimeRangeExtractor() {}

    /**
     * A range of {@code @timestamp} in epoch milliseconds, inclusive on both ends, each of which is {@code null} when open.
     */
    record TimeRange(@Nullable Long from, @Nullable Long to) {
        static final TimeRange UNBOUNDED = new TimeRange(null, null);

        boolean isUnbounded() {
            return from == null && to == null;
        }

        /** The range that both ranges allow. */
        TimeRange intersect(TimeRange other) {
            return new TimeRange(
                from == null ? other.from : other.from == null ? from : Long.valueOf(Math.max(from, other.from)),
                to == null ? other.to : other.to == null ? to : Long.valueOf(Math.min(to, other.to))
            );
        }

        /** The smallest range that covers both ranges. */
        TimeRange cover(TimeRange other) {
            return new TimeRange(
                from == null || other.from == null ? null : Long.valueOf(Math.min(from, other.from)),
                to == null || other.to == null ? null : Long.valueOf(Math.max(to, other.to))
            );
        }

        /**
         * The range as a filter for field caps. The upper end is sent as the start of the next millisecond, exclusive, so that
         * a {@code date_nanos} field keeps the values that fall within the last millisecond of the range.
         */
        QueryBuilder toQuery() {
            RangeQueryBuilder range = new RangeQueryBuilder(TIMESTAMP).format("epoch_millis");
            if (from != null) {
                range.gte(from);
            }
            if (to != null && to < Long.MAX_VALUE) {
                range.lt(to + 1);
            }
            return range;
        }
    }

    /**
     * The filter on {@code @timestamp} to resolve each index pattern of the plan with. A pattern has no entry when the query
     * does not bound its {@code @timestamp}, or when it is read more than once and one of those reads is not bounded.
     */
    static Map<IndexPattern, QueryBuilder> extract(LogicalPlan parsed, Configuration configuration, EsqlFunctionRegistry functionRegistry) {
        Map<IndexPattern, QueryBuilder> filters = new HashMap<>();
        for (Map.Entry<IndexPattern, TimeRange> range : ranges(parsed, configuration, functionRegistry).entrySet()) {
            if (range.getValue().isUnbounded() == false) {
                filters.put(range.getKey(), range.getValue().toQuery());
            }
        }
        return filters;
    }

    /**
     * The range that the query restricts each of its index patterns to, the widest one for a pattern that is read more than once.
     * <p>
     * The walk is the one that external sources are listed with, see {@link PartitionFilterHintExtractor#forEachGuardedRelation}:
     * it hands over each relation with the conjuncts of the filters that apply to its rows.
     */
    static Map<IndexPattern, TimeRange> ranges(LogicalPlan parsed, Configuration configuration, EsqlFunctionRegistry functionRegistry) {
        Map<IndexPattern, TimeRange> ranges = new HashMap<>();
        Context context = new Context(
            configuration,
            configuration.now().toEpochMilli(),
            QuerySettings.TIME_ZONE.get(configuration.resolvedSettings())
        );
        PartitionFilterHintExtractor.forEachGuardedRelation(
            parsed,
            plan -> plan instanceof UnresolvedRelation || plan instanceof PromqlCommand,
            (plan, conjuncts) -> {
                if (plan instanceof PromqlCommand promql) {
                    // Its rows are not those of the index, a filter on them says nothing about it. Its own time range does.
                    TimeRange range = rangeOf(promql);
                    for (UnresolvedRelation relation : promql.child().collect(UnresolvedRelation.class)) {
                        add(relation, range, ranges);
                    }
                } else {
                    TimeRange range = TimeRange.UNBOUNDED;
                    for (Expression conjunct : conjuncts) {
                        // The constant dates of the condition become literals, the same way that external sources fold them.
                        Expression folded = DateFunctionComparisonRewriter.foldConstants(conjunct, configuration, functionRegistry);
                        range = range.intersect(rangeOf(folded, context));
                    }
                    add((UnresolvedRelation) plan, range, ranges);
                }
            }
        );
        return ranges;
    }

    private static void add(UnresolvedRelation relation, TimeRange range, Map<IndexPattern, TimeRange> ranges) {
        if (relation.indexMode() != IndexMode.LOOKUP) {
            ranges.merge(relation.indexPattern(), range, TimeRange::cover);
        }
    }

    /**
     * What the constants of a query are evaluated with. The time that {@code NOW()} returns and the time zone are read once,
     * so that every bound of a query is taken from the same instant, the one that the query itself runs with.
     */
    private record Context(Configuration configuration, long now, ZoneId zone) {}

    /**
     * The range of {@code @timestamp} that a PromQL query with a {@code start} and an {@code end} can read samples from.
     * <p>
     * A selector evaluated at time {@code t} reads the samples of {@code (t - offset - window, t - offset]}, and {@code t}
     * goes from {@code start} to {@code end}. The window taken here is at least as long as any that the query can look back
     * for: the longest range of its range selectors, the lookback of an instant selector, the step that samples are bucketed
     * by, and the scrape interval that an implicit range falls back to. With the largest and the smallest offset of its
     * selectors, the range covers what every selector reads.
     * <p>
     * A query that evaluates a selector at a fixed time ({@code @}), or over a subquery, reads samples that are not tied to
     * {@code start} and {@code end} this simply, and gets no range. Neither does a query without both ends, whose range comes
     * from the request filter, which already filters index resolution.
     */
    static TimeRange rangeOf(PromqlCommand promql) {
        if (promql.start().value() instanceof Number start && promql.end().value() instanceof Number end) {
            LogicalPlan query = promql.promqlPlan();
            if (query.anyMatch(p -> p instanceof org.elasticsearch.xpack.esql.expression.promql.subquery.Subquery)) {
                return TimeRange.UNBOUNDED;
            }
            Duration window = PromqlCommand.DEFAULT_LOOKBACK;
            Duration maxOffset = Duration.ZERO;
            Duration minOffset = Duration.ZERO;
            for (Selector selector : query.collect(Selector.class)) {
                if (selector instanceof LiteralSelector) {
                    continue;
                }
                Evaluation evaluation = selector.evaluation();
                if (evaluation != null) {
                    if (evaluation.at() != null && evaluation.at().value() != null) {
                        return TimeRange.UNBOUNDED;
                    }
                    Duration offset = evaluation.offsetDuration();
                    maxOffset = offset.compareTo(maxOffset) > 0 ? offset : maxOffset;
                    minOffset = offset.compareTo(minOffset) < 0 ? offset : minOffset;
                }
                if (selector instanceof RangeSelector rangeSelector) {
                    if (rangeSelector.range() instanceof Literal range && range.value() instanceof Duration duration) {
                        // The placeholder of an implicit range is negative. It stands for the step or the scrape interval.
                        window = duration.compareTo(window) > 0 ? duration : window;
                    } else {
                        return TimeRange.UNBOUNDED;
                    }
                }
            }
            try {
                if (promql.isRangeQuery()) {
                    // The step as given, or as it follows from the number of buckets, for which the type of @timestamp is enough.
                    Duration step = PromqlLogicalPlanBuilder.foldStep(
                        new ReferenceAttribute(promql.source(), null, TIMESTAMP, DataType.DATETIME),
                        promql.start(),
                        promql.end(),
                        promql.step(),
                        promql.buckets()
                    );
                    window = step.compareTo(window) > 0 ? step : window;
                }
                if (promql.scrapeInterval().value() instanceof Duration scrapeInterval) {
                    window = scrapeInterval.compareTo(window) > 0 ? scrapeInterval : window;
                }
                return new TimeRange(
                    Math.subtractExact(start.longValue(), Math.addExact(window.toMillis(), maxOffset.toMillis())),
                    Math.subtractExact(end.longValue(), minOffset.toMillis())
                );
            } catch (RuntimeException e) {
                // A step or a range that does not work out, which is for the analysis of the command to report.
                return TimeRange.UNBOUNDED;
            }
        }
        return TimeRange.UNBOUNDED;
    }

    /**
     * The range of {@code @timestamp} outside of which {@code condition} cannot hold, {@link TimeRange#UNBOUNDED} if unknown.
     */
    private static TimeRange rangeOf(Expression condition, Context context) {
        return switch (condition) {
            case And and -> rangeOf(and.left(), context).intersect(rangeOf(and.right(), context));
            case Or or -> rangeOf(or.left(), context).cover(rangeOf(or.right(), context));
            case EsqlBinaryComparison comparison -> rangeOf(comparison, context);
            case UnresolvedFunction function when isFunction(function, TRANGE) -> rangeOfTRange(function, context);
            default -> TimeRange.UNBOUNDED;
        };
    }

    /**
     * The range of a comparison of {@code @timestamp}, or of {@code DATE_TRUNC(interval, @timestamp)}, with a constant date.
     */
    private static TimeRange rangeOf(EsqlBinaryComparison comparison, Context context) {
        boolean timestampOnLeft = isTimestamp(comparison.left()) || truncationOfTimestamp(comparison.left()) != null;
        Expression timestamp = timestampOnLeft ? comparison.left() : comparison.right();
        if (timestampOnLeft == false && isTimestamp(timestamp) == false && truncationOfTimestamp(timestamp) == null) {
            return TimeRange.UNBOUNDED;
        }
        Long value = millis(timestampOnLeft ? comparison.right() : comparison.left(), context);
        if (value == null) {
            return TimeRange.UNBOUNDED;
        }
        TimeRange range = switch (comparison) {
            case Equals eq -> new TimeRange(value, value);
            case GreaterThan gt -> timestampOnLeft ? new TimeRange(value, null) : new TimeRange(null, value);
            case GreaterThanOrEqual gte -> timestampOnLeft ? new TimeRange(value, null) : new TimeRange(null, value);
            case LessThan lt -> timestampOnLeft ? new TimeRange(null, value) : new TimeRange(value, null);
            case LessThanOrEqual lte -> timestampOnLeft ? new TimeRange(null, value) : new TimeRange(value, null);
            default -> TimeRange.UNBOUNDED;
        };
        TemporalAmount interval = truncationOfTimestamp(timestamp);
        return interval == null ? range : rangeOfTruncated(range, interval, context);
    }

    /** The interval of {@code DATE_TRUNC(interval, @timestamp)}, {@code null} for any other expression. */
    private static TemporalAmount truncationOfTimestamp(Expression expression) {
        return expression instanceof UnresolvedFunction function
            && isFunction(function, DATE_TRUNC)
            && function.arguments().size() == 2
            && isTimestamp(function.arguments().get(1)) ? temporalAmount(function.arguments().get(0)) : null;
    }

    /**
     * The range of {@code @timestamp} given the range of its truncation to the interval. A truncated date is never after the
     * date itself, so the lower end holds as it is. An upper end of the truncated date allows any date up to the end of the
     * bucket that this end falls into.
     */
    private static TimeRange rangeOfTruncated(TimeRange truncated, TemporalAmount interval, Context context) {
        if (truncated.to() == null) {
            return truncated;
        }
        try {
            Rounding.Prepared rounding = DateTrunc.createRounding(interval, context.zone(), null, null);
            long nextBucket = rounding.nextRoundingValue(rounding.round(truncated.to()));
            return new TimeRange(truncated.from(), nextBucket - 1);
        } catch (RuntimeException e) {
            // An interval that cannot be rounded to, which is for the analyzer to report.
            return TimeRange.UNBOUNDED;
        }
    }

    /**
     * {@code TRANGE(offset)} keeps the rows from {@code offset} ago until now, {@code TRANGE(start, end)} those in between.
     */
    private static TimeRange rangeOfTRange(UnresolvedFunction trange, Context context) {
        List<Expression> arguments = trange.arguments();
        if (arguments.size() == 1 && temporalAmount(arguments.get(0)) instanceof TemporalAmount offset) {
            long now = context.now();
            Long from = plus(now, offset, -1, context.zone());
            return from == null ? TimeRange.UNBOUNDED : new TimeRange(from, now);
        }
        if (arguments.size() == 2) {
            Long from = trangeEnd(arguments.get(0), context);
            Long to = trangeEnd(arguments.get(1), context);
            // An end that is not understood leaves the function to fail or to filter on its own, nothing to conclude from it.
            return from == null || to == null ? TimeRange.UNBOUNDED : new TimeRange(from, to);
        }
        return TimeRange.UNBOUNDED;
    }

    /** An end of a two argument {@code TRANGE}, which also takes epoch milliseconds as a number. */
    private static Long trangeEnd(Expression expression, Context context) {
        if (expression instanceof Literal literal
            && (literal.dataType() == DataType.LONG || literal.dataType() == DataType.INTEGER)
            && literal.value() instanceof Number number) {
            return number.longValue();
        }
        return millis(expression, context);
    }

    private static boolean isTimestamp(Expression expression) {
        return expression instanceof UnresolvedAttribute attribute && attribute.qualifier() == null && TIMESTAMP.equals(attribute.name());
    }

    private static boolean isFunction(UnresolvedFunction function, String name) {
        return name.equals(function.name().toLowerCase(Locale.ROOT));
    }

    /**
     * The epoch milliseconds of a constant date, or {@code null} for anything else. The condition was folded by
     * {@link DateFunctionComparisonRewriter#foldConstants}, so a constant date is a literal here: a date, or the string
     * that the analyzer casts to one when it is compared with a date. Nanoseconds are rounded down to the millisecond,
     * which {@link TimeRange} allows for.
     */
    private static Long millis(Expression expression, Context context) {
        if (expression instanceof Literal literal && literal.value() != null) {
            if (literal.dataType() == DataType.DATETIME && literal.value() instanceof Long value) {
                return value;
            }
            if (literal.dataType() == DataType.DATE_NANOS && literal.value() instanceof Long value) {
                return Math.floorDiv(value, 1_000_000L);
            }
            if (literal.dataType() == DataType.KEYWORD) {
                return parse(literal, context);
            }
        }
        return null;
    }

    private static Long parse(Literal string, Context context) {
        try {
            return EsqlDataTypeConverter.convert(string.value(), DataType.DATETIME, context.configuration()) instanceof Long date
                ? date
                : null;
        } catch (RuntimeException e) {
            // Not a date, which is for the analyzer to report.
            return null;
        }
    }

    private static TemporalAmount temporalAmount(Expression expression) {
        return expression instanceof Literal literal && literal.value() instanceof TemporalAmount amount ? amount : null;
    }

    /** Adds the amount to the date, or subtracts it for a negative sign, the way date arithmetic is evaluated in a query. */
    private static Long plus(Long millis, TemporalAmount amount, int sign, ZoneId zoneId) {
        if (millis == null) {
            return null;
        }
        try {
            if (amount instanceof Duration duration) {
                return Math.addExact(millis, Math.multiplyExact(sign, duration.toMillis()));
            }
            var dateTime = DateUtils.asDateTime(millis, zoneId);
            return DateUtils.asMillis(sign > 0 ? dateTime.plus(amount) : dateTime.minus(amount));
        } catch (ArithmeticException | DateTimeException e) {
            return null;
        }
    }
}
