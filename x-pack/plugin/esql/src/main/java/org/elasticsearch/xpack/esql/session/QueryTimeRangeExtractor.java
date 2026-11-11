/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedMetadata;
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
import java.util.Set;

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
 *     only hold commands that neither depend on other rows nor redefine {@code @timestamp}, see {@link #collect}.</li>
 *     <li>The range is only ever wider than the condition. Unsupported parts of a conjunction are ignored, an unsupported part
 *     of a disjunction drops the range, and both ends are inclusive at millisecond resolution.</li>
 * </ul>
 * Like the request filter, a range that prunes an index also removes the fields that only that index maps from the resolved
 * mapping, and with that from the columns of a query that does not name its columns.
 */
final class QueryTimeRangeExtractor {

    private static final String TIMESTAMP = MetadataAttribute.TIMESTAMP_FIELD;
    private static final String NOW = "now";
    private static final String TRANGE = "trange";
    private static final String DATE_TRUNC = "date_trunc";
    private static final String DATE_PARSE = "date_parse";
    private static final Set<String> TO_DATETIME = Set.of("to_datetime", "to_dt");
    private static final Set<String> TO_DATE_NANOS = Set.of("to_date_nanos", "to_datenanos");

    /**
     * How many calls the walk of the plan may take, and the walk of each filter condition. Both recurse, and neither is worth
     * more: a plan or a condition beyond this is machine generated, and going without the time range just resolves the indices
     * as before.
     */
    private static final int MAX_CALLS = 100;

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
    static Map<IndexPattern, QueryBuilder> extract(LogicalPlan parsed, Configuration configuration) {
        Map<IndexPattern, QueryBuilder> filters = new HashMap<>();
        for (Map.Entry<IndexPattern, TimeRange> range : ranges(parsed, configuration).entrySet()) {
            if (range.getValue().isUnbounded() == false) {
                filters.put(range.getKey(), range.getValue().toQuery());
            }
        }
        return filters;
    }

    /**
     * The range that the query restricts each of its index patterns to, the widest one for a pattern that is read more than once.
     * Empty for a plan that takes more than {@link #MAX_CALLS} calls to walk: the patterns that the walk did not get to
     * may be read without a time range, so nothing can be said about any of them.
     */
    static Map<IndexPattern, TimeRange> ranges(LogicalPlan parsed, Configuration configuration) {
        Map<IndexPattern, TimeRange> ranges = new HashMap<>();
        Context context = new Context(
            configuration,
            configuration.now().toEpochMilli(),
            QuerySettings.TIME_ZONE.get(configuration.resolvedSettings())
        );
        return collect(parsed, TimeRange.UNBOUNDED, context, ranges, new Calls(MAX_CALLS)) ? ranges : Map.of();
    }

    /**
     * What the constants of a query are evaluated with. The time that {@code NOW()} returns and the time zone are read once,
     * so that every bound of a query is taken from the same instant, the one that the query itself runs with.
     */
    private record Context(Configuration configuration, long now, ZoneId zone) {}

    /** What is left of the calls that a recursion may make, which keeps it shallow and cheap on any plan or expression. */
    private static final class Calls {
        private int remaining;

        Calls(int limit) {
            this.remaining = limit;
        }

        /** Counts a call, returning {@code false} once the limit is exceeded. */
        boolean next() {
            return remaining-- > 0;
        }
    }

    /**
     * Walks down the plan carrying the range that the filters above {@code plan} restrict its rows to. The range survives the
     * commands that hand each row of their input on unchanged as far as {@code @timestamp} is concerned, and is reset by
     * everything else: commands whose output depends on other rows ({@code LIMIT}, {@code STATS}, ...), that may redefine
     * {@code @timestamp}, or that this class does not know about.
     *
     * @return {@code false} if the walk ran out of calls before it reached every relation
     */
    private static boolean collect(LogicalPlan plan, TimeRange range, Context context, Map<IndexPattern, TimeRange> ranges, Calls calls) {
        if (calls.next() == false) {
            return false;
        }
        switch (plan) {
            case UnresolvedRelation relation -> {
                if (relation.indexMode() != IndexMode.LOOKUP) {
                    ranges.merge(relation.indexPattern(), range, TimeRange::cover);
                }
                return true;
            }
            case Filter filter -> {
                TimeRange filtered = range.intersect(rangeOf(filter.condition(), context, new Calls(MAX_CALLS)));
                return collect(filter.child(), filtered, context, ranges, calls);
            }
            case Eval eval -> {
                return collect(eval.child(), defines(eval.fields()) ? TimeRange.UNBOUNDED : range, context, ranges, calls);
            }
            case Rename rename -> {
                return collect(rename.child(), defines(rename.renamings()) ? TimeRange.UNBOUNDED : range, context, ranges, calls);
            }
            case Keep keep -> {
                return collect(keep.child(), range, context, ranges, calls);
            }
            case Drop drop -> {
                return collect(drop.child(), range, context, ranges, calls);
            }
            case OrderBy orderBy -> {
                return collect(orderBy.child(), range, context, ranges, calls);
            }
            // The wrapper that FROM ... METADATA puts around its relation, which only adds the metadata fields.
            case UnresolvedMetadata metadata -> {
                return collect(metadata.child(), range, context, ranges, calls);
            }
            // The wrapper of a subquery or of a view body, whose columns are those of its child.
            case Subquery subquery -> {
                return collect(subquery.child(), range, context, ranges, calls);
            }
            // Its rows are not those of the index, a filter on them says nothing about it. Its own time range does.
            case PromqlCommand promql -> {
                return collect(promql.child(), rangeOf(promql), context, ranges, calls);
            }
            // A filter on the rows of a union filters the rows of each of its branches.
            case UnionAll union -> {
                for (LogicalPlan branch : union.children()) {
                    if (collect(branch, range, context, ranges, calls) == false) {
                        return false;
                    }
                }
                return true;
            }
            default -> {
                for (LogicalPlan child : plan.children()) {
                    if (collect(child, TimeRange.UNBOUNDED, context, ranges, calls) == false) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

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

    private static boolean defines(List<Alias> aliases) {
        for (Alias alias : aliases) {
            if (TIMESTAMP.equals(alias.name())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The range of {@code @timestamp} outside of which {@code condition} cannot hold, {@link TimeRange#UNBOUNDED} if unknown.
     * Whatever part of the condition is left once the calls run out counts as unknown, which only widens the range.
     */
    private static TimeRange rangeOf(Expression condition, Context context, Calls calls) {
        if (calls.next() == false) {
            return TimeRange.UNBOUNDED;
        }
        return switch (condition) {
            case And and -> rangeOf(and.left(), context, calls).intersect(rangeOf(and.right(), context, calls));
            case Or or -> rangeOf(or.left(), context, calls).cover(rangeOf(or.right(), context, calls));
            case EsqlBinaryComparison comparison -> rangeOf(comparison, context, calls);
            case UnresolvedFunction function when isFunction(function, TRANGE) -> rangeOfTRange(function, context, calls);
            default -> TimeRange.UNBOUNDED;
        };
    }

    /**
     * The range of a comparison of {@code @timestamp}, or of {@code DATE_TRUNC(interval, @timestamp)}, with a constant date.
     */
    private static TimeRange rangeOf(EsqlBinaryComparison comparison, Context context, Calls calls) {
        boolean timestampOnLeft = isTimestamp(comparison.left()) || truncationOfTimestamp(comparison.left()) != null;
        Expression timestamp = timestampOnLeft ? comparison.left() : comparison.right();
        if (timestampOnLeft == false && isTimestamp(timestamp) == false && truncationOfTimestamp(timestamp) == null) {
            return TimeRange.UNBOUNDED;
        }
        Long value = millis(timestampOnLeft ? comparison.right() : comparison.left(), context, calls);
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
    private static TimeRange rangeOfTRange(UnresolvedFunction trange, Context context, Calls calls) {
        List<Expression> arguments = trange.arguments();
        if (arguments.size() == 1 && temporalAmount(arguments.get(0)) instanceof TemporalAmount offset) {
            long now = context.now();
            Long from = plus(now, offset, -1, context.zone());
            return from == null ? TimeRange.UNBOUNDED : new TimeRange(from, now);
        }
        if (arguments.size() == 2) {
            Long from = trangeEnd(arguments.get(0), context, calls);
            Long to = trangeEnd(arguments.get(1), context, calls);
            // An end that is not understood leaves the function to fail or to filter on its own, nothing to conclude from it.
            return from == null || to == null ? TimeRange.UNBOUNDED : new TimeRange(from, to);
        }
        return TimeRange.UNBOUNDED;
    }

    /** An end of a two argument {@code TRANGE}, which also takes epoch milliseconds as a number. */
    private static Long trangeEnd(Expression expression, Context context, Calls calls) {
        if (expression instanceof Literal literal
            && (literal.dataType() == DataType.LONG || literal.dataType() == DataType.INTEGER)
            && literal.value() instanceof Number number) {
            return number.longValue();
        }
        return millis(expression, context, calls);
    }

    private static boolean isTimestamp(Expression expression) {
        return expression instanceof UnresolvedAttribute attribute && attribute.qualifier() == null && TIMESTAMP.equals(attribute.name());
    }

    private static boolean isFunction(UnresolvedFunction function, String name) {
        return name.equals(function.name().toLowerCase(Locale.ROOT));
    }

    /**
     * The epoch milliseconds that a constant date expression evaluates to, or {@code null} for any expression but the shapes
     * that time ranges are commonly written with: a date string, {@code NOW()}, casts to and parsing of a date, and time spans added to,
     * subtracted from or truncating those. Nanoseconds are rounded down to the millisecond, which {@link TimeRange} allows for.
     */
    private static Long millis(Expression expression, Context context, Calls calls) {
        if (calls.next() == false) {
            return null;
        }
        return switch (expression) {
            case Literal literal when literal.dataType() == DataType.DATETIME && literal.value() instanceof Long value -> value;
            // The conversion that the analyzer casts a string compared to a date with.
            case Literal literal when literal.dataType() == DataType.KEYWORD -> parse(literal, DataType.DATETIME, context);
            // The inline casts, which the parser turns into the conversion functions right away.
            case ToDatetime cast -> cast(cast.field(), DataType.DATETIME, context, calls);
            case ToDateNanos cast -> cast(cast.field(), DataType.DATE_NANOS, context, calls);
            case UnresolvedFunction function -> millis(function, context, calls);
            case Add add -> {
                if (temporalAmount(add.right()) instanceof TemporalAmount amount) {
                    yield plus(millis(add.left(), context, calls), amount, 1, context.zone());
                }
                if (temporalAmount(add.left()) instanceof TemporalAmount amount) {
                    yield plus(millis(add.right(), context, calls), amount, 1, context.zone());
                }
                yield null;
            }
            case Sub sub -> temporalAmount(sub.right()) instanceof TemporalAmount amount
                ? plus(millis(sub.left(), context, calls), amount, -1, context.zone())
                : null;
            default -> null;
        };
    }

    private static Long millis(UnresolvedFunction function, Context context, Calls calls) {
        List<Expression> arguments = function.arguments();
        String name = function.name().toLowerCase(Locale.ROOT);
        if (NOW.equals(name) && arguments.isEmpty()) {
            return context.now();
        }
        if (TO_DATETIME.contains(name) && arguments.size() == 1) {
            return cast(arguments.get(0), DataType.DATETIME, context, calls);
        }
        if (TO_DATE_NANOS.contains(name) && arguments.size() == 1) {
            return cast(arguments.get(0), DataType.DATE_NANOS, context, calls);
        }
        if (DATE_PARSE.equals(name)) {
            return dateParse(arguments, context);
        }
        if (DATE_TRUNC.equals(name) && arguments.size() == 2 && temporalAmount(arguments.get(0)) instanceof TemporalAmount interval) {
            Long date = millis(arguments.get(1), context, calls);
            if (date == null) {
                return null;
            }
            try {
                return DateTrunc.createRounding(interval, context.zone(), null, null).round(date);
            } catch (RuntimeException e) {
                // An interval that cannot be rounded to, which is for the analyzer to report.
                return null;
            }
        }
        return null;
    }

    /**
     * {@code DATE_PARSE(date)} and {@code DATE_PARSE(pattern, date)} of string literals. The options of the function, which
     * change the locale and the time zone of the pattern, are left alone.
     */
    private static Long dateParse(List<Expression> arguments, Context context) {
        if (arguments.isEmpty() || arguments.size() > 2) {
            return null;
        }
        BytesRef date = string(arguments.get(arguments.size() - 1));
        BytesRef pattern = arguments.size() == 2 ? string(arguments.get(0)) : null;
        if (date == null || (arguments.size() == 2 && pattern == null)) {
            return null;
        }
        try {
            DateFormatter formatter = pattern == null
                ? EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER
                : DateFormatter.forPattern(pattern.utf8ToString());
            return DateParse.process(date, formatter.withLocale(Locale.ROOT), context.zone(), Locale.ROOT);
        } catch (RuntimeException e) {
            // A pattern or a date that does not parse, which is for the analyzer or the evaluation to report.
            return null;
        }
    }

    private static BytesRef string(Expression expression) {
        return expression instanceof Literal literal && literal.dataType() == DataType.KEYWORD && literal.value() instanceof BytesRef value
            ? value
            : null;
    }

    /**
     * The epoch milliseconds of a cast to {@code target}, a date type. A string is parsed, a number counts the units of the
     * type since the epoch, and a date is converted between the two resolutions.
     */
    private static Long cast(Expression argument, DataType target, Context context, Calls calls) {
        if (argument instanceof Literal literal) {
            if (literal.dataType() == DataType.KEYWORD) {
                return parse(literal, target, context);
            }
            if ((literal.dataType() == DataType.LONG || literal.dataType() == DataType.INTEGER)
                && literal.value() instanceof Number number) {
                return epochMillis(number.longValue(), target);
            }
        }
        return millis(argument, context, calls);
    }

    private static Long parse(Literal string, DataType target, Context context) {
        try {
            return EsqlDataTypeConverter.convert(string.value(), target, context.configuration()) instanceof Long date
                ? epochMillis(date, target)
                : null;
        } catch (RuntimeException e) {
            // Not a date, which is for the analyzer to report.
            return null;
        }
    }

    /** The epoch milliseconds of a value of the date type. Nanoseconds do not go back beyond the epoch. */
    private static Long epochMillis(long value, DataType type) {
        if (type == DataType.DATE_NANOS) {
            return value < 0 ? null : Long.valueOf(Math.floorDiv(value, 1_000_000L));
        }
        return value;
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
