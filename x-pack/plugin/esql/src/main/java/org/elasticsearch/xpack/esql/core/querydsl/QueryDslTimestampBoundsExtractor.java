/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.time.Instant;
import java.time.ZoneId;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.esql.core.expression.MetadataAttribute.TIMESTAMP_FIELD;

/**
 * Extracts {@code @timestamp} bounds from Query DSL filters.
 * <p>
 * Used by PromQL planning to infer implicit start/end bounds from request filters, and by external-source
 * listing to inject timestamp hints that are never tighter than the row-filter rewrite.
 */
public final class QueryDslTimestampBoundsExtractor {
    private static final DateMathParser DEFAULT_PARSER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser();

    private QueryDslTimestampBoundsExtractor() {}

    /**
     * How range bounds are rounded and which clauses are eligible.
     * <p>
     * {@link #LEGACY} is the PromQL / {@code TBUCKET} / {@code TSTEP} path: both ends round down and a
     * {@code time_zone} is applied. {@link #RANGE_QUERY} matches the Query DSL range rewrite used as the
     * row filter: {@code gte}/{@code lt} round down, {@code lte}/{@code gt} round up, and a clause
     * {@link #unsupportedRangeReason} would drop contributes no listing narrowing.
     */
    public enum BoundSemantics {
        LEGACY,
        RANGE_QUERY
    }

    /**
     * Represents the {@code @timestamp} lower and upper bounds extracted from a query DSL filter.
     *
     * @param start the lower bound
     * @param end   the upper bound
     */
    public record TimestampBounds(Instant start, Instant end) {}

    /**
     * Extracts the {@code @timestamp} range bounds from a query DSL filter.
     * <p>
     * Supports:
     * <ul>
     *     <li>{@link RangeQueryBuilder} directly on {@code @timestamp}</li>
     *     <li>{@link BoolQueryBuilder} with the range nested in {@code filter} or {@code must} clauses</li>
     * </ul>
     *
     * @param filter the query DSL filter to inspect, may be {@code null}
     * @return extracted bounds, or {@code null} when no {@code @timestamp} range is found or bounds cannot be parsed
     */
    @Nullable
    public static TimestampBounds extractTimestampBounds(@Nullable QueryBuilder filter) {
        return extractTimestampBounds(filter, null);
    }

    /**
     * Extracts the {@code @timestamp} range bounds from a query DSL filter using the supplied {@code now} value
     * to resolve date math expressions consistently with the current request. Uses {@link BoundSemantics#LEGACY}.
     */
    @Nullable
    public static TimestampBounds extractTimestampBounds(@Nullable QueryBuilder filter, LongSupplier nowSupplier) {
        return extractTimestampBounds(filter, nowSupplier, BoundSemantics.LEGACY);
    }

    /**
     * Extracts {@code @timestamp} range bounds with the given rounding and drop semantics.
     */
    @Nullable
    public static TimestampBounds extractTimestampBounds(
        @Nullable QueryBuilder filter,
        LongSupplier nowSupplier,
        BoundSemantics semantics
    ) {
        if (filter == null) {
            return null;
        }
        var bounds = new Builder(semantics);
        collectTimestampBounds(filter, nowSupplier, bounds);
        return bounds.build();
    }

    /**
     * Recursively finds a {@link RangeQueryBuilder} on {@code @timestamp} in a {@link QueryBuilder} tree.
     */
    private static void collectTimestampBounds(QueryBuilder filter, LongSupplier nowSupplier, Builder bounds) {
        switch (filter) {
            case RangeQueryBuilder range when TIMESTAMP_FIELD.equals(range.fieldName()) -> bounds.add(range, nowSupplier);
            case BoolQueryBuilder bool -> {
                for (QueryBuilder clause : bool.filter()) {
                    collectTimestampBounds(clause, nowSupplier, bounds);
                }
                for (QueryBuilder clause : bool.must()) {
                    collectTimestampBounds(clause, nowSupplier, bounds);
                }
            }
            default -> {
            }
        }
    }

    /**
     * Why a range cannot be rewritten as a row filter, or {@code null} if it can. Listing extraction with
     * {@link BoundSemantics#RANGE_QUERY} returns no bounds from a clause this rejects, so listing is never
     * tighter than the applied row filter. Keep this list in one place: every new drop reason must feed
     * both this extractor and {@code QueryDslTranslator.range()}.
     */
    @Nullable
    public static String unsupportedRangeReason(RangeQueryBuilder range) {
        if (range.timeZone() != null) {
            return "range[time_zone]";
        }
        return null;
    }

    @Nullable
    private static Instant parseInstant(
        @Nullable Object value,
        @Nullable String format,
        @Nullable String timeZone,
        @Nullable LongSupplier nowSupplier,
        boolean roundUp,
        BoundSemantics semantics
    ) {
        if (value == null) {
            return null;
        }
        // RANGE_QUERY matches DateFieldMapper / the translator: a Number with no format (or epoch_millis)
        // is epoch millis. Stringifying would let "2024" parse as a year and listing would drop 1970 folders
        // the row filter still matches.
        if (semantics == BoundSemantics.RANGE_QUERY && value instanceof Number n && isEpochMillisFormat(format)) {
            return Instant.ofEpochMilli(n.longValue());
        }
        String stringValue = value.toString();
        if (nowSupplier == null && stringValue.contains("now")) {
            return null;
        }
        ZoneId zone = timeZone == null ? null : ZoneId.of(timeZone);
        DateMathParser parser = format != null ? DateFormatter.forPattern(format).toDateMathParser() : DEFAULT_PARSER;
        try {
            return parser.parse(stringValue, nowSupplier, roundUp, zone);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static boolean isEpochMillisFormat(@Nullable String format) {
        return format == null || "epoch_millis".equals(format);
    }

    private static final class Builder {
        private final BoundSemantics semantics;
        private Instant start;
        private Instant end;
        private boolean foundTimestampRange;
        private boolean invalid;

        private Builder(BoundSemantics semantics) {
            this.semantics = semantics;
        }

        private void add(RangeQueryBuilder range, LongSupplier nowSupplier) {
            if (semantics == BoundSemantics.RANGE_QUERY && unsupportedRangeReason(range) != null) {
                // The row filter drops this clause; listing must not narrow from it.
                return;
            }
            foundTimestampRange = true;
            boolean lowerRoundUp = semantics == BoundSemantics.RANGE_QUERY && range.includeLower() == false;
            boolean upperRoundUp = semantics == BoundSemantics.RANGE_QUERY && range.includeUpper();
            Instant lowerBound = parseInstant(range.from(), range.format(), range.timeZone(), nowSupplier, lowerRoundUp, semantics);
            if (range.from() != null && lowerBound == null) {
                invalid = true;
                return;
            }
            Instant upperBound = parseInstant(range.to(), range.format(), range.timeZone(), nowSupplier, upperRoundUp, semantics);
            if (range.to() != null && upperBound == null) {
                invalid = true;
                return;
            }
            if (lowerBound != null && (start == null || lowerBound.isAfter(start))) {
                start = lowerBound;
            }
            if (upperBound != null && (end == null || upperBound.isBefore(end))) {
                end = upperBound;
            }
        }

        @Nullable
        TimestampBounds build() {
            if (invalid || foundTimestampRange == false || start == null || end == null || start.isAfter(end)) {
                return null;
            }
            return new TimestampBounds(start, end);
        }
    }
}
