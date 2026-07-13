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
 * Used by PromQL planning to infer implicit start/end bounds from request filters.
 */
public final class QueryDslTimestampBoundsExtractor {
    private static final DateMathParser DEFAULT_PARSER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser();

    private QueryDslTimestampBoundsExtractor() {}

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
     * to resolve date math expressions consistently with the current request.
     */
    @Nullable
    public static TimestampBounds extractTimestampBounds(@Nullable QueryBuilder filter, LongSupplier nowSupplier) {
        if (filter == null) {
            return null;
        }
        var bounds = new Builder();
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

    @Nullable
    private static Instant parseInstant(
        @Nullable Object value,
        @Nullable String format,
        @Nullable String timeZone,
        @Nullable LongSupplier nowSupplier
    ) {
        if (value == null) {
            return null;
        }
        String stringValue = value.toString();
        if (nowSupplier == null && stringValue.contains("now")) {
            return null;
        }
        ZoneId zone = timeZone == null ? null : ZoneId.of(timeZone);
        DateMathParser parser = format != null ? DateFormatter.forPattern(format).toDateMathParser() : DEFAULT_PARSER;
        try {
            return parseInstant(parser, stringValue, zone, nowSupplier);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static Instant parseInstant(DateMathParser parser, String value, @Nullable ZoneId zone, @Nullable LongSupplier nowSupplier) {
        return parser.parse(value, nowSupplier, false, zone);
    }

    private static final class Builder {
        private Instant start;
        private Instant end;
        private boolean foundTimestampRange;
        private boolean invalid;

        private void add(RangeQueryBuilder range, LongSupplier nowSupplier) {
            foundTimestampRange = true;
            Instant lowerBound = parseInstant(range.from(), range.format(), range.timeZone(), nowSupplier);
            if (range.from() != null && lowerBound == null) {
                invalid = true;
                return;
            }
            Instant upperBound = parseInstant(range.to(), range.format(), range.timeZone(), nowSupplier);
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
