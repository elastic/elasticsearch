/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.time.Instant;

import static org.elasticsearch.xpack.esql.core.expression.MetadataAttribute.TIMESTAMP_FIELD;

/**
 * Extracts {@code @timestamp} bounds from Query DSL filters.
 * <p>
 * Used by PromQL planning to infer implicit start/end bounds from request filters.
 */
public final class QueryDslTimestampBoundsExtractor {

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
        if (filter == null) {
            return null;
        }
        RangeQueryBuilder range = findTimestampRange(filter);
        if (range == null) {
            return null;
        }
        Instant start = parseInstant(range.format(), range.from());
        Instant end = parseInstant(range.format(), range.to());
        if (start == null || end == null) {
            return null;
        }
        return new TimestampBounds(start, end);
    }

    /**
     * Recursively finds a {@link RangeQueryBuilder} on {@code @timestamp} in a {@link QueryBuilder} tree.
     */
    @Nullable
    private static RangeQueryBuilder findTimestampRange(QueryBuilder filter) {
        return switch (filter) {
            case RangeQueryBuilder range when TIMESTAMP_FIELD.equals(range.fieldName()) -> range;
            case BoolQueryBuilder bool -> {
                for (QueryBuilder clause : bool.filter()) {
                    RangeQueryBuilder found = findTimestampRange(clause);
                    if (found != null) yield found;
                }
                for (QueryBuilder clause : bool.must()) {
                    RangeQueryBuilder found = findTimestampRange(clause);
                    if (found != null) yield found;
                }
                yield null;
            }
            default -> null;
        };
    }

    @Nullable
    private static Instant parseInstant(@Nullable String format, @Nullable Object value) {
        if (format == null || value == null) {
            return null;
        }
        try {
            DateFormatter df = DateFormatter.forPattern(format);
            return Instant.from(df.parse(value.toString()));
        } catch (RuntimeException e) {
            return null;
        }
    }
}
