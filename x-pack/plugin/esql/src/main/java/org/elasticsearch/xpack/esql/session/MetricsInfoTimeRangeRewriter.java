/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.BoundSemantics;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.function.LongSupplier;

/**
 * Caps how far back {@code METRICS_INFO} and {@code TS_INFO} read, see {@link EsqlFlags#ESQL_METRICS_INFO_MAX_TIME_RANGE}.
 * These commands read every time series in the requested time range, which is slow when the range is wide or when there is
 * no range at all.
 */
final class MetricsInfoTimeRangeRewriter {

    private MetricsInfoTimeRangeRewriter() {}

    /**
     * Caps the time range filter of {@code METRICS_INFO} and {@code TS_INFO} to {@code maxTimeRange}, and warns if that narrows
     * the time range.
     */
    static LogicalPlan capTimeRange(LogicalPlan plan, TimeValue maxTimeRange, @Nullable QueryBuilder requestFilter, Instant now) {
        if (maxTimeRange.millis() <= 0 || plan.anyMatch(p -> p instanceof TsInfo || p instanceof MetricsInfo) == false) {
            return plan;
        }
        return capTimeRange(plan, maxTimeRange, requestedRange(requestFilter, now), now);
    }

    @Nullable
    private static TimestampBounds requestedRange(@Nullable QueryBuilder requestFilter, Instant now) {
        LongSupplier nowMillis = now::toEpochMilli;
        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(
            requestFilter,
            nowMillis,
            BoundSemantics.RANGE_QUERY
        );
        if (bounds != null) {
            return bounds;
        }
        // retry with legacy semantic
        return QueryDslTimestampBoundsExtractor.extractTimestampBounds(requestFilter, nowMillis, BoundSemantics.LEGACY);
    }

    private static LogicalPlan capTimeRange(LogicalPlan plan, TimeValue maxTimeRange, @Nullable TimestampBounds requested, Instant now) {
        Instant end = requested == null ? now : requested.end();
        Instant from = Instant.ofEpochMilli(Math.max(end.toEpochMilli() - maxTimeRange.millis(), 0));
        if (requested != null && from.isAfter(requested.start()) == false) {
            return plan;
        }
        var rewritten = plan.transformUp(UnaryPlan.class, p -> {
            if (p instanceof TsInfo || p instanceof MetricsInfo) {
                LogicalPlan capped = p.child().transformUp(EsRelation.class, r -> capRelation(r, from));
                return capped == p.child() ? p : p.replaceChild(capped);
            }
            return p;
        });
        if (rewritten != plan) {
            rewritten.forEachDown(LogicalPlan.class, LogicalPlan::setAnalyzed);
            HeaderWarning.addWarning("METRICS_INFO and TS_INFO are limited to the most recent [{}]", maxTimeRange);
        }
        return rewritten;
    }

    private static LogicalPlan capRelation(EsRelation r, Instant from) {
        if (r.indexMode().isTsdb() == false) {
            return r;
        }
        Attribute timestamp = r.output().stream().filter(a -> MetadataAttribute.TIMESTAMP_FIELD.equals(a.name())).findFirst().orElse(null);
        if (timestamp == null) {
            return r;
        }
        Literal bound = lowerBound(r.source(), timestamp.dataType(), from);
        if (bound == null) {
            return r;
        }
        return new Filter(r.source(), r, new GreaterThanOrEqual(r.source(), timestamp, bound, ZoneOffset.UTC));
    }

    @Nullable
    private static Literal lowerBound(Source source, DataType timestampType, Instant from) {
        if (timestampType == DataType.DATETIME) {
            return Literal.dateTime(source, from);
        }
        if (timestampType == DataType.DATE_NANOS) {
            return new Literal(source, ChronoUnit.NANOS.between(Instant.EPOCH, from), DataType.DATE_NANOS);
        }
        return null;
    }

}
