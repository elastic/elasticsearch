/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Links the date buckets of a plan to the interval and time zone they were bucketed with.
 */
public final class ColumnIntervals {

    /**
     * The interval of a date bucket, as ES|QL writes it ({@code 1 hour}, {@code 1 month}), and the time zone that
     * aligns it.
     */
    public record Interval(String interval, String timeZone) {}

    private ColumnIntervals() {}

    /**
     * Returns the interval of every column of {@code plan} that buckets dates with a constant interval, keyed by
     * attribute id. Renames keep it, and so do merges whose branches agree on it.
     */
    public static Map<NameId, Interval> collect(LogicalPlan plan) {
        Map<NameId, Interval> intervals = new HashMap<>();
        plan.forEachUp(node -> {
            node.forEachExpression(Alias.class, alias -> {
                Interval interval = alias.child() instanceof Attribute source ? intervals.get(source.id()) : of(alias.child());
                if (interval != null) {
                    intervals.put(alias.id(), interval);
                }
            });
            if (node instanceof MergePlan merge) {
                ColumnSemantics.mergeBranches(merge, intervals, (a, b) -> a.equals(b) ? a : null);
            }
        });
        return intervals;
    }

    private static Interval of(Expression e) {
        if (e instanceof Bucket bucket) {
            return of(bucket.buckets(), bucket.configuration());
        }
        if (e instanceof TBucket tbucket) {
            return of(tbucket.buckets(), tbucket.configuration());
        }
        if (e instanceof DateTrunc dateTrunc) {
            return of(dateTrunc.interval(), dateTrunc.configuration());
        }
        return null;
    }

    /**
     * Null when the bucketing function takes a target number of buckets, or an interval that is not a constant.
     */
    private static Interval of(Expression span, Configuration configuration) {
        if (DataType.isTemporalAmount(span.dataType()) == false || span.foldable() == false) {
            return null;
        }
        String interval = format(span.fold(FoldContext.small()));
        if (interval == null) {
            return null;
        }
        ZoneId zone = QuerySettings.TIME_ZONE.get(configuration.resolvedSettings());
        return new Interval(interval, zone.equals(ZoneOffset.UTC) ? "UTC" : zone.getId());
    }

    static String format(Object span) {
        if (span instanceof Duration duration) {
            long millis = duration.toMillis();
            if (millis % 3_600_000 == 0) {
                return amount(millis / 3_600_000, "hour");
            }
            if (millis % 60_000 == 0) {
                return amount(millis / 60_000, "minute");
            }
            if (millis % 1_000 == 0) {
                return amount(millis / 1_000, "second");
            }
            return amount(millis, "millisecond");
        }
        if (span instanceof Period period) {
            if (period.getDays() == 0 && period.getMonths() == 0) {
                return amount(period.getYears(), "year");
            }
            if (period.getYears() == 0 && period.getDays() == 0) {
                return amount(period.getMonths(), "month");
            }
            if (period.getYears() == 0 && period.getMonths() == 0) {
                return period.getDays() % 7 == 0 ? amount(period.getDays() / 7, "week") : amount(period.getDays(), "day");
            }
        }
        return null;
    }

    private static String amount(long value, String unit) {
        return value + " " + unit + (value == 1 ? "" : "s");
    }
}
