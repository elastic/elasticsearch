/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;

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
            return bucket.field().dataType().isDate() ? of(bucket) : null;
        }
        if (e instanceof TBucket tbucket) {
            boolean bounded = tbucket.buckets().dataType().isWholeNumber() == false || (tbucket.from() != null && tbucket.to() != null);
            return bounded && tbucket.surrogate() instanceof Bucket bucket ? of(bucket) : null;
        }
        if (e instanceof DateTrunc dateTrunc) {
            return of(dateTrunc.timeBucketSpecRef());
        }
        return null;
    }

    /**
     * The interval {@code _meta.bucket} reports for the same bucket, or null when its arguments are not constant.
     */
    private static Interval of(Bucket bucket) {
        Rounding.Interval interval = bucket.dateInterval(FoldContext.small());
        if (interval == null) {
            return null;
        }
        ZoneId zone = QuerySettings.TIME_ZONE.get(bucket.configuration().resolvedSettings());
        return new Interval(format(interval), zone.equals(ZoneOffset.UTC) ? "UTC" : zone.getId());
    }

    static String format(Rounding.Interval interval) {
        return interval.size() + " " + interval.unit() + (interval.size() == 1 ? "" : "s");
    }
}
