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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
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

    /**
     * The rounding of a bucket, which tells a fixed {@code 24 hours} from a calendar {@code 1 day}.
     */
    private record Bucketing(Rounding rounding, String timeZone) {}

    private ColumnIntervals() {}

    /**
     * Returns the interval of every column of {@code plan} that buckets dates with a constant interval, keyed by
     * attribute id. Renames keep it, and so do merges whose branches bucket the same way. Bucket arguments are
     * folded with {@code foldContext}.
     */
    public static Map<NameId, Interval> collect(LogicalPlan plan, FoldContext foldContext) {
        Map<NameId, Expression> constants = new HashMap<>();
        Map<NameId, Bucketing> buckets = new HashMap<>();
        plan.forEachUp(node -> {
            node.forEachExpression(Alias.class, alias -> {
                if (alias.child() instanceof Attribute source) {
                    Bucketing bucketing = buckets.get(source.id());
                    if (bucketing != null) {
                        buckets.put(alias.id(), bucketing);
                    }
                    return;
                }
                Expression child = alias.child().transformUp(ReferenceAttribute.class, r -> constants.getOrDefault(r.id(), r));
                if (child.foldable()) {
                    constants.put(alias.id(), child);
                }
                Bucketing bucketing = of(child, foldContext);
                if (bucketing != null) {
                    buckets.put(alias.id(), bucketing);
                }
            });
            if (node instanceof MergePlan merge) {
                ColumnSemantics.mergeBranches(merge, buckets, (a, b) -> a.equals(b) ? a : null);
            }
        });
        Map<NameId, Interval> intervals = new HashMap<>();
        buckets.forEach((id, b) -> intervals.put(id, new Interval(format(b.rounding().getInterval()), b.timeZone())));
        return intervals;
    }

    private static Bucketing of(Expression e, FoldContext foldContext) {
        if (e instanceof Bucket bucket) {
            return bucket.field().dataType().isDate() ? of(bucket, foldContext) : null;
        }
        if (e instanceof TBucket tbucket) {
            boolean bounded = tbucket.buckets().dataType().isWholeNumber() == false || (tbucket.from() != null && tbucket.to() != null);
            return bounded
                && constant(foldContext, tbucket.buckets(), tbucket.from(), tbucket.to())
                && tbucket.surrogate() instanceof Bucket bucket ? of(bucket, foldContext) : null;
        }
        if (e instanceof TStep tstep) {
            return constant(foldContext, tstep.stepOrBuckets(), tstep.from(), tstep.to())
                && tstep.from() != null
                && tstep.to() != null
                && tstep.surrogate() instanceof Bucket bucket ? of(bucket, foldContext) : null;
        }
        if (e instanceof DateTrunc dateTrunc) {
            return of(dateTrunc.timeBucketSpecRef(), foldContext);
        }
        return null;
    }

    /**
     * The rounding {@code _meta.bucket} reports for the same bucket, or null when its arguments are not constant.
     */
    private static Bucketing of(Bucket bucket, FoldContext foldContext) {
        Rounding rounding = bucket.dateRounding(foldContext);
        if (rounding == null) {
            return null;
        }
        ZoneId zone = QuerySettings.TIME_ZONE.get(bucket.configuration().resolvedSettings());
        return new Bucketing(rounding, zone.equals(ZoneOffset.UTC) ? "UTC" : zone.getId());
    }

    /**
     * Whether every given argument is either absent or a non-null constant.
     */
    private static boolean constant(FoldContext foldContext, Expression... args) {
        for (Expression arg : args) {
            if (arg != null && (arg.foldable() == false || arg.fold(foldContext) == null)) {
                return false;
            }
        }
        return true;
    }

    static String format(Rounding.Interval interval) {
        return interval.size() + " " + interval.unit() + (interval.size() == 1 ? "" : "s");
    }
}
