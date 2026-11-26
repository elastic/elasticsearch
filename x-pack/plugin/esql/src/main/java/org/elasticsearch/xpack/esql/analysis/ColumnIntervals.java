/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
     * The rounding of a bucket, which tells a fixed {@code 24 hours} from a calendar {@code 1 day}, and its
     * interval, which tells {@code 2 months} from {@code 6 months}.
     */
    private record Bucketing(Rounding rounding, Rounding.Interval interval, String timeZone) {}

    private ColumnIntervals() {}

    /**
     * Returns the interval of every column of {@code plan} that buckets dates with a constant interval, keyed by
     * attribute id. Renames keep it, and so do merges whose branches bucket the same way. Only the interval arguments of
     * buckets are folded, with {@code foldContext}, since other constants, such as inference functions, may not fold yet.
     */
    public static Map<NameId, Interval> collect(LogicalPlan plan, FoldContext foldContext) {
        Constants constants = new Constants(foldContext);
        Map<NameId, Bucketing> buckets = new HashMap<>();
        plan.forEachUp(node -> {
            node.forEachExpression(Alias.class, alias -> {
                constants.define(alias);
                Bucketing bucketing = alias.child() instanceof Attribute source
                    ? buckets.get(source.id())
                    : of(alias.child(), constants, foldContext);
                if (bucketing != null) {
                    buckets.put(alias.id(), bucketing);
                }
            });
            if (node instanceof MergePlan merge) {
                ColumnSemantics.mergeBranches(merge, buckets, (a, b) -> a.equals(b) ? a : null);
            }
        });
        Map<NameId, Interval> intervals = new HashMap<>();
        buckets.forEach((id, b) -> intervals.put(id, new Interval(format(b.interval()), b.timeZone())));
        return intervals;
    }

    /**
     * The aliases of a plan, folded on demand, each at most once, so that only the ones a bucket reads are folded.
     */
    private static final class Constants {
        private final FoldContext foldContext;
        private final Map<NameId, Expression> definitions = new HashMap<>();
        private final Map<NameId, Optional<Literal>> folded = new HashMap<>();

        Constants(FoldContext foldContext) {
            this.foldContext = foldContext;
        }

        void define(Alias alias) {
            definitions.put(alias.id(), alias.child());
        }

        /**
         * {@code e} with every reference to a constant alias replaced by its value.
         */
        Expression resolve(Expression e) {
            return e.transformUp(ReferenceAttribute.class, r -> {
                Literal value = value(r.id());
                return value != null ? value : r;
            });
        }

        /**
         * The value of the alias {@code id}, or {@code null} if it is not constant. The aliases it reads are folded first,
         * from a worklist rather than by recursion, so that long chains of aliases don't grow the stack.
         */
        @Nullable
        private Literal value(NameId id) {
            Optional<Literal> cached = folded.get(id);
            if (cached != null) {
                return cached.orElse(null);
            }
            Deque<NameId> pending = new ArrayDeque<>();
            pending.push(id);
            while (pending.isEmpty() == false) {
                NameId next = pending.peek();
                if (folded.containsKey(next)) {
                    pending.pop();
                    continue;
                }
                Expression definition = definitions.get(next);
                if (definition == null) {
                    folded.put(next, Optional.empty());
                    pending.pop();
                    continue;
                }
                int before = pending.size();
                definition.forEachDown(ReferenceAttribute.class, r -> {
                    if (folded.containsKey(r.id()) == false) {
                        pending.push(r.id());
                    }
                });
                if (pending.size() == before) {
                    pending.pop();
                    Expression resolved = resolve(definition);
                    folded.put(next, resolved.foldable() ? Optional.of(Literal.of(foldContext, resolved)) : Optional.empty());
                }
            }
            return folded.get(id).orElse(null);
        }
    }

    private static Bucketing of(Expression e, Constants constants, FoldContext foldContext) {
        e = switch (e) {
            case Bucket bucket -> resolveInterval(bucket, 0, constants);
            case TBucket tbucket -> resolveInterval(tbucket, 1, constants);
            case TStep tstep -> resolveInterval(tstep, 1, constants);
            case DateTrunc dateTrunc -> resolveInterval(dateTrunc, 1, constants);
            default -> e;
        };
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
     * {@code bucket} with the constant aliases its interval arguments read replaced by their values. The dates it buckets,
     * its child at {@code datesIndex}, are left as they are, since they don't change the interval and may not fold yet.
     */
    private static Expression resolveInterval(Expression bucket, int datesIndex, Constants constants) {
        List<Expression> children = new ArrayList<>(bucket.children());
        for (int i = 0; i < children.size(); i++) {
            if (i != datesIndex) {
                children.set(i, constants.resolve(children.get(i)));
            }
        }
        return bucket.replaceChildrenSameSize(children);
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
        return new Bucketing(rounding, rounding.getInterval(), zone.equals(ZoneOffset.UTC) ? "UTC" : zone.getId());
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
