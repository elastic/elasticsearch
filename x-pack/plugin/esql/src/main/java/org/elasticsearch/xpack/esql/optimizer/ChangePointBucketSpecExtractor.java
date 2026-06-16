/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.List;
import java.util.Optional;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;
import static org.elasticsearch.compute.operator.ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT;
import static org.elasticsearch.compute.operator.ChangePointBucketFillUtils.countBucketsInRange;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

/**
 * Extracts {@link Bucket} metadata from a {@link ChangePoint} plan for sparse-bucket gap filling.
 */
public final class ChangePointBucketSpecExtractor {

    public record ChangePointBucketFillSpec(
        Rounding.Prepared rounding,
        long minDate,
        long maxDate,
        Attribute key,
        Attribute value,
        List<Expression> groupings
    ) {}

    private ChangePointBucketSpecExtractor() {}

    public static Optional<ChangePointBucketFillSpec> extract(ChangePoint changePoint, LogicalOptimizerContext context) {
        LogicalPlan plan = changePoint.child();
        if (plan instanceof Limit limit) {
            plan = limit.child();
        } else if (plan instanceof LimitBy limitBy) {
            plan = limitBy.child();
        } else {
            return Optional.empty();
        }
        if (plan instanceof OrderBy orderBy) {
            return extractFromStatsSubtree(
                changePoint.source(),
                orderBy.child(),
                changePoint.key(),
                changePoint.value(),
                changePoint.groupings(),
                context
            );
        }
        if (plan instanceof TopN topN) {
            return extractFromStatsSubtree(
                changePoint.source(),
                topN.child(),
                changePoint.key(),
                changePoint.value(),
                changePoint.groupings(),
                context
            );
        }
        return Optional.empty();
    }

    private static Optional<ChangePointBucketFillSpec> extractFromStatsSubtree(
        Source source,
        LogicalPlan plan,
        Attribute key,
        Attribute value,
        List<Expression> groupings,
        LogicalOptimizerContext context
    ) {
        AttributeMap.Builder<Expression> evalExprs = AttributeMap.builder();
        Aggregate aggregate = findAggregate(plan, evalExprs);
        if (aggregate == null) {
            return Optional.empty();
        }

        Bucket bucket = findBucketForKey(aggregate, key, evalExprs.build());
        if (bucket == null) {
            return Optional.empty();
        }

        DataType fieldType = bucket.field().dataType();
        if (fieldType != DataType.DATETIME && fieldType != DataType.DATE_NANOS) {
            return Optional.empty();
        }

        Optional<long[]> timerange = resolveTimerange(bucket, context);
        if (timerange.isEmpty()) {
            return Optional.empty();
        }

        long minDate = timerange.get()[0];
        long maxDate = timerange.get()[1];
        Rounding.Prepared rounding = bucket.getDateRounding(context.foldCtx(), minDate, maxDate);

        int possibleBuckets = countBucketsInRange(rounding, minDate, maxDate);
        if (possibleBuckets < MINIMUM_BUCKET_COUNT) {
            addWarning(
                "Line {}:{}: CHANGE_POINT bucket zero-fill skipped; the requested time range and bucket interval allow only [{}] buckets "
                    + "but at least [{}] are required",
                source.source().getLineNumber(),
                source.source().getColumnNumber(),
                possibleBuckets,
                MINIMUM_BUCKET_COUNT
            );
            return Optional.empty();
        }

        return Optional.of(new ChangePointBucketFillSpec(rounding, minDate, maxDate, key, value, groupings));
    }

    private static Aggregate findAggregate(LogicalPlan plan, AttributeMap.Builder<Expression> evalExprs) {
        if (plan instanceof Eval eval) {
            collectEvalAliases(eval, evalExprs);
            return findAggregate(eval.child(), evalExprs);
        }
        if (plan instanceof Aggregate aggregate) {
            collectEvalExprsFromChild(aggregate.child(), evalExprs);
            return aggregate;
        }
        if (plan instanceof UnaryPlan unary) {
            return findAggregate(unary.child(), evalExprs);
        }
        return null;
    }

    private static void collectEvalExprsFromChild(LogicalPlan plan, AttributeMap.Builder<Expression> evalExprs) {
        if (plan instanceof Eval eval) {
            collectEvalAliases(eval, evalExprs);
            collectEvalExprsFromChild(eval.child(), evalExprs);
        } else if (plan instanceof Project project) {
            collectEvalExprsFromChild(project.child(), evalExprs);
        } else if (plan instanceof UnaryPlan unary) {
            collectEvalExprsFromChild(unary.child(), evalExprs);
        }
    }

    private static void collectEvalAliases(Eval eval, AttributeMap.Builder<Expression> evalExprs) {
        for (NamedExpression field : eval.fields()) {
            if (field instanceof Alias alias) {
                evalExprs.put(alias.toAttribute(), alias.child());
            }
        }
    }

    private static Bucket findBucketForKey(Aggregate aggregate, Attribute key, AttributeMap<Expression> evalExprs) {
        for (Expression grouping : aggregate.groupings()) {
            Attribute groupingAttr = null;
            Expression bucketExpr = null;
            if (grouping instanceof Attribute attribute) {
                groupingAttr = attribute;
                bucketExpr = evalExprs.get(attribute);
            } else if (grouping instanceof Alias alias) {
                groupingAttr = alias.toAttribute();
                bucketExpr = evalExprs.get(groupingAttr);
                if (bucketExpr == null) {
                    bucketExpr = alias.child();
                }
            }
            if (groupingAttr == null || key.name().equals(groupingAttr.name()) == false) {
                continue;
            }
            if (bucketExpr instanceof Bucket bucket) {
                return bucket;
            }
        }
        for (var entry : evalExprs.entrySet()) {
            if (key.name().equals(entry.getKey().name()) && entry.getValue() instanceof Bucket bucket) {
                return bucket;
            }
        }
        return null;
    }

    private static Optional<long[]> resolveTimerange(Bucket bucket, LogicalOptimizerContext context) {
        FoldContext foldCtx = context.foldCtx();
        if (bucket.from() != null && bucket.to() != null && bucket.from().foldable() && bucket.to().foldable()) {
            return Optional.of(new long[] { foldToLong(foldCtx, bucket.from()), foldToLong(foldCtx, bucket.to()) });
        }
        TimestampBounds bounds = context.timestampBounds();
        if (bounds != null) {
            return Optional.of(new long[] { bounds.start().toEpochMilli(), bounds.end().toEpochMilli() });
        }
        return Optional.empty();
    }

    private static long foldToLong(FoldContext ctx, Expression e) {
        Object value = Foldables.valueOf(ctx, e);
        return DataType.isDateTime(e.dataType()) ? ((Number) value).longValue() : dateTimeToLong(((BytesRef) value).utf8ToString());
    }
}
