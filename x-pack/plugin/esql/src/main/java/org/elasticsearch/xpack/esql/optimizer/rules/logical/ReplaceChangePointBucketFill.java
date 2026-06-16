/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.ChangePointBucketSpecExtractor;
import org.elasticsearch.xpack.esql.optimizer.ChangePointBucketSpecExtractor.ChangePointBucketFillSpec;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.ChangePointFillEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import java.util.Optional;

/**
 * Inserts {@link ChangePointFillEmptyBuckets} before {@link ChangePoint} when the sort key traces to a
 * literal date {@code BUCKET} grouping and a timerange is available.
 */
public final class ReplaceChangePointBucketFill extends OptimizerRules.ParameterizedOptimizerRule<ChangePoint, LogicalOptimizerContext> {

    public ReplaceChangePointBucketFill() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(ChangePoint changePoint, LogicalOptimizerContext context) {
        Optional<ChangePointBucketFillSpec> spec = ChangePointBucketSpecExtractor.extract(changePoint, context);
        if (spec.isEmpty()) {
            return changePoint;
        }

        LogicalPlan child = changePoint.child();
        LogicalPlan fillChild;
        if (child instanceof Limit limit && limit.child() instanceof OrderBy ob) {
            fillChild = ob;
        } else if (child instanceof LimitBy limitBy && limitBy.child() instanceof OrderBy ob) {
            fillChild = ob;
        } else if (child instanceof TopN topN) {
            fillChild = topN;
        } else {
            return changePoint;
        }

        ChangePointBucketFillSpec fillSpec = spec.get();
        ChangePointFillEmptyBuckets fill = new ChangePointFillEmptyBuckets(
            changePoint.source(),
            fillChild,
            fillSpec.value(),
            fillSpec.key(),
            fillSpec.groupings(),
            fillSpec.rounding(),
            fillSpec.minDate(),
            fillSpec.maxDate()
        );

        LogicalPlan newChild;
        if (child instanceof Limit limit) {
            newChild = new Limit(limit.source(), limit.limit(), fill, limit.duplicated(), limit.local());
        } else if (child instanceof LimitBy limitBy) {
            newChild = new LimitBy(limitBy.source(), limitBy.limitPerGroup(), fill, limitBy.groupings());
        } else {
            newChild = fill;
        }

        return new ChangePoint(
            changePoint.source(),
            newChild,
            changePoint.value(),
            changePoint.key(),
            changePoint.targetType(),
            changePoint.targetPvalue(),
            changePoint.groupings()
        );
    }
}
