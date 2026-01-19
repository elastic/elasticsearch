/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.BucketSerializationTests;

import java.io.IOException;
import java.util.List;

public class TimeSeriesAggregateSerializationTests extends AbstractLogicalPlanSerializationTests<TimeSeriesAggregate> {
    @Override
    protected TimeSeriesAggregate createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        List<Expression> groupings = randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList();
        List<? extends NamedExpression> aggregates = AggregateSerializationTests.randomAggregates();
        Bucket timeBucket = BucketSerializationTests.createRandomBucket(configuration());
        TimeSeriesAggregate.TimeRange timeRange = null;
        if (randomBoolean()) {
            timeRange = randomTimeRange();
        }
        return new TimeSeriesAggregate(
            source,
            child,
            groupings,
            aggregates,
            timeBucket,
            AbstractExpressionSerializationTests.randomChild(),
            timeRange
        );
    }

    private static TimeSeriesAggregate.TimeRange randomTimeRange() {
        TimeSeriesAggregate.TimeRange timeRange;
        long start = randomLongBetween(0, 1_000_000);
        long end = randomLongBetween(start, start + 1_000_000);
        timeRange = new TimeSeriesAggregate.TimeRange(start, end);
        return timeRange;
    }

    @Override
    protected TimeSeriesAggregate mutateInstance(TimeSeriesAggregate instance) throws IOException {
        LogicalPlan child = instance.child();
        List<Expression> groupings = instance.groupings();
        List<? extends NamedExpression> aggregates = instance.aggregates();
        Bucket timeBucket = instance.timeBucket();
        TimeSeriesAggregate.TimeRange timeRange = null;
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 2 -> aggregates = randomValueOtherThan(aggregates, AggregateSerializationTests::randomAggregates);
            case 3 -> timeBucket = randomValueOtherThan(timeBucket, () -> BucketSerializationTests.createRandomBucket(configuration()));
            case 4 -> timeRange = randomValueOtherThan(timeRange, TimeSeriesAggregateSerializationTests::randomTimeRange);
        }
        return new TimeSeriesAggregate(instance.source(), child, groupings, aggregates, timeBucket, instance.timestamp(), timeRange);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
