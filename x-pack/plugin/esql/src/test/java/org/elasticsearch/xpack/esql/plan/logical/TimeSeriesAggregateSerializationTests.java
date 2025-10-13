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
        Bucket timeBucket = BucketSerializationTests.createRandomBucket();
        return new TimeSeriesAggregate(source, child, groupings, aggregates, timeBucket);
    }

    @Override
    protected TimeSeriesAggregate mutateInstance(TimeSeriesAggregate instance) throws IOException {
        LogicalPlan child = instance.child();
        List<Expression> groupings = instance.groupings();
        List<? extends NamedExpression> aggregates = instance.aggregates();
        Bucket timeBucket = instance.timeBucket();
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 2 -> aggregates = randomValueOtherThan(aggregates, AggregateSerializationTests::randomAggregates);
            case 3 -> timeBucket = randomValueOtherThan(timeBucket, BucketSerializationTests::createRandomBucket);
        }
        return new TimeSeriesAggregate(instance.source(), child, groupings, aggregates, timeBucket);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
