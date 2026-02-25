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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TimeSeriesAggregateSerializationTests extends AbstractLogicalPlanSerializationTests<TimeSeriesAggregate> {
    @Override
    protected TimeSeriesAggregate createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        List<Expression> groupings = randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList();
        List<? extends NamedExpression> aggregates = AggregateSerializationTests.randomAggregates();
        Bucket timeBucket = BucketSerializationTests.createRandomBucket(configuration());
        Set<String> excluded = randomExcludedDimensions();
        return new TimeSeriesAggregate(
            source,
            child,
            groupings,
            aggregates,
            timeBucket,
            AbstractExpressionSerializationTests.randomChild(),
            excluded
        );
    }

    @Override
    protected TimeSeriesAggregate mutateInstance(TimeSeriesAggregate instance) throws IOException {
        LogicalPlan child = instance.child();
        List<Expression> groupings = instance.groupings();
        List<? extends NamedExpression> aggregates = instance.aggregates();
        Bucket timeBucket = instance.timeBucket();
        Set<String> excluded = instance.excludedDimensions();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 2 -> aggregates = randomValueOtherThan(aggregates, AggregateSerializationTests::randomAggregates);
            case 3 -> timeBucket = randomValueOtherThan(timeBucket, () -> BucketSerializationTests.createRandomBucket(configuration()));
            case 4 -> excluded = excluded.isEmpty() ? Set.of(randomAlphaOfLength(8)) : Set.of();
        }
        return new TimeSeriesAggregate(instance.source(), child, groupings, aggregates, timeBucket, instance.timestamp(), excluded);
    }

    private static Set<String> randomExcludedDimensions() {
        return randomBoolean()
            ? Set.of()
            : IntStream.rangeClosed(1, between(1, 3)).mapToObj(i -> randomAlphaOfLength(i + 3)).collect(Collectors.toSet());
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
