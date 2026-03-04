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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TimeSeriesAggregateSerializationTests extends AbstractLogicalPlanSerializationTests<TimeSeriesAggregate> {
    @Override
    protected TimeSeriesAggregate createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        List<Expression> groupings = randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList();
        List<? extends NamedExpression> aggregates = AggregateSerializationTests.randomAggregates();
        Bucket timeBucket = BucketSerializationTests.createRandomBucket(configuration());
        TsidGroupingParams tsidGroupingParams = randomTsidGroupingParams();
        return new TimeSeriesAggregate(
            source,
            child,
            groupings,
            aggregates,
            timeBucket,
            AbstractExpressionSerializationTests.randomChild(),
            tsidGroupingParams
        );
    }

    @Override
    protected TimeSeriesAggregate mutateInstance(TimeSeriesAggregate instance) throws IOException {
        LogicalPlan child = instance.child();
        List<Expression> groupings = instance.groupings();
        List<? extends NamedExpression> aggregates = instance.aggregates();
        Bucket timeBucket = instance.timeBucket();
        TsidGroupingParams tsidGroupingParams = instance.tsidGroupingParams();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 2 -> aggregates = randomValueOtherThan(aggregates, AggregateSerializationTests::randomAggregates);
            case 3 -> timeBucket = randomValueOtherThan(timeBucket, () -> BucketSerializationTests.createRandomBucket(configuration()));
            case 4 -> tsidGroupingParams = tsidGroupingParams == null ? createNonNullTsidGroupingParams() : null;
        }
        return new TimeSeriesAggregate(
            instance.source(),
            child,
            groupings,
            aggregates,
            timeBucket,
            instance.timestamp(),
            tsidGroupingParams
        );
    }

    private TsidGroupingParams randomTsidGroupingParams() {
        if (randomBoolean()) {
            return null;
        }
        return createNonNullTsidGroupingParams();
    }

    private TsidGroupingParams createNonNullTsidGroupingParams() {
        Set<String> excluded = new HashSet<>();
        int count = between(1, 3);
        for (int i = 0; i < count; i++) {
            excluded.add(randomAlphaOfLength(5));
        }
        return new TsidGroupingParams(excluded);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
