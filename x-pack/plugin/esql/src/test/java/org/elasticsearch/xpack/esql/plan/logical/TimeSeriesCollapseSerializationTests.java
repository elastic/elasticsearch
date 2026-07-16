/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class TimeSeriesCollapseSerializationTests extends AbstractLogicalPlanSerializationTests<TimeSeriesCollapse> {
    @Override
    protected TimeSeriesCollapse createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        Attribute value = randomFieldAttributes(1, 1, false).get(0);
        Attribute step = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> dimensions = randomFieldAttributes(0, 5, false);
        long startMillis = randomLongBetween(0, Long.MAX_VALUE - 100_000);
        long endMillis = randomLongBetween(startMillis, startMillis + 100_000);
        long stepMillis = randomLongBetween(1, 10_000);
        Literal start = Literal.dateTime(source, Instant.ofEpochMilli(startMillis));
        Literal end = Literal.dateTime(source, Instant.ofEpochMilli(endMillis));
        Expression stepBucketSize = Literal.timeDuration(source, Duration.ofMillis(stepMillis));
        return new TimeSeriesCollapse(source, child, value, step, dimensions, start, end, stepBucketSize);
    }

    @Override
    protected TimeSeriesCollapse mutateInstance(TimeSeriesCollapse instance) throws IOException {
        LogicalPlan child = instance.child();
        Attribute value = instance.value();
        Attribute step = instance.step();
        List<Attribute> dimensions = instance.dimensions();
        Literal start = instance.startLiteral();
        Literal end = instance.endLiteral();
        Expression stepBucketSize = instance.stepBucketSize();
        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> value = randomValueOtherThan(value, () -> randomFieldAttributes(1, 1, false).get(0));
            case 2 -> step = randomValueOtherThan(step, () -> randomFieldAttributes(1, 1, false).get(0));
            case 3 -> dimensions = randomValueOtherThan(dimensions, () -> randomFieldAttributes(0, 5, false));
            case 4 -> start = randomValueOtherThan(
                start,
                () -> Literal.dateTime(instance.source(), Instant.ofEpochMilli(randomLongBetween(0, Long.MAX_VALUE - 100_000)))
            );
            case 5 -> stepBucketSize = randomValueOtherThan(
                stepBucketSize,
                () -> Literal.timeDuration(instance.source(), Duration.ofMillis(randomLongBetween(1, 10_000)))
            );
            default -> throw new IllegalStateException();
        }
        return new TimeSeriesCollapse(instance.source(), child, value, step, dimensions, start, end, stepBucketSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
