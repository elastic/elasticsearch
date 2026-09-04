/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests.randomFieldAttributes;

public class TimeSeriesCollapseExecSerializationTests extends AbstractPhysicalPlanSerializationTests<TimeSeriesCollapseExec> {
    @Override
    protected TimeSeriesCollapseExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        Attribute value = randomFieldAttributes(1, 1, false).get(0);
        Attribute step = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> dimensions = randomFieldAttributes(0, 5, false);
        long start = randomLongBetween(0, Long.MAX_VALUE - 100_000);
        long end = randomLongBetween(start, start + 100_000);
        long stepMillis = randomLongBetween(1, 10_000);
        return new TimeSeriesCollapseExec(source, child, value, step, dimensions, start, end, stepMillis);
    }

    @Override
    protected TimeSeriesCollapseExec mutateInstance(TimeSeriesCollapseExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute value = instance.value();
        Attribute step = instance.step();
        List<Attribute> dimensions = instance.dimensions();
        long start = instance.start();
        long end = instance.end();
        long stepMillis = instance.stepMillis();
        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> value = randomValueOtherThan(value, () -> randomFieldAttributes(1, 1, false).get(0));
            case 2 -> step = randomValueOtherThan(step, () -> randomFieldAttributes(1, 1, false).get(0));
            case 3 -> dimensions = randomValueOtherThan(dimensions, () -> randomFieldAttributes(0, 5, false));
            case 4 -> {
                start = randomValueOtherThan(start, () -> randomLongBetween(0, Long.MAX_VALUE - 100_000));
                if (end < start) {
                    end = start;
                }
            }
            case 5 -> stepMillis = randomValueOtherThan(stepMillis, () -> randomLongBetween(1, 10_000));
            default -> throw new IllegalStateException();
        }
        return new TimeSeriesCollapseExec(instance.source(), child, value, step, dimensions, start, end, stepMillis);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
