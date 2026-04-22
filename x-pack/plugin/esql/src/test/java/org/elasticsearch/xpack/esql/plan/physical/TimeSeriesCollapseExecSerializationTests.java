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
        Attribute timestamp = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> values = randomFieldAttributes(1, 5, false);
        return new TimeSeriesCollapseExec(source, child, timestamp, values);
    }

    @Override
    protected TimeSeriesCollapseExec mutateInstance(TimeSeriesCollapseExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute timestamp = instance.timestamp();
        List<Attribute> values = instance.values();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> timestamp = randomValueOtherThan(timestamp, () -> randomFieldAttributes(1, 1, false).get(0));
            case 2 -> values = randomValueOtherThan(values, () -> randomFieldAttributes(1, 5, false));
            default -> throw new IllegalStateException();
        }
        return new TimeSeriesCollapseExec(instance.source(), child, timestamp, values);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
