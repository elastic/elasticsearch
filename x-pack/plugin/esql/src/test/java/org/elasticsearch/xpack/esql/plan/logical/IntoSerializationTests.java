/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.into.IntoStats;

import java.io.IOException;
import java.util.List;

public class IntoSerializationTests extends AbstractLogicalPlanSerializationTests<Into> {
    @Override
    protected Into createTestInstance() {
        return new Into(randomSource(), randomChild(0), randomAlphaOfLength(10));
    }

    @Override
    protected Into mutateInstance(Into instance) throws IOException {
        LogicalPlan child = instance.child();
        String destination = instance.destination();
        // The coordinator plan around a data-node Into refers to its output by id, so the ids must survive the wire
        List<Attribute> output = instance.output();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> destination = randomValueOtherThan(destination, () -> randomAlphaOfLength(10));
            case 2 -> output = IntoStats.createAttributes(instance.source());
            default -> throw new AssertionError("Unexpected case");
        }
        return new Into(instance.source(), child, destination, output);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
