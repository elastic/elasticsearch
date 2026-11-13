/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class IntoSerializationTests extends AbstractLogicalPlanSerializationTests<Into> {
    @Override
    protected Into createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        String destination = randomAlphaOfLength(10);
        return new Into(source, child, destination);
    }

    @Override
    protected Into mutateInstance(Into instance) throws IOException {
        LogicalPlan child = instance.child();
        String destination = instance.destination();
        switch (between(0, 1)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> destination = randomValueOtherThan(destination, () -> randomAlphaOfLength(10));
            default -> throw new AssertionError("Unexpected case");
        }
        return new Into(instance.source(), child, destination);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
