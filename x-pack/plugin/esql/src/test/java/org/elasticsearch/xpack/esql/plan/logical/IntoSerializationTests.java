/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class IntoSerializationTests extends AbstractLogicalPlanSerializationTests<Into> {
    @Override
    protected Into createTestInstance() {
        return new Into(randomSource(), randomChild(0), randomAlphaOfLength(10));
    }

    /** The coordinator plan around a data-node {@code Into} refers to its output by id, so the ids must survive the wire. */
    public void testOutputSurvivesTheWire() throws IOException {
        Into original = createTestInstance();
        Into roundTripped = copyInstance(original, TransportVersion.current());
        assertThat(roundTripped.output(), equalTo(original.output()));
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
