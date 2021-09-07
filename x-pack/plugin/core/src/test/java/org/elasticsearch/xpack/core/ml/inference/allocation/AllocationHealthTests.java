/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AllocationHealthTests extends AbstractSerializingTestCase<AllocationHealth> {

    public static AllocationHealth randomInstance() {
        return new AllocationHealth(randomInt(10), randomIntBetween(1, 10));
    }

    @Override
    protected AllocationHealth doParseInstance(XContentParser parser) throws IOException {
        return AllocationHealth.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<AllocationHealth> instanceReader() {
        return AllocationHealth::new;
    }

    @Override
    protected AllocationHealth createTestInstance() {
        return randomInstance();
    }

    public void testCalculateState() {
        int targetAllocation = randomIntBetween(2, 10);

        assertThat(
            new AllocationHealth(randomIntBetween(1, targetAllocation - 1), targetAllocation).calculateState(),
            equalTo(AllocationHealth.State.STARTED)
        );

        assertThat(
            new AllocationHealth(0, targetAllocation).calculateState(),
            equalTo(AllocationHealth.State.STARTING)
        );

        assertThat(
            new AllocationHealth(targetAllocation, targetAllocation).calculateState(),
            equalTo(AllocationHealth.State.FULLY_ALLOCATED)
        );
    }

}
