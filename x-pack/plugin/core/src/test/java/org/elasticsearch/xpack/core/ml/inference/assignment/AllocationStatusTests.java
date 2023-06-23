/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AllocationStatusTests extends AbstractXContentSerializingTestCase<AllocationStatus> {

    public static AllocationStatus randomInstance() {
        return new AllocationStatus(randomInt(10), randomIntBetween(1, 10));
    }

    @Override
    protected AllocationStatus doParseInstance(XContentParser parser) throws IOException {
        return AllocationStatus.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<AllocationStatus> instanceReader() {
        return AllocationStatus::new;
    }

    @Override
    protected AllocationStatus createTestInstance() {
        return randomInstance();
    }

    @Override
    protected AllocationStatus mutateInstance(AllocationStatus instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testCalculateState() {
        int targetAllocation = randomIntBetween(2, 10);

        assertThat(
            new AllocationStatus(randomIntBetween(1, targetAllocation - 1), targetAllocation).calculateState(),
            equalTo(AllocationStatus.State.STARTED)
        );

        assertThat(new AllocationStatus(0, targetAllocation).calculateState(), equalTo(AllocationStatus.State.STARTING));

        assertThat(
            new AllocationStatus(targetAllocation, targetAllocation).calculateState(),
            equalTo(AllocationStatus.State.FULLY_ALLOCATED)
        );
    }

}
