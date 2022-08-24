/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class StartTrainedModelDeploymentRequestTests extends AbstractSerializingTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.parseRequest(null, parser);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return createRandom();
    }

    public static Request createRandom() {
        Request request = new Request(randomAlphaOfLength(10));
        if (randomBoolean()) {
            request.setTimeout(TimeValue.parseTimeValue(randomTimeValue(), Request.TIMEOUT.getPreferredName()));
        }
        if (randomBoolean()) {
            request.setWaitForState(randomFrom(AllocationStatus.State.values()));
        }
        if (randomBoolean()) {
            request.setThreadsPerAllocation(randomIntBetween(1, 8));
        }
        if (randomBoolean()) {
            request.setNumberOfAllocations(randomIntBetween(1, 8));
        }
        if (randomBoolean()) {
            request.setQueueCapacity(randomIntBetween(1, 1000000));
        }
        return request;
    }

    public void testValidate_GivenThreadsPerAllocationIsZero() {
        Request request = createRandom();
        request.setThreadsPerAllocation(0);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[threads_per_allocation] must be a positive integer"));
    }

    public void testValidate_GivenThreadsPerAllocationIsNegative() {
        Request request = createRandom();
        request.setThreadsPerAllocation(randomIntBetween(-100, -1));

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[threads_per_allocation] must be a positive integer"));
    }

    public void testValidate_GivenThreadsPerAllocationIsNotPowerOf2() {
        Set<Integer> powersOf2 = IntStream.range(0, 10).map(n -> (int) Math.pow(2, n)).boxed().collect(Collectors.toSet());
        List<Integer> input = IntStream.range(1, 33).filter(n -> powersOf2.contains(n) == false).boxed().toList();

        for (int n : input) {
            Request request = createRandom();
            request.setThreadsPerAllocation(n);

            ActionRequestValidationException e = request.validate();

            assertThat(e, is(not(nullValue())));
            assertThat(e.getMessage(), containsString("[threads_per_allocation] must be a power of 2 less than or equal to 32"));
        }
    }

    public void testValidate_GivenThreadsPerAllocationIsValid() {
        for (int n : List.of(1, 2, 4, 8, 16, 32)) {
            Request request = createRandom();
            request.setThreadsPerAllocation(n);

            ActionRequestValidationException e = request.validate();

            assertThat(e, is(nullValue()));
        }
    }

    public void testValidate_GivenNumberOfAllocationsIsZero() {
        Request request = createRandom();
        request.setNumberOfAllocations(0);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[number_of_allocations] must be a positive integer"));
    }

    public void testValidate_GivenNumberOfAllocationsIsNegative() {
        Request request = createRandom();
        request.setNumberOfAllocations(randomIntBetween(-100, -1));

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[number_of_allocations] must be a positive integer"));
    }

    public void testValidate_GivenQueueCapacityIsZero() {
        Request request = createRandom();
        request.setQueueCapacity(0);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[queue_capacity] must be a positive integer"));
    }

    public void testValidate_GivenQueueCapacityIsNegative() {
        Request request = createRandom();
        request.setQueueCapacity(randomIntBetween(Integer.MIN_VALUE, -1));

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[queue_capacity] must be a positive integer"));
    }

    public void testValidate_GivenQueueCapacityIsAtLimit() {
        Request request = createRandom();
        request.setQueueCapacity(1_000_000);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(nullValue()));
    }

    public void testValidate_GivenQueueCapacityIsOverLimit() {
        Request request = createRandom();
        request.setQueueCapacity(1_000_001);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[queue_capacity] must be less than 1000000"));
    }

    public void testDefaults() {
        Request request = new Request(randomAlphaOfLength(10));
        assertThat(request.getTimeout(), equalTo(TimeValue.timeValueSeconds(20)));
        assertThat(request.getWaitForState(), equalTo(AllocationStatus.State.STARTED));
        assertThat(request.getNumberOfAllocations(), equalTo(1));
        assertThat(request.getThreadsPerAllocation(), equalTo(1));
        assertThat(request.getQueueCapacity(), equalTo(1024));
    }
}
