/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction.Request;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ScheduleNowTransformActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLengthBetween(1, 20), TimeValue.parseTimeValue(randomTimeValue(), "timeout"));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String id = instance.getId();
        TimeValue timeout = instance.getTimeout();

        switch (between(0, 1)) {
            case 0 -> id += randomAlphaOfLengthBetween(1, 5);
            case 1 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            default -> throw new AssertionError("Illegal randomization branch");
        }

        return new ScheduleNowTransformAction.Request(id, timeout);
    }

    public void testValidationSuccess() {
        Request request = new Request("id", TimeValue.ZERO);
        assertThat(request.validate(), is(nullValue()));
    }

    public void testValidationFailure() {
        Request request = new Request("_all", TimeValue.ZERO);
        ActionRequestValidationException e = request.validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), contains("_schedule_now API does not support _all wildcard"));
    }
}
