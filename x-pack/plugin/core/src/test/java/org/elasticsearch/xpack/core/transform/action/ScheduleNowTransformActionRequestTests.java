/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction.Request;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ScheduleNowTransformActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLengthBetween(1, 20), randomTimeValue(), randomBoolean());
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String id = instance.getId();
        TimeValue timeout = instance.getTimeout();
        boolean defer = instance.defer();

        switch (between(0, 2)) {
            case 0 -> id += randomAlphaOfLengthBetween(1, 5);
            case 1 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            case 2 -> defer = defer == false;
            default -> throw new AssertionError("Illegal randomization branch");
        }

        return new ScheduleNowTransformAction.Request(id, timeout, defer);
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        return version.supports(Request.SCHEDULE_NOW_DEFER) ? instance : new Request(instance.getId(), instance.getTimeout());
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
