/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction.Request;

import java.io.IOException;

public class DeleteTransformActionRequestTests extends AbstractWireSerializingTestCase<Request> {
    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLengthBetween(1, 20), randomBoolean(), TimeValue.parseTimeValue(randomTimeValue(), "timeout"));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        String id = instance.getId();
        boolean force = instance.isForce();
        TimeValue timeout = instance.timeout();

        switch (between(0, 2)) {
            case 0 -> id += randomAlphaOfLengthBetween(1, 5);
            case 1 -> force ^= true;
            case 2 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            default -> throw new AssertionError("Illegal randomization branch");
        }

        return new Request(id, force, timeout);
    }
}
