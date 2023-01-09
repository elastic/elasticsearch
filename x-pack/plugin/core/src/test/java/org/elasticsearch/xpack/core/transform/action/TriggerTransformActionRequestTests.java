/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.TriggerTransformAction.Request;

public class TriggerTransformActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(
            randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : Metadata.ALL,
            TimeValue.parseTimeValue(randomTimeValue(), "timeout")
        );
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

        return new TriggerTransformAction.Request(id, timeout);
    }
}
