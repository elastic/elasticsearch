/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.UpgradeTransformsAction.Request;

import java.io.IOException;

public class UpgradeTransformsActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomBoolean(), TimeValue.parseTimeValue(randomTimeValue(), "timeout"));
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        boolean dryRun = instance.isDryRun();
        TimeValue timeout = instance.timeout();

        switch (between(0, 1)) {
            case 0 -> dryRun ^= true;
            case 1 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            default -> throw new AssertionError("Illegal randomization branch");
        }

        return new Request(dryRun, timeout);
    }

}
