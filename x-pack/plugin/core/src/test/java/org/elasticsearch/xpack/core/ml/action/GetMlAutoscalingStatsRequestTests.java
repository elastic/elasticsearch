/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Request;

import java.io.IOException;

public class GetMlAutoscalingStatsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(TimeValue.parseTimeValue(randomTimeValue(0, 10_000), "timeout"));
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        return new Request(TimeValue.timeValueMillis(instance.timeout().millis() + randomIntBetween(1, 1000)));
    }
}
