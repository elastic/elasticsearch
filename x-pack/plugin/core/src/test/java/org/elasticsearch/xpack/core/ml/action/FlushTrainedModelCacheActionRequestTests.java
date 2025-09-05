/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.FlushTrainedModelCacheAction.Request;

import java.io.IOException;

public class FlushTrainedModelCacheActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {
    @Override
    protected Request createTestInstance() {
        return randomBoolean() ? new Request() : new Request(randomTimeout());
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        return new Request(randomValueOtherThan(instance.ackTimeout(), this::randomTimeout));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        return instance;
    }

    private TimeValue randomTimeout() {
        return randomTimeValue();
    }
}
