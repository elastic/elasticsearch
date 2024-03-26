/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.async.AsyncExecutionIdTests.randomAsyncId;

public class GetAsyncStatusRequestTests extends AbstractWireSerializingTestCase<GetAsyncStatusRequest> {
    @Override
    protected Writeable.Reader<GetAsyncStatusRequest> instanceReader() {
        return GetAsyncStatusRequest::new;
    }

    @Override
    protected GetAsyncStatusRequest createTestInstance() {
        GetAsyncStatusRequest req = new GetAsyncStatusRequest(randomSearchId());
        if (randomBoolean()) {
            req.setKeepAlive(TimeValue.timeValueMillis(randomIntBetween(1, 10000)));
        }
        return req;
    }

    @Override
    protected GetAsyncStatusRequest mutateInstance(GetAsyncStatusRequest instance) {
        final GetAsyncStatusRequest statusRequest = new GetAsyncStatusRequest(randomInt() + "");
        if (instance.getKeepAlive() != TimeValue.MINUS_ONE && randomBoolean()) {
            statusRequest.setKeepAlive(TimeValue.MINUS_ONE);
        } else {
            statusRequest.setKeepAlive(new TimeValue(randomLongBetween(0, 9999999), TimeUnit.SECONDS));
        }
        return statusRequest;
    }

    public static String randomSearchId() {
        return randomAsyncId().getEncoded();
    }
}
