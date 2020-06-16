/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.elasticsearch.xpack.core.async.AsyncExecutionIdTests.randomAsyncId;

public class GetAsyncResultRequestTests extends AbstractWireSerializingTestCase<GetAsyncResultRequest> {
    @Override
    protected Writeable.Reader<GetAsyncResultRequest> instanceReader() {
        return GetAsyncResultRequest::new;
    }

    @Override
    protected GetAsyncResultRequest createTestInstance() {
        GetAsyncResultRequest req = new GetAsyncResultRequest(randomSearchId());
        if (randomBoolean()) {
            req.setWaitForCompletionTimeout(TimeValue.timeValueMillis(randomIntBetween(1, 10000)));
        }
        if (randomBoolean()) {
            req.setKeepAlive(TimeValue.timeValueMillis(randomIntBetween(1, 10000)));
        }
        return req;
    }

    public static String randomSearchId() {
        return randomAsyncId().getEncoded();
    }
}
