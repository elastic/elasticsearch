/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

public class GetAsyncSearchRequestTests extends AbstractWireSerializingTestCase<GetAsyncSearchAction.Request> {
    @Override
    protected Writeable.Reader<GetAsyncSearchAction.Request> instanceReader() {
        return GetAsyncSearchAction.Request::new;
    }

    @Override
    protected GetAsyncSearchAction.Request createTestInstance() {
        GetAsyncSearchAction.Request req = new GetAsyncSearchAction.Request(randomSearchId());
        if (randomBoolean()) {
            req.setWaitForCompletion(TimeValue.timeValueMillis(randomIntBetween(1, 10000)));
        }
        if (randomBoolean()) {
            req.setKeepAlive(TimeValue.timeValueMillis(randomIntBetween(1, 10000)));
        }
        return req;
    }

    static String randomSearchId() {
        return AsyncExecutionId.encode(UUIDs.randomBase64UUID(),
            new TaskId(randomAlphaOfLengthBetween(10, 20), randomLongBetween(0, Long.MAX_VALUE)));
    }
}
