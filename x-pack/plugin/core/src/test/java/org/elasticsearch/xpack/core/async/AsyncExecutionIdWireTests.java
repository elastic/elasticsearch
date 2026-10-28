/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class AsyncExecutionIdWireTests extends AbstractWireSerializingTestCase<AsyncExecutionId> {
    @Override
    protected Writeable.Reader<AsyncExecutionId> instanceReader() {
        return AsyncExecutionId::readFrom;
    }

    @Override
    protected AsyncExecutionId createTestInstance() {
        return new AsyncExecutionId(randomAlphaOfLength(15), new TaskId(randomAlphaOfLength(10), randomLong()));
    }

    @Override
    protected AsyncExecutionId mutateInstance(AsyncExecutionId instance) throws IOException {
        return new AsyncExecutionId(
            instance.getDocId(),
            new TaskId(instance.getTaskId().getNodeId(), instance.getTaskId().getId() * 12345)
        );
    }
}
