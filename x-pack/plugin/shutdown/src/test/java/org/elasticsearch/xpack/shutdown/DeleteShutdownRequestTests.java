/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class DeleteShutdownRequestTests extends AbstractWireSerializingTestCase<DeleteShutdownRequestTests.RequestWrapper> {

    /**
     * Wraps a {@link DeleteShutdownNodeAction.Request} to add proper equality checks
     */
    record RequestWrapper(String nodeId, TaskId parentTask, TimeValue masterNodeTimeout, TimeValue ackTimeout) implements Writeable {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final var request = new DeleteShutdownNodeAction.Request(masterNodeTimeout, ackTimeout, nodeId);
            request.setParentTask(parentTask);
            request.writeTo(out);
        }
    }

    @Override
    protected Writeable.Reader<RequestWrapper> instanceReader() {
        return in -> {
            final var request = new DeleteShutdownNodeAction.Request(in);
            return new RequestWrapper(request.getNodeId(), request.getParentTask(), request.masterNodeTimeout(), request.ackTimeout());
        };
    }

    @Override
    protected RequestWrapper createTestInstance() {
        return new RequestWrapper(randomIdentifier(), randomTaskId(), randomTimeValue(), randomTimeValue());
    }

    private static TaskId randomTaskId() {
        return randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomIdentifier(), randomNonNegativeLong());
    }

    @Override
    protected RequestWrapper mutateInstance(RequestWrapper instance) {
        return switch (between(1, 4)) {
            case 1 -> new RequestWrapper(
                randomValueOtherThan(instance.nodeId, ESTestCase::randomIdentifier),
                instance.parentTask,
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 2 -> new RequestWrapper(
                instance.nodeId,
                randomValueOtherThan(instance.parentTask, DeleteShutdownRequestTests::randomTaskId),
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 3 -> new RequestWrapper(
                instance.nodeId,
                instance.parentTask,
                randomValueOtherThan(instance.ackTimeout, ESTestCase::randomTimeValue),
                instance.masterNodeTimeout
            );
            case 4 -> new RequestWrapper(
                instance.nodeId,
                instance.parentTask,
                instance.ackTimeout,
                randomValueOtherThan(instance.masterNodeTimeout, ESTestCase::randomTimeValue)
            );
            default -> throw new AssertionError("impossible");
        };
    }
}
