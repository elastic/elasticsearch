/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PutShutdownRequestTests extends AbstractWireSerializingTestCase<PutShutdownRequestTests.RequestWrapper> {

    /**
     * Wraps a {@link org.elasticsearch.xpack.shutdown.PutShutdownNodeAction.Request} to add proper equality checks
     */
    record RequestWrapper(
        String nodeId,
        SingleNodeShutdownMetadata.Type type,
        String reason,
        TimeValue allocationDelay,
        String targetNodeName,
        TimeValue gracePeriod,
        TaskId parentTask,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout
    ) implements Writeable {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final var request = new PutShutdownNodeAction.Request(
                masterNodeTimeout,
                ackTimeout,
                nodeId,
                type,
                reason,
                allocationDelay,
                targetNodeName,
                gracePeriod
            );
            request.setParentTask(parentTask);
            request.writeTo(out);
        }
    }

    @Override
    protected Writeable.Reader<RequestWrapper> instanceReader() {
        return in -> {
            final var request = new PutShutdownNodeAction.Request(in);
            return new RequestWrapper(
                request.getNodeId(),
                request.getType(),
                request.getReason(),
                request.getAllocationDelay(),
                request.getTargetNodeName(),
                request.getGracePeriod(),
                request.getParentTask(),
                request.masterNodeTimeout(),
                request.ackTimeout()
            );
        };
    }

    @Override
    protected RequestWrapper createTestInstance() {
        return new RequestWrapper(
            randomIdentifier(),
            randomFrom(SingleNodeShutdownMetadata.Type.values()),
            randomIdentifier(),
            randomOptionalTimeValue(),
            randomOptionalIdentifier(),
            randomOptionalTimeValue(),
            randomTaskId(),
            randomTimeValue(),
            randomTimeValue()
        );
    }

    private static String randomOptionalIdentifier() {
        return randomBoolean() ? null : randomIdentifier();
    }

    private static TimeValue randomOptionalTimeValue() {
        return randomBoolean() ? null : randomTimeValue();
    }

    private static TaskId randomTaskId() {
        return randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomIdentifier(), randomNonNegativeLong());
    }

    @Override
    protected RequestWrapper mutateInstance(RequestWrapper instance) {
        return switch (between(1, 9)) {
            case 1 -> new RequestWrapper(
                randomValueOtherThan(instance.nodeId, ESTestCase::randomIdentifier),
                instance.type,
                instance.reason,
                instance.allocationDelay,
                instance.targetNodeName,
                instance.gracePeriod,
                instance.parentTask,
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 2 -> new RequestWrapper(
                instance.nodeId,
                randomValueOtherThan(instance.type, () -> randomFrom(SingleNodeShutdownMetadata.Type.values())),
                instance.reason,
                instance.allocationDelay,
                instance.targetNodeName,
                instance.gracePeriod,
                instance.parentTask,
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 3 -> new RequestWrapper(
                instance.nodeId,
                instance.type,
                randomValueOtherThan(instance.reason, ESTestCase::randomIdentifier),
                instance.allocationDelay,
                instance.targetNodeName,
                instance.gracePeriod,
                instance.parentTask,
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 4 -> new RequestWrapper(
                instance.nodeId,
                instance.type,
                instance.reason,
                randomValueOtherThan(instance.allocationDelay, PutShutdownRequestTests::randomOptionalTimeValue),
                instance.targetNodeName,
                instance.gracePeriod,
                instance.parentTask,
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 5 -> new RequestWrapper(
                instance.nodeId,
                instance.type,
                instance.reason,
                instance.allocationDelay,
                randomValueOtherThan(instance.targetNodeName, PutShutdownRequestTests::randomOptionalIdentifier),
                instance.gracePeriod,
                instance.parentTask,
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 6 -> new RequestWrapper(
                instance.nodeId,
                instance.type,
                instance.reason,
                instance.allocationDelay,
                instance.targetNodeName,
                randomValueOtherThan(instance.gracePeriod, PutShutdownRequestTests::randomOptionalTimeValue),
                instance.parentTask,
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 7 -> new RequestWrapper(
                instance.nodeId,
                instance.type,
                instance.reason,
                instance.allocationDelay,
                instance.targetNodeName,
                instance.gracePeriod,
                randomValueOtherThan(instance.parentTask, PutShutdownRequestTests::randomTaskId),
                instance.ackTimeout,
                instance.masterNodeTimeout
            );
            case 8 -> new RequestWrapper(
                instance.nodeId,
                instance.type,
                instance.reason,
                instance.allocationDelay,
                instance.targetNodeName,
                instance.gracePeriod,
                instance.parentTask,
                randomValueOtherThan(instance.ackTimeout, ESTestCase::randomTimeValue),
                instance.masterNodeTimeout
            );
            case 9 -> new RequestWrapper(
                instance.nodeId,
                instance.type,
                instance.reason,
                instance.allocationDelay,
                instance.targetNodeName,
                instance.gracePeriod,
                instance.parentTask,
                instance.ackTimeout,
                randomValueOtherThan(instance.masterNodeTimeout, ESTestCase::randomTimeValue)
            );
            default -> throw new AssertionError("impossible");
        };
    }
}
