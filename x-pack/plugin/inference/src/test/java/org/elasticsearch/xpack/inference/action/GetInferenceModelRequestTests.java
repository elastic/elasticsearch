/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;

import static org.elasticsearch.TransportVersions.INFERENCE_DONT_PERSIST_ON_READ_BACKPORT_8_16;

public class GetInferenceModelRequestTests extends AbstractWireSerializingTestCase<GetInferenceModelAction.Request> {

    public static GetInferenceModelAction.Request randomTestInstance() {
        return new GetInferenceModelAction.Request(randomAlphaOfLength(8), randomFrom(TaskType.values()), randomBoolean());
    }

    public void testShouldReadPersistDefault() {
        var previousVersion = new TransportVersion(INFERENCE_DONT_PERSIST_ON_READ_BACKPORT_8_16.id() - 1000);
        assertFalse(GetInferenceModelAction.Request.shouldReadPersistDefault(previousVersion));

        var patchOnSameVersion = new TransportVersion(INFERENCE_DONT_PERSIST_ON_READ_BACKPORT_8_16.id() + 1);
        assertTrue(GetInferenceModelAction.Request.shouldReadPersistDefault(patchOnSameVersion));

        var nextVersion = new TransportVersion(INFERENCE_DONT_PERSIST_ON_READ_BACKPORT_8_16.id() + 1000);
        assertFalse(GetInferenceModelAction.Request.shouldReadPersistDefault(nextVersion));

        var versionChangedInForwardBranch = new TransportVersion(8_776_00_0);
        assertTrue(GetInferenceModelAction.Request.shouldReadPersistDefault(versionChangedInForwardBranch));
    }

    @Override
    protected Writeable.Reader<GetInferenceModelAction.Request> instanceReader() {
        return GetInferenceModelAction.Request::new;
    }

    @Override
    protected GetInferenceModelAction.Request createTestInstance() {
        return randomTestInstance();
    }

    @Override
    protected GetInferenceModelAction.Request mutateInstance(GetInferenceModelAction.Request instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new GetInferenceModelAction.Request(instance.getInferenceEntityId() + "foo", instance.getTaskType());
            case 1 -> {
                var nextTaskType = TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length];
                yield new GetInferenceModelAction.Request(instance.getInferenceEntityId(), nextTaskType);
            }
            case 2 -> new GetInferenceModelAction.Request(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.isPersistDefaultConfig() == false
            );
            default -> throw new UnsupportedOperationException();
        };
    }
}
