/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;

public class GetInferenceModelRequestTests extends AbstractWireSerializingTestCase<GetInferenceModelAction.Request> {

    public static GetInferenceModelAction.Request randomTestInstance() {
        return new GetInferenceModelAction.Request(randomAlphaOfLength(8), randomFrom(TaskType.values()), randomBoolean());
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
