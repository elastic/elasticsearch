/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class InferenceActionRequestTests extends AbstractWireSerializingTestCase<InferenceAction.Request> {

    @Override
    protected Writeable.Reader<InferenceAction.Request> instanceReader() {
        return InferenceAction.Request::new;
    }

    @Override
    protected InferenceAction.Request createTestInstance() {
        return new InferenceAction.Request(randomFrom(TaskType.values()), randomAlphaOfLength(6));
    }

    @Override
    protected InferenceAction.Request mutateInstance(InferenceAction.Request instance) throws IOException {
        int select = randomIntBetween(0, 1);
        switch (select) {
            case 0 -> {
                var nextTask = TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length];
                return new InferenceAction.Request(nextTask, instance.getModelId());
            }
            case 1 -> {
                return new InferenceAction.Request(instance.getTaskType(), instance.getModelId() + "foo");
            }
            default -> {
                throw new UnsupportedOperationException();
            }
        }
    }
}
