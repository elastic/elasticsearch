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
import org.elasticsearch.xcontent.XContentType;

public class PutInferenceModelRequestTests extends AbstractWireSerializingTestCase<PutInferenceModelAction.Request> {
    @Override
    protected Writeable.Reader<PutInferenceModelAction.Request> instanceReader() {
        return PutInferenceModelAction.Request::new;
    }

    @Override
    protected PutInferenceModelAction.Request createTestInstance() {
        return new PutInferenceModelAction.Request(
            randomFrom(TaskType.values()).toString(),
            randomAlphaOfLength(6),
            randomBytesReference(50),
            randomFrom(XContentType.values())
        );
    }

    @Override
    protected PutInferenceModelAction.Request mutateInstance(PutInferenceModelAction.Request instance) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new PutInferenceModelAction.Request(
                TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length].toString(),
                instance.getModelId(),
                instance.getContent(),
                instance.getContentType()
            );
            case 1 -> new PutInferenceModelAction.Request(
                instance.getTaskType().toString(),
                instance.getModelId() + "foo",
                instance.getContent(),
                instance.getContentType()
            );
            case 2 -> new PutInferenceModelAction.Request(
                instance.getTaskType().toString(),
                instance.getModelId(),
                randomBytesReference(instance.getContent().length() + 1),
                instance.getContentType()
            );
            case 3 -> new PutInferenceModelAction.Request(
                instance.getTaskType().toString(),
                instance.getModelId(),
                instance.getContent(),
                XContentType.values()[(instance.getContentType().ordinal() + 1) % XContentType.values().length]
            );
            default -> throw new IllegalStateException();
        };
    }
}
