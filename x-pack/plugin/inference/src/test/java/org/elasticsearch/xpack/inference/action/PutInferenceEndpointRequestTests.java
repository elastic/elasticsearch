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
import org.elasticsearch.xpack.core.inference.action.PutInferenceEndpointAction;

public class PutInferenceEndpointRequestTests extends AbstractWireSerializingTestCase<PutInferenceEndpointAction.Request> {
    @Override
    protected Writeable.Reader<PutInferenceEndpointAction.Request> instanceReader() {
        return PutInferenceEndpointAction.Request::new;
    }

    @Override
    protected PutInferenceEndpointAction.Request createTestInstance() {
        return new PutInferenceEndpointAction.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomBytesReference(50),
            randomFrom(XContentType.values())
        );
    }

    @Override
    protected PutInferenceEndpointAction.Request mutateInstance(PutInferenceEndpointAction.Request instance) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new PutInferenceEndpointAction.Request(
                TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length],
                instance.getInferenceEntityId(),
                instance.getContent(),
                instance.getContentType()
            );
            case 1 -> new PutInferenceEndpointAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId() + "foo",
                instance.getContent(),
                instance.getContentType()
            );
            case 2 -> new PutInferenceEndpointAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId(),
                randomBytesReference(instance.getContent().length() + 1),
                instance.getContentType()
            );
            case 3 -> new PutInferenceEndpointAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId(),
                instance.getContent(),
                XContentType.values()[(instance.getContentType().ordinal() + 1) % XContentType.values().length]
            );
            default -> throw new IllegalStateException();
        };
    }
}
