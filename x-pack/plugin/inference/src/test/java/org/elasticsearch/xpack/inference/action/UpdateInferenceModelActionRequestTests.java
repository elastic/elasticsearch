/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;

import java.io.IOException;

public class UpdateInferenceModelActionRequestTests extends AbstractWireSerializingTestCase<UpdateInferenceModelAction.Request> {

    @Override
    protected Writeable.Reader<UpdateInferenceModelAction.Request> instanceReader() {
        return UpdateInferenceModelAction.Request::new;
    }

    @Override
    protected UpdateInferenceModelAction.Request createTestInstance() {
        return new UpdateInferenceModelAction.Request(
            randomAlphaOfLength(5),
            randomBytesReference(50),
            randomFrom(XContentType.values()),
            randomFrom(TaskType.values()),
            randomTimeValue()
        );
    }

    @Override
    protected UpdateInferenceModelAction.Request mutateInstance(UpdateInferenceModelAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }
}
