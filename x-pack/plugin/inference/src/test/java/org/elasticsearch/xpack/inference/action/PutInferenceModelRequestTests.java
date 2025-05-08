/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

public class PutInferenceModelRequestTests extends AbstractBWCWireSerializationTestCase<PutInferenceModelAction.Request> {
    @Override
    protected Writeable.Reader<PutInferenceModelAction.Request> instanceReader() {
        return PutInferenceModelAction.Request::new;
    }

    @Override
    protected PutInferenceModelAction.Request createTestInstance() {
        return new PutInferenceModelAction.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomBytesReference(50),
            randomFrom(XContentType.values()),
            randomTimeValue()
        );
    }

    @Override
    protected PutInferenceModelAction.Request mutateInstance(PutInferenceModelAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutInferenceModelAction.Request mutateInstanceForVersion(PutInferenceModelAction.Request instance, TransportVersion version) {
        if (version.onOrAfter(TransportVersions.INFERENCE_ADD_TIMEOUT_PUT_ENDPOINT_8_19)) {
            return instance;
        } else if (version.onOrAfter(TransportVersions.V_8_0_0)) {
            return new PutInferenceModelAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId(),
                instance.getContent(),
                instance.getContentType(),
                InferenceAction.Request.DEFAULT_TIMEOUT
            );
        } else {
            return new PutInferenceModelAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId(),
                instance.getContent(),
                /*
                 * See XContentHelper.java#L733
                 * for versions prior to 8.0.0, the content type does not have the VND_ instances
                 */
                XContentType.ofOrdinal(instance.getContentType().canonical().ordinal()),
                InferenceAction.Request.DEFAULT_TIMEOUT
            );
        }
    }
}
