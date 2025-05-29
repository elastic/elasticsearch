/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

record SageMakerOpenAiTaskSettings(@Nullable String user) implements SageMakerStoredTaskSchema {
    static final String NAME = "sagemaker_openai_task_settings";
    private static final String USER_FIELD = "user";

    SageMakerOpenAiTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalString());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_SAGEMAKER_CHAT_COMPLETION_8_19;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return user != null ? builder.field(USER_FIELD, user) : builder;
    }

    @Override
    public boolean isEmpty() {
        return user == null;
    }

    @Override
    public SageMakerOpenAiTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        var validationException = new ValidationException();
        var newTaskSettings = fromMap(newSettings, validationException);
        validationException.throwIfValidationErrorsExist();

        return new SageMakerOpenAiTaskSettings(newTaskSettings.user() != null ? newTaskSettings.user() : user);
    }

    static SageMakerOpenAiTaskSettings fromMap(Map<String, Object> map, ValidationException exception) {
        var user = extractOptionalString(map, USER_FIELD, ModelConfigurations.TASK_SETTINGS, exception);
        return new SageMakerOpenAiTaskSettings(user);
    }
}
