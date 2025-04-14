/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;

import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Stream;

public interface SageMakerSchemaPayload {
    String api();

    EnumSet<TaskType> supportedTasks();

    default SageMakerStoredServiceSchema extraServiceSettings(
        Map<String, Object> serviceSettings,
        ValidationException validationException
    ) {
        return SageMakerStoredServiceSchema.NO_OP;
    }

    default SageMakerStoredTaskSchema extraTaskSettings(Map<String, Object> taskSettings, ValidationException validationException) {
        return SageMakerStoredTaskSchema.NO_OP;
    }

    /**
     * Automatically register the required registry entries with {@link org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider}.
     */
    default Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return Stream.of();
    }

    default Exception createUnsupportedSchemaException(SageMakerModel model) {
        return new IllegalArgumentException(
            Strings.format(
                "Unsupported SageMaker settings for api [%s] and task type [%s]: [%s] and [%s]",
                model.api(),
                model.getTaskType(),
                model.extraServiceSettings().getWriteableName(),
                model.extraTaskSettings().getWriteableName()
            )
        );
    }

    String accept(SageMakerModel model);

    String contentType(SageMakerModel model);

    SdkBytes requestBytes(SageMakerModel model, SageMakerInferenceRequest request) throws Exception;

    InferenceServiceResults responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception;

}
