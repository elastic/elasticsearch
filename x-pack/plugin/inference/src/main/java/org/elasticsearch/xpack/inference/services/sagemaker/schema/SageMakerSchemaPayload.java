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

    /**
     * The model API keyword that users will supply in the service settings when creating the request.
     * Automatically registered in {@link SageMakerSchemas}.
     */
    String api();

    /**
     * The supported TaskTypes for this model API.
     * Automatically registered in {@link SageMakerSchemas}.
     */
    EnumSet<TaskType> supportedTasks();

    /**
     * Implement this if the model requires extra ServiceSettings that can be saved to the model index.
     * This can be accessed via {@link SageMakerModel#apiServiceSettings()}.
     */
    default SageMakerStoredServiceSchema apiServiceSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        return SageMakerStoredServiceSchema.NO_OP;
    }

    /**
     * Implement this if the model requires extra TaskSettings that can be saved to the model index.
     * This can be accessed via {@link SageMakerModel#apiTaskSettings()}.
     */
    default SageMakerStoredTaskSchema apiTaskSettings(Map<String, Object> taskSettings, ValidationException validationException) {
        return SageMakerStoredTaskSchema.NO_OP;
    }

    /**
     * This must be thrown if {@link SageMakerModel#apiServiceSettings()} or {@link SageMakerModel#apiTaskSettings()} return the wrong
     * object types.
     */
    default Exception createUnsupportedSchemaException(SageMakerModel model) {
        return new IllegalArgumentException(
            Strings.format(
                "Unsupported SageMaker settings for api [%s] and task type [%s]: [%s] and [%s]",
                model.api(),
                model.getTaskType(),
                model.apiServiceSettings().getWriteableName(),
                model.apiTaskSettings().getWriteableName()
            )
        );
    }

    /**
     * Automatically register the required registry entries with {@link org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider}.
     */
    default Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return Stream.of();
    }

    /**
     * The MIME type of the response from SageMaker.
     */
    String accept(SageMakerModel model);

    /**
     * The MIME type of the request to SageMaker.
     */
    String contentType(SageMakerModel model);

    /**
     * Translate to the body of the request in the MIME type specified by {@link #contentType(SageMakerModel)}.
     */
    SdkBytes requestBytes(SageMakerModel model, SageMakerInferenceRequest request) throws Exception;

    /**
     * Translate from the body of the response in the MIME type specified by {@link #accept(SageMakerModel)}.
     */
    InferenceServiceResults responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception;

}
