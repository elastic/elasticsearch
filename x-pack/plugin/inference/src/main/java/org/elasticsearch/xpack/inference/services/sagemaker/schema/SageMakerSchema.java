/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import software.amazon.awssdk.services.sagemakerruntime.model.InternalDependencyException;
import software.amazon.awssdk.services.sagemakerruntime.model.InternalFailureException;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;
import software.amazon.awssdk.services.sagemakerruntime.model.ModelErrorException;
import software.amazon.awssdk.services.sagemakerruntime.model.ModelNotReadyException;
import software.amazon.awssdk.services.sagemakerruntime.model.ServiceUnavailableException;
import software.amazon.awssdk.services.sagemakerruntime.model.ValidationErrorException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;

import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Stream;

public class SageMakerSchema {
    private static final String INTERNAL_DEPENDENCY_ERROR = "Received an internal dependency error from SageMaker for [%s]";
    private static final String INTERNAL_FAILURE = "Received an internal failure from SageMaker for [%s]";
    private static final String MODEL_ERROR = "Received a model error from SageMaker for [%s]";
    private static final String MODEL_NOT_READY_ERROR = "Received a model not ready error from SageMaker for [%s]";
    private static final String SERVICE_UNAVAILABLE = "SageMaker is unavailable for [%s]";
    private static final String VALIDATION_ERROR = "Received a validation error from SageMaker for [%s]";
    private static final String UNKNOWN_ERROR = "Received an error from SageMaker for [%s]";

    private final SageMakerSchemaPayload schemaPayload;

    public SageMakerSchema(SageMakerSchemaPayload schemaPayload) {
        this.schemaPayload = schemaPayload;
    }

    public InvokeEndpointRequest request(SageMakerModel model, SageMakerInferenceRequest request) throws Exception {
        try {
            return createRequest(model).accept(schemaPayload.accept(model))
                .contentType(schemaPayload.contentType(model))
                .body(schemaPayload.requestBytes(model, request))
                .build();
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            throw new ElasticsearchStatusException(
                "Failed to create SageMaker request for [%s]",
                RestStatus.INTERNAL_SERVER_ERROR,
                e,
                model.getInferenceEntityId()
            );
        }
    }

    private InvokeEndpointRequest.Builder createRequest(SageMakerModel model) {
        var request = InvokeEndpointRequest.builder();
        request.endpointName(model.endpointName());
        model.customAttributes().ifPresent(request::customAttributes);
        model.enableExplanations().ifPresent(request::enableExplanations);
        model.inferenceComponentName().ifPresent(request::inferenceComponentName);
        model.inferenceIdForDataCapture().ifPresent(request::inferenceId);
        model.sessionId().ifPresent(request::sessionId);
        model.targetContainerHostname().ifPresent(request::targetContainerHostname);
        model.targetModel().ifPresent(request::targetModel);
        model.targetVariant().ifPresent(request::targetVariant);
        return request;
    }

    public InferenceServiceResults response(SageMakerModel model, InvokeEndpointResponse response) throws Exception {
        try {
            return schemaPayload.responseBody(model, response);
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            throw new ElasticsearchStatusException(
                "Failed to translate SageMaker response for [%s]",
                RestStatus.INTERNAL_SERVER_ERROR,
                e,
                model.getInferenceEntityId()
            );
        }
    }

    public Exception error(SageMakerModel model, Exception e) {
        if (e instanceof ElasticsearchException ee) {
            return ee;
        }
        var error = errorMessageAndStatus(model, e);
        return new ElasticsearchStatusException(error.v1(), error.v2(), e, model.getInferenceEntityId());
    }

    protected Tuple<String, RestStatus> errorMessageAndStatus(SageMakerModel model, Exception e) {
        String errorMessageTemplate;
        RestStatus restStatus;
        if (e instanceof InternalDependencyException) {
            errorMessageTemplate = INTERNAL_DEPENDENCY_ERROR;
            restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        } else if (e instanceof InternalFailureException) {
            errorMessageTemplate = INTERNAL_FAILURE;
            restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        } else if (e instanceof ModelErrorException) {
            errorMessageTemplate = MODEL_ERROR;
            restStatus = RestStatus.FAILED_DEPENDENCY;
        } else if (e instanceof ModelNotReadyException) {
            errorMessageTemplate = MODEL_NOT_READY_ERROR;
            restStatus = RestStatus.TOO_MANY_REQUESTS;
        } else if (e instanceof ServiceUnavailableException) {
            errorMessageTemplate = SERVICE_UNAVAILABLE;
            restStatus = RestStatus.SERVICE_UNAVAILABLE;
        } else if (e instanceof ValidationErrorException) {
            errorMessageTemplate = VALIDATION_ERROR;
            restStatus = RestStatus.BAD_REQUEST;
        } else {
            errorMessageTemplate = UNKNOWN_ERROR;
            restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        }

        var errorMessage = String.format(errorMessageTemplate, model.getInferenceEntityId());
        return Tuple.tuple(errorMessage, restStatus);
    }

    public String api() {
        return schemaPayload.api();
    }

    public EnumSet<TaskType> supportedTasks() {
        return schemaPayload.supportedTasks();
    }

    public SageMakerStoredServiceSchema extraServiceSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        return schemaPayload.extraServiceSettings(serviceSettings, validationException);
    }

    public SageMakerStoredTaskSchema extraTaskSettings(Map<String, Object> taskSettings, ValidationException validationException) {
        return schemaPayload.extraTaskSettings(taskSettings, validationException);
    }

    public Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return schemaPayload.namedWriteables();
    }
}
