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
import software.amazon.awssdk.services.sagemakerruntime.model.SageMakerRuntimeException;
import software.amazon.awssdk.services.sagemakerruntime.model.ServiceUnavailableException;
import software.amazon.awssdk.services.sagemakerruntime.model.ValidationErrorException;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;

import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

/**
 * All the logic that is required to call any SageMaker model is handled within this Schema class.
 * Any model-specific logic is handled within the associated {@link SageMakerSchemaPayload}.
 * This schema is specific for SageMaker's non-streaming API. For streaming, see {@link SageMakerStreamSchema}.
 */
public class SageMakerSchema {
    private static final String CUSTOM_ATTRIBUTES_HEADER = "X-elastic-sagemaker-custom-attributes";
    private static final String NEW_SESSION_HEADER = "X-elastic-sagemaker-new-session-id";
    private static final String CLOSED_SESSION_HEADER = "X-elastic-sagemaker-closed-session-id";

    private static final String ACCESS_DENIED_CODE = "AccessDeniedException";
    private static final String INCOMPLETE_SIGNATURE = "IncompleteSignature";
    private static final String INVALID_ACTION = "InvalidAction";
    private static final String INVALID_CLIENT_TOKEN = "InvalidClientTokenId";
    private static final String NOT_AUTHORIZED = "NotAuthorized";
    private static final String OPT_IN_REQUIRED = "OptInRequired";
    private static final String REQUEST_EXPIRED = "RequestExpired";
    private static final String THROTTLING_EXCEPTION = "ThrottlingException";

    private final SageMakerSchemaPayload schemaPayload;

    public SageMakerSchema(SageMakerSchemaPayload schemaPayload) {
        this.schemaPayload = schemaPayload;
    }

    public InvokeEndpointRequest request(SageMakerModel model, SageMakerInferenceRequest request) {
        try {
            return createRequest(model).accept(schemaPayload.accept(model))
                .contentType(schemaPayload.contentType(model))
                .body(schemaPayload.requestBytes(model, request))
                .build();
        } catch (ElasticsearchStatusException e) {
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

    public InferenceServiceResults response(SageMakerModel model, InvokeEndpointResponse response, ThreadContext threadContext)
        throws Exception {
        try {
            addHeaders(response, threadContext);
            return schemaPayload.responseBody(model, response);
        } catch (ElasticsearchStatusException e) {
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

    private void addHeaders(InvokeEndpointResponse response, ThreadContext threadContext) {
        if (response.customAttributes() != null) {
            threadContext.addResponseHeader(CUSTOM_ATTRIBUTES_HEADER, response.customAttributes());
        }
        if (response.newSessionId() != null) {
            threadContext.addResponseHeader(NEW_SESSION_HEADER, response.newSessionId());
        }
        if (response.closedSessionId() != null) {
            threadContext.addResponseHeader(CLOSED_SESSION_HEADER, response.closedSessionId());
        }
    }

    public Exception error(SageMakerModel model, Exception e) {
        if (e instanceof ElasticsearchStatusException ee) {
            return ee;
        }
        var error = errorMessageAndStatus(model, e);
        return new ElasticsearchStatusException(error.v1(), error.v2(), e);
    }

    /**
     * Protected because {@link SageMakerStreamSchema} will reuse this to create a Chat Completion error message.
     */
    protected Tuple<String, RestStatus> errorMessageAndStatus(SageMakerModel model, Exception e) {
        String errorMessage = null;
        RestStatus restStatus = null;
        if (e instanceof InternalDependencyException) {
            errorMessage = format("Received an internal dependency error from SageMaker for [%s]", model.getInferenceEntityId());
            restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        } else if (e instanceof InternalFailureException) {
            errorMessage = format("Received an internal failure from SageMaker for [%s]", model.getInferenceEntityId());
            restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        } else if (e instanceof ModelErrorException) {
            errorMessage = format("Received a model error from SageMaker for [%s]", model.getInferenceEntityId());
            restStatus = RestStatus.FAILED_DEPENDENCY;
        } else if (e instanceof ModelNotReadyException) {
            errorMessage = format("Received a model not ready error from SageMaker for [%s]", model.getInferenceEntityId());
            restStatus = RestStatus.TOO_MANY_REQUESTS;
        } else if (e instanceof ServiceUnavailableException) {
            errorMessage = format("SageMaker is unavailable for [%s]", model.getInferenceEntityId());
            restStatus = RestStatus.SERVICE_UNAVAILABLE;
        } else if (e instanceof ValidationErrorException) {
            errorMessage = format("Received a validation error from SageMaker for [%s]", model.getInferenceEntityId());
            restStatus = RestStatus.BAD_REQUEST;
        }

        // if we have a SageMakerRuntimeException that isn't one of the child exceptions, we can parse the error from AwsErrorDetails
        // https://docs.aws.amazon.com/sagemaker/latest/APIReference/CommonErrors.html
        if (errorMessage == null && e instanceof SageMakerRuntimeException re && re.awsErrorDetails() != null) {
            switch (re.awsErrorDetails().errorCode()) {
                case ACCESS_DENIED_CODE, NOT_AUTHORIZED -> {
                    errorMessage = format(
                        "Access and Secret key stored in [%s] do not have sufficient permissions.",
                        model.getInferenceEntityId()
                    );
                    restStatus = RestStatus.BAD_REQUEST;
                }
                case INCOMPLETE_SIGNATURE -> {
                    errorMessage = format("The request signature does not conform to AWS standards [%s]", model.getInferenceEntityId());
                    restStatus = RestStatus.INTERNAL_SERVER_ERROR; // this shouldn't happen and isn't anything the user can do about it
                }
                case INVALID_ACTION -> {
                    errorMessage = format("The requested action is not valid for [%s]", model.getInferenceEntityId());
                    restStatus = RestStatus.BAD_REQUEST;
                }
                case INVALID_CLIENT_TOKEN -> {
                    errorMessage = format("Access key stored in [%s] does not exist in AWS", model.getInferenceEntityId());
                    restStatus = RestStatus.FORBIDDEN;
                }
                case OPT_IN_REQUIRED -> {
                    errorMessage = format("Access key stored in [%s] needs a subscription for the service", model.getInferenceEntityId());
                    restStatus = RestStatus.FORBIDDEN;
                }
                case REQUEST_EXPIRED -> {
                    errorMessage = format(
                        "The request reached SageMaker more than 15 minutes after the date stamp on the request for [%s]",
                        model.getInferenceEntityId()
                    );
                    restStatus = RestStatus.BAD_REQUEST;
                }
                case THROTTLING_EXCEPTION -> {
                    errorMessage = format("SageMaker denied the request for [%s] due to request throttling", model.getInferenceEntityId());
                    restStatus = RestStatus.BAD_REQUEST;
                }
            }
        }

        if (errorMessage == null) {
            errorMessage = format("Received an error from SageMaker for [%s]", model.getInferenceEntityId());
            restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        }

        return Tuple.tuple(errorMessage, restStatus);
    }

    public SageMakerStoredServiceSchema apiServiceSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        return schemaPayload.apiServiceSettings(serviceSettings, validationException);
    }

    public SageMakerStoredTaskSchema apiTaskSettings(Map<String, Object> taskSettings, ValidationException validationException) {
        return schemaPayload.apiTaskSettings(taskSettings, validationException);
    }

    public Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return schemaPayload.namedWriteables();
    }
}
