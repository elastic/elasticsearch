/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamResponseHandler;
import software.amazon.awssdk.services.sagemakerruntime.model.ResponseStream;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerClient;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;

import java.util.Locale;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;

/**
 * All the logic that is required to call any SageMaker model is handled within this Schema class.
 * Any model-specific logic is handled within the associated {@link SageMakerStreamSchemaPayload}.
 * This schema is specific for SageMaker's streaming API. For non-streaming, see {@link SageMakerSchema}.
 */
public class SageMakerStreamSchema extends SageMakerSchema {

    private final SageMakerStreamSchemaPayload payload;

    public SageMakerStreamSchema(SageMakerStreamSchemaPayload payload) {
        super(payload);
        this.payload = payload;
    }

    public InvokeEndpointWithResponseStreamRequest streamRequest(SageMakerModel model, SageMakerInferenceRequest request) {
        return streamRequest(model, () -> payload.requestBytes(model, request));
    }

    private InvokeEndpointWithResponseStreamRequest streamRequest(SageMakerModel model, CheckedSupplier<SdkBytes, Exception> body) {
        try {
            return createStreamRequest(model).accept(payload.accept(model))
                .contentType(payload.contentType(model))
                .body(body.get())
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

    public InferenceServiceResults streamResponse(SageMakerModel model, SageMakerClient.SageMakerStream response) {
        return new StreamingChatCompletionResults(streamResponse(model, response, payload::streamResponseBody, this::error));
    }

    private <T> Flow.Publisher<T> streamResponse(
        SageMakerModel model,
        SageMakerClient.SageMakerStream response,
        CheckedBiFunction<SageMakerModel, SdkBytes, T, Exception> parseFunction,
        BiFunction<SageMakerModel, Exception, Exception> errorFunction
    ) {
        return downstream -> {
            response.responseStream().subscribe(new Flow.Subscriber<>() {
                private volatile Flow.Subscription upstream;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    this.upstream = subscription;
                    downstream.onSubscribe(subscription);
                }

                @Override
                public void onNext(ResponseStream item) {
                    if (item.sdkEventType() == ResponseStream.EventType.PAYLOAD_PART) {
                        item.accept(InvokeEndpointWithResponseStreamResponseHandler.Visitor.builder().onPayloadPart(payloadPart -> {
                            try {
                                downstream.onNext(parseFunction.apply(model, payloadPart.bytes()));
                            } catch (Exception e) {
                                downstream.onError(errorFunction.apply(model, e));
                            }
                        }).build());
                    } else {
                        assert upstream != null : "upstream is unset";
                        upstream.request(1);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof Exception e) {
                        downstream.onError(errorFunction.apply(model, e));
                    } else {
                        ExceptionsHelper.maybeError(throwable).ifPresent(ExceptionsHelper::maybeDieOnAnotherThread);
                        var e = new RuntimeException("Fatal while streaming SageMaker response for [" + model.getInferenceEntityId() + "]");
                        downstream.onError(errorFunction.apply(model, e));
                    }

                }

                @Override
                public void onComplete() {
                    downstream.onComplete();
                }
            });
        };
    }

    public InvokeEndpointWithResponseStreamRequest chatCompletionStreamRequest(SageMakerModel model, UnifiedCompletionRequest request) {
        return streamRequest(model, () -> payload.chatCompletionRequestBytes(model, request));
    }

    public InferenceServiceResults chatCompletionStreamResponse(SageMakerModel model, SageMakerClient.SageMakerStream response) {
        return new StreamingUnifiedChatCompletionResults(
            streamResponse(model, response, payload::chatCompletionResponseBody, this::chatCompletionError)
        );
    }

    public UnifiedChatCompletionException chatCompletionError(SageMakerModel model, Exception e) {
        if (e instanceof UnifiedChatCompletionException ucce) {
            return ucce;
        }

        var error = errorMessageAndStatus(model, e);
        return new UnifiedChatCompletionException(error.v2(), error.v1(), "error", error.v2().name().toLowerCase(Locale.ROOT));
    }

    private InvokeEndpointWithResponseStreamRequest.Builder createStreamRequest(SageMakerModel model) {
        var request = InvokeEndpointWithResponseStreamRequest.builder();
        request.endpointName(model.endpointName());
        model.customAttributes().ifPresent(request::customAttributes);
        model.inferenceComponentName().ifPresent(request::inferenceComponentName);
        model.inferenceIdForDataCapture().ifPresent(request::inferenceId);
        model.sessionId().ifPresent(request::sessionId);
        model.targetContainerHostname().ifPresent(request::targetContainerHostname);
        model.targetVariant().ifPresent(request::targetVariant);
        return request;
    }
}
