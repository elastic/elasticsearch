/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiStreamingProcessor;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedStreamingProcessor;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStreamSchemaPayload;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Handles chat completion requests and responses for OpenAI models in SageMaker.
 * This class implements the SageMakerStreamSchemaPayload interface to provide
 * the necessary methods for handling OpenAI chat completions.
 */
public class OpenAiCompletionPayload implements SageMakerStreamSchemaPayload {

    private static final XContent jsonXContent = JsonXContent.jsonXContent;
    private static final String APPLICATION_JSON = jsonXContent.type().mediaTypeWithoutParameters();
    private static final XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
        LoggingDeprecationHandler.INSTANCE
    );
    private static final String USER_FIELD = "user";
    private static final String USER_ROLE = "user";
    private static final String MAX_COMPLETION_TOKENS_FIELD = "max_completion_tokens";
    private static final OpenAiUnifiedChatCompletionResponseHandler ERROR_HANDLER = new OpenAiUnifiedChatCompletionResponseHandler(
        "sagemaker openai chat completion",
        ((request, result) -> {
            assert false : "do not call this";
            throw new UnsupportedOperationException("SageMaker should not call this object's response parser.");
        })
    );

    @Override
    public SdkBytes chatCompletionRequestBytes(SageMakerModel model, UnifiedCompletionRequest request) throws Exception {
        return completion(model, new UnifiedChatCompletionRequestEntity(request, true), request.maxCompletionTokens());
    }

    private SdkBytes completion(SageMakerModel model, UnifiedChatCompletionRequestEntity requestEntity, @Nullable Long maxCompletionTokens)
        throws Exception {
        if (model.apiTaskSettings() instanceof SageMakerOpenAiTaskSettings apiTaskSettings) {
            return SdkBytes.fromUtf8String(Strings.toString((builder, params) -> {
                requestEntity.toXContent(builder, params);

                if (Strings.isNullOrEmpty(apiTaskSettings.user()) == false) {
                    builder.field(USER_FIELD, apiTaskSettings.user());
                }

                if (maxCompletionTokens != null) {
                    builder.field(MAX_COMPLETION_TOKENS_FIELD, maxCompletionTokens);
                }
                return builder;
            }));
        } else {
            throw createUnsupportedSchemaException(model);
        }
    }

    @Override
    public StreamingUnifiedChatCompletionResults.Results chatCompletionResponseBody(SageMakerModel model, SdkBytes response) {
        var serverSentEvents = serverSentEvents(response);
        var results = serverSentEvents.flatMap(event -> {
            if ("error".equals(event.type())) {
                throw ERROR_HANDLER.buildMidStreamChatCompletionError(model.getInferenceEntityId(), event.data(), null);
            } else {
                try {
                    return OpenAiUnifiedStreamingProcessor.parse(parserConfig, event);
                } catch (Exception e) {
                    throw ERROR_HANDLER.buildMidStreamChatCompletionError(model.getInferenceEntityId(), event.data(), e);
                }
            }
        })
            .collect(
                () -> new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(),
                ArrayDeque::offer,
                ArrayDeque::addAll
            );
        return new StreamingUnifiedChatCompletionResults.Results(results);
    }

    /*
     * We should be safe to use ServerSentEventParser. It was built knowing Apache HTTP will have leftover bytes for us to manage,
     * but SageMaker uses Netty and (likely, hopefully) doesn't have that problem.
     */
    private Stream<ServerSentEvent> serverSentEvents(SdkBytes response) {
        return new ServerSentEventParser().parse(response.asByteArray()).stream().filter(ServerSentEvent::hasData);
    }

    @Override
    public String api() {
        return "openai";
    }

    @Override
    public SageMakerStoredTaskSchema apiTaskSettings(Map<String, Object> taskSettings, ValidationException validationException) {
        return SageMakerOpenAiTaskSettings.fromMap(taskSettings, validationException);
    }

    @Override
    public Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return Stream.of(
            new NamedWriteableRegistry.Entry(
                SageMakerStoredTaskSchema.class,
                SageMakerOpenAiTaskSettings.NAME,
                SageMakerOpenAiTaskSettings::new
            )
        );
    }

    @Override
    public String accept(SageMakerModel model) {
        return APPLICATION_JSON;
    }

    @Override
    public String contentType(SageMakerModel model) {
        return APPLICATION_JSON;
    }

    @Override
    public SdkBytes requestBytes(SageMakerModel model, SageMakerInferenceRequest request) throws Exception {
        return completion(
            model,
            new UnifiedChatCompletionRequestEntity(new UnifiedChatInput(request.input(), USER_ROLE, request.stream())),
            null
        );
    }

    @Override
    public InferenceServiceResults responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception {
        return OpenAiChatCompletionResponseEntity.fromResponse(response.body().asByteArray());
    }

    @Override
    public StreamingChatCompletionResults.Results streamResponseBody(SageMakerModel model, SdkBytes response) {
        var serverSentEvents = serverSentEvents(response);
        var results = serverSentEvents.flatMap(event -> OpenAiStreamingProcessor.parse(parserConfig, event))
            .collect(() -> new ArrayDeque<StreamingChatCompletionResults.Result>(), ArrayDeque::offer, ArrayDeque::addAll);
        return new StreamingChatCompletionResults.Results(results);
    }
}
