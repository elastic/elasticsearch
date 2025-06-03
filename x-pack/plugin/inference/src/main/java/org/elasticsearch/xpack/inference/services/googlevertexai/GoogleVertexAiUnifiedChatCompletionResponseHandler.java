/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;

import static org.elasticsearch.core.Strings.format;

/**
 * Handles streaming chat completion responses and error parsing for Google Vertex AI inference endpoints.
 * This handler is designed to work with the unified Google Vertex AI chat completion API.
 */
public class GoogleVertexAiUnifiedChatCompletionResponseHandler extends GoogleVertexAiResponseHandler {

    private static final String ERROR_FIELD = "error";
    private static final String ERROR_CODE_FIELD = "code";
    private static final String ERROR_MESSAGE_FIELD = "message";
    private static final String ERROR_STATUS_FIELD = "status";

    private static final ResponseParser noopParseFunction = (a, b) -> null;

    public GoogleVertexAiUnifiedChatCompletionResponseHandler(String requestType) {
        super(requestType, noopParseFunction, GoogleVertexAiErrorResponse::fromResponse, true);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        assert request.isStreaming() : "GoogleVertexAiUnifiedChatCompletionResponseHandler only supports streaming requests";

        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var googleVertexAiProcessor = new GoogleVertexAiUnifiedStreamingProcessor(
            (m, e) -> buildMidStreamChatCompletionError(request.getInferenceEntityId(), m, e)
        );

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(googleVertexAiProcessor);
        return new StreamingUnifiedChatCompletionResults(googleVertexAiProcessor);
    }

    @Override
    protected UnifiedChatCompletionException buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        return buildChatCompletionError(message, request, result, errorResponse, GoogleVertexAiErrorResponse.class);
    }

    @Override
    protected UnifiedChatCompletionException buildProviderSpecificChatCompletionError(
        ErrorResponse errorResponse,
        String errorMessage,
        RestStatus restStatus
    ) {
        var vertexAIErrorResponse = (GoogleVertexAiErrorResponse) errorResponse;
        return new UnifiedChatCompletionException(
            restStatus,
            errorMessage,
            vertexAIErrorResponse.status(),
            String.valueOf(vertexAIErrorResponse.code()),
            null
        );
    }

    @Override
    public UnifiedChatCompletionException buildMidStreamChatCompletionError(String inferenceEntityId, String message, Exception e) {
        return buildMidStreamChatCompletionError(inferenceEntityId, message, e, GoogleVertexAiErrorResponse.class);
    }

    @Override
    protected UnifiedChatCompletionException buildProviderSpecificMidStreamChatCompletionError(
        String inferenceEntityId,
        ErrorResponse errorResponse
    ) {
        var vertexAIErrorResponse = (GoogleVertexAiErrorResponse) errorResponse;
        return new UnifiedChatCompletionException(
            RestStatus.INTERNAL_SERVER_ERROR,
            format(
                "%s for request from inference entity id [%s]. Error message: [%s]",
                SERVER_ERROR_OBJECT,
                inferenceEntityId,
                errorResponse.getErrorMessage()
            ),
            vertexAIErrorResponse.status(),
            String.valueOf(vertexAIErrorResponse.code()),
            null
        );
    }

    @Override
    protected ErrorResponse extractMidStreamChatCompletionErrorResponse(String message) {
        return GoogleVertexAiErrorResponse.fromString(message);
    }

    private static class GoogleVertexAiErrorResponse extends ErrorResponse {
        private static final ConstructingObjectParser<Optional<ErrorResponse>, Void> ERROR_PARSER = new ConstructingObjectParser<>(
            "google_vertex_ai_error_wrapper",
            true,
            args -> Optional.ofNullable((GoogleVertexAiErrorResponse) args[0])
        );

        private static final ConstructingObjectParser<GoogleVertexAiErrorResponse, Void> ERROR_BODY_PARSER = new ConstructingObjectParser<>(
            "google_vertex_ai_error_body",
            true,
            args -> new GoogleVertexAiErrorResponse((Integer) args[0], (String) args[1], (String) args[2])
        );

        static {
            ERROR_BODY_PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ERROR_CODE_FIELD));
            ERROR_BODY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(ERROR_MESSAGE_FIELD));
            ERROR_BODY_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ERROR_STATUS_FIELD));

            ERROR_PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                ERROR_BODY_PARSER,
                null,
                new ParseField(ERROR_FIELD)
            );
        }

        static ErrorResponse fromResponse(HttpResult response) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, response.body())
            ) {
                return ERROR_PARSER.apply(parser, null).orElse(ErrorResponse.UNDEFINED_ERROR);
            } catch (Exception e) {
                var resultAsString = new String(response.body(), StandardCharsets.UTF_8);
                return new ErrorResponse(Strings.format("Unable to parse the Google Vertex AI error, response body: [%s]", resultAsString));
            }
        }

        static ErrorResponse fromString(String response) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, response)
            ) {
                return ERROR_PARSER.apply(parser, null).orElse(ErrorResponse.UNDEFINED_ERROR);
            } catch (Exception e) {
                return new ErrorResponse(Strings.format("Unable to parse the Google Vertex AI error, response body: [%s]", response));
            }
        }

        private final int code;
        @Nullable
        private final String status;

        GoogleVertexAiErrorResponse(Integer code, String errorMessage, @Nullable String status) {
            super(Objects.requireNonNull(errorMessage));
            this.code = code == null ? 0 : code;
            this.status = status;
        }

        public int code() {
            return code;
        }

        @Nullable
        public String status() {
            return status != null ? status : "google_vertex_ai_error";
        }
    }
}
