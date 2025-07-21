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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ChatCompletionErrorResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.googlevertexai.response.GoogleVertexAiCompletionResponseEntity;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.Flow;

public class GoogleVertexAiUnifiedChatCompletionResponseHandler extends GoogleVertexAiResponseHandler {

    private static final String ERROR_FIELD = "error";
    private static final String ERROR_CODE_FIELD = "code";
    private static final String ERROR_MESSAGE_FIELD = "message";
    private static final String ERROR_STATUS_FIELD = "status";
    private static final GoogleVertexAiErrorParser ERROR_PARSER = new GoogleVertexAiErrorParser();

    private final ChatCompletionErrorResponseHandler chatCompletionErrorResponseHandler;

    public GoogleVertexAiUnifiedChatCompletionResponseHandler(String requestType) {
        super(requestType, GoogleVertexAiCompletionResponseEntity::fromResponse, GoogleVertexAiErrorResponse::fromResponse, true);
        this.chatCompletionErrorResponseHandler = new ChatCompletionErrorResponseHandler(ERROR_PARSER);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        assert request.isStreaming() : "GoogleVertexAiUnifiedChatCompletionResponseHandler only supports streaming requests";

        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var googleVertexAiProcessor = new GoogleVertexAiUnifiedStreamingProcessor(
            (m, e) -> chatCompletionErrorResponseHandler.buildMidStreamChatCompletionError(request.getInferenceEntityId(), m, e)
        );

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(googleVertexAiProcessor);
        return new StreamingUnifiedChatCompletionResults(googleVertexAiProcessor);
    }

    @Override
    protected UnifiedChatCompletionException buildError(String message, Request request, HttpResult result) {
        return chatCompletionErrorResponseHandler.buildChatCompletionError(message, request, result);
    }

    @Override
    protected void checkForErrorObject(Request request, HttpResult result) {
        chatCompletionErrorResponseHandler.checkForErrorObject(request, result);
    }

    private static class GoogleVertexAiErrorParser implements UnifiedChatCompletionErrorParser {

        @Override
        public UnifiedChatCompletionErrorResponse parse(HttpResult result) {
            return GoogleVertexAiErrorResponse.fromResponse(result);
        }

        @Override
        public UnifiedChatCompletionErrorResponse parse(String response) {
            return GoogleVertexAiErrorResponse.fromString(response);
        }
    }

    public static class GoogleVertexAiErrorResponse extends UnifiedChatCompletionErrorResponse {
        private static final ConstructingObjectParser<Optional<UnifiedChatCompletionErrorResponse>, Void> ERROR_PARSER =
            new ConstructingObjectParser<>(
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

        public static UnifiedChatCompletionErrorResponse fromResponse(HttpResult response) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, response.body())
            ) {
                return ERROR_PARSER.apply(parser, null).orElse(UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR);
            } catch (Exception e) {
                var resultAsString = new String(response.body(), StandardCharsets.UTF_8);
                return new GoogleVertexAiErrorResponse(
                    Strings.format("Unable to parse the Google Vertex AI error, response body: [%s]", resultAsString)
                );
            }
        }

        static UnifiedChatCompletionErrorResponse fromString(String response) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, response)
            ) {
                return ERROR_PARSER.apply(parser, null).orElse(UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR);
            } catch (Exception e) {
                return new GoogleVertexAiErrorResponse(
                    Strings.format("Unable to parse the Google Vertex AI error, response body: [%s]", response)
                );
            }
        }

        GoogleVertexAiErrorResponse(@Nullable Integer code, String errorMessage, @Nullable String status) {
            super(errorMessage, status != null ? status : "google_vertex_ai_error", code == null ? "0" : String.valueOf(code), null);
        }

        GoogleVertexAiErrorResponse(String errorMessage) {
            super(errorMessage, "google_vertex_ai_error", null, null);
        }
    }
}
