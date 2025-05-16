/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import org.elasticsearch.xpack.inference.external.response.streaming.JsonArrayPartsEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.JsonArrayPartsEventProcessor;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;

import static org.elasticsearch.core.Strings.format;

public class GoogleVertexAiUnifiedChatCompletionResponseHandler extends GoogleVertexAiResponseHandler {

    private static final String ERROR_FIELD = "error";
    private static final String ERROR_CODE_FIELD = "code";
    private static final String ERROR_MESSAGE_FIELD = "message";
    private static final String ERROR_STATUS_FIELD = "status";

    public GoogleVertexAiUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, GoogleVertexAiErrorResponse::fromResponse, true);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        assert request.isStreaming() : "GoogleVertexAiUnifiedChatCompletionResponseHandler only supports streaming requests";

        var serverSentEventProcessor = new JsonArrayPartsEventProcessor(new JsonArrayPartsEventParser());
        var googleVertexAiProcessor = new GoogleVertexAiUnifiedStreamingProcessor((m, e) -> buildMidStreamError(request, m, e));

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(googleVertexAiProcessor);
        return new StreamingUnifiedChatCompletionResults(googleVertexAiProcessor);
    }

    @Override
    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        var errorMessage = errorMessage(message, request, result, errorResponse, responseStatusCode);
        var restStatus = toRestStatus(responseStatusCode);

        return errorResponse instanceof GoogleVertexAiErrorResponse vertexAIErrorResponse
            ? new UnifiedChatCompletionException(
                restStatus,
                errorMessage,
                vertexAIErrorResponse.status(),
                String.valueOf(vertexAIErrorResponse.code()),
                null
            )
            : new UnifiedChatCompletionException(
                restStatus,
                errorMessage,
                errorResponse != null ? errorResponse.getClass().getSimpleName() : "unknown",
                restStatus.name().toLowerCase(Locale.ROOT)
            );
    }

    private static Exception buildMidStreamError(Request request, String message, Exception e) {
        var errorResponse = GoogleVertexAiErrorResponse.fromString(message);
        if (errorResponse instanceof GoogleVertexAiErrorResponse gver) {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format(
                    "%s for request from inference entity id [%s]. Error message: [%s]",
                    SERVER_ERROR_OBJECT,
                    request.getInferenceEntityId(),
                    errorResponse.getErrorMessage()
                ),
                gver.status(),
                String.valueOf(gver.code()),
                null
            );
        } else if (e != null) {
            return UnifiedChatCompletionException.fromThrowable(e);
        } else {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format("%s for request from inference entity id [%s]", SERVER_ERROR_OBJECT, request.getInferenceEntityId()),
                errorResponse != null ? errorResponse.getClass().getSimpleName() : "unknown",
                "stream_error"
            );
        }
    }

    private static class GoogleVertexAiErrorResponse extends ErrorResponse {
        private static final Logger logger = LogManager.getLogger(GoogleVertexAiErrorResponse.class);
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
                logger.warn("Failed to parse Google Vertex AI error response body", e);
            }
            return ErrorResponse.UNDEFINED_ERROR;
        }

        static ErrorResponse fromString(String response) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, response)
            ) {
                return ERROR_PARSER.apply(parser, null).orElse(ErrorResponse.UNDEFINED_ERROR);
            } catch (Exception e) {
                logger.warn("Failed to parse Google Vertex AI error string", e);
            }
            return ErrorResponse.UNDEFINED_ERROR;
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
