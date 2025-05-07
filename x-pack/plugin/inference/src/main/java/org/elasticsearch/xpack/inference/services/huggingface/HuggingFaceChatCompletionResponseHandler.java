/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.Locale;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

/**
 * Handles streaming chat completion responses and error parsing for Hugging Face inference endpoints.
 * Adapts the OpenAI handler to support Hugging Face's simpler error schema with fields like "message" and "http_status_code".
 */
public class HuggingFaceChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String HUGGING_FACE_ERROR = "hugging_face_error";

    public HuggingFaceChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, HuggingFaceErrorResponseEntity::fromResponse);
    }

    @Override
    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        if (request.isStreaming()) {
            var errorMessage = errorMessage(message, request, result, errorResponse, responseStatusCode);
            var restStatus = toRestStatus(responseStatusCode);
            return errorResponse instanceof HuggingFaceErrorResponseEntity
                ? new UnifiedChatCompletionException(
                    restStatus,
                    errorMessage,
                    HUGGING_FACE_ERROR,
                    restStatus.name().toLowerCase(Locale.ROOT)
                )
                : new UnifiedChatCompletionException(
                    restStatus,
                    errorMessage,
                    createErrorType(errorResponse),
                    restStatus.name().toLowerCase(Locale.ROOT)
                );
        } else {
            return super.buildError(message, request, result, errorResponse);
        }
    }

    @Override
    protected Exception buildMidStreamError(Request request, String message, Exception e) {
        var errorResponse = StreamingHuggingFaceErrorResponseEntity.fromString(message);
        if (errorResponse instanceof StreamingHuggingFaceErrorResponseEntity streamingHuggingFaceErrorResponseEntity) {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format(
                    "%s for request from inference entity id [%s]. Error message: [%s]",
                    SERVER_ERROR_OBJECT,
                    request.getInferenceEntityId(),
                    errorResponse.getErrorMessage()
                ),
                HUGGING_FACE_ERROR,
                extractErrorCode(streamingHuggingFaceErrorResponseEntity)
            );
        } else if (e != null) {
            return UnifiedChatCompletionException.fromThrowable(e);
        } else {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format("%s for request from inference entity id [%s]", SERVER_ERROR_OBJECT, request.getInferenceEntityId()),
                createErrorType(errorResponse),
                "stream_error"
            );
        }
    }

    private static String extractErrorCode(StreamingHuggingFaceErrorResponseEntity streamingHuggingFaceErrorResponseEntity) {
        return streamingHuggingFaceErrorResponseEntity.httpStatusCode() != null
            ? String.valueOf(streamingHuggingFaceErrorResponseEntity.httpStatusCode())
            : null;
    }

    /**
     * Represents a structured error response specifically for streaming operations
     * using HuggingFace APIs. This is separate from non-streaming error responses,
     * which are handled by {@link HuggingFaceErrorResponseEntity}.
     * An example error response for failed field validation for streaming operation would look like
     * <code>
     *     {
     *       "error": "Input validation error: cannot compile regex from schema",
     *       "http_status_code": 422
     *     }
     * </code>
     */
    private static class StreamingHuggingFaceErrorResponseEntity extends ErrorResponse {
        private static final ConstructingObjectParser<Optional<ErrorResponse>, Void> ERROR_PARSER = new ConstructingObjectParser<>(
            HUGGING_FACE_ERROR,
            true,
            args -> Optional.ofNullable((StreamingHuggingFaceErrorResponseEntity) args[0])
        );
        private static final ConstructingObjectParser<StreamingHuggingFaceErrorResponseEntity, Void> ERROR_BODY_PARSER =
            new ConstructingObjectParser<>(
                HUGGING_FACE_ERROR,
                true,
                args -> new StreamingHuggingFaceErrorResponseEntity(args[0] != null ? (String) args[0] : "unknown", (Integer) args[1])
            );

        static {
            ERROR_BODY_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("message"));
            ERROR_BODY_PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField("http_status_code"));

            ERROR_PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                ERROR_BODY_PARSER,
                null,
                new ParseField("error")
            );
        }

        /**
         * Parses a streaming HuggingFace error response from a JSON string.
         *
         * @param response the raw JSON string representing an error
         * @return a parsed {@link ErrorResponse} or {@link ErrorResponse#UNDEFINED_ERROR} if parsing fails
         */
        private static ErrorResponse fromString(String response) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, response)
            ) {
                return ERROR_PARSER.apply(parser, null).orElse(ErrorResponse.UNDEFINED_ERROR);
            } catch (Exception e) {
                // swallow the error
            }

            return ErrorResponse.UNDEFINED_ERROR;
        }

        @Nullable
        private final Integer httpStatusCode;

        StreamingHuggingFaceErrorResponseEntity(String errorMessage, @Nullable Integer httpStatusCode) {
            super(errorMessage);
            this.httpStatusCode = httpStatusCode;
        }

        @Nullable
        public Integer httpStatusCode() {
            return httpStatusCode;
        }

    }
}
