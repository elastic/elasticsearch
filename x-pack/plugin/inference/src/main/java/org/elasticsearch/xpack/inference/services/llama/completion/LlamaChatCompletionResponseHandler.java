/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

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
import org.elasticsearch.xpack.inference.services.llama.response.LlamaErrorResponse;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.Locale;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

/**
 * Handles streaming chat completion responses and error parsing for Llama inference endpoints.
 * This handler is designed to work with the unified Llama chat completion API.
 */
public class LlamaChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String LLAMA_ERROR = "llama_error";
    private static final String STREAM_ERROR = "stream_error";

    /**
     * Constructor for creating a LlamaChatCompletionResponseHandler with specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public LlamaChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, LlamaErrorResponse::fromResponse);
    }

    /**
     * Constructor for creating a LlamaChatCompletionResponseHandler with specified request type,
     * @param message the error message to include in the exception
     * @param request the request that caused the error
     * @param result the HTTP result containing the response
     * @param errorResponse the error response parsed from the HTTP result
     * @return an exception representing the error, specific to Llama chat completion
     */
    @Override
    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        if (request.isStreaming()) {
            var errorMessage = errorMessage(message, request, result, errorResponse, responseStatusCode);
            var restStatus = toRestStatus(responseStatusCode);
            return errorResponse instanceof LlamaErrorResponse
                ? new UnifiedChatCompletionException(restStatus, errorMessage, LLAMA_ERROR, restStatus.name().toLowerCase(Locale.ROOT))
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

    /**
     * Builds an exception for mid-stream errors encountered during Llama chat completion requests.
     *
     * @param request the request that caused the error
     * @param message the error message
     * @param e the exception that occurred, if any
     * @return a UnifiedChatCompletionException representing the error
     */
    @Override
    protected Exception buildMidStreamError(Request request, String message, Exception e) {
        var errorResponse = StreamingLlamaErrorResponseEntity.fromString(message);
        if (errorResponse instanceof StreamingLlamaErrorResponseEntity) {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format(
                    "%s for request from inference entity id [%s]. Error message: [%s]",
                    SERVER_ERROR_OBJECT,
                    request.getInferenceEntityId(),
                    errorResponse.getErrorMessage()
                ),
                LLAMA_ERROR,
                STREAM_ERROR
            );
        } else if (e != null) {
            return UnifiedChatCompletionException.fromThrowable(e);
        } else {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format("%s for request from inference entity id [%s]", SERVER_ERROR_OBJECT, request.getInferenceEntityId()),
                createErrorType(errorResponse),
                STREAM_ERROR
            );
        }
    }

    /**
     * StreamingLlamaErrorResponseEntity allows creation of {@link ErrorResponse} from a JSON string.
     * This entity is used to parse error responses from streaming Llama requests.
     * For non-streaming requests {@link LlamaErrorResponse} should be used.
     * Example error response for Bad Request error would look like:
     * <pre><code>
     *  {
     *      "error": {
     *          "message": "400: Invalid value: Model 'llama3.12:3b' not found"
     *      }
     *  }
     * </code></pre>
     */
    private static class StreamingLlamaErrorResponseEntity extends ErrorResponse {
        private static final ConstructingObjectParser<Optional<ErrorResponse>, Void> ERROR_PARSER = new ConstructingObjectParser<>(
            LLAMA_ERROR,
            true,
            args -> Optional.ofNullable((LlamaChatCompletionResponseHandler.StreamingLlamaErrorResponseEntity) args[0])
        );
        private static final ConstructingObjectParser<
            LlamaChatCompletionResponseHandler.StreamingLlamaErrorResponseEntity,
            Void> ERROR_BODY_PARSER = new ConstructingObjectParser<>(
                LLAMA_ERROR,
                true,
                args -> new LlamaChatCompletionResponseHandler.StreamingLlamaErrorResponseEntity(
                    args[0] != null ? (String) args[0] : "unknown"
                )
            );

        static {
            ERROR_BODY_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("message"));

            ERROR_PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                ERROR_BODY_PARSER,
                null,
                new ParseField("error")
            );
        }

        /**
         * Parses a streaming Llama error response from a JSON string.
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

        /**
         * Constructs a StreamingLlamaErrorResponseEntity with the specified error message.
         *
         * @param errorMessage the error message to include in the response entity
         */
        StreamingLlamaErrorResponseEntity(String errorMessage) {
            super(errorMessage);
        }
    }
}
