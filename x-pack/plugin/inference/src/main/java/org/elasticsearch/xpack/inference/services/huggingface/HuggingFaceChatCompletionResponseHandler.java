/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
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
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Flow;

import static org.elasticsearch.core.Strings.format;

/**
 * Handles streaming chat completion responses and error parsing for Hugging Face inference endpoints.
 * Adapts the OpenAI handler to support Hugging Face's simpler error schema with fields like "message" and "http_status_code".
 */
public class HuggingFaceChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        return super.parseResult(request, flow);
    }

    public HuggingFaceChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, HuggingFaceErrorResponse::fromResponse);
    }

    @Override
    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        if (request.isStreaming()) {
            var errorMessage = errorMessage(message, request, result, errorResponse, responseStatusCode);
            var restStatus = toRestStatus(responseStatusCode);
            return errorResponse instanceof HuggingFaceErrorResponse huggingFaceErrorResponse
                ? new UnifiedChatCompletionException(
                    restStatus,
                    errorMessage,
                    createErrorType(errorResponse),
                    extractErrorCode(huggingFaceErrorResponse)
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
        var errorResponse = HuggingFaceErrorResponse.fromString(message);
        if (errorResponse instanceof HuggingFaceErrorResponse huggingFaceErrorResponse) {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format(
                    "%s for request from inference entity id [%s]. Error message: [%s]",
                    SERVER_ERROR_OBJECT,
                    request.getInferenceEntityId(),
                    errorResponse.getErrorMessage()
                ),
                createErrorType(errorResponse),
                extractErrorCode(huggingFaceErrorResponse)
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

    private static String extractErrorCode(HuggingFaceErrorResponse huggingFaceErrorResponse) {
        return huggingFaceErrorResponse.httpStatusCode() != null ? String.valueOf(huggingFaceErrorResponse.httpStatusCode()) : null;
    }

    private static class HuggingFaceErrorResponse extends ErrorResponse {
        private static final ConstructingObjectParser<Optional<ErrorResponse>, Void> ERROR_PARSER = new ConstructingObjectParser<>(
            "hugging_face_error",
            true,
            args -> Optional.ofNullable((HuggingFaceErrorResponse) args[0])
        );
        private static final ConstructingObjectParser<HuggingFaceErrorResponse, Void> ERROR_BODY_PARSER = new ConstructingObjectParser<>(
            "hugging_face_error",
            true,
            args -> new HuggingFaceErrorResponse((String) args[0], (Integer) args[1])
        );

        static {
            ERROR_BODY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("message"));
            ERROR_BODY_PARSER.declareIntOrNull(ConstructingObjectParser.optionalConstructorArg(), -1, new ParseField("http_status_code"));

            ERROR_PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                ERROR_BODY_PARSER,
                null,
                new ParseField("error")
            );
        }

        private static ErrorResponse fromResponse(HttpResult response) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, response.body())
            ) {
                return ERROR_PARSER.apply(parser, null).orElse(ErrorResponse.UNDEFINED_ERROR);
            } catch (Exception e) {
                // swallow the error
            }

            return ErrorResponse.UNDEFINED_ERROR;
        }

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

        HuggingFaceErrorResponse(String errorMessage, @Nullable Integer httpStatusCode) {
            super(errorMessage);
            this.httpStatusCode = httpStatusCode;
        }

        @Nullable
        public Integer httpStatusCode() {
            return httpStatusCode;
        }

    }
}
