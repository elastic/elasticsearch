/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.Optional;

/**
 * Handles streaming chat completion responses and error parsing for Hugging Face inference endpoints.
 * Adapts the OpenAI handler to support Hugging Face's simpler error schema with fields like "message" and "http_status_code".
 */
public class HuggingFaceChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String HUGGING_FACE_ERROR = "hugging_face_error";
    private static final UnifiedChatCompletionErrorParserContract HUGGING_FACE_ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithObjectParser(StreamingHuggingFaceErrorResponseEntity.ERROR_PARSER);

    public HuggingFaceChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, HuggingFaceErrorResponseEntity::fromResponse, HUGGING_FACE_ERROR_PARSER);
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
    private static class StreamingHuggingFaceErrorResponseEntity extends UnifiedChatCompletionErrorResponse {
        private static final ConstructingObjectParser<Optional<UnifiedChatCompletionErrorResponse>, Void> ERROR_PARSER =
            new ConstructingObjectParser<>(
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

        StreamingHuggingFaceErrorResponseEntity(String errorMessage, @Nullable Integer httpStatusCode) {
            super(errorMessage, HUGGING_FACE_ERROR, httpStatusCode != null ? String.valueOf(httpStatusCode) : null, null);
        }
    }
}
