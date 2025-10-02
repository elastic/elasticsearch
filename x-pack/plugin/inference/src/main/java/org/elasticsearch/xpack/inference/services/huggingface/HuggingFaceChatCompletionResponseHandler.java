/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.io.IOException;
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
            new ConstructingObjectParser<>(HUGGING_FACE_ERROR, true, args -> {
                if (args[0] == null) {
                    return Optional.empty();
                }

                return Optional.of(new StreamingHuggingFaceErrorResponseEntity((ErrorField) args[0]));
            });

        static {
            ERROR_PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> parseErrorField(p),
                new ParseField("error"),
                // The expected value is an object, string, or null, using this value type to allow that combination
                // We'll check the current token in the called function to ensure it is only an object, string, or null
                ObjectParser.ValueType.VALUE_OBJECT_ARRAY
            );
        }

        StreamingHuggingFaceErrorResponseEntity(ErrorField errorField) {
            super(
                errorField.message,
                HUGGING_FACE_ERROR,
                errorField.httpStatusCode != null ? String.valueOf(errorField.httpStatusCode) : null,
                null
            );
        }
    }

    private static ErrorField parseErrorField(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return ErrorField.parseString(parser);
        } else if (token == XContentParser.Token.START_OBJECT) {
            return ErrorField.parseObject(parser);
        } else if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }

        throw new XContentParseException("Unexpected token: " + token);
    }

    private record ErrorField(String message, @Nullable Integer httpStatusCode) {
        private static final ConstructingObjectParser<ErrorField, Void> PARSER = new ConstructingObjectParser<>(
            ErrorField.class.getSimpleName(),
            true,
            args -> new ErrorField(args[0] != null ? (String) args[0] : "unknown", (Integer) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("message"));
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField("http_status_code"));
        }

        public static ErrorField parseObject(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public static ErrorField parseString(XContentParser parser) throws IOException {
            return new ErrorField(parser.text(), null);
        }
    }

}
