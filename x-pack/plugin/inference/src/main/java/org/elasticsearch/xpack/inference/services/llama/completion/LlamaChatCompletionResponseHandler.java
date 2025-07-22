/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.llama.response.LlamaErrorResponse;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.Optional;

/**
 * Handles streaming chat completion responses and error parsing for Llama inference endpoints.
 * This handler is designed to work with the unified Llama chat completion API.
 */
public class LlamaChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String LLAMA_ERROR = "llama_error";
    private static final UnifiedChatCompletionErrorParserContract LLAMA_STREAM_ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithObjectParser(StreamingLlamaErrorResponseEntity.ERROR_PARSER);

    /**
     * Constructor for creating a LlamaChatCompletionResponseHandler with specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public LlamaChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, LlamaErrorResponse::fromResponse, LLAMA_STREAM_ERROR_PARSER);
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
    private static class StreamingLlamaErrorResponseEntity extends UnifiedChatCompletionErrorResponse {
        private static final ConstructingObjectParser<Optional<UnifiedChatCompletionErrorResponse>, Void> ERROR_PARSER =
            new ConstructingObjectParser<>(LLAMA_ERROR, true, args -> Optional.ofNullable((StreamingLlamaErrorResponseEntity) args[0]));
        private static final ConstructingObjectParser<StreamingLlamaErrorResponseEntity, Void> ERROR_BODY_PARSER =
            new ConstructingObjectParser<>(
                LLAMA_ERROR,
                true,
                args -> new StreamingLlamaErrorResponseEntity(args[0] != null ? (String) args[0] : "unknown")
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
         * Constructs a StreamingLlamaErrorResponseEntity with the specified error message.
         *
         * @param errorMessage the error message to include in the response entity
         */
        StreamingLlamaErrorResponseEntity(String errorMessage) {
            super(errorMessage, LLAMA_ERROR, null, null);
        }
    }
}
