/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.DequeUtils;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedStreamingProcessor;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStreamSchemaPayload;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;

/**
 * Streaming payloads are expected to be in the exact format of the Elastic API. This does *not* use the Server-Sent Event transport
 * protocol, rather this expects the SageMaker client and the implemented Endpoint to use AWS's transport protocol to deliver entire chunks.
 * Each chunk should be in a valid JSON format, as that is the format the Elastic API uses.
 */
public class ElasticCompletionPayload implements SageMakerStreamSchemaPayload, ElasticPayload {
    private static final XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
        LoggingDeprecationHandler.INSTANCE
    );

    /**
     * {
     *    "completion": [
     *      {
     *          "result": "some result 1"
     *      },
     *      {
     *          "result": "some result 2"
     *      }
     *    ]
     * }
     */
    @Override
    public ChatCompletionResults responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception {
        try (var p = jsonXContent.createParser(XContentParserConfiguration.EMPTY, response.body().asInputStream())) {
            return Completion.PARSER.apply(p, null);
        }
    }

    /**
     * {
     *    "completion": [
     *      {
     *          "delta": "some result 1"
     *      },
     *      {
     *          "delta": "some result 2"
     *      }
     *    ]
     * }
     */
    @Override
    public StreamingChatCompletionResults.Results streamResponseBody(SageMakerModel model, SdkBytes response) throws Exception {
        try (var p = jsonXContent.createParser(XContentParserConfiguration.EMPTY, response.asInputStream())) {
            return StreamCompletion.PARSER.apply(p, null);
        }
    }

    @Override
    public SdkBytes chatCompletionRequestBytes(SageMakerModel model, UnifiedCompletionRequest request) {
        return SdkBytes.fromUtf8String(Strings.toString((builder, params) -> {
            request.toXContent(builder, UnifiedCompletionRequest.withMaxCompletionTokensTokens(params));
            return builder;
        }));
    }

    @Override
    public StreamingUnifiedChatCompletionResults.Results chatCompletionResponseBody(SageMakerModel model, SdkBytes response) {
        var responseData = response.asUtf8String();
        try {
            var results = OpenAiUnifiedStreamingProcessor.parse(parserConfig, responseData)
                .collect(
                    () -> new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(),
                    ArrayDeque::offer,
                    ArrayDeque::addAll
                );
            return new StreamingUnifiedChatCompletionResults.Results(results);
        } catch (Exception e) {
            throw OpenAiUnifiedChatCompletionResponseHandler.buildMidStreamError(model.getInferenceEntityId(), responseData, e);
        }
    }

    private static class Completion {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ChatCompletionResults, Void> PARSER = new ConstructingObjectParser<>(
            ChatCompletionResults.class.getSimpleName(),
            IGNORE_UNKNOWN_FIELDS,
            args -> new ChatCompletionResults((List<ChatCompletionResults.Result>) args[0])
        );
        private static final ConstructingObjectParser<ChatCompletionResults.Result, Void> RESULT_PARSER = new ConstructingObjectParser<>(
            ChatCompletionResults.Result.class.getSimpleName(),
            IGNORE_UNKNOWN_FIELDS,
            args -> new ChatCompletionResults.Result((String) args[0])
        );

        static {
            RESULT_PARSER.declareString(constructorArg(), new ParseField(ChatCompletionResults.Result.RESULT));
            PARSER.declareObjectArray(constructorArg(), RESULT_PARSER::apply, new ParseField(ChatCompletionResults.COMPLETION));
        }
    }

    private static class StreamCompletion {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<StreamingChatCompletionResults.Results, Void> PARSER = new ConstructingObjectParser<>(
            StreamingChatCompletionResults.Results.class.getSimpleName(),
            IGNORE_UNKNOWN_FIELDS,
            args -> new StreamingChatCompletionResults.Results((Deque<StreamingChatCompletionResults.Result>) args[0])
        );
        private static final ConstructingObjectParser<StreamingChatCompletionResults.Result, Void> RESULT_PARSER =
            new ConstructingObjectParser<>(
                StreamingChatCompletionResults.Result.class.getSimpleName(),
                IGNORE_UNKNOWN_FIELDS,
                args -> new StreamingChatCompletionResults.Result((String) args[0])
            );

        static {
            RESULT_PARSER.declareString(constructorArg(), new ParseField(StreamingChatCompletionResults.Result.RESULT));
            PARSER.declareField(constructorArg(), (p, c) -> {
                var currentToken = p.currentToken();

                // ES allows users to send single-value strings instead of an array of one value
                if (currentToken.isValue()
                    || currentToken == XContentParser.Token.VALUE_NULL
                    || currentToken == XContentParser.Token.START_OBJECT) {
                    return DequeUtils.of(RESULT_PARSER.apply(p, c));
                }

                var deque = new ArrayDeque<StreamingChatCompletionResults.Result>();
                XContentParser.Token token;
                while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token.isValue() || token == XContentParser.Token.VALUE_NULL || token == XContentParser.Token.START_OBJECT) {
                        deque.offer(RESULT_PARSER.apply(p, c));
                    } else {
                        throw new IllegalStateException("expected value but got [" + token + "]");
                    }
                }
                return deque;
            }, new ParseField(ChatCompletionResults.COMPLETION), ObjectParser.ValueType.OBJECT_ARRAY);
        }
    }
}
