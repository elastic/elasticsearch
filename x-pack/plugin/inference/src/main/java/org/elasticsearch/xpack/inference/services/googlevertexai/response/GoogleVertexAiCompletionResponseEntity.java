/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.response;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiUnifiedStreamingProcessor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public class GoogleVertexAiCompletionResponseEntity {
    /**
     * Parses the response from Google Vertex AI's generateContent endpoint
     * For a request like:
     * <pre>
     *     <code>
     *         {
     *             "inputs": "Please summarize this text: some text"
     *         }
     *     </code>
     * </pre>
     *
     * The response is a <a href="https://cloud.google.com/vertex-ai/docs/reference/rest/v1beta1/GenerateContentResponse">GenerateContentResponse</a> objects that looks like:
     *
     * <pre>
     *     <code>
     *
     * {
     *   "candidates": [
     *     {
     *       "content": {
     *         "role": "model",
     *         "parts": [
     *           {
     *             "text": "I am sorry, I cannot summarize the text because I do not have access to the text you are referring to."
     *           }
     *         ]
     *       },
     *       "finishReason": "STOP",
     *       "avgLogprobs": -0.19326641248620074
     *     }
     *   ],
     *   "usageMetadata": {
     *     "promptTokenCount": 71,
     *     "candidatesTokenCount": 23,
     *     "totalTokenCount": 94,
     *     "trafficType": "ON_DEMAND",
     *     "promptTokensDetails": [
     *       {
     *         "modality": "TEXT",
     *         "tokenCount": 71
     *       }
     *     ],
     *     "candidatesTokensDetails": [
     *       {
     *         "modality": "TEXT",
     *         "tokenCount": 23
     *       }
     *     ]
     *   },
     *   "modelVersion": "gemini-2.0-flash-001",
     *   "createTime": "2025-05-28T15:08:20.049493Z",
     *   "responseId": "5CY3aNWCA6mm4_UPr-zduAE"
     * }
     *    </code>
     * </pre>
     *
     * @param request The original request made to the service.
     **/
    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        var responseJson = new String(response.body(), StandardCharsets.UTF_8);

        // Response from generateContent has the same shape as streamGenerateContent. We reuse the already implemented
        // class to avoid code duplication

        StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk;
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, responseJson)
        ) {
            moveToFirstToken(parser);
            chunk = GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser.parse(parser);
        }
        var results = chunk.choices().stream().map(choice -> choice.delta().content()).map(ChatCompletionResults.Result::new).toList();

        return new ChatCompletionResults(results);
    }
}
