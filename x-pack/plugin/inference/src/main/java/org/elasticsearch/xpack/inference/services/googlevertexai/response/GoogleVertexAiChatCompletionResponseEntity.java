/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.response;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GoogleVertexAiChatCompletionResponseEntity {

    private static final ParseField CANDIDATES = new ParseField("candidates");
    private static final ParseField CONTENT = new ParseField("content");
    private static final ParseField PARTS = new ParseField("parts");
    private static final ParseField TEXT = new ParseField("text");

    /**
     * Parses the Google Vertex AI chat completion response.
     * For a request like
     *
     * <pre>
     *     <code>
     *{
     *   "contents": [
     *     {
     *       "role": "user",
     *       "parts": [
     *         {
     *           "text": "Hello!"
     *         }
     *       ]
     *     }
     *   ],
     *   "generationConfig": {
     *     "responseModalities": [
     *       "TEXT"
     *     ],
     *     "temperature": 1,
     *     "maxOutputTokens": 8192,
     *     "topP": 0.95
     *   }
     * }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *[{
     *   "candidates": [
     *     {
     *       "content": {
     *         "role": "model",
     *         "parts": [
     *           {
     *             "text": "Hello there! How"
     *           }
     *         ]
     *       }
     *     }
     *   ],
     *   "usageMetadata": {
     *     "trafficType": "ON_DEMAND"
     *   },
     *   "modelVersion": "gemini-2.0-flash-001",
     *   "createTime": "2025-04-29T16:55:36.576032Z",
     *   "responseId": "iAQRaKCUI_D7ld8Pq-aaaaa"
     * }
     * ,
     * {
     *   "candidates": [
     *     {
     *       "content": {
     *         "role": "model",
     *         "parts": [
     *           {
     *             "text": " can I help you today?\n"
     *           }
     *         ]
     *       },
     *       "finishReason": "STOP"
     *     }
     *   ],
     *   "usageMetadata": {
     *     "promptTokenCount": 2,
     *     "candidatesTokenCount": 11,
     *     "totalTokenCount": 13,
     *     "trafficType": "ON_DEMAND",
     *     "promptTokensDetails": [
     *       {
     *         "modality": "TEXT",
     *         "tokenCount": 2
     *       }
     *     ],
     *     "candidatesTokensDetails": [
     *       {
     *         "modality": "TEXT",
     *         "tokenCount": 11
     *       }
     *     ]
     *   },
     *   "modelVersion": "gemini-2.0-flash-001",
     *   "createTime": "2025-04-29T16:55:36.576032Z",
     *   "responseId": "iAQRaKCUI_D7ld8Pq-aaaaa"
     * }
     * ]
     *     </code>
     * </pre>
     */
    public static ChatCompletionResults fromResponse(Request request, HttpResult response) throws IOException {
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);

            StringBuilder fullText = new StringBuilder();

            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                Chunk chunk = Chunk.PARSER.apply(parser, null);
                chunk.extractText().ifPresent(fullText::append);
            }

            return new ChatCompletionResults(List.of(new ChatCompletionResults.Result(fullText.toString())));
        }
    }

    // --- Nested Records for Parsing ---

    public record Chunk(List<Candidate> candidates) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Chunk, Void> PARSER = new ConstructingObjectParser<>(
            Chunk.class.getSimpleName(),
            true, // Ignore unknown fields in the chunk object
            args -> new Chunk((List<Candidate>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), Candidate.PARSER::apply, CANDIDATES);
        }

        public Optional<String> extractText() {
            return Optional.ofNullable(candidates)
                .filter(list -> list.isEmpty() == false)
                .map(List::getFirst)
                .flatMap(Candidate::extractText);
        }
    }

    public record Candidate(Content content) {
        public static final ConstructingObjectParser<Candidate, Void> PARSER = new ConstructingObjectParser<>(
            Candidate.class.getSimpleName(),
            true,
            args -> new Candidate((Content) args[0])
        );

        static {
            PARSER.declareObject(constructorArg(), Content.PARSER::apply, CONTENT);
        }

        public Optional<String> extractText() {
            return Optional.ofNullable(content).flatMap(Content::extractText);
        }
    }

    public record Content(List<Part> parts) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Content, Void> PARSER = new ConstructingObjectParser<>(
            Content.class.getSimpleName(),
            true,
            args -> new Content((List<Part>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), Part.PARSER::apply, PARTS);
        }

        public Optional<String> extractText() {
            return Optional.ofNullable(parts).filter(list -> list.isEmpty() == false).map(List::getFirst).map(Part::text);
        }
    }

    public record Part(String text) {
        public static final ConstructingObjectParser<Part, Void> PARSER = new ConstructingObjectParser<>(
            Part.class.getSimpleName(),
            true,
            args -> new Part((String) args[0])
        );

        static {
            PARSER.declareString(optionalConstructorArg(), TEXT);
        }
    }
}
