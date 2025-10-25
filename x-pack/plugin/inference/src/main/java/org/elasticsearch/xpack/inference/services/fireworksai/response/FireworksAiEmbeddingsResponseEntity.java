/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.response;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Parses the FireworksAI embeddings API response.
 * FireworksAI uses an OpenAI-compatible embeddings API format.
 *
 * Example response:
 * <pre>
 * <code>
 * {
 *   "object": "list",
 *   "data": [
 *     {
 *       "object": "embedding",
 *       "embedding": [0.1, 0.2, 0.3, ...],
 *       "index": 0
 *     }
 *   ],
 *   "model": "fireworks/qwen3-embedding-8b",
 *   "usage": {
 *     "prompt_tokens": 10,
 *     "total_tokens": 10
 *   }
 * }
 * </code>
 * </pre>
 */
public class FireworksAiEmbeddingsResponseEntity {

    /**
     * Parses the FireworksAI embeddings response and converts it to DenseEmbeddingFloatResults.
     *
     * @param request  the original request
     * @param response the HTTP response from FireworksAI API
     * @return parsed embeddings as DenseEmbeddingFloatResults
     * @throws IOException if parsing fails
     */
    public static DenseEmbeddingFloatResults fromResponse(Request request, HttpResult response) throws IOException {
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body())) {
            return EmbeddingFloatResult.PARSER.apply(p, null).toDenseEmbeddingFloatResults();
        }
    }

    public record EmbeddingFloatResult(List<EmbeddingFloatResultEntry> embeddingResults) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingFloatResult, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingFloatResult.class.getSimpleName(),
            true,
            args -> new EmbeddingFloatResult((List<EmbeddingFloatResultEntry>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), EmbeddingFloatResultEntry.PARSER::apply, new ParseField("data"));
        }

        public DenseEmbeddingFloatResults toDenseEmbeddingFloatResults() {
            return new DenseEmbeddingFloatResults(
                embeddingResults.stream().map(entry -> DenseEmbeddingFloatResults.Embedding.of(entry.embedding)).toList()
            );
        }
    }

    public record EmbeddingFloatResultEntry(List<Float> embedding) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingFloatResultEntry, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingFloatResultEntry.class.getSimpleName(),
            true,
            args -> new EmbeddingFloatResultEntry((List<Float>) args[0])
        );

        static {
            PARSER.declareFloatArray(constructorArg(), new ParseField("embedding"));
        }
    }

    private FireworksAiEmbeddingsResponseEntity() {}
}
