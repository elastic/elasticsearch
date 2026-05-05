/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class OpenAiEmbeddingsResponseEntity {
    /**
     * Parses the OpenAI json response.
     * For a request like:
     *
     * <pre>
     *     <code>
     *        {
     *            "inputs": ["hello this is my name", "I wish I was there!"]
     *        }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     * <code>
     * {
     *  "object": "list",
     *  "data": [
     *      {
     *          "object": "embedding",
     *          "embedding": [
     *              -0.009327292,
     *              .... (1536 floats total for ada-002)
     *              -0.0028842222,
     *          ],
     *          "index": 0
     *      },
     *      {
     *          "object": "embedding",
     *          "embedding": [ ... ],
     *          "index": 1
     *      }
     *  ],
     *  "model": "text-embedding-ada-002",
     *  "usage": {
     *      "prompt_tokens": 8,
     *      "total_tokens": 8
     *  }
     * }
     * </code>
     * </pre>
     */
    public static EmbeddingFloatResults fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body())) {
            var result = EmbeddingFloatResult.PARSER.apply(p, null);
            if (outboundRequest.getTaskType().equals(TaskType.TEXT_EMBEDDING)) {
                return result.toDenseEmbeddingFloatResults();
            } else {
                return result.toGenericDenseEmbeddingFloatResults();
            }
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
                embeddingResults.stream().map(entry -> EmbeddingFloatResults.Embedding.of(entry.embedding)).toList()
            );
        }

        public GenericDenseEmbeddingFloatResults toGenericDenseEmbeddingFloatResults() {
            return new GenericDenseEmbeddingFloatResults(
                embeddingResults.stream().map(entry -> EmbeddingFloatResults.Embedding.of(entry.embedding)).toList()
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

    private OpenAiEmbeddingsResponseEntity() {}
}
