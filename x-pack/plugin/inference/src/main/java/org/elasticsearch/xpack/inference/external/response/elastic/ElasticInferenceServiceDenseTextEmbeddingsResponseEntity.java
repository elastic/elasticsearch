/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.elastic;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ElasticInferenceServiceDenseTextEmbeddingsResponseEntity {

    /**
     * Parses the Elastic Inference Service Dense Text Embeddings response.
     *
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": ["Embed this text", "Embed this text, too"]
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *             "data": [
     *                  [
     *                      2.1259406,
     *                      1.7073475,
     *                      0.9020516
     *                  ],
     *                  (...)
     *             ],
     *             "meta": {
     *                  "usage": {...}
     *             }
     *         }
     *     </code>
     * </pre>
     */
    public static TextEmbeddingFloatResults fromResponse(Request request, HttpResult response) throws IOException {
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body())) {
            return EmbeddingFloatResult.PARSER.apply(p, null).toTextEmbeddingFloatResults();
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
            // Custom field declaration to handle array of arrays format
            PARSER.declareField(
                constructorArg(),
                (parser, context) -> {
                    return XContentParserUtils.parseList(parser, (p, index) -> {
                        List<Float> embedding = XContentParserUtils.parseList(p, (innerParser, innerIndex) -> innerParser.floatValue());
                        return EmbeddingFloatResultEntry.fromFloatArray(embedding);
                    });
                },
                new ParseField("data"),
                org.elasticsearch.xcontent.ObjectParser.ValueType.OBJECT_ARRAY
            );
        }

        public TextEmbeddingFloatResults toTextEmbeddingFloatResults() {
            return new TextEmbeddingFloatResults(
                embeddingResults.stream().map(entry -> TextEmbeddingFloatResults.Embedding.of(entry.embedding)).toList()
            );
        }
    }

    /**
     * Represents a single embedding entry in the response.
     * For the Elastic Inference Service, each entry is just an array of floats (no wrapper object).
     * This is a simpler wrapper that just holds the float array.
     */
    public record EmbeddingFloatResultEntry(List<Float> embedding) {
        public static EmbeddingFloatResultEntry fromFloatArray(List<Float> floats) {
            return new EmbeddingFloatResultEntry(floats);
        }
    }

    private ElasticInferenceServiceDenseTextEmbeddingsResponseEntity() {}
}
