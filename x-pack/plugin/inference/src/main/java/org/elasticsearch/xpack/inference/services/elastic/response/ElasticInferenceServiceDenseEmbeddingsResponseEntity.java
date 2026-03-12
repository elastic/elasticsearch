/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Strings;
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
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceDenseEmbeddingsRequest;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ElasticInferenceServiceDenseEmbeddingsResponseEntity {

    /**
     * Parses the Elastic Inference Service Dense Embeddings response.
     * <br>
     * For a text_embedding request like:
     *
     * <pre>
     *  {
     *      "inputs": ["Embed this text", "Embed this text, too"]
     *  }
     * </pre>
     *
     * or a multimodal embedding request like:
     *
     * <pre>
     *  {
     *      "input": [
     *          {
     *              "content": [{"type": "image", "format": "base64", "value": "image data"}]
     *          },
     *          {
     *              "content": [{"type": "text", "format": "text", "value": "text input"}]
     *          }
     *      ]
     *  }
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *  {
     *      "data": [
     *          [
     *              2.1259406,
     *              1.7073475,
     *              0.9020516
     *          ],
     *          (...)
     *      ],
     *      "meta": {
     *          "usage": {...}
     *      }
     *  }
     * </pre>
     */
    public static EmbeddingFloatResults fromResponse(Request request, HttpResult response) throws IOException {
        if (request instanceof ElasticInferenceServiceDenseEmbeddingsRequest embeddingRequest) {
            try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body())) {
                return EmbeddingFloatResult.PARSER.apply(p, null).toEmbeddingFloatResults(embeddingRequest.getTaskType());
            }
        } else {
            throw new IllegalStateException(
                Strings.format(
                    "Invalid request type [%s]. Expected [%s]",
                    request.getClass().getSimpleName(),
                    ElasticInferenceServiceDenseEmbeddingsRequest.class.getSimpleName()
                )
            );
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
            PARSER.declareField(constructorArg(), (parser, context) -> XContentParserUtils.parseList(parser, (p, index) -> {
                List<Float> embedding = XContentParserUtils.parseList(p, (innerParser, innerIndex) -> innerParser.floatValue());
                return EmbeddingFloatResultEntry.fromFloatList(embedding);
            }), new ParseField("data"), org.elasticsearch.xcontent.ObjectParser.ValueType.OBJECT_ARRAY);
        }

        public EmbeddingFloatResults toEmbeddingFloatResults(TaskType taskType) {
            return switch (taskType) {
                case TEXT_EMBEDDING -> new DenseEmbeddingFloatResults(
                    embeddingResults.stream().map(entry -> EmbeddingFloatResults.Embedding.of(entry.embedding)).toList()
                );
                case EMBEDDING -> new GenericDenseEmbeddingFloatResults(
                    embeddingResults.stream().map(entry -> EmbeddingFloatResults.Embedding.of(entry.embedding)).toList()
                );
                case null, default -> throw new IllegalArgumentException(
                    Strings.format(
                        "Invalid task type [%s]. Must be one of %s",
                        taskType,
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING)
                    )
                );
            };
        }
    }

    /**
     * Represents a single embedding entry in the response.
     * For the Elastic Inference Service, each entry is just a list of floats (no wrapper object).
     * This is a simpler wrapper that just holds the float list.
     */
    public record EmbeddingFloatResultEntry(List<Float> embedding) {
        public static EmbeddingFloatResultEntry fromFloatList(List<Float> floats) {
            return new EmbeddingFloatResultEntry(floats);
        }
    }

    private ElasticInferenceServiceDenseEmbeddingsResponseEntity() {}
}
