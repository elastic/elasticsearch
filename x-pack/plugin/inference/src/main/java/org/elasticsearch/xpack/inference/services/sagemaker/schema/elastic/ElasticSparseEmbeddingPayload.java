/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;

public class ElasticSparseEmbeddingPayload implements ElasticPayload {

    private static final EnumSet<TaskType> SUPPORTED_TASKS = EnumSet.of(TaskType.SPARSE_EMBEDDING);

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SparseEmbeddingResults, Void> PARSER = new ConstructingObjectParser<>(
        SparseEmbeddingResults.class.getSimpleName(),
        IGNORE_UNKNOWN_FIELDS,
        args -> new SparseEmbeddingResults((List<SparseEmbeddingResults.Embedding>) args[0])
    );
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SparseEmbeddingResults.Embedding, Void> EMBEDDINGS_PARSER =
        new ConstructingObjectParser<>(
            SparseEmbeddingResults.class.getSimpleName(),
            IGNORE_UNKNOWN_FIELDS,
            args -> SparseEmbeddingResults.Embedding.create((List<WeightedToken>) args[0], (boolean) args[1])
        );

    static {
        EMBEDDINGS_PARSER.declareObject(
            constructorArg(),
            (p, c) -> p.map(HashMap::new, XContentParser::floatValue)
                .entrySet()
                .stream()
                .map(entry -> new WeightedToken(entry.getKey(), entry.getValue()))
                .toList(),
            new ParseField("embedding")
        );
        EMBEDDINGS_PARSER.declareBoolean(constructorArg(), new ParseField("is_truncated"));
        PARSER.declareObjectArray(constructorArg(), EMBEDDINGS_PARSER::apply, new ParseField("sparse_embedding"));
    }

    @Override
    public EnumSet<TaskType> supportedTasks() {
        return SUPPORTED_TASKS;
    }

    /**
     * Reads sparse embeddings format
     * {
     *      "sparse_embedding" : [
     *          {
     *              "is_truncated" : false,
     *              "embedding" : {
     *                  "token" : 0.1
     *              }
     *          },
     *          {
     *              "is_truncated" : false,
     *              "embedding" : {
     *                  "token2" : 0.2,
     *                  "token3" : 0.3
     *              }
     *          }
     *      ]
     * }
     */
    @Override
    public SparseEmbeddingResults responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception {
        try (var p = jsonXContent.createParser(XContentParserConfiguration.EMPTY, response.body().asInputStream())) {
            return PARSER.apply(p, null);
        }
    }
}
