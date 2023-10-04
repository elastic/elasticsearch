/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// TODO figure out the format for the response
public record OpenAiEmbeddingsResponseEntity(Embedding[] embeddings) implements InferenceResults {
    private static final String NAME = "openai_embeddings_response_entity";
    private static final String EMBEDDINGS_FIELD = "embeddings";

    /**
     * The response from OpenAI should look like this
     * <pre>
     * <code>
     * {
     *   "object": "list",
     *   "data": [
     *     {
     *       "object": "embedding",
     *       "embedding": [
     *         0.0023064255,
     *         -0.009327292,
     *         .... (1536 floats total for ada-002)
     *         -0.0028842222,
     *       ],
     *       "index": 0
     *     }
     *   ],
     *   "model": "text-embedding-ada-002",
     *   "usage": {
     *     "prompt_tokens": 8,
     *     "total_tokens": 8
     *   }
     * }
     * </code>
     * </pre>
     * <a href="https://platform.openai.com/docs/api-reference/embeddings/create">See here for more details</a>
     */
    public static OpenAiEmbeddingsResponseEntity fromResponse(ObjectMapper mapper, HttpResult response) throws IOException {
        JsonNode node = mapper.readTree(response.body());
        JsonNode dataNode = node.require().required("data");

        if (dataNode.isArray()) {
            ArrayNode arrayDataNode = (ArrayNode) dataNode;

            List<Embedding> embeddings = new ArrayList<>();
            for (JsonNode embeddingNode : arrayDataNode) {
                JsonNode embeddingField = embeddingNode.required("embedding");
                var embeddingArray = new OpenAiEmbeddingsResponseEntity.Embedding(
                    mapper.readerForArrayOf(float.class).readValue(embeddingField)
                );

                embeddings.add(embeddingArray);
            }

            return new OpenAiEmbeddingsResponseEntity(embeddings.toArray(OpenAiEmbeddingsResponseEntity.Embedding[]::new));
        } else {
            throw new IllegalArgumentException("Expected OpenAI response to include the 'data' field as an array");
        }
    }

    public OpenAiEmbeddingsResponseEntity {
        Objects.requireNonNull(embeddings);
    }

    public record Embedding(float[] embedding) implements ToXContentObject, Writeable {
        private static final String EMBEDDING_FIELD = "embedding";

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.array(EMBEDDING_FIELD, embedding);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeFloatArray(embedding);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.array(EMBEDDINGS_FIELD, (Object[]) embeddings);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(embeddings);
    }

    @Override
    public String getResultsField() {
        return EMBEDDINGS_FIELD;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(EMBEDDINGS_FIELD, embeddings);

        return map;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
