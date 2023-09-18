/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiEmbeddingsResponseEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OpenAiClient {

    // TODO this should handle throttling and retries
    // TODO maybe we don't need specific request types?

    // TODO look into using ObjectReader
    /**
     * It is expensive to construct the ObjectMapper so we'll do it once
     * <a href="https://github.com/FasterXML/jackson-docs/wiki/Presentation:-Jackson-Performance">See here for more details</a>
     */
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LogManager.getLogger(OpenAiClient.class);

    private final HttpClient httpClient;

    public OpenAiClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public OpenAiEmbeddingsResponseEntity send(OpenAiEmbeddingsRequest request) throws IOException {
        byte[] body = httpClient.send(request.createRequest());

        /*
         * The response should look like
         *
         * {
              "object": "list",
              "data": [
                {
                  "object": "embedding",
                  "embedding": [
                    0.0023064255,
                    -0.009327292,
                    .... (1536 floats total for ada-002)
                    -0.0028842222,
                  ],
                  "index": 0
                }
              ],
              "model": "text-embedding-ada-002",
              "usage": {
                "prompt_tokens": 8,
                "total_tokens": 8
              }
            }
         */
        try {
            JsonNode node = mapper.readTree(body);
            JsonNode dataNode = node.require().required("data");

            if (dataNode.isArray()) {
                ArrayNode arrayDataNode = (ArrayNode) dataNode;

                List<OpenAiEmbeddingsResponseEntity.Embedding> embeddings = new ArrayList<>();
                for (JsonNode embeddingNode : arrayDataNode) {
                    JsonNode embeddingField = embeddingNode.required("embedding");
                    var embeddingArray = new OpenAiEmbeddingsResponseEntity.Embedding(
                        mapper.readerForArrayOf(float.class).readValue(embeddingField)
                    );

                    embeddings.add(embeddingArray);
                }

                return new OpenAiEmbeddingsResponseEntity(embeddings.toArray(OpenAiEmbeddingsResponseEntity.Embedding[]::new));
            } else {
                throw new IllegalArgumentException("Expected OpenAI response to include embeddings as array");
            }
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchStatusException("OpenAI response was invalid", RestStatus.BAD_REQUEST, e);
        }

    }
}
