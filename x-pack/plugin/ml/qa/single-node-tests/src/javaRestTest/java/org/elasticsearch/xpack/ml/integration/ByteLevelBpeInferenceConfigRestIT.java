/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * REST coverage for {@code text_embedding} configs using {@code byte_level_bpe} tokenization (XContent round-trip).
 */
public class ByteLevelBpeInferenceConfigRestIT extends InferenceTestCase {

    private static final String MODEL_ID = "byte_level_bpe_rest_config_model";

    public void testPutAndGetTextEmbeddingByteLevelBpeTokenization() throws IOException {
        try {
            Request put = new Request("PUT", "_ml/trained_models/" + MODEL_ID);
            put.setJsonEntity("""
                {
                  "description": "byte_level_bpe tokenization XContent REST coverage",
                  "model_type": "pytorch",
                  "inference_config": {
                    "text_embedding": {
                      "tokenization": {
                        "byte_level_bpe": {
                          "with_special_tokens": false,
                          "add_prefix_space": true,
                          "max_sequence_length": 256,
                          "truncate": "first",
                          "unk_token": "<unk>",
                          "pad_token": "<pad>"
                        }
                      }
                    }
                  }
                }
                """);
            Response putResponse = client().performRequest(put);
            assertThat(putResponse.getStatusLine().getStatusCode(), equalTo(200));

            Request get = new Request("GET", "_ml/trained_models/" + MODEL_ID);
            Map<String, Object> body = entityAsMap(client().performRequest(get));

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> configs = (List<Map<String, Object>>) body.get("trained_model_configs");
            assertNotNull(configs);
            assertThat(configs.size(), equalTo(1));
            assertThat(configs.get(0).get("model_id"), equalTo(MODEL_ID));

            @SuppressWarnings("unchecked")
            Map<String, Object> inferenceConfig = (Map<String, Object>) configs.get(0).get("inference_config");
            assertNotNull(inferenceConfig);
            @SuppressWarnings("unchecked")
            Map<String, Object> textEmbedding = (Map<String, Object>) inferenceConfig.get("text_embedding");
            assertNotNull(textEmbedding);
            @SuppressWarnings("unchecked")
            Map<String, Object> tokenization = (Map<String, Object>) textEmbedding.get("tokenization");
            assertNotNull(tokenization);
            @SuppressWarnings("unchecked")
            Map<String, Object> byteLevelBpe = (Map<String, Object>) tokenization.get("byte_level_bpe");
            assertNotNull(byteLevelBpe);

            assertThat(byteLevelBpe.get("with_special_tokens"), equalTo(false));
            assertThat(byteLevelBpe.get("add_prefix_space"), equalTo(true));
            assertThat(byteLevelBpe.get("max_sequence_length"), equalTo(256));
            assertThat(byteLevelBpe.get("truncate"), equalTo("first"));
            assertThat(byteLevelBpe.get("unk_token"), equalTo("<unk>"));
            assertThat(byteLevelBpe.get("pad_token"), equalTo("<pad>"));
        } finally {
            deleteTrainedModelIfPresent(MODEL_ID);
        }
    }

    private static void deleteTrainedModelIfPresent(String modelId) throws IOException {
        try {
            Request delete = new Request("DELETE", "_ml/trained_models/" + modelId + "?force=true");
            client().performRequest(delete);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }
}
