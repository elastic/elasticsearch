/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class InferenceIngestInputConfigIT extends PyTorchModelRestTestCase {

    @SuppressWarnings("unchecked")
    public void testIngestWithInputFields() throws IOException {
        String modelId = "test_ingest_with_input_fields";
        createPassThroughModel(modelId);
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);

        String inputOutput = """
            [
              {
                "input_field": "body",
                "output_field": "body_tokens"
              }
            ]
            """;
        String docs = """
            [
                {
                  "_source": {
                    "body": "these are"
                  }
                },
                {
                  "_source": {
                    "body": "my words"
                  }
                }
              ]
            """;
        var simulateResponse = simulatePipeline(pipelineDefinition(modelId, inputOutput), docs);
        var responseMap = entityAsMap(simulateResponse);
        var simulatedDocs = (List<Map<String, Object>>) responseMap.get("docs");
        assertThat(simulatedDocs, hasSize(2));
        assertNotNull(MapHelper.dig("doc._source.body_tokens", simulatedDocs.get(0)));
        assertNotNull(MapHelper.dig("doc._source.body_tokens", simulatedDocs.get(1)));
    }

    @SuppressWarnings("unchecked")
    public void testIngestWithMultipleInputFields() throws IOException {
        String modelId = "test_ingest_with_multiple_input_fields";
        createPassThroughModel(modelId);
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);

        String inputOutput = """
            [
              {
                "input_field": "title",
                "output_field": "ml.body_tokens"
              },
              {
                "input_field": "body",
                "output_field": "ml.title_tokens"
              }
            ]
            """;

        String docs = """
            [
                {
                  "_source": {
                    "title": "my",
                    "body": "these are"
                  }
                },
                {
                  "_source": {
                    "title": "are",
                    "body": "my words"
                  }
                }
            ]
            """;
        var simulateResponse = simulatePipeline(pipelineDefinition(modelId, inputOutput), docs);
        var responseMap = entityAsMap(simulateResponse);
        var simulatedDocs = (List<Map<String, Object>>) responseMap.get("docs");
        assertThat(simulatedDocs, hasSize(2));
        assertNotNull(MapHelper.dig("doc._source.ml.title_tokens", simulatedDocs.get(0)));
        assertNotNull(MapHelper.dig("doc._source.ml.body_tokens", simulatedDocs.get(0)));
        assertNotNull(MapHelper.dig("doc._source.ml.title_tokens", simulatedDocs.get(1)));
        assertNotNull(MapHelper.dig("doc._source.ml.body_tokens", simulatedDocs.get(1)));
    }

    private static String pipelineDefinition(String modelId, String inputOutput) {
        return Strings.format("""
            {
              "processors": [
                {
                  "inference": {
                    "model_id": "%s",
                    "input_output": %s
                  }
                }
              ]
            }""", modelId, inputOutput);
    }
}
