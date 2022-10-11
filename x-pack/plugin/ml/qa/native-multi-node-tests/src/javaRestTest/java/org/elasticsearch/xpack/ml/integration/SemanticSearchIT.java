/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * This test uses a tiny text embedding model to simulate an trained
 * NLP model.The output tensor is randomly generated but the RNG is
 * seeded with the hash of the input IDS so the same input produces
 * the same output.
 *
 * The model was created using this simple Python code.
 * The saved TorchScript model is then base64 encoded and hardcoded here
 * for use in the test.
 *
 * ## Start Python
 * import torch
 * from torch import Tensor
 * from typing import Optional
 *
 * class TinyTextEmbedding(torch.nn.Module):
 *
 *  def forward(self,
 *          input_ids: Tensor,
 *          token_type_ids: Optional[Tensor] = None,
 *          position_ids: Optional[Tensor] = None,
 *          inputs_embeds: Optional[Tensor] = None):
 *
 *      torch.random.manual_seed(hash(str(input_ids)))
 *      return torch.rand(1, 100)
 *
 * if __name__ == '__main__':
 *      tte = TinyTextEmbedding()
 *      tte.eval()
 *      input_ids = torch.tensor([1, 2, 3, 4, 5])
 *      the_rest = torch.ones(5)
 *      traced_model =  torch.jit.script(tte, (input_ids, the_rest, the_rest, the_rest))
 *      torch.jit.save(traced_model, "simplemodel.pt")
 * ## End Python
 */
public class SemanticSearchIT extends PyTorchModelRestTestCase {

    static final String BASE_64_ENCODED_MODEL = "UEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAAUAA4Ac2ltcGxlbW9kZWwvZGF0YS5wa2xGQgoAWl"
        + "paWlpaWlpaWoACY19fdG9yY2hfXwpUaW55VGV4dEVtYmVkZGluZwpxACmBfShYCAAAAHRy"
        + "YWluaW5ncQGJWBYAAABfaXNfZnVsbF9iYWNrd2FyZF9ob29rcQJOdWJxAy5QSwcIsFTQsF"
        + "gAAABYAAAAUEsDBBQACAgIAAAAAAAAAAAAAAAAAAAAAAAdAB0Ac2ltcGxlbW9kZWwvY29k"
        + "ZS9fX3RvcmNoX18ucHlGQhkAWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWoWPMWvDMBCF9/"
        + "yKGy1IQ7Ia0q1j2yWbMYdsnWphWWd0Em3+fS3bBEopXd99j/dd77UI3Fy43+grvUwdGePC"
        + "R/XKJntS9QEAcdZRT5QoCiJcoWnXtMvW/ohS1C4sZaihY/YFcoI2e4+d7sdPHQ0OzONyf5"
        + "+T46B9U8DSNWTBcixMJeRtvQwkjv2AePpld1wKAC7MOaEzUsONgnDc4sQjBUz3mbbbY2qD"
        + "2usbB9rQmcWV47/gOiVIReAvUsHT8y5S7yKL/mnSIWuPQmSqLRm0DJWkWD0eUEqtjUgpx7"
        + "AXow6mai5HuJzPrTp8A1BLBwiD/6yJ6gAAAKkBAABQSwMEFAAICAgAAAAAAAAAAAAAAAAA"
        + "AAAAACcAQQBzaW1wbGVtb2RlbC9jb2RlL19fdG9yY2hfXy5weS5kZWJ1Z19wa2xGQj0AWl"
        + "paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpa"
        + "WlpaWlpaWo2Qz0rDQBDGk/5RmjfwlmMCbWivBZ9gWL0IFkRCdLcmmOwmuxu0N08O3r2rCO"
        + "rdx9CDgm/hWUUQMdugzUk6LCwzv++bGeak5YE1saoorNgCCwsbzFc9sm1PvivQo2zqToU8"
        + "iiT1FEunfadXRcLzUocJVWN3i3ElZF3W4pDxUM9yVrPNXCeCR+lOLdp1190NwVktzoVKDF"
        + "5COh+nQpbtsX+0/tjpOWYJuR8HMuJUZEEW8TJKQ8UY9eJIxZ7S0vvb3vf9yiCZLiV3Fz5v"
        + "1HdHw6HvFK3JWnUElWR5ygbz8TThB4NMUJYG+axowyoWHbiHBwQbSWbHHXiEJ4QWkmOTPM"
        + "MLQhvJaZOgSX49Z3a8uPq5Ia/whtBBctEkl4a8wwdCF8lVk1wb8glfCCtIbprkttntrkF0"
        + "0Q1+AFBLBwi4BIswOAEAAP0BAABQSwMEAAAICAAAAAAAAAAAAAAAAAAAAAAAABkAQQBzaW"
        + "1wbGVtb2RlbC9jb25zdGFudHMucGtsRkI9AFpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpa"
        + "WlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlqAAikuUEsHCG0vCVcEAAAABA"
        + "AAAFBLAwQAAAgIAAAAAAAAAAAAAAAAAAAAAAAAEwA7AHNpbXBsZW1vZGVsL3ZlcnNpb25G"
        + "QjcAWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWl"
        + "paWlpaWjMKUEsHCNGeZ1UCAAAAAgAAAFBLAQIAAAAACAgAAAAAAACwVNCwWAAAAFgAAAAU"
        + "AAAAAAAAAAAAAAAAAAAAAABzaW1wbGVtb2RlbC9kYXRhLnBrbFBLAQIAABQACAgIAAAAAA"
        + "CD/6yJ6gAAAKkBAAAdAAAAAAAAAAAAAAAAAKgAAABzaW1wbGVtb2RlbC9jb2RlL19fdG9y"
        + "Y2hfXy5weVBLAQIAABQACAgIAAAAAAC4BIswOAEAAP0BAAAnAAAAAAAAAAAAAAAAAPoBAA"
        + "BzaW1wbGVtb2RlbC9jb2RlL19fdG9yY2hfXy5weS5kZWJ1Z19wa2xQSwECAAAAAAgIAAAA"
        + "AAAAbS8JVwQAAAAEAAAAGQAAAAAAAAAAAAAAAADIAwAAc2ltcGxlbW9kZWwvY29uc3Rhbn"
        + "RzLnBrbFBLAQIAAAAACAgAAAAAAADRnmdVAgAAAAIAAAATAAAAAAAAAAAAAAAAAFQEAABz"
        + "aW1wbGVtb2RlbC92ZXJzaW9uUEsGBiwAAAAAAAAAHgMtAAAAAAAAAAAABQAAAAAAAAAFAA"
        + "AAAAAAAGoBAAAAAAAA0gQAAAAAAABQSwYHAAAAADwGAAAAAAAAAQAAAFBLBQYAAAAABQAFAGoBAADSBAAAAAA=";

    static final long RAW_MODEL_SIZE; // size of the model before base64 encoding
    static {
        RAW_MODEL_SIZE = Base64.getDecoder().decode(BASE_64_ENCODED_MODEL).length;
    }

    @SuppressWarnings("unchecked")
    public void testModel() throws IOException {
        String modelId = "semantic-search-test";
        String indexName = modelId + "-index";

        createTextEmbeddingModel(modelId);
        putModelDefinition(modelId, BASE_64_ENCODED_MODEL, RAW_MODEL_SIZE);
        putVocabulary(
            List.of("these", "are", "my", "words", "the", "washing", "machine", "is", "leaking", "octopus", "comforter", "smells"),
            modelId
        );
        startDeployment(modelId);

        List<String> inputs = List.of(
            "my words",
            "the machine is leaking",
            "washing machine",
            "these are my words",
            "the octopus comforter smells",
            "the octopus comforter is leaking",
            "washing machine smells"
        );
        List<List<Double>> embeddings = new ArrayList<>();

        // Generate the text embeddings via the inference API
        // then index them for search
        for (var input : inputs) {
            Response inference = infer(input, modelId);
            List<Map<String, Object>> responseMap = (List<Map<String, Object>>) entityAsMap(inference).get("inference_results");
            Map<String, Object> inferenceResult = responseMap.get(0);
            List<Double> embedding = (List<Double>) inferenceResult.get("predicted_value");
            embeddings.add(embedding);
        }

        // index dense vectors
        createVectorSearchIndex(indexName);
        bulkIndexDocs(inputs, embeddings, indexName);
        forceMergeIndex(indexName);

        // Test semantic search against the indexed vectors
        for (int i = 0; i < 5; i++) {
            int randomInput = randomIntBetween(0, inputs.size() - 1);
            var semanticSearchResponse = semanticSearch(indexName, inputs.get(randomInput), modelId, "embedding");
            assertOkWithErrorMessage(semanticSearchResponse);

            Map<String, Object> responseMap = responseAsMap(semanticSearchResponse);
            List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
            Map<String, Object> topHit = hits.get(0);
            String sourceText = (String) MapHelper.dig("_source.source_text", topHit);
            assertEquals(inputs.get(randomInput), sourceText);
        }
    }

    private void createVectorSearchIndex(String indexName) throws IOException {
        Request createIndex = new Request("PUT", "/" + indexName);
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "source_text": {
                    "type": "text"
                  },
                  "embedding": {
                    "type": "dense_vector",
                    "dims": 100,
                    "index": true,
                    "similarity": "cosine"
                  }
                }
              }
            }""");
        var response = client().performRequest(createIndex);
        assertOkWithErrorMessage(response);
    }

    private void bulkIndexDocs(List<String> inputs, List<List<Double>> embeddings, String indexName) throws IOException {
        String createAction = "{\"create\": {\"_index\": \"" + indexName + "\"}}\n";

        StringBuilder bulkBuilder = new StringBuilder();

        for (int i = 0; i < inputs.size(); i++) {
            bulkBuilder.append(createAction);
            bulkBuilder.append("{\"source_text\": \"")
                .append(inputs.get(i))
                .append("\", \"embedding\":")
                .append(embeddings.get(i))
                .append("}\n");
        }

        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulkBuilder.toString());
        bulkRequest.addParameter("refresh", "true");
        var bulkResponse = client().performRequest(bulkRequest);
        assertOkWithErrorMessage(bulkResponse);
    }
}
