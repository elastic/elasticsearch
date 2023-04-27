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
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * This test uses a tiny token weight model to simulate a trained
 * NLP model. The output is an array the size of the highest value
 * input ID and all tokens have a weight of 1.0.
 *
 * The model was created using this simple Python code.
 * The saved TorchScript model is then base64 encoded and hardcoded
 * for use in the test.
 *
 * ## Start Python
 * import torch
 * from torch import Tensor
 * from typing import Optional

 * class TinyTextExpansion(torch.nn.Module):

    def forward(self,
         input_ids: Tensor,
         token_type_ids: Optional[Tensor] = None,
         position_ids: Optional[Tensor] = None,
         inputs_embeds: Optional[Tensor] = None):

         torch.random.manual_seed(hash(str(input_ids)))

         weights = torch.zeros(1, input_ids.max() + 1)
         for i in input_ids:
             weights[0][i] = 1.0

        return weights

 if __name__ == '__main__':
     tte = TinyTextExpansion()
     tte.eval()
     input_ids = torch.tensor([6, 2, 7, 4, 10])
     the_rest = torch.ones(11)
     traced_model =  torch.jit.script(tte, (input_ids, the_rest, the_rest, the_rest))
     torch.jit.save(traced_model, "simplemodel.pt")

 * ## End Python
 */
public class TextExpansionQueryIT extends PyTorchModelRestTestCase {

    static final String BASE_64_ENCODED_MODEL = "UEsDBAAACAgAAAAAAAAAAAAAAAAAA"
        + "AAAAAAUAA4Ac2ltcGxlbW9kZWwvZGF0YS5wa2xGQgoAWlpaWlpaWlpaWoACY19fdG9yY2hfXwpUaW55VG"
        + "V4dEV4cGFuc2lvbgpxACmBfShYCAAAAHRyYWluaW5ncQGJWBYAAABfaXNfZnVsbF9iYWNrd2FyZF9ob29"
        + "rcQJOdWJxAy5QSwcIITmbsFgAAABYAAAAUEsDBBQACAgIAAAAAAAAAAAAAAAAAAAAAAAdAB0Ac2ltcGxl"
        + "bW9kZWwvY29kZS9fX3RvcmNoX18ucHlGQhkAWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWoWRT4+cMAzF7"
        + "/spfASJomF3e0Ga3nrrn8vcELIyxAzRhAQlpjvbT19DWDrdquqBA/bvPT87nVUxwsm41xPd+PNtUi4a77"
        + "KvXs+W8voBAHFSQY3EFCIiHKFp1+p57vs/ShyUccZdoIaz93aBTMR+thbPqru+qKBx8P4q/e8TyxRlmwVc"
        + "tJp66H1YmCyS7WsZwD50A2L5V7pCBADGTTOj0bGGE7noQyqzv5JDfp0o9fZRCWqP37yjhE4+mqX5X3AdF"
        + "ZHGM/2TzOHDpy1IvQWR+OWo3KwsRiKdpcqg4pBFDtm+QJ7nqwIPckrlnGfFJG0uNhOl38Sjut3pCqg26Qu"
        + "Zy8BR9In7ScHHrKkKMW0TIucFrGQXCMpdaDO05O6DpOiy8e4kr0Ed/2YKOIhplW8gPr4ntygrd9ixpx3j9"
        + "UZZVRagl2c6+imWUzBjuf5m+Ch7afphuvvW+r/0dsfn+2N9MZGb9+/SFtCYdhd83CMYp+mGy0LiKNs8y/e"
        + "UuEA8B/d2z4dfUEsHCFSE3IaCAQAAIAMAAFBLAwQUAAgICAAAAAAAAAAAAAAAAAAAAAAAJwApAHNpbXBsZ"
        + "W1vZGVsL2NvZGUvX190b3JjaF9fLnB5LmRlYnVnX3BrbEZCJQBaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlp"
        + "aWlpaWlpaWlpaWlpahZHLbtNAFIZtp03rSVIuLRKXjdk5ojitKJsiFq24lem0KKSqpRIZt55gE9/GM+lNL"
        + "Fgx4i1Ys2aHhIBXgAVICNggHgNm6rqJN2BZGv36/v/MOWeea/Z5RVHurLfRUsfZXOnccx522itrd53O0vL"
        + "qbaKYtsAKUe1pcege7hm9JNtzM8+kOOzNApIX0A3xBXE6YE7g0UWjg2OaZAJXbKvALOnj2GEHKc496ykLkt"
        + "gNt3Jz17hprCUxFqExe7YIpQkNpO1/kfHhPUdtUAdH2/gfmeYiIFW7IkM6IBP2wrDNbMe3Mjf2ksiK3Hjg"
        + "hg7F2DN9l/omZZl5Mmez2QRk0q4WUUB0+1oh9nDwxGdUXJdXPMRZQs352eGaRPV9s2lcMeZFGWBfKJJiw0Y"
        + "gbCMLBaRmXyy4flx6a667Fch55q05QOq2Jg2ANOyZwplhNsjiohVApo7aa21QnNGW5+4GXv8gxK1beBeHSR"
        + "rhmLXWVh+0aBhErZ7bx1ejxMOhlR6QU4ycNqGyk8/yNGCWkwY7/RCD7UEQek4QszCgDJAzZtfErA0VqHBy9"
        + "ugQP9pUfUmgCjVYgWNwHFbhBJyEOgSwBuuwARWZmoI6J9PwLfzEocpRpPrT8DP8wqHG0b4UX+E3DiscvRgl"
        + "XIoi81KKPwioHI5x9EooNKWiy0KOc/T6WF4SssrRuzJ9L2VNRXUhJzj6UKYfS4W/q/5wuh/l4M9R9qsU+y2"
        + "dpoo2hJzkaEET8r6KRONicnRdK9EbUi6raFVIwNGjsrlbpk6ZPi7TbS3fv3LyNjPiEKzG0aG0tvNb6xw90/"
        + "whe6ONjnJcUxobHDUqQ8bIOW79BVBLBwhfSmPKdAIAAE4EAABQSwMEAAAICAAAAAAAAAAAAAAAAAAAAAAAA"
        + "BkABQBzaW1wbGVtb2RlbC9jb25zdGFudHMucGtsRkIBAFqAAikuUEsHCG0vCVcEAAAABAAAAFBLAwQAAAgI"
        + "AAAAAAAAAAAAAAAAAAAAAAAAEwA7AHNpbXBsZW1vZGVsL3ZlcnNpb25GQjcAWlpaWlpaWlpaWlpaWlpaWlp"
        + "aWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWjMKUEsHCNGeZ1UCAAAAAgAAAFBLAQIAAA"
        + "AACAgAAAAAAAAhOZuwWAAAAFgAAAAUAAAAAAAAAAAAAAAAAAAAAABzaW1wbGVtb2RlbC9kYXRhLnBrbFBLA"
        + "QIAABQACAgIAAAAAABUhNyGggEAACADAAAdAAAAAAAAAAAAAAAAAKgAAABzaW1wbGVtb2RlbC9jb2RlL19f"
        + "dG9yY2hfXy5weVBLAQIAABQACAgIAAAAAABfSmPKdAIAAE4EAAAnAAAAAAAAAAAAAAAAAJICAABzaW1wbGVt"
        + "b2RlbC9jb2RlL19fdG9yY2hfXy5weS5kZWJ1Z19wa2xQSwECAAAAAAgIAAAAAAAAbS8JVwQAAAAEAAAAGQAA"
        + "AAAAAAAAAAAAAACEBQAAc2ltcGxlbW9kZWwvY29uc3RhbnRzLnBrbFBLAQIAAAAACAgAAAAAAADRnmdVAgAA"
        + "AAIAAAATAAAAAAAAAAAAAAAAANQFAABzaW1wbGVtb2RlbC92ZXJzaW9uUEsGBiwAAAAAAAAAHgMtAAAAAAAA"
        + "AAAABQAAAAAAAAAFAAAAAAAAAGoBAAAAAAAAUgYAAAAAAABQSwYHAAAAALwHAAAAAAAAAQAAAFBLBQYAAAAABQAFAGoBAABSBgAAAAA=";

    static final long RAW_MODEL_SIZE; // size of the model before base64 encoding
    static {
        RAW_MODEL_SIZE = Base64.getDecoder().decode(BASE_64_ENCODED_MODEL).length;
    }

    @SuppressWarnings("unchecked")
    public void testTextExpansionQuery() throws IOException {
        String modelId = "text-expansion-test";
        String indexName = modelId + "-index";

        createTextExpansionModel(modelId);
        putModelDefinition(modelId, BASE_64_ENCODED_MODEL, RAW_MODEL_SIZE);
        putVocabulary(
            List.of("these", "are", "my", "words", "the", "washing", "machine", "is", "leaking", "octopus", "comforter", "smells"),
            modelId
        );
        startDeployment(modelId);

        // All tokens have a weight of 1.0.
        // By using unique combinations of words the top doc returned
        // by the search should be the same as the query text
        List<String> inputs = List.of(
            "my words comforter",
            "the machine is leaking",
            "these are my words",
            "the octopus comforter smells",
            "the octopus comforter is leaking",
            "washing machine smells"
        );

        List<Map<String, Float>> tokenWeights = new ArrayList<>();
        // Generate the rank feature weights via the inference API
        // then index them for search
        for (var input : inputs) {
            Response inference = infer(input, modelId);
            List<Map<String, Object>> responseMap = (List<Map<String, Object>>) entityAsMap(inference).get("inference_results");
            Map<String, Object> inferenceResult = responseMap.get(0);
            var idWeights = (Map<String, Float>) inferenceResult.get("predicted_value");
            tokenWeights.add(idWeights);
        }

        // index tokens
        createRankFeaturesIndex(indexName);
        bulkIndexDocs(inputs, tokenWeights, indexName);

        // Test text expansion search against the indexed rank features
        for (int i = 0; i < 5; i++) {
            int randomInput = randomIntBetween(0, inputs.size() - 1);
            var textExpansionSearchResponse = textExpansionSearch(indexName, inputs.get(randomInput), modelId, "ml.tokens");
            assertOkWithErrorMessage(textExpansionSearchResponse);

            Map<String, Object> responseMap = responseAsMap(textExpansionSearchResponse);
            List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
            Map<String, Object> topHit = hits.get(0);
            String sourceText = (String) MapHelper.dig("_source.text_field", topHit);
            assertEquals(inputs.get(randomInput), sourceText);
        }
    }

    public void testWithPipelineIngest() throws IOException {
        String modelId = "text-expansion-pipeline-test";
        String indexName = modelId + "-index";

        createTextExpansionModel(modelId);
        putModelDefinition(modelId, BASE_64_ENCODED_MODEL, RAW_MODEL_SIZE);
        putVocabulary(
            List.of("these", "are", "my", "words", "the", "washing", "machine", "is", "leaking", "octopus", "comforter", "smells"),
            modelId
        );
        startDeployment(modelId);

        // All tokens have a weight of 1.0.
        // By using unique combinations of words the top doc returned
        // by the search should be the same as the query text
        List<String> inputs = List.of(
            "my words comforter",
            "the machine is leaking",
            "these are my words",
            "the octopus comforter smells",
            "the octopus comforter is leaking",
            "washing machine smells"
        );

        // index tokens
        createRankFeaturesIndex(indexName);
        var pipelineId = putPipeline(modelId);
        bulkIndexThroughPipeline(inputs, indexName, pipelineId);

        // Test text expansion search against the indexed rank features
        for (int i = 0; i < 5; i++) {
            int randomInput = randomIntBetween(0, inputs.size() - 1);
            var textExpansionSearchResponse = textExpansionSearch(indexName, inputs.get(randomInput), modelId, "ml.tokens");
            assertOkWithErrorMessage(textExpansionSearchResponse);

            Map<String, Object> responseMap = responseAsMap(textExpansionSearchResponse);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
            Map<String, Object> topHit = hits.get(0);
            String sourceText = (String) MapHelper.dig("_source.text_field", topHit);
            assertEquals(inputs.get(randomInput), sourceText);
        }
    }

    public void testWithDotsInTokenNames() throws IOException {
        String modelId = "text-expansion-dots-in-tokens";
        String indexName = modelId + "-index";

        createTextExpansionModel(modelId);
        putModelDefinition(modelId, BASE_64_ENCODED_MODEL, RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words", "the", "washing", "machine", ".", "##."), modelId);
        startDeployment(modelId);

        // '.' are invalid rank feature field names and will be replaced with '__'
        List<String> inputs = List.of("these are my words.");

        // index tokens
        createRankFeaturesIndex(indexName);
        var pipelineId = putPipeline(modelId);
        bulkIndexThroughPipeline(inputs, indexName, pipelineId);

        // Test text expansion search against the indexed rank features
        for (var input : inputs) {
            var textExpansionSearchResponse = textExpansionSearch(indexName, input, modelId, "ml.tokens");
            assertOkWithErrorMessage(textExpansionSearchResponse);
            Map<String, Object> responseMap = responseAsMap(textExpansionSearchResponse);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
            Map<String, Object> topHit = hits.get(0);
            String sourceText = (String) MapHelper.dig("_source.text_field", topHit);
            assertEquals(input, sourceText);
            // check the token names containing dots have been changed
            Object s = MapHelper.dig("_source.ml.tokens.__", topHit);
            assertNotNull(s);
        }
    }

    public void testSearchWithMissingModel() throws IOException {
        String modelId = "missing-model";
        String indexName = modelId + "-index";
        var e = expectThrows(ResponseException.class, () -> textExpansionSearch(indexName, "the machine is leaking", modelId, "ml.tokens"));
        assertThat(e.getMessage(), containsString("Could not find trained model [missing-model]"));
    }

    protected Response textExpansionSearch(String index, String modelText, String modelId, String fieldName) throws IOException {
        Request request = new Request("GET", index + "/_search?error_trace=true");

        request.setJsonEntity(Strings.format("""
            {
                "query": {
                  "text_expansion": {
                    "%s": {
                      "model_id": "%s",
                      "model_text": "%s"
                    }
                  }
                }
            }""", fieldName, modelId, modelText));
        return client().performRequest(request);
    }

    protected void createTextExpansionModel(String modelId) throws IOException {
        // with_special_tokens: false for this test with limited vocab
        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
        request.setJsonEntity("""
            {
               "description": "a text expansion model",
               "model_type": "pytorch",
               "inference_config": {
                 "text_expansion": {
                   "tokenization": {
                     "bert": {
                       "with_special_tokens": false
                     }
                   }
                 }
               }
             }""");
        client().performRequest(request);
    }

    private void createRankFeaturesIndex(String indexName) throws IOException {
        Request createIndex = new Request("PUT", "/" + indexName);
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "text_field": {
                    "type": "text"
                  },
                  "ml.tokens": {
                    "type": "rank_features"
                  }
                }
              }
            }""");
        var response = client().performRequest(createIndex);
        assertOkWithErrorMessage(response);
    }

    private void bulkIndexDocs(List<String> sourceText, List<Map<String, Float>> tokenWeights, String indexName) throws IOException {
        String createAction = "{\"create\": {\"_index\": \"" + indexName + "\"}}\n";

        StringBuilder bulkBuilder = new StringBuilder();

        for (int i = 0; i < sourceText.size(); i++) {
            bulkBuilder.append(createAction);
            bulkBuilder.append("{\"text_field\": \"").append(sourceText.get(i)).append("\", \"ml.tokens\":{");

            for (var entry : tokenWeights.get(i).entrySet()) {
                writeToken(entry, bulkBuilder).append(',');
            }
            bulkBuilder.deleteCharAt(bulkBuilder.length() - 1); // delete the trailing ','
            bulkBuilder.append("}}\n");
        }

        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulkBuilder.toString());
        bulkRequest.addParameter("refresh", "true");
        var bulkResponse = client().performRequest(bulkRequest);
        assertOkWithErrorMessage(bulkResponse);
    }

    private void bulkIndexThroughPipeline(List<String> sourceText, String indexName, String pipelineId) throws IOException {
        String createAction = "{\"create\": {\"_index\": \"" + indexName + "\"}}\n";

        StringBuilder bulkBuilder = new StringBuilder();

        for (int i = 0; i < sourceText.size(); i++) {
            bulkBuilder.append(createAction);
            bulkBuilder.append("{\"text_field\": \"").append(sourceText.get(i)).append("\"}\n");
        }

        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulkBuilder.toString());
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.addParameter("pipeline", pipelineId);
        var bulkResponse = client().performRequest(bulkRequest);
        assertOkWithErrorMessage(bulkResponse);
    }

    private StringBuilder writeToken(Map.Entry<String, Float> entry, StringBuilder builder) {
        return builder.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
    }

    private String putPipeline(String modelId) throws IOException {
        String def = Strings.format("""
            {
              "processors": [
                {
                  "inference": {
                    "model_id": "%s",
                    "field_map": {
                      "text_field": "input"
                    },
                    "target_field": "ml",
                    "inference_config": {
                      "text_expansion": {
                        "results_field": "tokens"
                      }
                    }
                  }
                }
              ]
            }""", modelId);
        Request request = new Request("PUT", "_ingest/pipeline/" + modelId + "-pipeline");
        request.setJsonEntity(def);
        assertOkWithErrorMessage(client().performRequest(request));
        return modelId + "-pipeline";
    }
}
