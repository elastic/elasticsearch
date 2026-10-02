/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CustomElandModelIT extends InferenceBaseRestTest {

    // The model definition is taken from org.elasticsearch.xpack.ml.integration.TextExpansionQueryIT

    static final String SPARSE_EMBEDDING_BASE_64_ENCODED_MODEL = "UEsDBAAACAgAAAAAAAAAAAAAAAAAA"
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

    static final long SPARSE_EMBEDDING_RAW_MODEL_SIZE; // size of the model before base64 encoding
    static {
        SPARSE_EMBEDDING_RAW_MODEL_SIZE = Base64.getDecoder().decode(SPARSE_EMBEDDING_BASE_64_ENCODED_MODEL).length;
    }

    // The model definition is taken from org.elasticsearch.xpack.ml.integration.TextEmbeddingQueryIT
    static final String TEXT_EMBEDDING_BASE_64_ENCODED_MODEL = "UEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAAUAA4Ac2ltcGxlbW9kZWwvZGF0YS5wa2xGQgoAWl"
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

    static final long TEXT_EMBEDDING_RAW_MODEL_SIZE; // size of the model before base64 encoding
    static {
        TEXT_EMBEDDING_RAW_MODEL_SIZE = Base64.getDecoder().decode(TEXT_EMBEDDING_BASE_64_ENCODED_MODEL).length;
    }

    // Test a sparse embedding model deployed with the ml trained models APIs
    public void testSparse() throws IOException {
        String modelId = "custom-text-expansion-model";

        createTextExpansionModel(modelId, client());
        putModelDefinition(modelId, SPARSE_EMBEDDING_BASE_64_ENCODED_MODEL, SPARSE_EMBEDDING_RAW_MODEL_SIZE, client());
        putVocabulary(
            List.of("these", "are", "my", "words", "the", "washing", "machine", "is", "leaking", "octopus", "comforter", "smells"),
            modelId,
            client()
        );

        var inferenceConfig = """
            {
              "service": "elasticsearch",
              "service_settings": {
                "model_id": "custom-text-expansion-model",
                "num_allocations": 1,
                "num_threads": 1
              }
            }
            """;

        var inferenceId = "sparse-inf";
        putModel(inferenceId, inferenceConfig, TaskType.SPARSE_EMBEDDING);
        var results = infer(inferenceId, List.of("washing", "machine"));
        deleteModel(inferenceId);
        assertNotNull(results.get(SparseEmbeddingResults.SPARSE_EMBEDDING));
    }

    public void testCannotStopDeployment() throws IOException {
        String modelId = "custom-model-that-cannot-be-stopped";

        createTextExpansionModel(modelId, client());
        putModelDefinition(modelId, SPARSE_EMBEDDING_BASE_64_ENCODED_MODEL, SPARSE_EMBEDDING_RAW_MODEL_SIZE, client());
        putVocabulary(
            List.of("these", "are", "my", "words", "the", "washing", "machine", "is", "leaking", "octopus", "comforter", "smells"),
            modelId,
            client()
        );

        var inferenceConfig = """
            {
              "service": "elasticsearch",
              "service_settings": {
                "model_id": "custom-model-that-cannot-be-stopped",
                "num_allocations": 1,
                "num_threads": 1
              }
            }
            """;

        var inferenceId = "sparse-inf";
        putModel(inferenceId, inferenceConfig, TaskType.SPARSE_EMBEDDING);
        infer(inferenceId, List.of("washing", "machine"));

        // Stopping the deployment using the ML trained models API should fail
        // because the deployment was created by the inference endpoint API
        String stopEndpoint = org.elasticsearch.common.Strings.format("_ml/trained_models/%s/deployment/_stop?error_trace", inferenceId);
        Request stopRequest = new Request("POST", stopEndpoint);
        var e = expectThrows(ResponseException.class, () -> client().performRequest(stopRequest));
        assertThat(
            e.getMessage(),
            containsString("Cannot stop deployment [sparse-inf] as it was created by inference endpoint [sparse-inf]")
        );

        // Force stop works
        String forceStopEndpoint = org.elasticsearch.common.Strings.format("_ml/trained_models/%s/deployment/_stop?force", inferenceId);
        assertStatusOkOrCreated(client().performRequest(new Request("POST", forceStopEndpoint)));
    }

    /**
     * Verifies that when a text-embedding inference endpoint create fails after the model deployment has been started
     * (the new {@code stopModelDeploymentIfStarted} behavior), the ML deployment is actually stopped and does not
     * remain running as an orphan.
     *
     * <p>The failure is forced by the incompatible-semantic_text-mapping check in
     * {@code checkForExistingUsesOfInferenceId}: a document is indexed with the semantic_text field
     * (populating {@code model_settings} in the mapping), then the endpoint is force-deleted, and a recreation
     * attempt with a different similarity setting triggers the incompatibility which fires after validation has
     * already deployed the model.
     */
    @SuppressWarnings("unchecked")
    public void testStopsDeployment_WhenCreateFailsAfterDeployment() throws Exception {
        var modelId = "stop-deployment-on-failure-model";
        var inferenceId = "stop-deployment-on-failure-inference";
        var indexName = "stop-deployment-on-failure-index";

        // Upload the text-embedding model (not yet deployed)
        createMlNodeTextEmbeddingModel(modelId, client());

        // Create the inference endpoint with the default similarity (cosine).
        // Validation deploys the model and returns the embedding dimensions.
        putModel(inferenceId, Strings.format("""
            {
              "service": "elasticsearch",
              "service_settings": {
                "model_id": "%s",
                "num_allocations": 1,
                "num_threads": 1
              }
            }
            """, modelId), TaskType.TEXT_EMBEDDING);

        // Create an index with a semantic_text field referencing the endpoint
        putSemanticText(inferenceId, indexName);

        // Index a document so that model_settings (task_type, similarity, dimensions, element_type) are
        // written into the index mapping — required for the incompatibility check to fire later
        var indexRequest = new Request("PUT", indexName + "/_create/1");
        indexRequest.setJsonEntity("{\"inference_field\": \"hello world\"}");
        assertStatusOkOrCreated(client().performRequest(indexRequest));
        assertStatusOkOrCreated(client().performRequest(new Request("GET", "_refresh")));

        // Force-delete the endpoint (stops the deployment while the index mapping still references it)
        deleteModel(inferenceId, "force=true");

        // Attempt to recreate the same inference endpoint with an incompatible similarity (dot_product).
        // The validator redeploys the model, but checkForExistingUsesOfInferenceId then detects the
        // conflict with the existing mapping and fails. stopModelDeploymentIfStarted must stop the
        // newly-started deployment.
        var e = expectThrows(ResponseException.class, () -> putModel(inferenceId, Strings.format("""
            {
              "service": "elasticsearch",
              "service_settings": {
                "model_id": "%s",
                "num_allocations": 1,
                "num_threads": 1,
                "similarity": "dot_product"
              }
            }
            """, modelId), TaskType.TEXT_EMBEDDING));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(e.getMessage(), containsString("incompatible settings"));

        // The deployment that validation started must have been stopped by stopModelDeploymentIfStarted.
        // Use assertBusy because the ML stop is asynchronous relative to the HTTP response.
        assertBusy(() -> {
            var stats = (List<Map<String, Object>>) getTrainedModelStats(modelId).get("trained_model_stats");
            assertNull(
                "Deployment should have been stopped after the failed inference endpoint create, but deployment_stats is still present",
                stats.get(0).get("deployment_stats")
            );
        }, 30, TimeUnit.SECONDS);

        deleteIndex(indexName);
    }

    static void createTextExpansionModel(String modelId, RestClient client) throws IOException {
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
        client.performRequest(request);
    }

    static void putVocabulary(List<String> vocabulary, String modelId, RestClient client) throws IOException {
        List<String> vocabularyWithPad = new ArrayList<>();
        vocabularyWithPad.add("[PAD]");
        vocabularyWithPad.add("[UNK]");
        vocabularyWithPad.addAll(vocabulary);
        String quotedWords = vocabularyWithPad.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));

        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/vocabulary");
        request.setJsonEntity(Strings.format("""
            { "vocabulary": [%s] }
            """, quotedWords));
        client.performRequest(request);
    }

    static void putModelDefinition(String modelId, String base64EncodedModel, long unencodedModelSize, RestClient client)
        throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/definition/0");
        String body = Strings.format("""
            {"total_definition_length":%s,"definition": "%s","total_parts": 1}""", unencodedModelSize, base64EncodedModel);
        request.setJsonEntity(body);
        client.performRequest(request);
    }

    // Create the model including definition and vocab
    static void createMlNodeTextExpansionModel(String modelId, RestClient client) throws IOException {
        createTextExpansionModel(modelId, client);
        putModelDefinition(modelId, SPARSE_EMBEDDING_BASE_64_ENCODED_MODEL, SPARSE_EMBEDDING_RAW_MODEL_SIZE, client);
        putVocabulary(
            List.of("these", "are", "my", "words", "the", "washing", "machine", "is", "leaking", "octopus", "comforter", "smells"),
            modelId,
            client
        );
    }

    private static void createTextEmbeddingModel(String modelId, RestClient client) throws IOException {
        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
        request.setJsonEntity("""
            {
               "description": "a text embedding model",
               "model_type": "pytorch",
               "inference_config": {
                 "text_embedding": {
                   "tokenization": {
                     "bert": {
                       "with_special_tokens": false
                     }
                   }
                 }
               }
             }""");
        client.performRequest(request);
    }

    // Creates a text-embedding trained model with definition and vocabulary (model NOT deployed)
    static void createMlNodeTextEmbeddingModel(String modelId, RestClient client) throws IOException {
        createTextEmbeddingModel(modelId, client);
        putModelDefinition(modelId, TEXT_EMBEDDING_BASE_64_ENCODED_MODEL, TEXT_EMBEDDING_RAW_MODEL_SIZE, client);
        putVocabulary(
            List.of("these", "are", "my", "words", "the", "washing", "machine", "is", "leaking", "octopus", "comforter", "smells"),
            modelId,
            client
        );
    }

}
