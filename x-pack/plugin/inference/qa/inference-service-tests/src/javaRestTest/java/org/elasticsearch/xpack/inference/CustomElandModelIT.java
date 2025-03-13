/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class CustomElandModelIT extends InferenceBaseRestTest {

    // The model definition is taken from org.elasticsearch.xpack.ml.integration.TextExpansionQueryIT

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

    // Test a sparse embedding model deployed with the ml trained models APIs
    public void testSparse() throws IOException {
        String modelId = "custom-text-expansion-model";

        createTextExpansionModel(modelId, client());
        putModelDefinition(modelId, BASE_64_ENCODED_MODEL, RAW_MODEL_SIZE, client());
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
        assertNotNull(results.get("sparse_embedding"));
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
        putModelDefinition(modelId, BASE_64_ENCODED_MODEL, RAW_MODEL_SIZE, client);
        putVocabulary(
            List.of("these", "are", "my", "words", "the", "washing", "machine", "is", "leaking", "octopus", "comforter", "smells"),
            modelId,
            client
        );
    }

}
