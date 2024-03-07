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

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

/**
 * This test uses the same tiny text embedding model defined in {@link TextEmbeddingQueryIT}.
 */
public class KnnQueryWithTextEmbeddingIT extends PyTorchModelRestTestCase {

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
    public void testKnnWithTextEmbeddingQuery() throws IOException {
        String modelId = "text-embedding-test";
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
        List<String> filters = List.of("foo", "bar", "baz", "foo", "bar", "baz", "foo");
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

        // Index dense vectors
        createVectorSearchIndex(indexName);
        bulkIndexDocs(inputs, filters, embeddings, indexName);
        forceMergeIndex(indexName);

        // Test knn search with model text against the indexed vectors
        for (int i = 0; i < 5; i++) {
            int randomInput = randomIntBetween(0, inputs.size() - 1);
            var response = knnModelTextSearch(indexName, inputs.get(randomInput), modelId, "embedding");
            assertOkWithErrorMessage(response);

            Map<String, Object> responseMap = responseAsMap(response);
            List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
            Map<String, Object> topHit = hits.get(0);
            String sourceText = (String) MapHelper.dig("_source.source_text", topHit);
            assertEquals(inputs.get(randomInput), sourceText);
        }

        // Test knn search with model text with filters
        {
            var response = knnModelTextSearchWithTermsFilter(indexName, inputs.get(0), "foo", modelId, "embedding");
            assertOkWithErrorMessage(response);

            Map<String, Object> responseMap = responseAsMap(response);
            List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
            assertThat(hits, hasSize(3));
            for (var hit : hits) {
                String filter = (String) MapHelper.dig("_source.filter_field", hit);
                assertEquals("foo", filter);
            }
        }
        {
            var response = knnModelTextSearchWithTermsFilter(indexName, inputs.get(2), "baz", modelId, "embedding");
            assertOkWithErrorMessage(response);

            Map<String, Object> responseMap = responseAsMap(response);
            List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
            assertThat(hits, hasSize(2));
            for (var hit : hits) {
                String filter = (String) MapHelper.dig("_source.filter_field", hit);
                assertEquals("baz", filter);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testHybridSearch() throws IOException {
        String modelId = "hybrid-semantic-search-test";
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
        List<String> filters = List.of("foo", "bar", "baz", "foo", "bar", "baz", "foo");
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
        bulkIndexDocs(inputs, filters, embeddings, indexName);
        forceMergeIndex(indexName);

        {
            // combined query should return size documents where size > k
            Request request = new Request("GET", indexName + "/_search");
            request.setJsonEntity(Strings.format("""
                {
                     "query": {
                         "bool": {
                             "should": [
                                 {
                                     "match_all": {}
                                 },
                                 {
                                     "knn": {
                                         "field": "embedding",
                                         "query_vector_builder": {
                                             "text_embedding": {
                                                 "model_id": "%s",
                                                 "model_text": "my words"
                                             }
                                         },
                                         "num_candidates": 10,
                                         "boost": 10
                                     }
                                 }
                             ]
                         }
                     },
                     "size": 7
                 }""", modelId));
            var semanticSearchResponse = client().performRequest(request);
            assertOkWithErrorMessage(semanticSearchResponse);

            Map<String, Object> responseMap = responseAsMap(semanticSearchResponse);
            int hitCount = (Integer) MapHelper.dig("hits.total.value", responseMap);
            assertEquals(7, hitCount);
        }
        {
            // boost the knn score, as the query is an exact match the unboosted
            // score should be close to 1.0. Use an unrelated query so scores are
            // not combined
            Request request = new Request("GET", indexName + "/_search");
            request.setJsonEntity(Strings.format("""
                {
                     "query": {
                         "bool": {
                             "should": [
                                 {
                                     "match": {
                                         "source_text": {
                                             "query": "apricot unrelated",
                                             "boost": 1
                                         }
                                     }
                                 },
                                 {
                                     "knn": {
                                         "field": "embedding",
                                         "query_vector_builder": {
                                             "text_embedding": {
                                                 "model_id": "%s",
                                                 "model_text": "my words"
                                             }
                                         },
                                         "num_candidates": 10,
                                         "boost": 10
                                     }
                                 }
                             ]
                         }
                     },
                     "size": 7
                 }""", modelId));
            var semanticSearchResponse = client().performRequest(request);
            assertOkWithErrorMessage(semanticSearchResponse);

            Map<String, Object> responseMap = responseAsMap(semanticSearchResponse);
            List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
            boolean found = false;
            for (var hit : hits) {
                String source = (String) MapHelper.dig("_source.source_text", hit);
                if (source.equals("my words")) {
                    assertThat((Double) MapHelper.dig("_score", hit), closeTo(10.0, 0.01));
                    found = true;
                }
            }
            assertTrue("should have found hit for string 'my words'", found);
        }
    }

    public void testSearchWithMissingModel() {
        String modelId = "missing-model";
        String indexName = modelId + "-index";

        var e = expectThrows(ResponseException.class, () -> knnModelTextSearch(indexName, "the machine is leaking", modelId, "embedding"));
        assertThat(e.getMessage(), containsString("[missing-model] is not an inference service model or a deployed ml model"));
    }

    @SuppressWarnings("unchecked")
    public void testModelWithPrefixStrings() throws IOException {
        String modelId = "model-with-prefix-strings";
        String ingestPrefix = "passage: ";
        String searchPrefix = "query: ";

        createTextEmbeddingModelWithPrefixString(modelId, searchPrefix, ingestPrefix);
        putModelDefinition(modelId, BASE_64_ENCODED_MODEL, RAW_MODEL_SIZE);
        putVocabulary(
            List.of(
                "these",
                "are",
                "my",
                "words",
                "the",
                "washing",
                "machine",
                "is",
                "leaking",
                "octopus",
                "comforter",
                "smells",
                ingestPrefix,
                searchPrefix
            ),
            modelId
        );
        startDeployment(modelId);

        String pipelineDefinition = Strings.format("""
            {
              "processors": [
                {
                  "inference": {
                    "model_id": "%s",
                    "input_output": {
                      "input_field": "source_text",
                      "output_field": "embedding"
                    },
                    "inference_config": {
                      "text_embedding": {
                      }
                    }
                  }
                }
              ]
            }
            """, modelId);

        String docSource = """
            [
                {"_source": {
                  "source_text": "the washing machine is leaking"}}
            ]
            """;

        // At ingest the prefix is automatically added
        var simulateResponse = simulatePipeline(pipelineDefinition, docSource);
        var simulateResponseMap = entityAsMap(simulateResponse);
        var simulatedDocs = (List<Map<String, Object>>) simulateResponseMap.get("docs");
        List<Double> pipelineEmbedding = (List<Double>) MapHelper.dig("doc._source.embedding", simulatedDocs.get(0));
        assertNotNull(simulateResponseMap.toString(), pipelineEmbedding);

        // Create the embedding for the same input text used in
        // simulate pipeline ingest. Here the ingest prefix is
        // manually added, the resulting embeddings should be
        // the same.
        var inferenceResponse = infer(ingestPrefix + "the washing machine is leaking", modelId);
        Map<String, Object> inferenceResult = ((List<Map<String, Object>>) entityAsMap(inferenceResponse).get("inference_results")).get(0);
        List<Double> inferenceEmbedding = (List<Double>) inferenceResult.get("predicted_value");
        assertNotNull(inferenceResult.toString(), inferenceEmbedding);
        // embeddings are exactly equal
        assertEquals(inferenceEmbedding, pipelineEmbedding);

        // Now check the search prefix
        List<String> inputs = List.of(
            searchPrefix + "my words",
            "the machine is leaking",
            "washing machine",
            "these are my words",
            "the octopus comforter smells"
        );
        List<String> filters = List.of("foo", "bar", "baz", "foo", "bar");
        List<List<Double>> embeddings = new ArrayList<>();

        // Generate the text embeddings via the inference API
        // then index them for search
        for (var input : inputs) {
            Response inference = infer(input, modelId);
            List<Map<String, Object>> responseMap = (List<Map<String, Object>>) entityAsMap(inference).get("inference_results");
            List<Double> embedding = (List<Double>) responseMap.get(0).get("predicted_value");
            embeddings.add(embedding);
        }

        // index dense vectors
        String indexName = modelId + "_index";
        createVectorSearchIndex(indexName);
        bulkIndexDocs(inputs, filters, embeddings, indexName);
        forceMergeIndex(indexName);

        // the input "my words" should be prefixed with searchPrefix
        var response = knnModelTextSearch(indexName, "my words", modelId, "embedding");
        assertOkWithErrorMessage(response);

        Map<String, Object> responseMap = responseAsMap(response);
        List<Map<String, Object>> hits = (List<Map<String, Object>>) MapHelper.dig("hits.hits", responseMap);
        Map<String, Object> topHit = hits.get(0);
        String sourceText = (String) MapHelper.dig("_source.source_text", topHit);
        // The top hit should have the search prefix
        assertEquals(searchPrefix + "my words", sourceText);
        List<Double> foundEmbedding = (List<Double>) MapHelper.dig("_source.embedding", topHit);
        assertEquals(embeddings.get(0), foundEmbedding);
    }

    protected Response knnModelTextSearch(String index, String modelText, String modelId, String denseVectorFieldName) throws IOException {
        Request request = new Request("GET", index + "/_search?error_trace=true");

        request.setJsonEntity(Strings.format("""
            {
              "query": {
                  "knn" : {
                      "field": "%s",
                      "num_candidates": 10,
                      "query_vector_builder": {
                        "text_embedding": {
                          "model_id": "%s",
                          "model_text": "%s"
                        }
                      }
                  }
              }
            }""", denseVectorFieldName, modelId, modelText));
        return client().performRequest(request);
    }

    protected Response knnModelTextSearchWithTermsFilter(
        String index,
        String modelText,
        String filter,
        String modelId,
        String denseVectorFieldName
    ) throws IOException {
        Request request = new Request("GET", index + "/_search?error_trace=true");

        String termsFilter = Strings.format("""
            {"term": {"filter_field": "%s"}}
            """, filter);

        request.setJsonEntity(Strings.format("""
            {
              "knn": {
                  "field": "%s",
                  "num_candidates": 10,
                  "filter": %s,
                  "query_vector_builder": {
                    "text_embedding": {
                      "model_id": "%s",
                      "model_text": "%s"
                    }
                  }
              }
            }""", denseVectorFieldName, termsFilter, modelId, modelText));
        return client().performRequest(request);
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
                  "filter_field": {
                    "type": "keyword"
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

    private void bulkIndexDocs(List<String> sourceText, List<String> filters, List<List<Double>> embeddings, String indexName)
        throws IOException {
        String createAction = "{\"create\": {\"_index\": \"" + indexName + "\"}}\n";

        StringBuilder bulkBuilder = new StringBuilder();

        for (int i = 0; i < sourceText.size(); i++) {
            bulkBuilder.append(createAction);
            bulkBuilder.append("{\"source_text\": \"")
                .append(sourceText.get(i))
                .append("\", \"filter_field\":\"")
                .append(filters.get(i))
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

    protected void createTextEmbeddingModelWithPrefixString(String modelId, String searchPrefix, String ingestPrefix) throws IOException {
        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
        request.setJsonEntity(Strings.format("""
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
               },
               "prefix_strings": {
                 "search": "%s",
                 "ingest": "%s"
               }
             }""", searchPrefix, ingestPrefix));
        client().performRequest(request);
    }
}
