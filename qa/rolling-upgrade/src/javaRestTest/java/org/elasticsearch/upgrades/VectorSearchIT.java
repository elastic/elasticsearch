/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class VectorSearchIT extends AbstractRollingUpgradeTestCase {
    public VectorSearchIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    private static final String FLOAT_INDEX_NAME = "float_vector_index";
    private static final String SCRIPT_VECTOR_INDEX_NAME = "script_vector_index";
    private static final String SCRIPT_BYTE_INDEX_NAME = "script_byte_vector_index";
    private static final String BYTE_INDEX_NAME = "byte_vector_index";
    private static final String QUANTIZED_INDEX_NAME = "quantized_vector_index";
    private static final String BBQ_INDEX_NAME = "bbq_vector_index";
    private static final String FLAT_QUANTIZED_INDEX_NAME = "flat_quantized_vector_index";
    private static final String FLAT_BBQ_INDEX_NAME = "flat_bbq_vector_index";
    private static final String FLOAT_VECTOR_SEARCH_VERSION = "8.4.0";
    private static final String BYTE_VECTOR_SEARCH_VERSION = "8.6.0";
    private static final String QUANTIZED_VECTOR_SEARCH_VERSION = "8.12.1";
    private static final String FLAT_QUANTIZED_VECTOR_SEARCH_VERSION = "8.13.0";
    private static final String BBQ_VECTOR_SEARCH_VERSION = "8.18.0";

    public void testScriptByteVectorSearch() throws Exception {
        assumeTrue("byte vector search is not supported on this version", getOldClusterTestVersion().onOrAfter(BYTE_VECTOR_SEARCH_VERSION));
        if (isOldCluster()) {
            // create index and index 10 random floating point vectors
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "element_type": "byte",
                      "index": false
                    }
                  }
                }
                """;
            createIndex(SCRIPT_BYTE_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(SCRIPT_BYTE_INDEX_NAME);
        }
        // search with a script query
        Request searchRequest = new Request("POST", "/" + SCRIPT_BYTE_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "script_score": {
                  "query": {
                    "exists": {
                      "field": "vector"
                    }
                  },
                  "script": {
                    "source": "cosineSimilarity(params.query, 'vector') + 1.0",
                    "params": {
                      "query": [4, 5, 6]
                    }
                  }
                }
              }
            }
            """);
        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(7));
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("0"));
        assertThat((double) hits.get(0).get("_score"), closeTo(1.9869276, 0.0001));
    }

    public void testScriptVectorSearch() throws Exception {
        assumeTrue(
            "Float vector search is not supported on this version",
            getOldClusterTestVersion().onOrAfter(FLOAT_VECTOR_SEARCH_VERSION)
        );
        if (isOldCluster()) {
            // create index and index 10 random floating point vectors
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "index": false
                    }
                  }
                }
                """;
            createIndex(SCRIPT_VECTOR_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(SCRIPT_VECTOR_INDEX_NAME);
        }
        // search with a script query
        Request searchRequest = new Request("POST", "/" + SCRIPT_VECTOR_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "script_score": {
                  "query": {
                    "exists": {
                      "field": "vector"
                    }
                  },
                  "script": {
                    "source": "cosineSimilarity(params.query, 'vector') + 1.0",
                    "params": {
                      "query": [4, 5, 6]
                    }
                  }
                }
              }
            }
            """);
        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(7));
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("0"));
        assertThat((double) hits.get(0).get("_score"), closeTo(1.9869276, 0.0001));
    }

    public void testFloatVectorSearch() throws Exception {
        assumeTrue(
            "Float vector search is not supported on this version",
            getOldClusterTestVersion().onOrAfter(FLOAT_VECTOR_SEARCH_VERSION)
        );
        if (isOldCluster()) {
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "index": true,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "hnsw",
                        "ef_construction": 100,
                        "m": 16
                      }
                    }
                  }
                }
                """;
            // create index and index 10 random floating point vectors
            createIndex(FLOAT_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(FLOAT_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + FLOAT_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }
        // search with a script query
        Request searchRequest = new Request("POST", "/" + FLOAT_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "script_score": {
                  "query": {
                    "exists": {
                      "field": "vector"
                    }
                  },
                  "script": {
                   "source": "cosineSimilarity(params.query, 'vector') + 1.0",
                    "params": {
                      "query": [4, 5, 6]
                    }
                  }
                }
              }
            }
            """);
        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(7));
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("0"));
        assertThat((double) hits.get(0).get("_score"), closeTo(1.9869276, 0.0001));

        // search with knn
        searchRequest = new Request("POST", "/" + FLOAT_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
                "knn": {
                "field": "vector",
                  "query_vector": [4, 5, 6],
                  "k": 2,
                  "num_candidates": 5
                }
            }
            """);
        response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(2));
        hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("2"));
        assertThat((double) hits.get(0).get("_score"), closeTo(0.028571429, 0.0001));
    }

    public void testByteVectorSearch() throws Exception {
        assumeTrue("Byte vector search is not supported on this version", getOldClusterTestVersion().onOrAfter(BYTE_VECTOR_SEARCH_VERSION));
        if (isOldCluster()) {
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "element_type": "byte",
                      "index": true,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "hnsw",
                        "ef_construction": 100,
                        "m": 16
                      }
                    }
                  }
                }
                """;
            // create index and index 10 random floating point vectors
            createIndex(BYTE_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(BYTE_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + BYTE_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }
        // search with a script query
        Request searchRequest = new Request("POST", "/" + BYTE_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "script_score": {
                  "query": {
                    "exists": {
                      "field": "vector"
                    }
                  },
                  "script": {
                   "source": "cosineSimilarity(params.query, 'vector') + 1.0",
                    "params": {
                      "query": [4, 5, 6]
                    }
                  }
                }
              }
            }
            """);
        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(7));
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("0"));
        assertThat((double) hits.get(0).get("_score"), closeTo(1.9869276, 0.0001));

        // search with knn
        searchRequest = new Request("POST", "/" + BYTE_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
                "knn": {
                "field": "vector",
                  "query_vector": [4, 5, 6],
                  "k": 2,
                  "num_candidates": 5
                }
            }
            """);
        response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(2));
        hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("2"));
        assertThat((double) hits.get(0).get("_score"), closeTo(0.028571429, 0.0001));
    }

    public void testQuantizedVectorSearch() throws Exception {
        assumeTrue(
            "Quantized vector search is not supported on this version",
            getOldClusterTestVersion().onOrAfter(QUANTIZED_VECTOR_SEARCH_VERSION)
        );
        if (isOldCluster()) {
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "index": true,
                      "similarity": "cosine",
                      "index_options": {
                        "type": "int8_hnsw",
                        "ef_construction": 100,
                        "m": 16
                      }
                    }
                  }
                }
                """;
            // create index and index 10 random floating point vectors
            createIndex(QUANTIZED_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(QUANTIZED_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + QUANTIZED_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }
        Request searchRequest = new Request("POST", "/" + QUANTIZED_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "script_score": {
                  "query": {
                    "exists": {
                      "field": "vector"
                    }
                  },
                  "script": {
                   "source": "cosineSimilarity(params.query, 'vector') + 1.0",
                    "params": {
                      "query": [4, 5, 6]
                    }
                  }
                }
              }
            }
            """);
        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(7));
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("0"));
        assertThat((double) hits.get(0).get("_score"), closeTo(1.9869276, 0.0001));

        // search with knn
        searchRequest = new Request("POST", "/" + QUANTIZED_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
                "knn": {
                "field": "vector",
                  "query_vector": [4, 5, 6],
                  "k": 2,
                  "num_candidates": 5
                }
            }
            """);
        response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(2));
        hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("0"));
        assertThat((double) hits.get(0).get("_score"), closeTo(0.9934857, 0.005));
    }

    public void testFlatQuantizedVectorSearch() throws Exception {
        assumeTrue(
            "Quantized vector search is not supported on this version",
            getOldClusterTestVersion().onOrAfter(FLAT_QUANTIZED_VECTOR_SEARCH_VERSION)
        );
        if (isOldCluster()) {
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "index": true,
                      "similarity": "cosine",
                      "index_options": {
                        "type": "int8_flat"
                      }
                    }
                  }
                }
                """;
            // create index and index 10 random floating point vectors
            createIndex(FLAT_QUANTIZED_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(FLAT_QUANTIZED_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + FLAT_QUANTIZED_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }
        Request searchRequest = new Request("POST", "/" + FLAT_QUANTIZED_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "script_score": {
                  "query": {
                    "exists": {
                      "field": "vector"
                    }
                  },
                  "script": {
                   "source": "cosineSimilarity(params.query, 'vector') + 1.0",
                    "params": {
                      "query": [4, 5, 6]
                    }
                  }
                }
              }
            }
            """);
        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(7));
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("0"));
        assertThat((double) hits.get(0).get("_score"), closeTo(1.9869276, 0.0001));

        // search with knn
        searchRequest = new Request("POST", "/" + FLAT_QUANTIZED_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
                "knn": {
                "field": "vector",
                  "query_vector": [4, 5, 6],
                  "k": 2,
                  "num_candidates": 5
                }
            }
            """);
        response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(2));
        hits = extractValue(response, "hits.hits");
        assertThat(hits.get(0).get("_id"), equalTo("0"));
        assertThat((double) hits.get(0).get("_score"), closeTo(0.9934857, 0.005));
    }

    public void testBBQVectorSearch() throws Exception {
        assumeTrue(
            "Quantized vector search is not supported on this version",
            getOldClusterTestVersion().onOrAfter(BBQ_VECTOR_SEARCH_VERSION)
        );
        if (isOldCluster()) {
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 64,
                      "index": true,
                      "similarity": "cosine",
                      "index_options": {
                        "type": "bbq_hnsw",
                        "ef_construction": 100,
                        "m": 16
                      }
                    }
                  }
                }
                """;
            // create index and index 10 random floating point vectors
            createIndex(
                BBQ_INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(),
                mapping
            );
            index64DimVectors(BBQ_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + BBQ_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }
        Request searchRequest = new Request("POST", "/" + BBQ_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "script_score": {
                  "query": {
                    "exists": {
                      "field": "vector"
                    }
                  },
                  "script": {
                   "source": "cosineSimilarity(params.query, 'vector') + 1.0",
                    "params": {
                      "query": [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
                       5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
                    }
                  }
                }
              }
            }
            """);
        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(7));
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat("hits: " + response, hits.get(0).get("_id"), equalTo("0"));
        assertThat("hits: " + response, (double) hits.get(0).get("_score"), closeTo(1.9869276, 0.0001));

        // search with knn
        searchRequest = new Request("POST", "/" + BBQ_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
                "knn": {
                "field": "vector",
                  "query_vector": [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
                   5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6],
                  "k": 2,
                  "num_candidates": 5
                }
            }
            """);
        response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(2));
        hits = extractValue(response, "hits.hits");
        assertThat("expected: 0 received" + hits.get(0).get("_id") + " hits: " + response, hits.get(0).get("_id"), equalTo("0"));
        assertThat(
            "expected_near: 0.99 received" + hits.get(0).get("_score") + "hits: " + response,
            (double) hits.get(0).get("_score"),
            closeTo(0.9934857, 0.005)
        );
    }

    public void testFlatBBQVectorSearch() throws Exception {
        assumeTrue(
            "Quantized vector search is not supported on this version",
            getOldClusterTestVersion().onOrAfter(BBQ_VECTOR_SEARCH_VERSION)
        );
        if (isOldCluster()) {
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 64,
                      "index": true,
                      "similarity": "cosine",
                      "index_options": {
                        "type": "bbq_flat"
                      }
                    }
                  }
                }
                """;
            // create index and index 10 random floating point vectors
            createIndex(
                FLAT_BBQ_INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(),
                mapping
            );
            index64DimVectors(FLAT_BBQ_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + FLAT_BBQ_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }
        Request searchRequest = new Request("POST", "/" + FLAT_BBQ_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "script_score": {
                  "query": {
                    "exists": {
                      "field": "vector"
                    }
                  },
                  "script": {
                   "source": "cosineSimilarity(params.query, 'vector') + 1.0",
                    "params": {
                      "query": [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
                       5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
                    }
                  }
                }
              }
            }
            """);
        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(7));
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat("hits: " + response, hits.get(0).get("_id"), equalTo("0"));
        assertThat("hits: " + response, (double) hits.get(0).get("_score"), closeTo(1.9869276, 0.0001));

        // search with knn
        searchRequest = new Request("POST", "/" + FLAT_BBQ_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
                "knn": {
                "field": "vector",
                  "query_vector": [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
                   5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6],
                  "k": 2,
                  "num_candidates": 5
                }
            }
            """);
        response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(2));
        hits = extractValue(response, "hits.hits");
        assertThat("expected: 0 received" + hits.get(0).get("_id") + " hits: " + response, hits.get(0).get("_id"), equalTo("0"));
        assertThat(
            "expected_near: 0.99 received" + hits.get(0).get("_score") + "hits: " + response,
            (double) hits.get(0).get("_score"),
            closeTo(0.9934857, 0.005)
        );
    }

    private void index64DimVectors(String indexName) throws Exception {
        String[] vectors = new String[] {
            "{\"vector\":[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "
                + "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}",
            "{\"vector\":[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "
                + "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]}",
            "{\"vector\":[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "
                + "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]}",
            "{\"vector\":[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, "
                + "2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}",
            "{\"vector\":[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, "
                + "3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}",
            "{\"vector\":[2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "
                + "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}",
            "{\"vector\":[3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "
                + "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}",
            "{}" };
        for (int i = 0; i < vectors.length; i++) {
            Request indexRequest = new Request("PUT", "/" + indexName + "/_doc/" + i);
            indexRequest.setJsonEntity(vectors[i]);
            assertOK(client().performRequest(indexRequest));
        }
        // always refresh to ensure the data is visible
        flush(indexName, true);
        refresh(indexName);
    }

    private void indexVectors(String indexName) throws Exception {
        String[] vectors = new String[] {
            "{\"vector\":[1, 1, 1]}",
            "{\"vector\":[1, 1, 2]}",
            "{\"vector\":[1, 1, 3]}",
            "{\"vector\":[1, 2, 1]}",
            "{\"vector\":[1, 3, 1]}",
            "{\"vector\":[2, 1, 1]}",
            "{\"vector\":[3, 1, 1]}",
            "{}" };
        for (int i = 0; i < vectors.length; i++) {
            Request indexRequest = new Request("PUT", "/" + indexName + "/_doc/" + i);
            indexRequest.setJsonEntity(vectors[i]);
            assertOK(client().performRequest(indexRequest));
        }
        // always refresh to ensure the data is visible
        refresh(indexName);
    }

    private static Map<String, Object> search(Request request) throws IOException {
        final Response response = client().performRequest(request);
        assertOK(response);
        return responseAsMap(response);
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
