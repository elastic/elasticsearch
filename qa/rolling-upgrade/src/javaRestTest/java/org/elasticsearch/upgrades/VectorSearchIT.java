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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
    private static final String HNSW_BIT_INDEX_NAME = "hnsw_bit_vector_index";
    private static final String FLAT_BIT_INDEX_NAME = "flat_bit_vector_index";
    private static final String POST_FILTER_INDEX_NAME = "post_filter_vector_index";
    private static final int POST_FILTER_DOC_COUNT = 200;

    // TODO: replace these with proper test features
    private static final String FLOAT_VECTOR_SEARCH_TEST_FEATURE = "gte_v8.4.0";
    private static final String BYTE_VECTOR_SEARCH_TEST_FEATURE = "gte_v8.6.0";
    private static final String QUANTIZED_VECTOR_SEARCH_TEST_FEATURE = "gte_v8.12.1";
    private static final String FLAT_QUANTIZED_VECTOR_SEARCH_TEST_FEATURE = "gte_v8.13.0";
    private static final String BBQ_VECTOR_SEARCH_TEST_FEATURE = "gte_v8.18.0";
    private static final String BIT_VECTOR_SEARCH_TEST_FEATURE = "gte_v8.15.0";

    public void testBitVectors() throws Exception {
        assumeTrue("Bit vector search is not supported on this version", oldClusterHasFeature(BIT_VECTOR_SEARCH_TEST_FEATURE));
        if (isOldCluster()) {
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 24,
                      "element_type": "bit",
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
            createIndex(HNSW_BIT_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(HNSW_BIT_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + HNSW_BIT_INDEX_NAME + "/_forcemerge?max_num_segments=1"));

            mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 24,
                      "element_type": "bit",
                      "index": true,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "flat"
                      }
                    }
                  }
                }
                """;
            // create index and index 10 random floating point vectors
            createIndex(FLAT_BIT_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(FLAT_BIT_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + FLAT_BIT_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }

        // search hnsw index first
        {
            Request searchRequest = new Request("POST", "/" + HNSW_BIT_INDEX_NAME + "/_search");
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
                       "source": "hamming(params.query, 'vector')",
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
            assertThat(hits.get(0).get("_id"), equalTo("3"));
            assertThat((double) hits.get(0).get("_score"), closeTo(8.0, 0.0001));

            // search with knn
            searchRequest = new Request("POST", "/" + HNSW_BIT_INDEX_NAME + "/_search");
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
            assertThat(hits.get(0).get("_id"), equalTo("1"));
            assertThat((double) hits.get(0).get("_score"), closeTo(0.8333333, 0.0001));
        }
        // search the flat
        {
            Request searchRequest = new Request("POST", "/" + FLAT_BIT_INDEX_NAME + "/_search");
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
                       "source": "hamming(params.query, 'vector')",
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
            assertThat(hits.get(0).get("_id"), equalTo("3"));
            assertThat((double) hits.get(0).get("_score"), closeTo(8.0, 0.0001));

            // search with knn
            searchRequest = new Request("POST", "/" + FLAT_BIT_INDEX_NAME + "/_search");
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
            assertThat(hits.get(0).get("_id"), equalTo("1"));
            assertThat((double) hits.get(0).get("_score"), closeTo(0.833333, 0.0001));
        }
    }

    public void testScriptByteVectorSearch() throws Exception {
        assumeTrue("byte vector search is not supported on this version", oldClusterHasFeature(BYTE_VECTOR_SEARCH_TEST_FEATURE));
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
        assumeTrue("Float vector search is not supported on this version", oldClusterHasFeature(FLOAT_VECTOR_SEARCH_TEST_FEATURE));
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
        assumeTrue("Float vector search is not supported on this version", oldClusterHasFeature(FLOAT_VECTOR_SEARCH_TEST_FEATURE));
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
        assumeTrue("Byte vector search is not supported on this version", oldClusterHasFeature(BYTE_VECTOR_SEARCH_TEST_FEATURE));
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
        assumeTrue("Quantized vector search is not supported on this version", oldClusterHasFeature(QUANTIZED_VECTOR_SEARCH_TEST_FEATURE));
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
            oldClusterHasFeature(FLAT_QUANTIZED_VECTOR_SEARCH_TEST_FEATURE)
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
        assumeTrue("Quantized vector search is not supported on this version", oldClusterHasFeature(BBQ_VECTOR_SEARCH_TEST_FEATURE));
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
        assumeTrue("Quantized vector search is not supported on this version", oldClusterHasFeature(BBQ_VECTOR_SEARCH_TEST_FEATURE));
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

    /**
     * Filtered kNN search on an index created before the upgrade. Every tenth document is tagged {@code rare}, so a filter on
     * {@code common} matches 90% of the vectors. In the old and mixed clusters the search only has to keep returning the right
     * documents, whichever node serves it. Once every node is upgraded, the same search is post-filtered under the new default,
     * with no change to the index. The kNN profile shows the path: a regular filtered search first materializes the filter into
     * a bitset ({@code into_bit_set_count > 0}), a post-filtered search never does. Updating the threshold on a live index is
     * covered by {@code PostFilterHnswKnnSearchIT}.
     */
    public void testFilteredKnnPostFiltering() throws Exception {
        assumeTrue("Float vector search is not supported on this version", oldClusterHasFeature(FLOAT_VECTOR_SEARCH_TEST_FEATURE));
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
                    },
                    "tag": {
                      "type": "keyword"
                    }
                  }
                }
                """;
            // a single shard keeps the filter's selectivity exactly 0.9
            createIndex(POST_FILTER_INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build(), mapping);
            StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < POST_FILTER_DOC_COUNT; i++) {
                bulk.append(Strings.format("{\"index\":{\"_id\":\"%d\"}}\n", i));
                bulk.append(Strings.format("{\"vector\":[%d, %d, %d], \"tag\":\"%s\"}\n", i, i, i, i % 10 == 0 ? "rare" : "common"));
            }
            Request bulkRequest = new Request("POST", "/" + POST_FILTER_INDEX_NAME + "/_bulk");
            bulkRequest.addParameter("refresh", "true");
            bulkRequest.setJsonEntity(bulk.toString());
            assertOK(client().performRequest(bulkRequest));
            client().performRequest(new Request("POST", "/" + POST_FILTER_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }

        filteredKnnSearch(false);

        if (isUpgradedCluster()) {
            // The index was created before the setting existed, and is post-filtered under the new default. Post-filtering never
            // turns the filter into a bitset, while filtering during the search always does.
            assertFalse(builtBitSet(knnProfile(filteredKnnSearch(true))));
        }
    }

    /**
     * Searches for the four vectors nearest to {@code [100, 100, 100]} among the {@code common} documents. Document 100 is the
     * nearest overall but is {@code rare}, so the filter must leave it out on either path.
     */
    private static Map<String, Object> filteredKnnSearch(boolean profile) throws IOException {
        Request searchRequest = new Request("POST", "/" + POST_FILTER_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity(Strings.format("""
            {
              "profile": %s,
              "knn": {
                "field": "vector",
                "query_vector": [100, 100, 100],
                "k": 4,
                "num_candidates": 100,
                "filter": {
                  "term": {
                    "tag": "common"
                  }
                }
              }
            }
            """, profile));
        Map<String, Object> response = search(searchRequest);
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits.stream().map(hit -> hit.get("_id")).toList(), containsInAnyOrder("98", "99", "101", "102"));
        return response;
    }

    /** The profiled queries of the kNN search, on the single shard of the index. */
    private static List<Map<String, Object>> knnProfile(Map<String, Object> searchResponse) throws IOException {
        return ObjectPath.evaluate(searchResponse, "profile.shards.0.dfs.knn.0.query");
    }

    /** Whether any of these queries, or the queries nested in them, turned its matches into a bitset. */
    @SuppressWarnings("unchecked")
    private static boolean builtBitSet(List<Map<String, Object>> queries) {
        for (Map<String, Object> query : queries) {
            Map<String, Object> breakdown = (Map<String, Object>) query.get("breakdown");
            List<Map<String, Object>> children = (List<Map<String, Object>>) query.getOrDefault("children", List.of());
            if (((Number) breakdown.getOrDefault("into_bit_set_count", 0)).longValue() > 0 || builtBitSet(children)) {
                return true;
            }
        }
        return false;
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
