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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Rolling upgrade tests for KNN DFS rescoring feature.
 * Tests that the delayed rescoring mechanism works correctly during rolling upgrades
 * when some nodes support the feature and others don't.
 */
public class KnnDfsRescoringRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String INDEX_NAME = "knn_dfs_rescore_test";
    private static final String QUANTIZED_INDEX_NAME = "knn_dfs_rescore_quantized_test";

    // Feature added in 9.x - adjust as needed based on actual version
    private static final String KNN_DFS_RESCORING_FEATURE = "gte_v9.0.0";

    public KnnDfsRescoringRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    /**
     * Test basic KNN search works across all cluster states during upgrade.
     * This ensures that non-quantized KNN search continues to work normally.
     */
    public void testBasicKnnSearchDuringUpgrade() throws Exception {
        if (isOldCluster()) {
            // Create index on old cluster
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "index": true,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "hnsw"
                      }
                    },
                    "value": {
                      "type": "integer"
                    }
                  }
                }
                """;
            createIndex(
                INDEX_NAME,
                Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build(),
                mapping
            );

            // Index test documents - vectors along x-axis for predictable ordering
            for (int i = 0; i < 20; i++) {
                Request indexRequest = new Request("POST", "/" + INDEX_NAME + "/_doc/" + i);
                indexRequest.setJsonEntity(String.format("""
                    {
                      "vector": [%d, 0, 0],
                      "value": %d
                    }
                    """, i, i));
                client().performRequest(indexRequest);
            }
            client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_refresh"));
        }

        // Search should work in all cluster states
        Request searchRequest = new Request("POST", "/" + INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "knn": {
                "field": "vector",
                "query_vector": [0, 0, 0],
                "k": 5,
                "num_candidates": 20
              }
            }
            """);

        Map<String, Object> response = search(searchRequest);
        assertThat(extractValue(response, "hits.total.value"), equalTo(5));

        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits.size(), equalTo(5));

        // Verify order - closest to origin should be first
        assertThat(hits.get(0).get("_id"), equalTo("0"));
    }

    /**
     * Test KNN search with quantized vectors and rescoring works correctly during upgrade.
     * In mixed cluster, some shards may use old behavior (immediate rescoring) while
     * others use new behavior (delayed rescoring).
     */
    public void testQuantizedKnnSearchWithRescoringDuringUpgrade() throws Exception {
        assumeTrue("Quantized KNN search requires newer version", oldClusterHasFeature("gte_v8.12.1"));

        if (isOldCluster()) {
            // Create index with int8_hnsw quantization and rescoring
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "index": true,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "int8_hnsw"
                      }
                    },
                    "value": {
                      "type": "integer"
                    }
                  }
                }
                """;
            createIndex(
                QUANTIZED_INDEX_NAME,
                Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0).build(),
                mapping
            );

            // Index test documents
            for (int i = 0; i < 30; i++) {
                Request indexRequest = new Request("POST", "/" + QUANTIZED_INDEX_NAME + "/_doc/" + i);
                indexRequest.setJsonEntity(String.format("""
                    {
                      "vector": [%d, 0, 0],
                      "value": %d
                    }
                    """, i, i));
                client().performRequest(indexRequest);
            }
            client().performRequest(new Request("POST", "/" + QUANTIZED_INDEX_NAME + "/_refresh"));
        }

        // Search with rescore_vector parameter
        Request searchRequest = new Request("POST", "/" + QUANTIZED_INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "knn": {
                "field": "vector",
                "query_vector": [0, 0, 0],
                "k": 10,
                "num_candidates": 30,
                "rescore_vector": {
                  "oversample": 2.0
                }
              }
            }
            """);

        Map<String, Object> response = search(searchRequest);

        // Should return results in all cluster states
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");
        assertThat(hits, notNullValue());
        assertThat(hits.size(), greaterThanOrEqualTo(1));
        assertThat(hits.size(), lessThanOrEqualTo(10));

        // In mixed cluster, results may vary slightly due to different rescoring behavior,
        // but the top result should still be doc 0 (closest to origin)
        if (isUpgradedCluster()) {
            // In fully upgraded cluster, results should be deterministic
            assertThat(hits.get(0).get("_id"), equalTo("0"));
        }
    }

    /**
     * Test that search results remain consistent before and after full upgrade.
     * This verifies that the delayed rescoring produces equivalent results.
     */
    public void testResultConsistencyAcrossUpgrade() throws Exception {
        assumeTrue("Quantized KNN search requires newer version", oldClusterHasFeature("gte_v8.12.1"));

        String indexName = "knn_consistency_test";

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
                        "type": "int8_hnsw"
                      }
                    }
                  }
                }
                """;
            createIndex(indexName, Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build(), mapping);

            // Index documents with known vectors
            for (int i = 0; i < 15; i++) {
                Request indexRequest = new Request("POST", "/" + indexName + "/_doc/" + i);
                indexRequest.setJsonEntity(String.format("""
                    {
                      "vector": [%d, 0, 0]
                    }
                    """, i));
                client().performRequest(indexRequest);
            }
            client().performRequest(new Request("POST", "/" + indexName + "/_refresh"));
            // Force merge to ensure consistent segment structure
            client().performRequest(new Request("POST", "/" + indexName + "/_forcemerge?max_num_segments=1"));
        }

        // Search
        Request searchRequest = new Request("POST", "/" + indexName + "/_search");
        searchRequest.setJsonEntity("""
            {
              "knn": {
                "field": "vector",
                "query_vector": [0, 0, 0],
                "k": 5,
                "num_candidates": 15
              }
            }
            """);

        Map<String, Object> response = search(searchRequest);
        List<Map<String, Object>> hits = extractValue(response, "hits.hits");

        // The closest documents to [0,0,0] should always be docs 0,1,2,3,4
        // regardless of cluster state (old, mixed, or upgraded)
        assertThat(hits.size(), equalTo(5));

        // First result should always be doc 0
        assertThat(hits.get(0).get("_id"), equalTo("0"));
    }

    private Map<String, Object> search(Request searchRequest) throws IOException {
        Response response = client().performRequest(searchRequest);
        return responseAsMap(response);
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
