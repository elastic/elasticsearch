/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class DiskBBQVectorSearchIT extends AbstractUpgradeTestCase {

    private static final String BBQ_DISK_SUPPORT_FEATURE = "mapper.bbq_disk_support";
    private static final String ES940_DISK_BBQ_FEATURE = "mapper.es940_disk_bbq";
    private static final String DISK_BBQ_INDEX_NAME = "diskbbq_vectors_index";
    private static final String DISK_BBQ_BITS_INDEX_PREFIX = "diskbbq_vectors_bits_index_";
    private static final int[] SUPPORTED_BITS = new int[] { 1, 2, 4, 7 };

    public void testSingleBitDiskBBQVectorSearch() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            assumeTrue("DiskBBQ vector format is not supported on this version", clusterSupportsFeature(BBQ_DISK_SUPPORT_FEATURE));
            String mapping = """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 3,
                      "index": true,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "bbq_disk"
                      }
                    }
                  }
                }
                """;
            // create index and index 10 random floating point vectors
            createIndexWithDenseVectorSettings(DISK_BBQ_INDEX_NAME, mapping);
            indexVectors(DISK_BBQ_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + DISK_BBQ_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        } else {
            assumeTrue("DiskBBQ index was not created on the old cluster", indexExistsOnCluster(DISK_BBQ_INDEX_NAME));
        }
        assertDiskBBQSearch(DISK_BBQ_INDEX_NAME);
    }

    public void testDiskBBQVectorSearchWithExplicitBits() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            assumeTrue("DiskBBQ bits are not supported on this version", clusterSupportsFeature(ES940_DISK_BBQ_FEATURE));
            for (int bits : SUPPORTED_BITS) {
                String mapping = String.format(Locale.ROOT, """
                    {
                      "properties": {
                        "vector": {
                          "type": "dense_vector",
                          "dims": 3,
                          "index": true,
                          "similarity": "l2_norm",
                          "index_options": {
                            "type": "bbq_disk",
                            "bits": %d
                          }
                        }
                      }
                    }
                    """, bits);
                String indexName = diskBbqBitsIndexName(bits);
                // create index and index 10 random floating point vectors
                createIndexWithDenseVectorSettings(indexName, mapping);
                indexVectors(indexName);
                // force merge the index
                client().performRequest(new Request("POST", "/" + indexName + "/_forcemerge?max_num_segments=1"));
            }
        } else {
            for (int bits : SUPPORTED_BITS) {
                assumeTrue("DiskBBQ bits index was not created on the old cluster", indexExistsOnCluster(diskBbqBitsIndexName(bits)));
            }
        }
        for (int bits : SUPPORTED_BITS) {
            assertDiskBBQSearch(diskBbqBitsIndexName(bits));
        }
    }

    private static String diskBbqBitsIndexName(int bits) {
        return DISK_BBQ_BITS_INDEX_PREFIX + bits;
    }

    private void assertDiskBBQSearch(String indexName) throws Exception {
        // search with a script query
        Request searchRequest = new Request("POST", "/" + indexName + "/_search");
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
        searchRequest = new Request("POST", "/" + indexName + "/_search");
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

    private boolean clusterSupportsFeature(String feature) throws IOException {
        return collectNodeInfos(adminClient()).stream().allMatch(node -> node.supportsFeature(feature));
    }

    private boolean indexExistsOnCluster(String indexName) throws IOException {
        Request request = new Request("HEAD", "/" + indexName);
        try {
            Response response = client().performRequest(request);
            return response.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
                return false;
            }
            throw e;
        }
    }

    private void createIndexWithDenseVectorSettings(String indexName, String mapping) throws IOException {
        Settings settings = Settings.builder().put("index.dense_vector.experimental_features", false).build();
        try {
            createIndex(indexName, settings, mapping);
        } catch (ResponseException e) {
            if (isUnknownDenseVectorSetting(e)) {
                createIndex(indexName, Settings.EMPTY, mapping);
            } else {
                throw e;
            }
        }
    }

    private static boolean isUnknownDenseVectorSetting(ResponseException e) {
        String message = e.getMessage();
        return message != null && message.contains("unknown setting") && message.contains("index.dense_vector.experimental_features");
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }

}
