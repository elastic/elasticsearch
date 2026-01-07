/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class DiskBBQVectorSearchIT extends AbstractUpgradeTestCase {

    private static final Version DISK_BBQ_VERSION = Version.V_9_2_0;

    private static final String DISK_BBQ_INDEX_NAME = "diskbbq_vectors_index";

    public void test() throws Exception {
        Version v = Version.fromString(UPGRADE_FROM_VERSION);
        assumeTrue("DiskBBQ vector format introduced in version " + DISK_BBQ_VERSION, v.onOrAfter(DISK_BBQ_VERSION));
        if (CLUSTER_TYPE == ClusterType.OLD) {
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
            createIndex(DISK_BBQ_INDEX_NAME, Settings.EMPTY, mapping);
            indexVectors(DISK_BBQ_INDEX_NAME);
            // force merge the index
            client().performRequest(new Request("POST", "/" + DISK_BBQ_INDEX_NAME + "/_forcemerge?max_num_segments=1"));
        }
        // search with a script query
        Request searchRequest = new Request("POST", "/" + DISK_BBQ_INDEX_NAME + "/_search");
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
        searchRequest = new Request("POST", "/" + DISK_BBQ_INDEX_NAME + "/_search");
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

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }

}
