/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Booleans;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;

/**
 * Basic test that indexed documents survive the rolling restart.
 * <p>
 * This test is an almost exact copy of <code>IndexingIT</code> in the
 * oss rolling restart tests. We should work on a way to remove this
 * duplication but for now we have no real way to share code.
 */
public class IndexingIT extends AbstractUpgradeTestCase {
    public void testIndexing() throws IOException {
        switch (CLUSTER_TYPE) {
        case OLD:
            break;
        case MIXED:
            ensureHealth((request -> {
                request.addParameter("timeout", "70s");
                request.addParameter("wait_for_nodes", "3");
                request.addParameter("wait_for_status", "yellow");
            }));
            break;
        case UPGRADED:
            ensureHealth("test_index,index_with_replicas,empty_index", (request -> {
                request.addParameter("wait_for_nodes", "3");
                request.addParameter("wait_for_status", "green");
                request.addParameter("timeout", "70s");
                request.addParameter("level", "shards");
            }));
            break;
        default:
            throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        if (CLUSTER_TYPE == ClusterType.OLD) {

            Request createTestIndex = new Request("PUT", "/test_index");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            client().performRequest(createTestIndex);

            String recoverQuickly = "{\"settings\": {\"index.unassigned.node_left.delayed_timeout\": \"100ms\"}}";
            Request createIndexWithReplicas = new Request("PUT", "/index_with_replicas");
            createIndexWithReplicas.setJsonEntity(recoverQuickly);
            client().performRequest(createIndexWithReplicas);

            Request createEmptyIndex = new Request("PUT", "/empty_index");
            // Ask for recovery to be quick
            createEmptyIndex.setJsonEntity(recoverQuickly);
            client().performRequest(createEmptyIndex);

            bulk("test_index", "_OLD", 5);
            bulk("index_with_replicas", "_OLD", 5);
        }

        int expectedCount;
        switch (CLUSTER_TYPE) {
        case OLD:
            expectedCount = 5;
            break;
        case MIXED:
            if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                expectedCount = 5;
            } else {
                expectedCount = 10;
            }
            break;
        case UPGRADED:
            expectedCount = 15;
            break;
        default:
            throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        assertCount("test_index", expectedCount);
        assertCount("index_with_replicas", 5);
        assertCount("empty_index", 0);

        if (CLUSTER_TYPE != ClusterType.OLD) {
            bulk("test_index", "_" + CLUSTER_TYPE, 5);
            Request toBeDeleted = new Request("PUT", "/test_index/_doc/to_be_deleted");
            toBeDeleted.addParameter("refresh", "true");
            toBeDeleted.setJsonEntity("{\"f1\": \"delete-me\"}");
            client().performRequest(toBeDeleted);
            assertCount("test_index", expectedCount + 6);

            Request delete = new Request("DELETE", "/test_index/_doc/to_be_deleted");
            delete.addParameter("refresh", "true");
            client().performRequest(delete);

            assertCount("test_index", expectedCount + 5);
        }
    }

    private void bulk(String index, String valueSuffix, int count) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < count; i++) {
            b.append("{\"index\": {\"_index\": \"").append(index).append("\"}}\n");
            b.append("{\"f1\": \"v").append(i).append(valueSuffix).append("\", \"f2\": ").append(i).append("}\n");
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(b.toString());
        client().performRequest(bulk);
    }

    static void assertCount(String index, int count) throws IOException {
        Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        searchTestIndexRequest.addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        assertEquals("{\"hits\":{\"total\":" + count + "}}",
                EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8));
    }
}
