/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.Booleans;

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
                ensureHealth(
                    (request -> request.addParameter("timeout", "70s")
                        .addParameter("wait_for_nodes", "3")
                        .addParameter("wait_for_status", "yellow"))
                );
                break;
            case UPGRADED:
                ensureHealth(
                    "test_index,index_with_replicas,empty_index",
                    (request -> request.addParameter("wait_for_nodes", "3")
                        .addParameter("wait_for_status", "green")
                        .addParameter("timeout", "70s")
                        .addParameter("level", "shards"))
                );
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        if (CLUSTER_TYPE == ClusterType.OLD) {

            var createTestIndex = new Request("PUT", "/test_index").setJsonEntity("""
                {"settings": {"index.number_of_replicas": 0}}""");
            client().performRequest(createTestIndex);

            String recoverQuickly = """
                {"settings": {"index.unassigned.node_left.delayed_timeout": "100ms"}}""";
            var createIndexWithReplicas = new Request("PUT", "/index_with_replicas").setJsonEntity(recoverQuickly);
            client().performRequest(createIndexWithReplicas);

            // Ask for recovery to be quick
            var createEmptyIndex = new Request("PUT", "/empty_index").setJsonEntity(recoverQuickly);
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
            var toBeDeleted = new Request("PUT", "/test_index/_doc/to_be_deleted").addParameter("refresh", "true")
                .setJsonEntity("{\"f1\": \"delete-me\"}");
            client().performRequest(toBeDeleted);
            assertCount("test_index", expectedCount + 6);

            var delete = new Request("DELETE", "/test_index/_doc/to_be_deleted").addParameter("refresh", "true");
            client().performRequest(delete);

            assertCount("test_index", expectedCount + 5);
        }
    }

    private void bulk(String index, String valueSuffix, int count) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < count; i++) {
            b.append(formatted("""
                {"index": {"_index": "%s"}}
                {"f1": "v%s%s", "f2": %s}
                """, index, i, valueSuffix, i));
        }
        client().performRequest(new Request("POST", "/_bulk").addParameter("refresh", "true").setJsonEntity(b.toString()));
    }

    static void assertCount(String index, int count) throws IOException {
        var searchTestIndexRequest = new Request("POST", "/" + index + "/_search").addParameter(TOTAL_HITS_AS_INT_PARAM, "true")
            .addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        assertEquals(formatted("""
            {"hits":{"total":%s}}\
            """, count), EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8));
    }
}
