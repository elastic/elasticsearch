/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.Strings;

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
public class IndexingIT extends AbstractXPackRollingUpgradeTestCase {

    public IndexingIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testIndexing() throws IOException {
        final String clusterTypeName;
        if (isOldCluster()) {
            clusterTypeName = "OLD";
        } else if (isMixedCluster()) {
            clusterTypeName = "MIXED";
            ensureHealth((request -> {
                request.addParameter("timeout", "70s");
                request.addParameter("wait_for_nodes", "3");
                request.addParameter("wait_for_status", "yellow");
            }));
        } else if (isUpgradedCluster()) {
            clusterTypeName = "UPGRADED";
            ensureHealth("test_index,index_with_replicas,empty_index", (request -> {
                request.addParameter("wait_for_nodes", "3");
                request.addParameter("wait_for_status", "green");
                request.addParameter("timeout", "70s");
                request.addParameter("level", "shards");
            }));
        } else {
            throw new AssertionError("Unknown cluster type");
        }

        if (isOldCluster()) {

            Request createTestIndex = new Request("PUT", "/test_index");
            createTestIndex.setJsonEntity("""
                {"settings": {"index.number_of_replicas": 0}}""");
            client().performRequest(createTestIndex);

            String recoverQuickly = """
                {"settings": {"index.unassigned.node_left.delayed_timeout": "100ms"}}""";
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
        if (isOldCluster()) {
            expectedCount = 5;
        } else if (isMixedCluster()) {
            if (isFirstMixedCluster()) {
                expectedCount = 5;
            } else {
                expectedCount = 10;
            }
        } else if (isUpgradedCluster()) {
            expectedCount = 15;
        } else {
            throw new AssertionError("Unknown cluster type");
        }

        assertCount("test_index", expectedCount);
        assertCount("index_with_replicas", 5);
        assertCount("empty_index", 0);

        if (isOldCluster() == false) {
            bulk("test_index", "_" + clusterTypeName, 5);
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
            b.append(Strings.format("""
                {"index": {"_index": "%s"}}
                {"f1": "v%s%s", "f2": %s}
                """, index, i, valueSuffix, i));
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
        assertEquals(Strings.format("""
            {"hits":{"total":%s}}\
            """, count), EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8));
    }
}
