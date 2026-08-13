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
import org.elasticsearch.test.cluster.ElasticsearchCluster;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;

/**
 * Basic test that indexed documents survive the rolling restart.
 */
public class IndexingIT extends AbstractXpackRollingUpgradeTestCase {

    @org.junit.ClassRule
    public static ElasticsearchCluster cluster = buildCluster();

    public IndexingIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testIndexing() throws IOException {
        if (isMixedCluster()) {
            ensureHealth((request -> {
                request.addParameter("timeout", "70s");
                request.addParameter("wait_for_nodes", "3");
                request.addParameter("wait_for_status", "yellow");
            }));
        } else if (isUpgradedCluster()) {
            ensureHealth("test_index,index_with_replicas,empty_index", (request -> {
                request.addParameter("wait_for_nodes", "3");
                request.addParameter("wait_for_status", "green");
                request.addParameter("timeout", "70s");
                request.addParameter("level", "shards");
            }));
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

        final int expectedCount;
        if (isOldCluster()) {
            expectedCount = 5;
        } else if (isMixedCluster()) {
            expectedCount = isFirstMixedCluster() ? 5 : 10;
        } else {
            assert isUpgradedCluster();
            expectedCount = 15;
        }

        assertCount("test_index", expectedCount);
        assertCount("index_with_replicas", 5);
        assertCount("empty_index", 0);

        if (isOldCluster() == false) {
            String suffix = isMixedCluster() ? "_MIXED" : "_UPGRADED";
            bulk("test_index", suffix, 5);
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
