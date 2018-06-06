/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

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
            Map<String, String> waitForYellow = new HashMap<>();
            waitForYellow.put("wait_for_nodes", "3");
            waitForYellow.put("wait_for_status", "yellow");
            client().performRequest("GET", "/_cluster/health", waitForYellow);
            break;
        case UPGRADED:
            Map<String, String> waitForGreen = new HashMap<>();
            waitForGreen.put("wait_for_nodes", "3");
            waitForGreen.put("wait_for_status", "green");
            // wait for long enough that we give delayed unassigned shards to stop being delayed
            waitForGreen.put("timeout", "70s");
            waitForGreen.put("level", "shards");
            client().performRequest("GET", "/_cluster/health/test_index,index_with_replicas,empty_index", waitForGreen);
            break;
        default:
            throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        if (CLUSTER_TYPE == ClusterType.OLD) {
            client().performRequest("PUT", "/test_index", emptyMap(),
                    new NStringEntity("{\"settings\": {\"index.number_of_replicas\": 0}}", ContentType.APPLICATION_JSON));

            NStringEntity recoverQuickly = new NStringEntity(
                    "{\"settings\": {\"index.unassigned.node_left.delayed_timeout\": \"100ms\"}}",
                    ContentType.APPLICATION_JSON);
            client().performRequest("PUT", "/index_with_replicas", emptyMap(), recoverQuickly);
            client().performRequest("PUT", "/empty_index", emptyMap(), recoverQuickly);

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
            Map<String, String> parameters = new HashMap<>();
            client().performRequest("PUT", "/test_index/doc/to_be_deleted", singletonMap("refresh", "true"),
                new NStringEntity("{\"f1\": \"delete-me\"}", ContentType.APPLICATION_JSON));
            assertCount("test_index", expectedCount + 6);

            client().performRequest("DELETE", "/test_index/doc/to_be_deleted", singletonMap("refresh", "true"));

            assertCount("test_index", expectedCount + 5);
        }
    }

    private void bulk(String index, String valueSuffix, int count) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < count; i++) {
            b.append("{\"index\": {\"_index\": \"").append(index).append("\", \"_type\": \"doc\"}}\n");
            b.append("{\"f1\": \"v").append(i).append(valueSuffix).append("\", \"f2\": ").append(i).append("}\n");
        }
        client().performRequest("POST", "/_bulk", singletonMap("refresh", "true"),
                new NStringEntity(b.toString(), ContentType.APPLICATION_JSON));
    }

    private void assertCount(String index, int count) throws IOException {
        Response searchTestIndexResponse = client().performRequest("POST", "/" + index + "/_search",
                singletonMap("filter_path", "hits.total"));
        assertEquals("{\"hits\":{\"total\":" + count + "}}",
                EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8));
    }
}
