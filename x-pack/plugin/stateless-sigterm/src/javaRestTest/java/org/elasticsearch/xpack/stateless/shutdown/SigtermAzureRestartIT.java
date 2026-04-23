/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class SigtermAzureRestartIT extends ESRestTestCase {

    // long timeout, which we will force kill before it occurs
    private static final TimeValue SIGTERM_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    @ClassRule
    public static StatelessElasticsearchCluster cluster = StatelessElasticsearchCluster.local()
        .module("stateless")
        .module("stateless-sigterm")
        .setting("xpack.ml.enabled", "false")
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.watcher.enabled", "false")
        .setting("stateless.sigterm.timeout", SIGTERM_TIMEOUT.getStringRep())
        .setting("cluster.stateless.heartbeat_frequency", "1s")
        .setting("logger.org.elasticsearch.xpack.stateless.shutdown", "DEBUG")
        // .setting("logger.org.elasticsearch.action.support.master", "TRACE")
        // .setting("logger.org.elasticsearch.http", "TRACE")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testRestart() throws Exception {
        // this should be the indexing node. it's important the indexing node is what is shut down,
        // as it will hang since there are no other indexing nodes to transfer shards to
        int nodeIndex = 0;
        var name = cluster.getName(nodeIndex);
        var nodeId = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "_nodes/" + name))).evaluate(
            "nodes._arbitrary_key_"
        );
        assertThat(nodeId, instanceOf(String.class));
        var shutdownStatus = new Request("GET", "_nodes/" + nodeId + "/shutdown");
        assertThat(assertOKAndCreateObjectPath(client().performRequest(shutdownStatus)).evaluate("nodes"), hasSize(0));

        final int numIndices = randomIntBetween(1, 10);
        AtomicInteger totalDocs = new AtomicInteger(0);
        Map<String, Integer> indexNameToNumDocsMap = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            String indexName = "test_index_" + i;
            int numDocs = createAndLoadIndex(indexName, Settings.EMPTY);
            indexNameToNumDocsMap.put(indexName, numDocs);
            totalDocs.addAndGet(numDocs);
        }

        cluster.restartNodeInPlace(nodeIndex, false);

        assertBusy(() -> {
            try {
                assertThat(getNodesInfo(client()).keySet(), hasSize(2));
            } catch (ResponseException e) {
                throw new AssertionError("Node not ready after restart", e);
            }
        });
        logger.info("--> Nodes reconnected after restart");

        // Shutdown metadata is cleaned up
        assertBusy(() -> assertThat(assertOKAndCreateObjectPath(client().performRequest(shutdownStatus)).evaluate("nodes"), hasSize(0)));
        // Indices should go green
        for (String indexName : indexNameToNumDocsMap.keySet()) {
            ensureGreen(indexName);
            logger.info("--> Index " + indexName + " is green");
        }
    }

    int createAndLoadIndex(String indexName, Settings settings) throws IOException {
        if (randomBoolean()) {
            settings = Settings.builder().put(settings).put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build();
        }
        createIndex(adminClient(), indexName, settings);
        int numDocs = randomIntBetween(1, 100);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        adminClient().performRequest(bulkRequest);

        ensureGreen(adminClient(), indexName);

        adminClient().performRequest(new Request("POST", "/" + indexName + "/_flush"));
        return numDocs;
    }
}
