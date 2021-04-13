/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class NodeShutdownIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testCRUD() throws Exception {
        Request nodesRequest = new Request("GET", "_nodes");
        Map<String, Object> nodesResponse = responseAsMap(client().performRequest(nodesRequest));
        Map<String, Object> nodesObject = (Map<String, Object>) nodesResponse.get("nodes");

        String nodeIdToShutdown = randomFrom(nodesObject.keySet());
        String reason = "testing node shutdown crud: " + randomAlphaOfLength(5);
        String type = randomFrom("RESTART", "REMOVE");

        // Ensure if we do a GET before the cluster metadata is set up, we don't get an error
        assertNoShuttingDownNodes(nodeIdToShutdown);

        // Put a shutdown request
        Request putShutdown = new Request("PUT", "_nodes/" + nodeIdToShutdown + "/shutdown");
        putShutdown.setJsonEntity("{\"type\":  \"" + type + "\", \"reason\":  \"" + reason + "\"}");
        assertOK(client().performRequest(putShutdown));

        // Ensure we can read it back
        {
            Request getShutdownStatus = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
            Map<String, Object> statusResponse = responseAsMap(client().performRequest(getShutdownStatus));
            List<Map<String, Object>> nodesArray = (List<Map<String, Object>>) statusResponse.get("nodes");
            assertThat(nodesArray, hasSize(1));
            assertThat(nodesArray.get(0).get("node_id"), equalTo(nodeIdToShutdown));
            assertThat(nodesArray.get(0).get("type"), equalTo(type));
            assertThat(nodesArray.get(0).get("reason"), equalTo(reason));
        }

        // Delete it and make sure it's deleted
        Request deleteRequest = new Request("DELETE", "_nodes/" + nodeIdToShutdown + "/shutdown");
        assertOK(client().performRequest(deleteRequest));
        assertNoShuttingDownNodes(nodeIdToShutdown);
    }

    @SuppressWarnings("unchecked")
    private void assertNoShuttingDownNodes(String nodeIdToShutdown) throws IOException {
        Request getShutdownStatus = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
        Map<String, Object> statusResponse = responseAsMap(client().performRequest(getShutdownStatus));
        List<Map<String, Object>> nodesArray = (List<Map<String, Object>>) statusResponse.get("nodes");
        assertThat(nodesArray, empty());
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(
            System.getProperty("tests.rest.cluster.username"),
            new SecureString(System.getProperty("tests.rest.cluster.password").toCharArray())
        );
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
