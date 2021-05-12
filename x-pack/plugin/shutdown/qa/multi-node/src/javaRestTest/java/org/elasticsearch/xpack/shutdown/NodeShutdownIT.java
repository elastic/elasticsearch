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
import java.util.Optional;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

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

    /**
     * A very basic smoke test to make sure the allocation decider is working.
     */
    @SuppressWarnings("unchecked")
    public void testAllocationPreventedForRemoval() throws Exception {
        Request nodesRequest = new Request("GET", "_nodes");
        Map<String, Object> nodesResponse = responseAsMap(client().performRequest(nodesRequest));
        Map<String, Object> nodesObject = (Map<String, Object>) nodesResponse.get("nodes");

        String nodeIdToShutdown = randomFrom(nodesObject.keySet());
        String reason = "testing node shutdown allocation rules";
        String type = "REMOVE";

        // Put a shutdown request
        Request putShutdown = new Request("PUT", "_nodes/" + nodeIdToShutdown + "/shutdown");
        putShutdown.setJsonEntity("{\"type\":  \"" + type + "\", \"reason\":  \"" + reason + "\"}");
        assertOK(client().performRequest(putShutdown));

        // Create an index with 1s/2r
        final String indexName = "test-idx";
        Request createIndexRequest = new Request("PUT", indexName);
        createIndexRequest.setJsonEntity("{\"settings\":  {\"number_of_shards\": 1, \"number_of_replicas\": 3}}");
        assertOK(client().performRequest(createIndexRequest));

        // Watch to ensure no shards gets allocated to the node that's shutting down
        Request checkShardsRequest = new Request("GET", "_cat/shards/" + indexName);
        checkShardsRequest.addParameter("format", "json");
        checkShardsRequest.addParameter("h", "index,shard,prirep,id,state");

        assertBusy(() -> {
            List<Object> shardsResponse = entityAsList(client().performRequest(checkShardsRequest));
            int startedShards = 0;
            int unassignedShards = 0;
            for (Object shard : shardsResponse) {
                Map<String, Object> shardMap = (Map<String, Object>) shard;
                assertThat(
                    "no shards should be assigned to a node shutting down for removal",
                    shardMap.get("id"),
                    not(equalTo(nodeIdToShutdown))
                );

                if (shardMap.get("id") == null) {
                    unassignedShards++;
                } else if (nodeIdToShutdown.equals(shardMap.get("id")) == false) {
                    assertThat("all other shards should be started", shardMap.get("state"), equalTo("STARTED"));
                    startedShards++;
                }
            }
            assertThat(unassignedShards, equalTo(1));
            assertThat(startedShards, equalTo(3));
        });
        // Now that we know all shards of the test index are assigned except one,
        // make sure it's unassigned because of the allocation decider.

        Request allocationExplainRequest = new Request("GET", "_cluster/allocation/explain");
        allocationExplainRequest.setJsonEntity("{\"index\": \"" + indexName + "\", \"shard\":  0, \"primary\":  false}");
        Map<String, Object> allocationExplainMap = entityAsMap(client().performRequest(allocationExplainRequest));
        List<Map<String, Object>> decisions = (List<Map<String, Object>>) allocationExplainMap.get("node_allocation_decisions");
        assertThat(decisions, notNullValue());

        Optional<Map<String, Object>> maybeDecision = decisions.stream()
            .filter(decision -> nodeIdToShutdown.equals(decision.get("node_id")))
            .findFirst();
        assertThat("expected decisions for node, but not found", maybeDecision.isPresent(), is(true));

        Map<String, Object> decision = maybeDecision.get();
        assertThat("node should have deciders", decision.containsKey("deciders"), is(true));

        List<Map<String, Object>> deciders = (List<Map<String, Object>>) decision.get("deciders");
        assertThat(
            "the node_shutdown allocation decider should have decided NO",
            deciders.stream()
                .filter(decider -> "node_shutdown".equals(decider.get("decider")))
                .allMatch(decider -> "NO".equals(decider.get("decision"))),
            is(true)
        );
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
