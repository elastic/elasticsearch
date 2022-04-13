/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.readiness.ReadinessClientProbe;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NodeShutdownIT extends ESRestTestCase implements ReadinessClientProbe {

    public void testRestartCRUD() throws Exception {
        checkCRUD(randomFrom("restart", "RESTART"), randomPositiveTimeValue(), null);
    }

    public void testRemoveCRUD() throws Exception {
        checkCRUD(randomFrom("remove", "REMOVE"), null, null);
    }

    public void testReplaceCRUD() throws Exception {
        checkCRUD(randomFrom("replace", "REPLACE"), null, randomAlphaOfLength(10));
    }

    public void checkCRUD(String type, @Nullable String allocationDelay, @Nullable String targetNodeName) throws Exception {
        String nodeIdToShutdown = getRandomNodeId();
        checkCRUD(nodeIdToShutdown, type, allocationDelay, targetNodeName, true);
    }

    @SuppressWarnings("unchecked")
    public void checkCRUD(
        String nodeIdToShutdown,
        String type,
        @Nullable String allocationDelay,
        @Nullable String targetNodeName,
        boolean delete
    ) throws Exception {
        // Ensure if we do a GET before the cluster metadata is set up, we don't get an error
        assertNoShuttingDownNodes(nodeIdToShutdown);

        putNodeShutdown(nodeIdToShutdown, type, allocationDelay, targetNodeName);

        // Ensure we can read it back
        {
            Request getShutdownStatus = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
            Map<String, Object> statusResponse = responseAsMap(client().performRequest(getShutdownStatus));
            List<Map<String, Object>> nodesArray = (List<Map<String, Object>>) statusResponse.get("nodes");
            assertThat(nodesArray, hasSize(1));
            assertThat(nodesArray.get(0).get("node_id"), equalTo(nodeIdToShutdown));
            assertThat((String) nodesArray.get(0).get("type"), equalToIgnoringCase(type));
            assertThat(nodesArray.get(0).get("reason"), equalTo(this.getTestName()));
            assertThat(nodesArray.get(0).get("allocation_delay"), equalTo(allocationDelay));
            assertThat(nodesArray.get(0).get("target_node_name"), equalTo(targetNodeName));
        }

        if (delete) {
            // Delete it and make sure it's deleted
            Request deleteRequest = new Request("DELETE", "_nodes/" + nodeIdToShutdown + "/shutdown");
            assertOK(client().performRequest(deleteRequest));
            assertNoShuttingDownNodes(nodeIdToShutdown);
        }
    }

    @SuppressWarnings("unchecked")
    public void testShutdownReadinessService() throws Exception {
        // Get a node from the cluster and find its readiness port
        Request getNodes = new Request("GET", "_nodes");
        Map<String, Object> nodesResponse = responseAsMap(client().performRequest(getNodes));
        Map<String, Object> nodesObject = (Map<String, Object>) nodesResponse.get("nodes");

        String nodeId = nodesObject.keySet().iterator().next();
        Map<String, Object> nodeObject = (Map<String, Object>) nodesObject.get(nodeId);
        Map<String, Object> httpObject = (Map<String, Object>) nodeObject.get("http");
        String publishAddress = (String) httpObject.get("publish_address");

        String readinessPorts = this.getTestReadinessPorts();
        String restPorts = this.getTestRestCluster();

        String[] restAddresses = restPorts.split(",");
        int nodeIndex = 0;
        for (String restAddress : restAddresses) {
            // skip ipv6 if any
            if (restAddress.startsWith("[")) {
                continue;
            }
            if (restAddress.equals(publishAddress)) {
                break;
            }
            nodeIndex++;
        }

        String[] readinessAddresses = readinessPorts.split(",");
        String readinessAddress = readinessAddresses[nodeIndex];

        String portStr = readinessAddress.split(":")[1];
        Integer port = Integer.parseInt(portStr);

        // Once we have the right port, check to see if it's ready, has to be for a properly started cluster
        tcpReadinessProbeTrue(port);

        // Mark the node for shutdown and check that it's not ready
        checkCRUD(nodeId, randomFrom("restart", "RESTART"), "1ms", null, false);
        tcpReadinessProbeFalse(port);

        // Delete the shutdown request and verify that the node is ready again
        Request deleteRequest = new Request("DELETE", "_nodes/" + nodeId + "/shutdown");
        assertOK(client().performRequest(deleteRequest));
        assertNoShuttingDownNodes(nodeId);

        tcpReadinessProbeTrue(port);
    }

    public void testPutShutdownIsIdempotentForRestart() throws Exception {
        checkPutShutdownIdempotency("RESTART");
    }

    public void testPutShutdownIsIdempotentForRemove() throws Exception {
        checkPutShutdownIdempotency("REMOVE");
    }

    @SuppressWarnings("unchecked")
    private void checkPutShutdownIdempotency(String type) throws Exception {
        String nodeIdToShutdown = getRandomNodeId();

        // PUT the shutdown once
        putNodeShutdown(nodeIdToShutdown, type);

        // now PUT it again, the same and we shouldn't get an error
        String newReason = "this reason is different";

        // Put a shutdown request
        Request putShutdown = new Request("PUT", "_nodes/" + nodeIdToShutdown + "/shutdown");
        putShutdown.setJsonEntity("{\"type\":  \"" + type + "\", \"reason\":  \"" + newReason + "\"}");
        assertOK(client().performRequest(putShutdown));

        // Ensure we can read it back and it has the new reason
        {
            Request getShutdownStatus = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
            Map<String, Object> statusResponse = responseAsMap(client().performRequest(getShutdownStatus));
            List<Map<String, Object>> nodesArray = (List<Map<String, Object>>) statusResponse.get("nodes");
            assertThat(nodesArray, hasSize(1));
            assertThat(nodesArray.get(0).get("node_id"), equalTo(nodeIdToShutdown));
            assertThat(nodesArray.get(0).get("type"), equalTo(type));
            assertThat(nodesArray.get(0).get("reason"), equalTo(newReason));
        }
    }

    public void testPutShutdownCanChangeTypeFromRestartToRemove() throws Exception {
        checkTypeChange("RESTART", "REMOVE");
    }

    public void testPutShutdownCanChangeTypeFromRemoveToRestart() throws Exception {
        checkTypeChange("REMOVE", "RESTART");
    }

    @SuppressWarnings("unchecked")
    public void checkTypeChange(String fromType, String toType) throws Exception {
        String nodeIdToShutdown = getRandomNodeId();
        String type = fromType;

        // PUT the shutdown once
        putNodeShutdown(nodeIdToShutdown, type);

        // now PUT it again, the same and we shouldn't get an error
        String newReason = "this reason is different";
        String newType = toType;

        // Put a shutdown request
        Request putShutdown = new Request("PUT", "_nodes/" + nodeIdToShutdown + "/shutdown");
        putShutdown.setJsonEntity("{\"type\":  \"" + newType + "\", \"reason\":  \"" + newReason + "\"}");
        assertOK(client().performRequest(putShutdown));

        // Ensure we can read it back and it has the new reason
        {
            Request getShutdownStatus = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
            Map<String, Object> statusResponse = responseAsMap(client().performRequest(getShutdownStatus));
            List<Map<String, Object>> nodesArray = (List<Map<String, Object>>) statusResponse.get("nodes");
            assertThat(nodesArray, hasSize(1));
            assertThat(nodesArray.get(0).get("node_id"), equalTo(nodeIdToShutdown));
            assertThat(nodesArray.get(0).get("type"), equalTo(newType));
            assertThat(nodesArray.get(0).get("reason"), equalTo(newReason));
        }
    }

    /**
     * A very basic smoke test to make sure the allocation decider is working.
     */
    @SuppressWarnings("unchecked")
    public void testAllocationPreventedForRemoval() throws Exception {
        String nodeIdToShutdown = getRandomNodeId();
        putNodeShutdown(nodeIdToShutdown, "REMOVE");

        // Create an index with enough replicas to ensure one would normally be allocated to each node
        final String indexName = "test-idx";
        Request createIndexRequest = new Request("PUT", indexName);
        createIndexRequest.setJsonEntity("{\"settings\":  {\"number_of_shards\": 1, \"number_of_replicas\": 3}}");
        assertOK(client().performRequest(createIndexRequest));

        // Watch to ensure no shards gets allocated to the node that's shutting down
        assertUnassignedShard(nodeIdToShutdown, indexName);

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

    /**
     * Checks that shards properly move off of a node that's marked for removal, including:
     * 1) A reroute needs to be triggered automatically when the node is registered for shutdown, otherwise shards won't start moving
     *    immediately.
     * 2) Ensures the status properly comes to rest at COMPLETE after the shards have moved.
     */
    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/77488")
    public void testShardsMoveOffRemovingNode() throws Exception {
        String nodeIdToShutdown = getRandomNodeId();

        final String indexName = "test-idx";
        Request createIndexRequest = new Request("PUT", indexName);
        createIndexRequest.setJsonEntity("{\"settings\":  {\"number_of_shards\": 4, \"number_of_replicas\": 0}}");
        assertOK(client().performRequest(createIndexRequest));

        ensureGreen(indexName);

        // Check to ensure there's a shard on the node we want to shut down
        Request checkShardsRequest = new Request("GET", "_cat/shards/" + indexName);
        checkShardsRequest.addParameter("format", "json");
        checkShardsRequest.addParameter("h", "index,shard,prirep,id,state");

        // Double check that there's actually a shard on the node we want to shut down
        {
            List<Object> shardsResponse = entityAsList(client().performRequest(checkShardsRequest));
            final long shardsOnNodeToShutDown = shardsResponse.stream()
                .map(shard -> (Map<String, Object>) shard)
                .filter(shard -> nodeIdToShutdown.equals(shard.get("id")))
                .filter(shard -> "STARTED".equals(shard.get("state")))
                .count();
            assertThat(shardsOnNodeToShutDown, is(1L));
        }

        // Register the shutdown
        putNodeShutdown(nodeIdToShutdown, "REMOVE");

        // assertBusy waiting for the shard to no longer be on that node
        AtomicReference<List<Object>> debug = new AtomicReference<>();
        assertBusy(() -> {
            List<Object> shardsResponse = entityAsList(client().performRequest(checkShardsRequest));
            final long shardsOnNodeToShutDown = shardsResponse.stream()
                .map(shard -> (Map<String, Object>) shard)
                .filter(shard -> nodeIdToShutdown.equals(shard.get("id")))
                .filter(shard -> "STARTED".equals(shard.get("state")) || "RELOCATING".equals(shard.get("state")))
                .count();
            assertThat(shardsOnNodeToShutDown, is(0L));
            debug.set(shardsResponse);
        });

        // Now check the shard migration status
        Request getStatusRequest = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
        Response statusResponse = client().performRequest(getStatusRequest);
        Map<String, Object> status = entityAsMap(statusResponse);
        assertThat(ObjectPath.eval("nodes.0.shard_migration.status", status), equalTo("COMPLETE"));
        assertThat(ObjectPath.eval("nodes.0.shard_migration.shard_migrations_remaining", status), equalTo(0));
        assertThat(ObjectPath.eval("nodes.0.shard_migration.explanation", status), nullValue());
    }

    public void testShardsCanBeAllocatedAfterShutdownDeleted() throws Exception {
        String nodeIdToShutdown = getRandomNodeId();
        putNodeShutdown(nodeIdToShutdown, "REMOVE");

        // Create an index with enough replicas to ensure one would normally be allocated to each node
        final String indexName = "test-idx";
        Request createIndexRequest = new Request("PUT", indexName);
        createIndexRequest.setJsonEntity("{\"settings\":  {\"number_of_shards\": 1, \"number_of_replicas\": 3}}");
        assertOK(client().performRequest(createIndexRequest));

        // Watch to ensure no shards gets allocated to the node that's shutting down
        assertUnassignedShard(nodeIdToShutdown, indexName);

        // Delete the shutdown
        Request deleteRequest = new Request("DELETE", "_nodes/" + nodeIdToShutdown + "/shutdown");
        assertOK(client().performRequest(deleteRequest));
        assertNoShuttingDownNodes(nodeIdToShutdown);

        // Check that the shard is assigned now
        ensureGreen(indexName);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/77456")
    public void testStalledShardMigrationProperlyDetected() throws Exception {
        String nodeIdToShutdown = getRandomNodeId();
        int numberOfShards = randomIntBetween(1, 5);

        // Create an index, pin the allocation to the node we're about to shut down
        final String indexName = "test-idx";
        Request createIndexRequest = new Request("PUT", indexName);
        createIndexRequest.setJsonEntity("""
            {
              "settings": {
                "number_of_shards": %s,
                "number_of_replicas": 0,
                "index.routing.allocation.require._id": "%s"
              }
            }""".formatted(numberOfShards, nodeIdToShutdown));
        assertOK(client().performRequest(createIndexRequest));

        // Mark the node for shutdown
        putNodeShutdown(nodeIdToShutdown, "remove");
        {
            // Now check the shard migration status
            Request getStatusRequest = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
            Response statusResponse = client().performRequest(getStatusRequest);
            Map<String, Object> status = entityAsMap(statusResponse);
            assertThat(ObjectPath.eval("nodes.0.shard_migration.status", status), equalTo("STALLED"));
            assertThat(ObjectPath.eval("nodes.0.shard_migration.shard_migrations_remaining", status), equalTo(numberOfShards));
            assertThat(
                ObjectPath.eval("nodes.0.shard_migration.explanation", status),
                allOf(
                    containsString(indexName),
                    containsString("cannot move, use the Cluster Allocation Explain API on this shard for details")
                )
            );
        }

        // Now update the allocation requirements to unblock shard relocation
        Request updateSettingsRequest = new Request("PUT", indexName + "/_settings");
        updateSettingsRequest.setJsonEntity("{\"index.routing.allocation.require._id\": null}");
        assertOK(client().performRequest(updateSettingsRequest));

        assertBusy(() -> {
            Request getStatusRequest = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
            Response statusResponse = client().performRequest(getStatusRequest);
            Map<String, Object> status = entityAsMap(statusResponse);
            assertThat(ObjectPath.eval("nodes.0.shard_migration.status", status), equalTo("COMPLETE"));
            assertThat(ObjectPath.eval("nodes.0.shard_migration.shard_migrations_remaining", status), equalTo(0));
            assertThat(ObjectPath.eval("nodes.0.shard_migration.explanation", status), nullValue());
        });
    }

    /**
     * Ensures that attempting to delete the status of a node that is not registered for shutdown gives a 404 response code.
     */
    public void testDeleteNodeNotRegisteredForShutdown() throws Exception {
        Request deleteReq = new Request("DELETE", "_nodes/this-node-doesnt-exist/shutdown");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(deleteReq));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(404));
    }

    @SuppressWarnings("unchecked")
    private void assertNoShuttingDownNodes(String nodeIdToShutdown) throws IOException {
        Request getShutdownStatus = new Request("GET", "_nodes/" + nodeIdToShutdown + "/shutdown");
        Map<String, Object> statusResponse = responseAsMap(client().performRequest(getShutdownStatus));
        List<Map<String, Object>> nodesArray = (List<Map<String, Object>>) statusResponse.get("nodes");
        assertThat(nodesArray, empty());
    }

    @SuppressWarnings("unchecked")
    private void assertUnassignedShard(String nodeIdToShutdown, String indexName) throws Exception {
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
    }

    private void putNodeShutdown(String nodeIdToShutdown, String type) throws IOException {
        putNodeShutdown(nodeIdToShutdown, type, null, null);
    }

    private void putNodeShutdown(String nodeIdToShutdown, String type, @Nullable String allocationDelay, @Nullable String targetNodeName)
        throws IOException {
        String reason = this.getTestName();

        // Put a shutdown request
        Request putShutdown = new Request("PUT", "_nodes/" + nodeIdToShutdown + "/shutdown");

        try (XContentBuilder putBody = JsonXContent.contentBuilder()) {
            putBody.startObject();
            {
                putBody.field("type", type);
                putBody.field("reason", reason);
                if (allocationDelay != null) {
                    assertThat("allocation delay parameter is only valid for RESTART-type shutdowns", type, equalToIgnoringCase("restart"));
                    putBody.field("allocation_delay", allocationDelay);
                }
                if (targetNodeName != null) {
                    assertThat("target node name parameter is only valid for REPLACE-type shutdowns", type, equalToIgnoringCase("replace"));
                    putBody.field("target_node_name", targetNodeName);
                } else {
                    assertThat("target node name is required for REPALCE-type shutdowns", type, not(equalToIgnoringCase("replace")));
                }
            }
            putBody.endObject();
            putShutdown.setJsonEntity(Strings.toString(putBody));
        }

        if (type.equalsIgnoreCase("restart") && allocationDelay != null) {
            assertNull("target node name parameter is only valid for REPLACE-type shutdowns", targetNodeName);
            try (XContentBuilder putBody = JsonXContent.contentBuilder()) {
                putBody.startObject();
                {
                    putBody.field("type", type);
                    putBody.field("reason", reason);
                    putBody.field("allocation_delay", allocationDelay);
                }
                putBody.endObject();
                putShutdown.setJsonEntity(Strings.toString(putBody));
            }
        } else {
            assertNull("allocation delay parameter is only valid for RESTART-type shutdowns", allocationDelay);
            try (XContentBuilder putBody = JsonXContent.contentBuilder()) {
                putBody.startObject();
                {
                    putBody.field("type", type);
                    putBody.field("reason", reason);
                    if (targetNodeName != null) {
                        assertThat(
                            "target node name parameter is only valid for REPLACE-type shutdowns",
                            type,
                            equalToIgnoringCase("replace")
                        );
                        putBody.field("target_node_name", targetNodeName);
                    }
                }
                putBody.endObject();
                putShutdown.setJsonEntity(Strings.toString(putBody));
            }
        }
        assertOK(client().performRequest(putShutdown));
    }

    @SuppressWarnings("unchecked")
    private String getRandomNodeId() throws IOException {
        Request nodesRequest = new Request("GET", "_nodes");
        Map<String, Object> nodesResponse = responseAsMap(client().performRequest(nodesRequest));
        Map<String, Object> nodesObject = (Map<String, Object>) nodesResponse.get("nodes");

        return randomFrom(nodesObject.keySet());
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
