/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class NodeShutdownAutoExpandIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testAutoExpandRace() throws Exception {
        // Create an index with 1 primary and 0-1 autoexpand
        final String indexName = "test-idx";
        Request createIndexRequest = new Request("PUT", indexName);
        createIndexRequest.setJsonEntity("{\"settings\":  {\"number_of_shards\": 1, \"auto_expand_replicas\": \"0-1\"}}");
        assertOK(client().performRequest(createIndexRequest));
        ensureGreen(indexName);

        // Find out the id of the node that has the primary shard
        final String nodeIdToShutDown = getNodeWithPrimaryShardForIndex(indexName);
        putNodeShutdown(nodeIdToShutDown, "REMOVE", null, null, null);

        // this should NOT pass
        boolean wasComplete = false;
        while (true) {
            Request getStatusRequest = new Request("GET", "_nodes/" + nodeIdToShutDown + "/shutdown");
            Response statusResponse = client().performRequest(getStatusRequest);
            Map<String, Object> status = entityAsMap(statusResponse);
            String shardMigrationStatus = ObjectPath.eval("nodes.0.shard_migration.status", status);
            if ("COMPLETE".equals(shardMigrationStatus)) {
                logger.warn("shard migration status: [{}]", shardMigrationStatus);
                wasComplete = true;
            } else if (wasComplete) {
                fail("shard migration was complete and then became not-complete: " + shardMigrationStatus);
            } else {
                logger.warn("shard migration status: [{}]", shardMigrationStatus);
            }
            final String nodeIdWithPrimary = getNodeWithPrimaryShardForIndex(indexName);
            if (nodeIdToShutDown.equals(nodeIdWithPrimary) == false) {
                logger.warn("shard successfully moved to the other node");
                break;
            }
//            assertThat(ObjectPath.eval("nodes.0.shard_migration.status", status), equalTo("COMPLETE"));
        }

    }

    @SuppressWarnings("unchecked")
    private static String getNodeWithPrimaryShardForIndex(String indexName) throws IOException {
        Request checkShardsRequest = new Request("GET", "_cat/shards/" + indexName);
        checkShardsRequest.addParameter("format", "json");
        checkShardsRequest.addParameter("h", "index,shard,prirep,id,state");

        List<Object> shardsResponse = entityAsList(client().performRequest(checkShardsRequest));
        final String nodeIdToShutDown = shardsResponse.stream()
            .map(shard -> (Map<String, Object>) shard)
            .filter(shard -> "p".equals(shard.get("prirep")))
            .map(shard -> shard.get("id").toString())
            .findFirst().get();
        return nodeIdToShutDown;
    }

    // TODO: copied from NodeShutdownIT for expediency in replicating this bug,
    // fix duplication before merge
    private void putNodeShutdown(
        String nodeIdToShutdown,
        String type,
        @Nullable TimeValue allocationDelay,
        @Nullable String targetNodeName,
        @Nullable TimeValue grace
    ) throws IOException {
        String reason = this.getTestName();

        // Put a shutdown request
        Request putShutdown = new Request("PUT", "_nodes/" + nodeIdToShutdown + "/shutdown");
        // maybeAddMasterNodeTimeout(putShutdown);

        try (XContentBuilder putBody = JsonXContent.contentBuilder()) {
            putBody.startObject();
            {
                putBody.field("type", type);
                putBody.field("reason", reason);
                if (allocationDelay != null) {
                    assertThat("allocation delay parameter is only valid for RESTART-type shutdowns", type, equalToIgnoringCase("restart"));
                    putBody.field("allocation_delay", allocationDelay.getStringRep());
                }
                if (targetNodeName != null) {
                    assertThat("target node name parameter is only valid for REPLACE-type shutdowns", type, equalToIgnoringCase("replace"));
                    putBody.field("target_node_name", targetNodeName);
                } else {
                    assertThat("target node name is required for REPLACE-type shutdowns", type, not(equalToIgnoringCase("replace")));
                }
                if (grace != null) {
                    assertThat("grace only valid for SIGTERM-type shutdowns", type, equalToIgnoringCase("sigterm"));
                    putBody.field("grace_period", grace.getStringRep());
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
                    putBody.field("allocation_delay", allocationDelay.getStringRep());
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
                    if (grace != null) {
                        assertThat("grace only valid for SIGTERM-type shutdowns", type, equalToIgnoringCase("sigterm"));
                        putBody.field("grace_period", grace.getStringRep());
                    }
                }
                putBody.endObject();
                putShutdown.setJsonEntity(Strings.toString(putBody));
            }
        }
        assertOK(client().performRequest(putShutdown));
    }
}
