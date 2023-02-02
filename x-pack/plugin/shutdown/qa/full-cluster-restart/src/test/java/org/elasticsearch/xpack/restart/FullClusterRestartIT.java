/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.restart;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.upgrades.AbstractFullClusterRestartTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class FullClusterRestartIT extends AbstractFullClusterRestartTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            // we increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    @SuppressWarnings("unchecked")
    public void testNodeShutdown() throws Exception {
        assumeTrue("no shutdown in versions before " + Version.V_7_15_0, getOldClusterVersion().onOrAfter(Version.V_7_15_0));

        if (isRunningAgainstOldCluster()) {
            final Request getNodesReq = new Request("GET", "_nodes");
            final Response getNodesResp = adminClient().performRequest(getNodesReq);
            final Map<String, Object> nodes = (Map<String, Object>) entityAsMap(getNodesResp).get("nodes");
            final String nodeIdToShutdown = randomFrom(nodes.keySet());

            final Request putShutdownRequest = new Request("PUT", "_nodes/" + nodeIdToShutdown + "/shutdown");
            try (XContentBuilder putBody = JsonXContent.contentBuilder()) {
                putBody.startObject();
                {
                    // Use the types available from as early as possible
                    final String type = randomFrom("restart", "remove");
                    putBody.field("type", type);
                    putBody.field("reason", this.getTestName());
                }
                putBody.endObject();
                putShutdownRequest.setJsonEntity(Strings.toString(putBody));
            }
            assertOK(client().performRequest(putShutdownRequest));
        }

        assertBusy(() -> {
            final Request getShutdownsReq = new Request("GET", "_nodes/shutdown");
            final Response getShutdownsResp = client().performRequest(getShutdownsReq);
            final Map<String, Object> stringObjectMap = entityAsMap(getShutdownsResp);

            final List<Map<String, Object>> shutdowns = (List<Map<String, Object>>) stringObjectMap.get("nodes");
            assertThat("there should be exactly one shutdown registered", shutdowns, hasSize(1));
            final Map<String, Object> shutdown = shutdowns.get(0);
            assertThat(shutdown.get("node_id"), notNullValue()); // Since we randomly determine the node ID, we can't check it
            assertThat(shutdown.get("reason"), equalTo(this.getTestName()));
            assertThat(
                (String) shutdown.get("status"),
                anyOf(
                    Arrays.stream(SingleNodeShutdownMetadata.Status.values())
                        .map(value -> equalToIgnoringCase(value.toString()))
                        .collect(Collectors.toList())
                )
            );
        }, 30, TimeUnit.SECONDS);
    }

    @Override
    protected void deleteAllNodeShutdownMetadata() throws IOException {
        // do not delete node shutdown
    }
}
