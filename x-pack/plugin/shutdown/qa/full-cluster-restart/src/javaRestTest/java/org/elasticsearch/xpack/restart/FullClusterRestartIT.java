/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.restart;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.BeforeClass;
import org.junit.ClassRule;

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

public class FullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        // some tests rely on the translog not being flushed
        .setting("indices.memory.shard_inactive_time", "60m")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.watcher.encrypt_sensitive_data", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .configFile("testnode.pem", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .keystore("xpack.watcher.encryption_key", Resource.fromClasspath("system_key"))
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .build();

    public FullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

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

    @BeforeClass
    public static void checkClusterVersion() {
        assumeTrue("no shutdown in versions before " + Version.V_7_15_0, getOldClusterVersion().onOrAfter(Version.V_7_15_0));
    }

    @SuppressWarnings("unchecked")
    public void testNodeShutdown() throws Exception {
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
                    putBody.field("reason", getRootTestName());
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
            assertThat(shutdown.get("reason"), equalTo(getRootTestName()));
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
