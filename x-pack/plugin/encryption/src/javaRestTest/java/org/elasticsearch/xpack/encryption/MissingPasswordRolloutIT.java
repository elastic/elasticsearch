/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.encryption;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * A partial rollout of the encryption password: one node's keystore has no password. The key must not be installed, since that node
 * could not persist it, until the password is added to its keystore and reloaded; then every node reports green.
 */
public class MissingPasswordRolloutIT extends ESRestTestCase {

    private static final String ACTIVE_PASSWORD_ID = "cluster.state.encryption.active_password_id";
    private static final String PASSWORD = "cluster.state.encryption.password.v1";
    private static final int NODES = 3;
    private static final int NODE_WITHOUT_PASSWORD = 2;
    // the FIPS test fixture uses the same one
    private static final String KEYSTORE_PASSWORD = "keystore-password";

    // every node starts without a password, so that no key is installed before the last node joins, which would then be refused
    private static final MutableSettingsProvider keystoreOfOtherNodes = new MutableSettingsProvider();
    private static final MutableSettingsProvider keystoreOfNodeWithoutPassword = new MutableSettingsProvider();

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-encryption")
        .module("x-pack-security")
        .nodes(NODES)
        // so that the readiness check, an internal action sent to every node, runs under security
        .setting("xpack.security.enabled", "true")
        .user("test-admin", "x-pack-test-password")
        .keystorePassword(KEYSTORE_PASSWORD)
        .node(0, n -> n.keystore(keystoreOfOtherNodes))
        .node(1, n -> n.keystore(keystoreOfOtherNodes))
        .node(NODE_WITHOUT_PASSWORD, n -> n.keystore(keystoreOfNodeWithoutPassword))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testKeyIsInstalledOnceTheLastNodeHasItsPassword() throws Exception {
        String nodeName = cluster.getName(NODE_WITHOUT_PASSWORD);
        // a master with a password would install the key right away if it didn't wait for every node
        client().performRequest(new Request("POST", "/_cluster/voting_config_exclusions?node_names=" + nodeName));
        try {
            runRollout(nodeName);
        } finally {
            client().performRequest(new Request("DELETE", "/_cluster/voting_config_exclusions?wait_for_removal=false"));
        }
    }

    private void runRollout(String nodeName) throws Exception {
        Request clusterHealth = new Request("GET", "/_cluster/health");
        clusterHealth.addParameter("wait_for_nodes", Integer.toString(NODES));
        client().performRequest(clusterHealth);

        addPassword(keystoreOfOtherNodes, 0, 1);
        assertBusy(() -> {
            boolean masterWaitsForNode = false;
            for (int node = 0; node < NODES; node++) {
                ObjectPath indicator = health(node);
                assertThat("no key while a node has no password", indicator.evaluate("details.active_key_id"), nullValue());
                List<?> waitingFor = indicator.evaluate("details.nodes_without_password");
                masterWaitsForNode |= waitingFor != null && waitingFor.contains(nodeName);
            }
            assertTrue("the elected master reports the node it waits for", masterWaitsForNode);
        }, 60, TimeUnit.SECONDS);

        addPassword(keystoreOfNodeWithoutPassword, NODE_WITHOUT_PASSWORD);
        assertBusy(() -> {
            for (int node = 0; node < NODES; node++) {
                ObjectPath indicator = health(node);
                assertThat(indicator.evaluate("details.active_key_id"), notNullValue());
                assertEquals("green", indicator.evaluate("status"));
            }
        }, 60, TimeUnit.SECONDS);
        // every node persists cluster state with the key, so it acknowledges updates
        updatePersistentSetting("52mb");
    }

    private void addPassword(MutableSettingsProvider keystore, int... nodes) throws IOException {
        keystore.put(ACTIVE_PASSWORD_ID, "v1");
        keystore.put(PASSWORD, "encryption-test-password");
        cluster.updateStoredSecureSettings();
        // a password-protected keystore can only be reloaded node by node without transport TLS
        for (int node : nodes) {
            Request reload = new Request("POST", "/_nodes/_local/reload_secure_settings");
            reload.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + "\"}");
            try (RestClient nodeClient = nodeClient(node)) {
                nodeClient.performRequest(reload);
            }
        }
    }

    private static void updatePersistentSetting(String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\":{\"indices.recovery.max_bytes_per_sec\":\"" + value + "\"}}");
        assertEquals(true, ObjectPath.createFromResponse(client().performRequest(request)).evaluate("acknowledged"));
    }

    /** The indicator as computed by node {@code node}, which the health API does locally. */
    private ObjectPath health(int node) throws IOException {
        try (RestClient nodeClient = nodeClient(node)) {
            return new ObjectPath(
                ObjectPath.createFromResponse(nodeClient.performRequest(new Request("GET", "/_health_report/cluster_state_encryption")))
                    .evaluate("indicators.cluster_state_encryption")
            );
        }
    }

    private RestClient nodeClient(int node) throws IOException {
        return buildClient(restClientSettings(), new HttpHost[] { HttpHost.create(cluster.getHttpAddress(node)) });
    }
}
