/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.encryption;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A partial rollout of the encryption password: one master-eligible node has no password in its keystore when the key is installed. It
 * must stay in the cluster reporting yellow, and turn green once the password is added to its keystore, reloaded and persisted.
 */
public class MissingPasswordRolloutIT extends ESRestTestCase {

    private static final String ACTIVE_PASSWORD_ID = "cluster.state.encryption.active_password_id";
    private static final String PASSWORD = "cluster.state.encryption.password.v1";
    private static final int NODE_WITHOUT_PASSWORD = 2;

    private static final MutableSettingsProvider keystoreOfNodeWithoutPassword = new MutableSettingsProvider();

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-encryption")
        .nodes(3)
        .setting("xpack.security.enabled", "false")
        .node(0, n -> n.keystore(ACTIVE_PASSWORD_ID, "v1").keystore(PASSWORD, "encryption-test-password"))
        .node(1, n -> n.keystore(ACTIVE_PASSWORD_ID, "v1").keystore(PASSWORD, "encryption-test-password"))
        .node(NODE_WITHOUT_PASSWORD, n -> n.keystore(keystoreOfNodeWithoutPassword))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testNodeWithoutPasswordRecoversAfterPasswordIsAdded() throws Exception {
        String nodeName = cluster.getName(NODE_WITHOUT_PASSWORD);
        // the master installs the key, so it must be a node with a password
        client().performRequest(new Request("POST", "/_cluster/voting_config_exclusions?node_names=" + nodeName));
        try (RestClient nodeClient = buildClient(restClientSettings(), nodeHosts(NODE_WITHOUT_PASSWORD))) {
            assertBusy(() -> assertThat(health(client()).evaluate("details.active_key_id"), notNullValue()));

            // a node that couldn't persist the key used to reject the publication, so the update was never acknowledged
            updatePersistentSetting("51mb");
            ObjectPath health = health(nodeClient);
            assertEquals("yellow", health.evaluate("status"));
            assertThat(health.evaluate("diagnosis.0.id"), containsString("missing_wrap_credentials"));
            assertEquals(false, health.evaluate("details.key_on_disk"));
            assertEquals(
                3,
                (int) ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_cluster/health")))
                    .evaluate("number_of_nodes")
            );

            keystoreOfNodeWithoutPassword.put(ACTIVE_PASSWORD_ID, "v1");
            keystoreOfNodeWithoutPassword.put(PASSWORD, "encryption-test-password");
            cluster.updateStoredSecureSettings();
            client().performRequest(new Request("POST", "/_nodes/" + nodeName + "/reload_secure_settings"));

            // the key is persisted with the next change to cluster-wide metadata
            updatePersistentSetting("52mb");
            health = health(nodeClient);
            assertEquals("green", health.evaluate("status"));
            assertEquals(true, health.evaluate("details.key_on_disk"));
        } finally {
            client().performRequest(new Request("DELETE", "/_cluster/voting_config_exclusions?wait_for_removal=false"));
        }
    }

    private static void updatePersistentSetting(String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\":{\"indices.recovery.max_bytes_per_sec\":\"" + value + "\"}}");
        assertEquals(true, ObjectPath.createFromResponse(client().performRequest(request)).evaluate("acknowledged"));
    }

    /** The indicator as computed by the node that {@code client} sends the request to. */
    private static ObjectPath health(RestClient client) throws IOException {
        Response response = client.performRequest(new Request("GET", "/_health_report/cluster_state_encryption"));
        return new ObjectPath(ObjectPath.createFromResponse(response).evaluate("indicators.cluster_state_encryption"));
    }

    private static HttpHost[] nodeHosts(int index) {
        return new HttpHost[] { HttpHost.create(cluster.getHttpAddress(index)) };
    }
}
