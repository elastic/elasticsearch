/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class RemoteClusterSecurityReloadCredentialsRestIT extends AbstractRemoteClusterSecurityTestCase {

    @BeforeClass
    public static void disableInFips() {
        assumeFalse(
            "Cannot run in FIPS mode since the keystore will be password protected and sending a password in the reload"
                + "settings api call, requires TLS to be configured for the transport layer",
            inFipsJvm()
        );
    }

    private static final MutableSettingsProvider keystoreSettings = new MutableSettingsProvider();
    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore(keystoreSettings)
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_SEARCH_USER, PASS.toString(), "read_remote_shared_logs", false)
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testCredentialsReload() throws Exception {
        final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
            {
              "search": [
                {
                    "names": ["*"]
                }
              ]
            }""");

        final boolean isProxyMode = randomBoolean();
        final boolean configureSettingsFirst = randomBoolean();
        // it's valid to first configure remote cluster, then credentials
        if (configureSettingsFirst) {
            putRemoteClusterSettings("my_remote_cluster", fulfillingCluster, false, isProxyMode, randomBoolean());
        }

        configureRemoteClusterCredentials("my_remote_cluster", (String) apiKeyMap.get("encoded"));

        // also valid to configure credentials, then cluster
        if (false == configureSettingsFirst) {
            configureRemoteCluster("my_remote_cluster");
        } else {
            // now that credentials are configured, we expect a successful connection
            checkRemoteConnection("my_remote_cluster", fulfillingCluster, false, isProxyMode);
        }

        assertOK(
            performRequestWithRemoteSearchUser(
                new Request("GET", String.format(Locale.ROOT, "/my_remote_cluster:*/_search?ccs_minimize_roundtrips=%s", randomBoolean()))
            )
        );
    }

    @SuppressWarnings("unchecked")
    private void configureRemoteClusterCredentials(String clusterAlias, String credentials) throws IOException {
        keystoreSettings.put("cluster.remote." + clusterAlias + ".credentials", credentials);
        queryCluster.updateStoredSecureSettings();
        final Response reloadResponse = adminClient().performRequest(new Request("POST", "/_nodes/reload_secure_settings"));
        assertOK(reloadResponse);
        final Map<String, Object> map = entityAsMap(reloadResponse);
        assertThat(map.get("nodes"), instanceOf(Map.class));
        final Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertThat(nodes, is(not(anEmptyMap())));
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            assertThat(entry.getValue(), instanceOf(Map.class));
            final Map<String, Object> node = (Map<String, Object>) entry.getValue();
            assertThat(node.get("reload_exception"), nullValue());
        }
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

}
