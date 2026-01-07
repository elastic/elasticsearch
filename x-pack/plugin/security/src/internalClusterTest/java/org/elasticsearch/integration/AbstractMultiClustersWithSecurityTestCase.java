/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.elasticsearch.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;
import static org.hamcrest.CoreMatchers.is;

public abstract class AbstractMultiClustersWithSecurityTestCase extends AbstractMultiClustersTestCase {
    @Override
    protected Settings nodeSettings(String clusterAlias) {
        Settings.Builder builder = Settings.builder().put(nodeSettings());
        if (apiKeyAuthenticationEnabled()) {
            builder.put("xpack.security.authc.token.enabled", true);
            builder.put("xpack.security.authc.api_key.enabled", true);

            if (clusterAlias.equals(LOCAL_CLUSTER)) {
                builder.put("xpack.security.remote_cluster_client.ssl.enabled", false);
            } else {
                builder.put(REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true);
                builder.put("xpack.security.remote_cluster_server.ssl.enabled", false);
                builder.put("remote_cluster.port", 0);
            }
        }

        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return enableSecurity() ? List.of(LocalStateSecurity.class) : List.of();
    }

    @Override
    protected NodeConfigurationSource nodeConfigurationSource() {
        return enableSecurity() ? new SecuritySettingsSource(false, createTempDir(), ESIntegTestCase.Scope.TEST) : null;
    }

    @Override
    protected void configureAndConnectsToRemoteClusters() throws Exception {
        if (apiKeyAuthenticationEnabled()) {
            Map<String, String> crossClusterApiKeys = new HashMap<>();

            for (String clusterAlias : remoteClusterAlias()) {
                if (clusterAlias.equals(LOCAL_CLUSTER) == false) {
                    String encodedApiKey = addCrossClusterApiKey(clusterAlias);
                    crossClusterApiKeys.put(clusterAlias, encodedApiKey);
                }
            }

            setRemoteClusterCredentials(crossClusterApiKeys);
        }

        super.configureAndConnectsToRemoteClusters();
    }

    @Override
    protected TransportAddress getTransportAddress(TransportService transportService) {
        return apiKeyAuthenticationEnabled()
            ? transportService.boundRemoteAccessAddress().publishAddress()
            : super.getTransportAddress(transportService);
    }

    private String addCrossClusterApiKey(String clusterAlias) throws Exception {
        Client client = internalClient(clusterAlias);
        CreateCrossClusterApiKeyRequest request = generateCreateCrossClusterApiKeyRequest(
            "cross_cluster_access_key",
            crossClusterAccessJson()
        );
        var responseFuture = client.execute(CreateCrossClusterApiKeyAction.INSTANCE, request);

        String encodedApiKey;
        var response = responseFuture.actionGet(TEST_REQUEST_TIMEOUT);
        try {
            encodedApiKey = response.getEncodedKey();
        } finally {
            response.decRef();
        }

        return encodedApiKey;
    }

    private void setRemoteClusterCredentials(Map<String, String> crossClusterApiKeys) throws Exception {
        InternalTestCluster localCluster = cluster(LOCAL_CLUSTER);
        for (String nodeName : localCluster.getNodeNames()) {
            Environment environment = localCluster.getInstance(Environment.class, nodeName);

            KeyStoreWrapper keystore = null;
            try {
                keystore = KeyStoreWrapper.load(environment.configDir());
                if (keystore == null) {
                    keystore = KeyStoreWrapper.create();
                } else {
                    keystore.decrypt(new char[0]);
                }

                for (var entry : crossClusterApiKeys.entrySet()) {
                    keystore.setString("cluster.remote." + entry.getKey() + ".credentials", entry.getValue().toCharArray());
                }
                keystore.save(environment.configDir(), new char[0]);
            } finally {
                if (keystore != null) {
                    keystore.close();
                }
            }
        }

        Client client = internalClient();
        NodesReloadSecureSettingsRequest reloadSecureSettingsRequest = new NodesReloadSecureSettingsRequest(Strings.EMPTY_ARRAY);
        try {
            assertResponse(client.execute(TransportNodesReloadSecureSettingsAction.TYPE, reloadSecureSettingsRequest), r -> {
                assertThat(r.hasFailures(), is(false));
            });
        } finally {
            reloadSecureSettingsRequest.decRef();
        }
    }

    protected boolean enableSecurity() {
        return true;
    }

    protected boolean useApiKeyAuthentication() {
        return true;
    }

    protected boolean apiKeyAuthenticationEnabled() {
        return enableSecurity() && useApiKeyAuthentication();
    }

    protected String crossClusterAccessJson() {
        return """
            {
              "search": [
                {
                  "names": [
                    "*"
                  ],
                  "allow_restricted_indices": false
                }
              ],
              "replication": [
                {
                  "names": [
                    "*"
                  ],
                  "allow_restricted_indices": false
                }
              ]
            }""";
    }

    private static CreateCrossClusterApiKeyRequest generateCreateCrossClusterApiKeyRequest(String name, String accessJson)
        throws IOException {
        CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder = CrossClusterApiKeyRoleDescriptorBuilder.parse(accessJson);
        var request = new CreateCrossClusterApiKeyRequest(name, roleDescriptorBuilder, null, null, null);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        return request;
    }
}
