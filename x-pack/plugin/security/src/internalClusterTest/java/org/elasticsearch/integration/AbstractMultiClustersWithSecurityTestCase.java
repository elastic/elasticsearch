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
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;
import static org.hamcrest.CoreMatchers.is;

public abstract class AbstractMultiClustersWithSecurityTestCase extends AbstractMultiClustersTestCase {
    // TODO: Need to clear this? How does this work with different values of reuseCluster?
    private final Map<String, String> crossClusterApiKeys = new HashMap<>();

    @Override
    protected Settings nodeSettings(String clusterAlias) {
        Settings.Builder builder = Settings.builder().put(nodeSettings());
        if (enableSecurity() && useApiKeyAuthentication()) {
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
    protected void customizeRemoteClusterConfig(String clusterAlias) throws Exception {
        if (enableSecurity() && useApiKeyAuthentication()) {
            Client client = internalClient(clusterAlias);

            CreateCrossClusterApiKeyRequest request = generateCreateCrossClusterApiKeyRequest("cross_cluster_access_key", crossClusterAccessJson());
            var responseFuture = client.execute(CreateCrossClusterApiKeyAction.INSTANCE, request);
            assertResponse(responseFuture, r -> {
                String encodedKey = r.getEncodedKey();
                crossClusterApiKeys.put(clusterAlias, encodedKey);
            });
        }
    }

    @Override
    protected void customizeLocalClusterConfig(String remoteClusterAlias) throws Exception {
        if (enableSecurity() && useApiKeyAuthentication()) {
            String crossClusterApiKey = crossClusterApiKeys.get(remoteClusterAlias);
            if (crossClusterApiKey == null) {
                throw new IllegalStateException("Cross-cluster API key does not exist for cluster [" + remoteClusterAlias + "]");
            }

            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("cluster.remote." + remoteClusterAlias + ".credentials", crossClusterApiKey);

            Client client = internalClient();
            Settings.Builder settings = Settings.builder().setSecureSettings(secureSettings);
            assertAcked(client.admin().cluster().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(settings));

            NodesReloadSecureSettingsRequest reloadSecureSettingsRequest = new NodesReloadSecureSettingsRequest(Strings.EMPTY_ARRAY);
            assertResponse(client.execute(TransportNodesReloadSecureSettingsAction.TYPE, reloadSecureSettingsRequest), r -> {
                assertThat(r.hasFailures(), is(false));
            });
        }
    }

    protected boolean enableSecurity() {
        return true;
    }

    protected boolean useApiKeyAuthentication() {
        return true;
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

    private static CreateCrossClusterApiKeyRequest generateCreateCrossClusterApiKeyRequest(String name, String accessJson) throws IOException {
        CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder = CrossClusterApiKeyRoleDescriptorBuilder.parse(accessJson);
        var request = new CreateCrossClusterApiKeyRequest(name, roleDescriptorBuilder, null, null, null);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        return request;
    }
}
