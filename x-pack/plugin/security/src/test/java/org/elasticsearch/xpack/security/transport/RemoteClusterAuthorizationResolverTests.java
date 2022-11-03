/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RemoteClusterAuthorizationResolverTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool(getTestName());
        this.clusterService = ClusterServiceUtils.createClusterService(this.threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        this.clusterService.close();
        terminate(this.threadPool);
    }

    public void testRemoteClusterApiKeyChanges() {
        final String clusterNameA = "clusterA";
        final String clusterNameB = "clusterB";
        final String clusterDoesNotExist = randomAlphaOfLength(10);
        final Settings.Builder initialSettingsBuilder = Settings.builder();
        initialSettingsBuilder.put("cluster.remote." + clusterNameA + ".authorization", "initialize");
        if (randomBoolean()) {
            initialSettingsBuilder.put("cluster.remote." + clusterNameB + ".authorization", "");
        }
        final Settings initialSettings = initialSettingsBuilder.build();
        RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver = new RemoteClusterAuthorizationResolver(
            initialSettings,
            this.clusterService.getClusterSettings()
        );
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterNameA), is(equalTo("initialize")));
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterNameB), is(nullValue()));
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterDoesNotExist), is(nullValue()));
        final DiscoveryNode masterNodeA = this.clusterService.state().nodes().getMasterNode();

        // Add clusterB authorization setting
        final String clusterBapiKey1 = randomApiKey();
        final Settings newSettingsAddClusterB = Settings.builder()
            .put("cluster.remote." + clusterNameA + ".authorization", "addB")
            .put("cluster.remote." + clusterNameB + ".authorization", clusterBapiKey1)
            .build();
        final ClusterState newClusterState1 = createClusterState(clusterNameA, masterNodeA, newSettingsAddClusterB);
        ClusterServiceUtils.setState(this.clusterService, newClusterState1);
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterNameA), is(equalTo("addB")));
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterNameB), is(equalTo(clusterBapiKey1)));
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterDoesNotExist), is(nullValue()));

        // Change clusterB authorization setting
        final String clusterBapiKey2 = randomApiKey();
        final Settings newSettingsUpdateClusterB = Settings.builder()
            .put("cluster.remote." + clusterNameA + ".authorization", "editB")
            .put("cluster.remote." + clusterNameB + ".authorization", clusterBapiKey2)
            .build();
        final ClusterState newClusterState2 = createClusterState(clusterNameA, masterNodeA, newSettingsUpdateClusterB);
        ClusterServiceUtils.setState(this.clusterService, newClusterState2);
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterNameA), is(equalTo("editB")));
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterNameB), is(equalTo(clusterBapiKey2)));
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterDoesNotExist), is(nullValue()));

        // Remove clusterB authorization setting
        final Settings.Builder newSettingsOmitClusterBBuilder = Settings.builder();
        newSettingsOmitClusterBBuilder.put("cluster.remote." + clusterNameA + ".authorization", "omitB");
        if (randomBoolean()) {
            initialSettingsBuilder.put("cluster.remote." + clusterNameB + ".authorization", "");
        }
        final Settings newSettingsOmitClusterB = newSettingsOmitClusterBBuilder.build();
        final ClusterState newClusterState3 = createClusterState(clusterNameA, masterNodeA, newSettingsOmitClusterB);
        ClusterServiceUtils.setState(this.clusterService, newClusterState3);
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterNameA), is(equalTo("omitB")));
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterNameB), is(nullValue()));
        assertThat(remoteClusterAuthorizationResolver.resolveAuthorization(clusterDoesNotExist), is(nullValue()));
    }

    private static ClusterState createClusterState(final String clusterName, final DiscoveryNode masterNode, final Settings newSettings) {
        final DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        discoBuilder.add(masterNode);
        discoBuilder.masterNodeId(masterNode.getId());

        final ClusterState.Builder state = ClusterState.builder(new ClusterName(clusterName));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().persistentSettings(newSettings).generateClusterUuidIfNeeded());
        state.routingTable(RoutingTable.builder().build());
        return state.build();
    }

    private String randomApiKey() {
        final String id = "apikey_" + randomAlphaOfLength(6);
        // Sufficient for testing. See ApiKeyService and ApiKeyService.ApiKeyCredentials for actual API Key generation.
        try (SecureString secret = UUIDs.randomBase64UUIDSecureString()) {
            final String apiKey = id + ":" + secret;
            return Base64.getEncoder().withoutPadding().encodeToString(apiKey.getBytes(StandardCharsets.UTF_8));
        }
    }
}
