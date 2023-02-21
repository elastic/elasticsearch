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
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

import static org.elasticsearch.xpack.security.transport.RemoteClusterCredentialsResolver.RemoteClusterCredentials;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RemoteClusterCredentialsResolverTests extends ESTestCase {

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
        clusterService.close();
        terminate(threadPool);
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
        RemoteClusterCredentialsResolver remoteClusterCredentialsResolver = new RemoteClusterCredentialsResolver(
            initialSettings,
            clusterService.getClusterSettings()
        );
        assertThat(
            remoteClusterCredentialsResolver.resolve(clusterNameA),
            is(equalTo(remoteClusterCredentials(clusterNameA, "initialize")))
        );
        assertThat(remoteClusterCredentialsResolver.resolve(clusterNameB), is(Optional.empty()));
        assertThat(remoteClusterCredentialsResolver.resolve(clusterDoesNotExist), is(Optional.empty()));
        final DiscoveryNode masterNodeA = clusterService.state().nodes().getMasterNode();

        // Add clusterB authorization setting
        final String clusterBapiKey1 = randomApiKey();
        final Settings newSettingsAddClusterB = Settings.builder()
            .put("cluster.remote." + clusterNameA + ".authorization", "addB")
            .put("cluster.remote." + clusterNameB + ".authorization", clusterBapiKey1)
            .build();
        final ClusterState newClusterState1 = createClusterState(clusterNameA, masterNodeA, newSettingsAddClusterB);
        ClusterServiceUtils.setState(clusterService, newClusterState1);
        assertThat(remoteClusterCredentialsResolver.resolve(clusterNameA), is(equalTo(remoteClusterCredentials(clusterNameA, "addB"))));
        assertThat(
            remoteClusterCredentialsResolver.resolve(clusterNameB),
            is(equalTo(remoteClusterCredentials(clusterNameB, clusterBapiKey1)))
        );
        assertThat(remoteClusterCredentialsResolver.resolve(clusterDoesNotExist), is(Optional.empty()));

        // Change clusterB authorization setting
        final String clusterBapiKey2 = randomApiKey();
        final Settings newSettingsUpdateClusterB = Settings.builder()
            .put("cluster.remote." + clusterNameA + ".authorization", "editB")
            .put("cluster.remote." + clusterNameB + ".authorization", clusterBapiKey2)
            .build();
        final ClusterState newClusterState2 = createClusterState(clusterNameA, masterNodeA, newSettingsUpdateClusterB);
        ClusterServiceUtils.setState(clusterService, newClusterState2);
        assertThat(remoteClusterCredentialsResolver.resolve(clusterNameA), is(equalTo(remoteClusterCredentials(clusterNameA, "editB"))));
        assertThat(
            remoteClusterCredentialsResolver.resolve(clusterNameB),
            is(equalTo(remoteClusterCredentials(clusterNameB, clusterBapiKey2)))
        );
        assertThat(remoteClusterCredentialsResolver.resolve(clusterDoesNotExist), is(Optional.empty()));

        // Remove clusterB authorization setting
        final Settings.Builder newSettingsOmitClusterBBuilder = Settings.builder();
        newSettingsOmitClusterBBuilder.put("cluster.remote." + clusterNameA + ".authorization", "omitB");
        if (randomBoolean()) {
            initialSettingsBuilder.put("cluster.remote." + clusterNameB + ".authorization", "");
        }
        final Settings newSettingsOmitClusterB = newSettingsOmitClusterBBuilder.build();
        final ClusterState newClusterState3 = createClusterState(clusterNameA, masterNodeA, newSettingsOmitClusterB);
        ClusterServiceUtils.setState(clusterService, newClusterState3);
        assertThat(remoteClusterCredentialsResolver.resolve(clusterNameA), is(equalTo(remoteClusterCredentials(clusterNameA, "omitB"))));
        assertThat(remoteClusterCredentialsResolver.resolve(clusterNameB), is(Optional.empty()));
        assertThat(remoteClusterCredentialsResolver.resolve(clusterDoesNotExist), is(Optional.empty()));
    }

    private static Optional<RemoteClusterCredentials> remoteClusterCredentials(String clusterAlias, String encodedApiKeyValue) {
        return Optional.of(new RemoteClusterCredentials(clusterAlias, ApiKeyService.withApiKeyPrefix(encodedApiKeyValue)));
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
