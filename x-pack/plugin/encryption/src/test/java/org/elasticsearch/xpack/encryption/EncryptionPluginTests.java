/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.encryption;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The join checks that keep a node that can't persist the key out of a cluster that has one: the joining node's own join validator, and
 * the join attribute it reports for the validator on the master. The cluster service and plugin services are mocked, since only the
 * cluster state they return matters here.
 */
public class EncryptionPluginTests extends ESTestCase {

    private static final String CAN_PERSIST = EncryptionPlugin.CAN_PERSIST_KEY_JOIN_ATTRIBUTE;

    private final DiscoveryNode localNode = DiscoveryNodeUtils.create("local");
    private final DiscoveryNode otherNode = DiscoveryNodeUtils.create("other");
    private final AtomicReference<ClusterState> state = new AtomicReference<>();
    private EncryptionPlugin plugin;

    @Before
    public void createPlugin() {
        state.set(stateWith(null));
        plugin = new EncryptionPlugin(Settings.EMPTY);
        plugin.createComponents(pluginServices());
    }

    public void testNodeWithoutPasswordCannotJoinOnceKeyIsInstalled() {
        state.set(stateWith(key()));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> plugin.getJoinValidator().accept(localNode, state.get()));
        assertThat(e.getMessage(), containsString("this node cannot join the cluster: the cluster uses a cluster state encryption key"));

        plugin.reload(EncryptionIntegTestUtils.passwordSettings("v1", "test-password-fips"));
        plugin.getJoinValidator().accept(localNode, state.get());
    }

    public void testNodeWithoutPasswordJoinsWhileThereIsNoUsableKey() {
        plugin.getJoinValidator().accept(localNode, stateWith(null));
        plugin.getJoinValidator()
            .accept(localNode, stateWith(ProjectEncryptionKeyMetadata.degraded("k1", "v1", TestPekEncryption.NO_OP, "no")));
    }

    /** The master runs the join validators too, for the joining node, which its own settings say nothing about. */
    public void testJoinValidatorIgnoresOtherNodes() {
        plugin.getJoinValidator().accept(otherNode, stateWith(key()));
    }

    public void testJoinAttributeReportsWhetherThisNodeCanPersistTheKey() {
        assertEquals(Map.of(CAN_PERSIST, "false"), plugin.getJoinAttributes());
        plugin.reload(EncryptionIntegTestUtils.passwordSettings("v1", "test-password-fips"));
        assertEquals(Map.of(CAN_PERSIST, "true"), plugin.getJoinAttributes());
    }

    public void testMasterRejectsNodeThatReportsItCannotPersistTheKey() {
        ClusterState withKey = stateWith(key());
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> plugin.getJoinAttributesValidator().validate(otherNode, Map.of(CAN_PERSIST, "false"), withKey)
        );
        assertThat(e.getMessage(), containsString("node [" + otherNode.getName() + "] cannot join the cluster"));

        plugin.getJoinAttributesValidator().validate(otherNode, Map.of(CAN_PERSIST, "true"), withKey);
        // a node on an earlier version doesn't report it, and its own join validator checks it
        plugin.getJoinAttributesValidator().validate(otherNode, Map.of(), withKey);
        plugin.getJoinAttributesValidator().validate(otherNode, Map.of(CAN_PERSIST, "false"), stateWith(null));
    }

    /** On stateless with the password in cluster secrets, a keystore reload, which doesn't have it, doesn't replace it. */
    public void testKeystoreReloadKeepsPasswordFromClusterSecretsOnStateless() throws IOException {
        String json = Strings.format(
            "{\"string_secrets\":{\"%s\":\"v1\",\"%sv1\":\"test-password-fips\"}}",
            ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY,
            ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
        );
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            ClusterSecrets secrets = new ClusterSecrets(1, SecureClusterStateSettings.fromXContent(parser));
            state.set(ClusterState.builder(stateWith(null)).putCustom(ClusterSecrets.TYPE, secrets).build());
        }
        plugin = new EncryptionPlugin(Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build());
        plugin.createComponents(pluginServices());

        plugin.reload(Settings.EMPTY);
        assertEquals(Map.of(CAN_PERSIST, "true"), plugin.getJoinAttributes());
    }

    private static ProjectEncryptionKeyMetadata key() {
        byte[] bytes = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(bytes);
        return new ProjectEncryptionKeyMetadata(Map.of("k1", new KeyEntry(bytes, 0L)), "k1", "v1", Map.of(), TestPekEncryption.NO_OP);
    }

    private ClusterState stateWith(ProjectEncryptionKeyMetadata pek) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (pek != null) {
            project.putCustom(ProjectEncryptionKeyMetadata.TYPE, pek);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(project.build()).build())
            .nodes(DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).masterNodeId(otherNode.getId()))
            .build();
    }

    private Plugin.PluginServices pluginServices() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenAnswer(invocation -> state.get());
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(ProjectEncryptionKeyPasswordSettings.ENCRYPTION_REQUIRED))
        );
        when(clusterService.createTaskQueue(anyString(), any(), any())).thenReturn(null);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.clusterService()).thenReturn(clusterService);
        when(services.threadPool()).thenReturn(threadPool);
        when(services.projectResolver()).thenReturn(DefaultProjectResolver.INSTANCE);
        when(services.featureService()).thenReturn(mock(FeatureService.class));
        when(services.client()).thenReturn(mock(Client.class));
        return services;
    }
}
