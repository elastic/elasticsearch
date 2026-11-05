/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.elasticsearch.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Sets secure settings through {@link UpdateSecureSettingsAction} and checks that every node's reloadable plugins see the values,
 * that values are stored encrypted, and that they survive a full cluster restart.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UpdateSecureSettingsIT extends ESIntegTestCase {

    private static final String DYNAMIC = "test.secure_settings_api.dynamic";
    private static final String STATIC = "test.secure_settings_api.static";
    private static final String OPERATOR = "test.secure_settings_api.operator";

    @BeforeClass
    public static void checkFeatureFlagEnabled() {
        assumeTrue(
            "secure_settings_api feature flag must be enabled",
            UpdateSecureSettingsAction.SECURE_SETTINGS_API_FEATURE_FLAG.isEnabled()
        );
    }

    private static final Map<String, String> KEYSTORE = Map.of(
        "cluster.state.encryption.active_password_id",
        "v1",
        "cluster.state.encryption.password.v1",
        "encryption-test-password"
    );

    private Path configDir;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        MockSecureSettings secure = new MockSecureSettings();
        KEYSTORE.forEach(secure::setString);
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).setSecureSettings(secure).build();
    }

    /**
     * All nodes share one config directory, so {@link #testReloadSecureSettingsKeepsApiSetValues} can write one keystore file for
     * {@code reload_secure_settings} to read on every node.
     */
    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        if (configDir == null) {
            configDir = createTempDir();
        }
        return configDir;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        plugins.add(SecureSettingsPlugin.class);
        plugins.add(RecordingReloadablePlugin.class);
        return plugins;
    }

    public void testSetRotateAndRemove() throws Exception {
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        updateSecureSettings(Map.of(DYNAMIC, new SecureString("first".toCharArray())));
        assertBusy(() -> assertReloadedValueOnAllNodes("first"));
        assertStoredEncrypted("first");

        updateSecureSettings(Map.of(DYNAMIC, new SecureString("second".toCharArray())));
        assertBusy(() -> assertReloadedValueOnAllNodes("second"));

        Map<String, SecureString> remove = new HashMap<>();
        remove.put(DYNAMIC, null);
        updateSecureSettings(remove);
        assertBusy(() -> assertReloadedValueOnAllNodes(null));
        assertThat(
            internalCluster().clusterService().state().metadata().getProject(ProjectId.DEFAULT).custom(SecureSettingsMetadata.TYPE),
            nullValue()
        );
    }

    public void testValuesSurviveFullClusterRestart() throws Exception {
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        updateSecureSettings(Map.of(DYNAMIC, new SecureString("persisted".toCharArray())));
        assertBusy(() -> assertReloadedValueOnAllNodes("persisted"));

        internalCluster().fullRestart();
        ensureStableCluster(2);

        assertBusy(() -> assertReloadedValueOnAllNodes("persisted"));
    }

    public void testReloadSecureSettingsKeepsApiSetValues() throws Exception {
        assumeFalse("an unprotected keystore can't be saved in FIPS mode", inFipsJvm());
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        updateSecureSettings(Map.of(DYNAMIC, new SecureString("api".toCharArray())));
        assertBusy(() -> assertReloadedValueOnAllNodes("api"));

        try (KeyStoreWrapper keystore = KeyStoreWrapper.create()) {
            KEYSTORE.forEach((name, value) -> keystore.setString(name, value.toCharArray()));
            keystore.setString(DYNAMIC, "keystore".toCharArray());
            keystore.save(configDir, new char[0]);
        }
        NodesReloadSecureSettingsRequest request = new NodesReloadSecureSettingsRequest(Strings.EMPTY_ARRAY);
        try {
            NodesReloadSecureSettingsResponse response = client().execute(TransportNodesReloadSecureSettingsAction.TYPE, request)
                .actionGet();
            assertThat(response.failures(), empty());
            response.getNodes().forEach(node -> assertThat(node.reloadException(), nullValue()));
        } finally {
            request.decRef();
        }
        assertReloadedValueOnAllNodes("api");

        Map<String, SecureString> remove = new HashMap<>();
        remove.put(DYNAMIC, null);
        updateSecureSettings(remove);
        assertBusy(() -> assertReloadedValueOnAllNodes("keystore"));
    }

    public void testRejectsSecureSettingDefinedInReservedState() throws Exception {
        internalCluster().startNode();
        ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        safeAwait(listener -> clusterService.submitUnbatchedStateUpdateTask("reserve secure setting", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ReservedStateMetadata reservedState = ReservedStateMetadata.builder("test_namespace")
                    .version(1L)
                    .putHandler(new ReservedStateHandlerMetadata(ClusterSecrets.NAME, Set.of(DYNAMIC)))
                    .build();
                return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata()).put(reservedState)).build();
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(null);
            }
        }));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> updateSecureSettings(Map.of(DYNAMIC, new SecureString("value".toCharArray())))
        );
        assertThat(e.getMessage(), containsString("[" + DYNAMIC + "] set as read-only by [test_namespace]"));
    }

    public void testRejectsSettingsThatCannotBeSetThroughTheApi() {
        internalCluster().startNode();

        IllegalArgumentException notDynamic = expectThrows(
            IllegalArgumentException.class,
            () -> updateSecureSettings(Map.of(STATIC, new SecureString("value".toCharArray())))
        );
        assertThat(notDynamic.getMessage(), containsString("cannot be set through the API"));

        IllegalArgumentException unknown = expectThrows(
            IllegalArgumentException.class,
            () -> updateSecureSettings(Map.of("test.secure_settings_api.*", new SecureString("value".toCharArray())))
        );
        assertThat(unknown.getMessage(), containsString("is not recognized"));

        IllegalArgumentException operatorOnly = expectThrows(
            IllegalArgumentException.class,
            () -> updateSecureSettings(Map.of(OPERATOR, new SecureString("value".toCharArray())))
        );
        assertThat(operatorOnly.getMessage(), containsString("operator-only secure setting [" + OPERATOR + "]"));

        IllegalArgumentException empty = expectThrows(
            IllegalArgumentException.class,
            () -> updateSecureSettings(Map.of(DYNAMIC, new SecureString(new char[0])))
        );
        assertThat(empty.getMessage(), containsString("cannot be empty"));
    }

    private void updateSecureSettings(Map<String, SecureString> settings) {
        client().execute(
            UpdateSecureSettingsAction.INSTANCE,
            new UpdateSecureSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, settings)
        ).actionGet();
    }

    private void assertReloadedValueOnAllNodes(String expected) {
        for (String node : internalCluster().getNodeNames()) {
            RecordingReloadablePlugin plugin = internalCluster().getInstance(PluginsService.class, node)
                .filterPlugins(RecordingReloadablePlugin.class)
                .findFirst()
                .orElseThrow();
            assertThat("node [" + node + "]", plugin.lastValue, equalTo(expected));
        }
    }

    private void assertStoredEncrypted(String plaintext) {
        var metadata = internalCluster().clusterService().state().metadata();
        SecureSettingsMetadata stored = metadata.getProject(ProjectId.DEFAULT).custom(SecureSettingsMetadata.TYPE);
        assertThat(stored, notNullValue());
        assertThat(stored.settings().get(DYNAMIC), notNullValue());
        assertThat(stored.toString(), not(containsString(plaintext)));
    }

    public static class RecordingReloadablePlugin extends Plugin implements ReloadablePlugin {

        static final Setting<SecureString> DYNAMIC_SETTING = SecureSetting.secureString(DYNAMIC, null, Setting.Property.Dynamic);
        static final Setting<SecureString> STATIC_SETTING = SecureSetting.secureString(STATIC, null);
        static final Setting<SecureString> OPERATOR_SETTING = SecureSetting.secureString(OPERATOR, null, Setting.Property.OperatorDynamic);

        volatile String lastValue;

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(DYNAMIC_SETTING, STATIC_SETTING, OPERATOR_SETTING);
        }

        @Override
        public void reload(Settings settings) {
            try (SecureString value = DYNAMIC_SETTING.get(settings)) {
                lastValue = value == null || value.length() == 0 ? null : value.toString();
            }
        }
    }
}
