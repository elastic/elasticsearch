/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SystemIndexSettingsUpdateServiceIT extends ESIntegTestCase {

    static final String UNMANAGED_INDEX = ".test-settings-unmanaged-1";
    static final String UNMANAGED_INDEX_PATTERN = ".test-settings-unmanaged*";

    static final String MANAGED_ALIAS = ".test-settings-managed";
    static final String MANAGED_PRIMARY_INDEX = ".test-settings-managed-1";
    static final String MANAGED_INDEX_PATTERN = ".test-settings-managed*";

    static final String DATA_STREAM_NAME = ".test-settings-ds";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestPlugin.class);
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    /**
     * Single plugin registering all system resources used by this test class:
     * an unmanaged index, a managed index (with {@code auto_expand_replicas=0-1}),
     * and an internal system data stream.
     */
    public static class TestPlugin extends Plugin implements SystemIndexPlugin {

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(UNMANAGED_INDEX_PATTERN)
                    .setDescription("Test unmanaged system index for settings tests")
                    .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                    .build(),
                SystemIndexDescriptor.builder()
                    .setIndexPattern(MANAGED_INDEX_PATTERN)
                    .setPrimaryIndex(MANAGED_PRIMARY_INDEX)
                    .setAliasName(MANAGED_ALIAS)
                    .setDescription("Test managed system index for settings tests")
                    .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
                    .setMappings(
                        "{\"_meta\":{\""
                            + SystemIndexDescriptor.VERSION_META_KEY
                            + "\":1},"
                            + "\"properties\":{\"field\":{\"type\":\"keyword\"}}}"
                    )
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                            .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
                            .build()
                    )
                    .setOrigin("test")
                    .build()
            );
        }

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            try {
                return List.of(
                    new SystemDataStreamDescriptor(
                        DATA_STREAM_NAME,
                        "Test system data stream for settings tests",
                        SystemDataStreamDescriptor.Type.INTERNAL,
                        ComposableIndexTemplate.builder()
                            .template(
                                new Template(null, new CompressedXContent("{\"properties\":{\"@timestamp\":{\"type\":\"date\"}}}"), null)
                            )
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of(),
                        "test",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    )
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String getFeatureName() {
            return "test-settings-update";
        }

        @Override
        public String getFeatureDescription() {
            return "Test plugin for SystemIndexSettingsUpdateServiceIT";
        }
    }

    /**
     * Dynamically changing {@link SystemIndices#NUMBER_OF_REPLICAS_SETTING} should update all existing
     * system resources: unmanaged indices, managed indices, and data stream backing indices.
     */
    public void testDynamicNumberOfReplicasSettingUpdatesExistingSystemResources() throws Exception {
        internalCluster().startNodes(3);
        createIndex(UNMANAGED_INDEX);
        createIndex(MANAGED_ALIAS);
        createDataStream();
        ensureGreen();

        updateClusterSettings(
            Settings.builder()
                .put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "false")
                .put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
        );

        assertBusy(() -> {
            assertThat(getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
            assertThat(getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
        });
    }

    /**
     * Dynamically changing {@link SystemIndices#AUTO_EXPAND_REPLICAS_SETTING} should update all existing
     * system resources, overriding values from descriptor / template settings where applicable.
     */
    public void testDynamicAutoExpandReplicasSettingUpdatesExistingSystemResources() throws Exception {
        internalCluster().startNodes(1);
        createIndex(UNMANAGED_INDEX);
        ensureGreen(UNMANAGED_INDEX);
        createIndex(MANAGED_ALIAS);  // descriptor has auto_expand=0-1, green on single node
        ensureGreen(MANAGED_ALIAS);
        createDataStream();  // default number_of_replicas=1; may be yellow until auto_expand kicks in

        updateClusterSettings(Settings.builder().put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-2"));

        assertBusy(() -> {
            assertThat(getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-2"));
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-2"));
            assertThat(getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-2"));
        });
    }

    /**
     * Removing {@link SystemIndices#AUTO_EXPAND_REPLICAS_SETTING} after it was set should reset each
     * system resource to its descriptor value, not to the setting's generic default.  For an unmanaged
     * index or a data stream whose template does not specify {@code auto_expand_replicas}, the generic
     * default ("false") is used; for a managed index whose descriptor specifies
     * {@code auto_expand_replicas=0-1}, that descriptor value is used instead.
     */
    public void testRemovingAutoExpandReplicasSettingResetsToDescriptorValue() throws Exception {
        internalCluster().startNodes(3);
        createIndex(UNMANAGED_INDEX);
        createIndex(MANAGED_ALIAS);  // descriptor has auto_expand=0-1
        createDataStream();
        ensureGreen();

        // Override auto_expand for all indices to something other than their descriptor values
        updateClusterSettings(Settings.builder().put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-2"));
        assertBusy(() -> {
            assertThat(getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-2"));
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-2"));
            assertThat(getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-2"));
        });

        // Remove the cluster override — each resource should revert to its descriptor value
        updateClusterSettings(Settings.builder().putNull(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey()));
        assertBusy(() -> {
            // Unmanaged index: no descriptor setting → falls back to the index-level generic default
            assertThat(
                getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS),
                equalTo(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getDefault(Settings.EMPTY).toString())
            );
            // Managed index: descriptor specifies auto_expand_replicas=0-1 → reset to that value
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-1"));
            // Data stream backing index: template has no auto_expand setting → falls back to the generic default
            assertThat(
                getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS),
                equalTo(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getDefault(Settings.EMPTY).toString())
            );
        });
    }

    /**
     * Removing {@link SystemIndices#NUMBER_OF_REPLICAS_SETTING} after it was set should reset all
     * existing system resources back to the default replica count.
     */
    public void testRemovingNumberOfReplicasSettingResetsToDefault() throws Exception {
        internalCluster().startNodes(3);
        createIndex(UNMANAGED_INDEX);
        createIndex(MANAGED_ALIAS);
        createDataStream();
        ensureGreen();

        updateClusterSettings(Settings.builder().put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "false"));
        updateClusterSettings(Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2));
        assertBusy(() -> {
            assertThat(getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
            assertThat(getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
        });

        updateClusterSettings(Settings.builder().putNull(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey()));
        assertBusy(() -> {
            assertThat(getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
            assertThat(getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
        });
    }

    /**
     * When cluster state already has system-index replica settings (set via the cluster API),
     * those values must survive a restart even if the node settings (e.g. {@code elasticsearch.yml})
     * carry a different value — the cluster state has higher priority than node config.
     */
    public void testClusterStateSettingsTakePriorityOverNodeSettingsOnRestart() throws Exception {
        internalCluster().startNodes(2);
        createIndex(UNMANAGED_INDEX);
        createIndex(MANAGED_ALIAS);
        createDataStream();

        // Set both settings via the cluster API so they are persisted in the cluster state.
        updateClusterSettings(
            Settings.builder()
                .put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "false")
                .put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
        );
        assertBusy(() -> {
            assertThat(getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
            assertThat(getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
        });
        ensureGreen();

        // Restart with number_of_replicas=2 in node config — the cluster state value (1) must win.
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "false")
                    .put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                    .build();
            }
        });
        ensureGreen();

        assertBusy(() -> {
            assertThat(getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
            assertThat(getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("1"));
        });
    }

    public void testManagedSystemIndexCreatedAfterClusterSettingAlreadySetPicksUpSetting() throws Exception {
        internalCluster().startNodes(1);

        // Disable auto-expand and set fixed replicas BEFORE any index is created
        updateClusterSettings(
            Settings.builder()
                .put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "false")
                .put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
        );

        // The managed descriptor has auto_expand_replicas=0-1 — the cluster setting must win at creation time
        createIndex(MANAGED_ALIAS);
        ensureGreen(MANAGED_ALIAS);

        assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("false"));
        assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("0"));
    }

    /**
     * Settings configured in node settings (e.g. {@code elasticsearch.yml}) should be applied to
     * all pre-existing system resources the first time the local node is elected master after a restart.
     */
    public void testNodeSettingsAppliedToExistingResourcesOnMasterElection() throws Exception {
        internalCluster().startNodes(3);
        createIndex(UNMANAGED_INDEX);
        createIndex(MANAGED_ALIAS);  // descriptor has auto_expand=0-1
        createDataStream();          // default replicas=1, green on three nodes
        ensureGreen();

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "false")
                    .put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                    .build();
            }
        });
        ensureGreen();

        assertBusy(() -> {
            assertThat(getSetting(UNMANAGED_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
            assertThat(getSetting(MANAGED_ALIAS, MANAGED_PRIMARY_INDEX, IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
            assertThat(getSetting(dataStreamBackingIndex(), IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
        });
    }

    private void createDataStream() {
        assertTrue(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, DATA_STREAM_NAME)
            ).actionGet().isAcknowledged()
        );
    }

    @SuppressWarnings("removal")
    private String dataStreamBackingIndex() {
        var dataStream = clusterService().state().metadata().getProject().dataStreams().get(DATA_STREAM_NAME);
        return dataStream.getIndices().getFirst().getName();
    }

    private String getSetting(String index, String settingName) {
        return getSetting(index, index, settingName);
    }

    /** Fetches a setting from a concrete index, using {@code requestIndex} to resolve via alias if needed. */
    private String getSetting(String requestIndex, String concreteIndex, String settingName) {
        GetSettingsResponse response = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(requestIndex))
            .actionGet();
        return response.getSetting(concreteIndex, settingName);
    }
}
