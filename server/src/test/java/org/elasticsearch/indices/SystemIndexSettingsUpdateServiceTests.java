/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class SystemIndexSettingsUpdateServiceTests extends ESTestCase {

    private static final DiscoveryNode MASTER_NODE = DiscoveryNodeUtils.create("master");
    private static final DiscoveryNode OTHER_NODE = DiscoveryNodeUtils.create("other");

    // ---- helpers ----

    private static ClusterChangedEvent event(ClusterState newState, ClusterState previousState) {
        return new ClusterChangedEvent("test", newState, previousState);
    }

    /** Cluster state where the local node IS the elected master, with no global blocks. */
    private static ClusterState masterState(Metadata metadata) {
        return masterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK, metadata);
    }

    private static ClusterState masterState(ClusterBlocks blocks, Metadata metadata) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(MASTER_NODE).masterNodeId(MASTER_NODE.getId()).localNodeId(MASTER_NODE.getId()))
            .blocks(blocks)
            .metadata(metadata)
            .build();
    }

    /** Cluster state where the local node is NOT the master. */
    private static ClusterState nonMasterState(Metadata metadata) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder().add(MASTER_NODE).add(OTHER_NODE).masterNodeId(MASTER_NODE.getId()).localNodeId(OTHER_NODE.getId())
            )
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .metadata(metadata)
            .build();
    }

    /** Metadata with a single system index and optional cluster-level persistent settings. */
    private static Metadata metadataWithSystemIndex(Settings clusterPersistentSettings) {
        IndexMetadata sysIndex = IndexMetadata.builder(".system-test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(true)
            .build();
        return Metadata.builder()
            .persistentSettings(clusterPersistentSettings)
            .put(ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).put(sysIndex, false))
            .build();
    }

    private static SystemIndexSettingsUpdateService service(MetadataUpdateSettingsService updateService, Settings nodeSettings) {
        return new SystemIndexSettingsUpdateService(updateService, new SystemIndices(List.of()), nodeSettings);
    }

    // ---- tests ----

    public void testSkipsWhenNotMaster() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(
            updateService,
            Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2).build()
        );

        ClusterState state = nonMasterState(metadataWithSystemIndex(Settings.EMPTY));
        service.clusterChanged(event(state, state));

        verifyNoInteractions(updateService);
    }

    public void testSkipsWhileMetadataReadBlockPresent() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(
            updateService,
            Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2).build()
        );

        ClusterState blocked = masterState(
            ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build(),
            metadataWithSystemIndex(Settings.EMPTY)
        );
        assertTrue(blocked.blocks().hasGlobalBlockWithLevel(ClusterBlockLevel.METADATA_READ));
        service.clusterChanged(event(blocked, blocked));

        verifyNoInteractions(updateService);
    }

    public void testAppliesNumberOfReplicasNodeSettingOnFirstMasterElection() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(
            updateService,
            Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2).build()
        );

        ClusterState state = masterState(metadataWithSystemIndex(Settings.EMPTY));
        service.clusterChanged(event(state, state));

        ArgumentCaptor<UpdateSettingsClusterStateUpdateRequest> captor = ArgumentCaptor.forClass(
            UpdateSettingsClusterStateUpdateRequest.class
        );
        verify(updateService).updateSettings(captor.capture(), any());
        assertThat(captor.getValue().settings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
    }

    public void testAppliesAutoExpandReplicasNodeSettingOnFirstMasterElection() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(
            updateService,
            Settings.builder().put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-2").build()
        );

        ClusterState state = masterState(metadataWithSystemIndex(Settings.EMPTY));
        service.clusterChanged(event(state, state));

        ArgumentCaptor<UpdateSettingsClusterStateUpdateRequest> captor = ArgumentCaptor.forClass(
            UpdateSettingsClusterStateUpdateRequest.class
        );
        verify(updateService).updateSettings(captor.capture(), any());
        assertThat(captor.getValue().settings().get(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-2"));
    }

    public void testClusterStateNumberOfReplicasOverridesNodeSetting() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(
            updateService,
            Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2).build()
        );

        // Cluster state already has the setting (e.g. set via the cluster API before restart)
        Settings clusterSettings = Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 1).build();
        ClusterState state = masterState(metadataWithSystemIndex(clusterSettings));
        service.clusterChanged(event(state, state));

        verifyNoInteractions(updateService);
    }

    public void testNodeSettingsAppliedOnlyOnFirstMasterElection() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(
            updateService,
            Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2).build()
        );

        ClusterState state = masterState(metadataWithSystemIndex(Settings.EMPTY));
        service.clusterChanged(event(state, state));
        service.clusterChanged(event(state, state)); // second call — flag already set

        verify(updateService, times(1)).updateSettings(any(), any());
    }

    public void testRetryApplyingNodeSettingsAfterMetadataReadBlockLifted() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(
            updateService,
            Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2).build()
        );

        // First event: METADATA_READ block present → skip, flag NOT set
        ClusterState blocked = masterState(
            ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build(),
            metadataWithSystemIndex(Settings.EMPTY)
        );
        service.clusterChanged(event(blocked, blocked));
        verifyNoInteractions(updateService);

        // Second event: block lifted → node settings applied
        ClusterState recovered = masterState(metadataWithSystemIndex(Settings.EMPTY));
        service.clusterChanged(event(recovered, blocked));

        ArgumentCaptor<UpdateSettingsClusterStateUpdateRequest> captor = ArgumentCaptor.forClass(
            UpdateSettingsClusterStateUpdateRequest.class
        );
        verify(updateService).updateSettings(captor.capture(), any());
        assertThat(captor.getValue().settings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
    }

    public void testDynamicClusterSettingChangePropagated() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(updateService, Settings.EMPTY);

        // Prime the service: no node settings, so initial block is a no-op but marks flag true
        ClusterState initial = masterState(metadataWithSystemIndex(Settings.EMPTY));
        service.clusterChanged(event(initial, initial));
        verifyNoInteractions(updateService);

        // Cluster API sets NUMBER_OF_REPLICAS_SETTING=2 dynamically
        Settings newClusterSettings = Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2).build();
        ClusterState updated = masterState(metadataWithSystemIndex(newClusterSettings));
        service.clusterChanged(event(updated, initial));

        ArgumentCaptor<UpdateSettingsClusterStateUpdateRequest> captor = ArgumentCaptor.forClass(
            UpdateSettingsClusterStateUpdateRequest.class
        );
        verify(updateService).updateSettings(captor.capture(), any());
        assertThat(captor.getValue().settings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("2"));
    }

    public void testDynamicClusterSettingRemovalResetsToDefault() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(updateService, Settings.EMPTY);

        Settings clusterSettings = Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 3).build();
        ClusterState withSetting = masterState(metadataWithSystemIndex(clusterSettings));
        service.clusterChanged(event(withSetting, withSetting)); // prime: marks flag true, no dynamic change
        verifyNoInteractions(updateService);

        // Cluster API removes the setting
        ClusterState withoutSetting = masterState(metadataWithSystemIndex(Settings.EMPTY));
        service.clusterChanged(event(withoutSetting, withSetting));

        ArgumentCaptor<UpdateSettingsClusterStateUpdateRequest> captor = ArgumentCaptor.forClass(
            UpdateSettingsClusterStateUpdateRequest.class
        );
        verify(updateService).updateSettings(captor.capture(), any());
        // Reset to INDEX_NUMBER_OF_REPLICAS_SETTING default = 1
        assertThat(
            captor.getValue().settings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            equalTo(String.valueOf(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getDefault(Settings.EMPTY)))
        );
    }

    public void testDynamicClusterSettingRemovalUsesDescriptorValueForManagedIndex() {
        SystemIndexDescriptor descriptor = SystemIndexDescriptor.builder()
            .setIndexPattern(".system-test-managed*")
            .setPrimaryIndex(".system-test-managed-1")
            .setAliasName(".system-test-managed")
            .setMappings("{\"_meta\":{\"" + SystemIndexDescriptor.VERSION_META_KEY + "\":1},\"dynamic\":\"strict\",\"properties\":{}}")
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
                    .build()
            )
            .setOrigin("test")
            .build();
        SystemIndices systemIndices = new SystemIndices(List.of(new SystemIndices.Feature("test", "test feature", List.of(descriptor))));

        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = new SystemIndexSettingsUpdateService(updateService, systemIndices, Settings.EMPTY);

        IndexMetadata managedIndex = IndexMetadata.builder(".system-test-managed-1")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(true)
            .build();
        Metadata metadata = Metadata.builder().put(ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).put(managedIndex, false)).build();

        Settings clusterWithAutoExpand = Settings.builder().put(SystemIndices.AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-2").build();
        ClusterState withSetting = masterState(Metadata.builder(metadata).persistentSettings(clusterWithAutoExpand).build());
        service.clusterChanged(event(withSetting, withSetting)); // prime: marks flag true
        verifyNoInteractions(updateService);

        // Cluster API removes the setting → descriptor value "0-1" should be used, not generic default "false"
        ClusterState withoutSetting = masterState(Metadata.builder(metadata).persistentSettings(Settings.EMPTY).build());
        service.clusterChanged(event(withoutSetting, withSetting));

        ArgumentCaptor<UpdateSettingsClusterStateUpdateRequest> captor = ArgumentCaptor.forClass(
            UpdateSettingsClusterStateUpdateRequest.class
        );
        verify(updateService).updateSettings(captor.capture(), any());
        assertThat(captor.getValue().settings().get(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), equalTo("0-1"));
    }

    public void testNoUpdateWhenNoSystemIndicesExist() {
        MetadataUpdateSettingsService updateService = mock(MetadataUpdateSettingsService.class);
        SystemIndexSettingsUpdateService service = service(
            updateService,
            Settings.builder().put(SystemIndices.NUMBER_OF_REPLICAS_SETTING.getKey(), 2).build()
        );

        // Cluster state has no system indices
        Metadata emptyProject = Metadata.builder().put(ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)).build();
        ClusterState state = masterState(emptyProject);
        service.clusterChanged(event(state, state));

        verifyNoInteractions(updateService);
    }
}
