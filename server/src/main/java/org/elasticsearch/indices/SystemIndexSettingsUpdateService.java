/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;

/**
 * Propagates changes to {@link SystemIndices#NUMBER_OF_REPLICAS_SETTING} and
 * {@link SystemIndices#AUTO_EXPAND_REPLICAS_SETTING} to all existing system indices.
 * Changes made via the cluster settings API are applied immediately; settings configured
 * in {@code elasticsearch.yml} are applied once the local node is first elected master.
 */
public class SystemIndexSettingsUpdateService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SystemIndexSettingsUpdateService.class);

    private static final Map<Setting<?>, Setting<?>> SETTINGS = Map.of(
        SystemIndices.NUMBER_OF_REPLICAS_SETTING,
        INDEX_NUMBER_OF_REPLICAS_SETTING,
        SystemIndices.AUTO_EXPAND_REPLICAS_SETTING,
        INDEX_AUTO_EXPAND_REPLICAS_SETTING
    );

    private final MetadataUpdateSettingsService metadataUpdateSettingsService;
    private final SystemIndices systemIndices;
    private final Settings nodeSettings;
    private volatile boolean appliedInitialSettings = false;

    public SystemIndexSettingsUpdateService(
        MetadataUpdateSettingsService metadataUpdateSettingsService,
        SystemIndices systemIndices,
        Settings settings
    ) {
        this.metadataUpdateSettingsService = metadataUpdateSettingsService;
        this.systemIndices = systemIndices;
        this.nodeSettings = settings;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            return;
        }

        ClusterState newClusterState = event.state();
        if (newClusterState.blocks().hasGlobalBlockWithLevel(ClusterBlockLevel.METADATA_READ)) {
            return;
        }
        Settings.Builder uniformSettingsBuilder = Settings.builder();
        Set<Setting<?>> settingsToReset = new HashSet<>();

        // Apply settings from elasticsearch.yml once, on first master election after cluster state
        // recovery, but only for settings not already present in the cluster state — cluster state
        // has higher priority than node config (e.g. a setting applied via the cluster API before
        // a restart should not be overridden by a value in elasticsearch.yml).
        // Skip while any global METADATA_READ block is present: before that point the cluster
        // state has not yet been fully recovered, so indices are absent and any update would be a
        // no-op. Setting the flag prematurely would prevent a retry once the block is lifted.
        if (appliedInitialSettings == false) {
            appliedInitialSettings = true;
            Settings clusterStateSettings = newClusterState.metadata().settings();
            for (Map.Entry<Setting<?>, Setting<?>> entry : SETTINGS.entrySet()) {
                Setting<?> clusterSetting = entry.getKey();
                Setting<?> indexSetting = entry.getValue();
                if (clusterSetting.exists(nodeSettings) && clusterSetting.exists(clusterStateSettings) == false) {
                    uniformSettingsBuilder.put(indexSetting.getKey(), clusterSetting.get(nodeSettings).toString());
                }
            }
        }

        // Detect dynamic changes to system-index replica settings in the cluster state.
        // We do this here rather than via addSettingsUpdateConsumer because settings update
        // consumers fire before the new cluster state is published (see ClusterApplierService
        // .applyChanges). By detecting changes in clusterChanged we see all settings that
        // changed in a single cluster state update and can propagate them in one atomic
        // index settings update, avoiding races between separate consumer-submitted tasks.
        if (event.metadataChanged()) {
            Settings previousClusterSettings = event.previousState().metadata().settings();
            Settings newClusterSettings = newClusterState.metadata().settings();
            for (Map.Entry<Setting<?>, Setting<?>> entry : SETTINGS.entrySet()) {
                Setting<?> clusterSetting = entry.getKey();
                Setting<?> indexSetting = entry.getValue();
                boolean wasPresent = clusterSetting.exists(previousClusterSettings);
                boolean isPresent = clusterSetting.exists(newClusterSettings);
                if (isPresent == false && wasPresent == false) {
                    continue;
                }
                if (isPresent == false) {
                    // Setting was removed — reset each index to its descriptor value (or generic
                    // default if the descriptor does not specify the setting).
                    settingsToReset.add(indexSetting);
                } else if (wasPresent == false
                    || clusterSetting.get(previousClusterSettings).equals(clusterSetting.get(newClusterSettings)) == false) {
                        // Setting was newly added (propagate even if value equals the default, to override
                        // any value the index may already have, e.g. auto_expand_replicas from a descriptor)
                        // or the value changed.
                        uniformSettingsBuilder.put(indexSetting.getKey(), clusterSetting.get(newClusterSettings).toString());
                    }
            }
        }

        Settings uniformSettings = uniformSettingsBuilder.build();
        if (uniformSettings.isEmpty() == false || settingsToReset.isEmpty() == false) {
            updateExistingSystemIndicesSettings(uniformSettings, settingsToReset, newClusterState);
        }
    }

    private void updateExistingSystemIndicesSettings(Settings uniformSettings, Set<Setting<?>> settingsToReset, ClusterState clusterState) {
        if (clusterState.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }
        // TODO: these cluster settings should be project-scoped so that a change only affects system indices within a single project,
        // not all projects on the cluster. For now use the single (default) project.
        @FixForMultiProject
        ProjectMetadata projectMetadata = clusterState.metadata().getProject();

        // Pre-compute descriptor settings for data stream backing indices: look up each data stream
        // descriptor once and map all its backing indices to the same Settings object, so the
        // per-index loop below can do O(1) lookups instead of repeating the descriptor search for
        // every backing index of the same stream.
        Map<String, Settings> backingIndexToDescriptorSettings = new HashMap<>();
        if (settingsToReset.isEmpty() == false) {
            for (DataStream ds : projectMetadata.dataStreams().values()) {
                SystemDataStreamDescriptor dsDescriptor = systemIndices.findMatchingDataStreamDescriptor(ds.getName());
                if (dsDescriptor == null) {
                    continue;
                }
                Settings dsSettings = Settings.EMPTY;
                Template template = dsDescriptor.getComposableIndexTemplate().template();
                if (template != null && template.settings() != null) {
                    dsSettings = template.settings();
                }
                for (Index idx : ds.getIndices()) {
                    backingIndexToDescriptorSettings.put(idx.getName(), dsSettings);
                }
                for (Index idx : ds.getFailureIndices()) {
                    backingIndexToDescriptorSettings.put(idx.getName(), dsSettings);
                }
            }
        }

        // Group system indices by their effective settings so we can issue one update per group.
        // When settingsToReset is non-empty, different indices may need different reset values —
        // e.g. a managed index whose descriptor specifies auto_expand_replicas=0-1 vs. an unmanaged
        // index that falls back to the setting's generic default.
        Map<Settings, List<Index>> groups = new HashMap<>();
        for (IndexMetadata indexMeta : projectMetadata.indices().values()) {
            if (indexMeta.isSystem() == false) {
                continue;
            }
            Settings.Builder effective = Settings.builder().put(uniformSettings);
            if (settingsToReset.isEmpty() == false) {
                // Look up the descriptor settings once for this index, then apply all reset settings.
                Settings descriptorSettings = descriptorSettings(indexMeta.getIndex().getName(), backingIndexToDescriptorSettings);
                for (Setting<?> indexSetting : settingsToReset) {
                    String value = indexSetting.exists(descriptorSettings)
                        ? descriptorSettings.get(indexSetting.getKey())
                        : indexSetting.getDefault(Settings.EMPTY).toString();
                    effective.put(indexSetting.getKey(), value);
                }
            }
            Settings effectiveSettings = effective.build();
            if (effectiveSettings.isEmpty() == false) {
                groups.computeIfAbsent(effectiveSettings, k -> new ArrayList<>()).add(indexMeta.getIndex());
            }
        }

        for (Map.Entry<Settings, List<Index>> entry : groups.entrySet()) {
            Settings settings = entry.getKey();
            Index[] targets = entry.getValue().toArray(Index[]::new);
            metadataUpdateSettingsService.updateSettings(
                new UpdateSettingsClusterStateUpdateRequest(
                    projectMetadata.id(),
                    TimeValue.MAX_VALUE,
                    TimeValue.ZERO,
                    settings,
                    UpdateSettingsClusterStateUpdateRequest.OnExisting.OVERWRITE,
                    UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REJECT,
                    targets
                ),
                ActionListener.wrap(
                    r -> logger.debug("Updated settings {} on system indices", settings),
                    e -> logger.warn("Failed to update settings on system indices", e)
                )
            );
        }
    }

    /**
     * Returns the descriptor-specified settings for {@code indexName} when resetting a cluster-level
     * override.  For data stream backing indices the result is pre-computed (see
     * {@code backingIndexToDescriptorSettings}); for regular system indices the matching
     * {@link SystemIndexDescriptor} is consulted.  Returns {@link Settings#EMPTY} when no
     * descriptor-level value exists, causing callers to fall back to the setting's generic default.
     */
    private Settings descriptorSettings(String indexName, Map<String, Settings> backingIndexToDescriptorSettings) {
        // null means the index is not a backing index of any system data stream.
        Settings backing = backingIndexToDescriptorSettings.get(indexName);
        if (backing != null) {
            return backing;
        }
        SystemIndexDescriptor sysDescriptor = systemIndices.findMatchingDescriptor(indexName);
        if (sysDescriptor != null && sysDescriptor.isAutomaticallyManaged()) {
            return sysDescriptor.getSettings();
        }
        return Settings.EMPTY;
    }
}
