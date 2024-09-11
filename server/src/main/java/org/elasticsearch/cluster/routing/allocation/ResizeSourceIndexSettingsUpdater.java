/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link RoutingChangesObserver} that removes index settings used to resize indices (Clone/Split/Shrink) once all primaries are started.
 */
public class ResizeSourceIndexSettingsUpdater implements RoutingChangesObserver {

    private final Set<Index> changes = new HashSet<>();

    @Override
    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        if (startedShard.primary() && (initializingShard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS)) {
            assert startedShard.recoverySource() == null : "recovery source should have been removed once shard is started";
            changes.add(startedShard.shardId().getIndex());
        }
    }

    public Metadata applyChanges(Metadata metadata, GlobalRoutingTable routingTable) {
        final GlobalRoutingTable.ProjectLookup projectLookup = routingTable.getProjectLookup();
        if (changes.isEmpty() == false) {
            final Map<ProjectId, Map<Index, Settings>> updatesByProject = Maps.newHashMapWithExpectedSize(routingTable.size());
            for (Index index : changes) {
                final ProjectId projectId = projectLookup.project(index);
                var indexMetadata = metadata.getProject(projectId).getIndexSafe(index);
                if (routingTable.routingTable(projectId).index(index).allPrimaryShardsActive()) {
                    assert indexMetadata.getResizeSourceIndex() != null : "no resize source index for " + index;

                    Settings.Builder builder = Settings.builder().put(indexMetadata.getSettings());
                    builder.remove(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY);
                    builder.remove(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY);
                    if (Strings.isNullOrEmpty(indexMetadata.getLifecyclePolicyName())) {
                        // Required by ILM after an index has been shrunk
                        builder.remove(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY);
                    }

                    final Map<Index, Settings> updates = updatesByProject.computeIfAbsent(
                        projectId,
                        ignore -> Maps.newMapWithExpectedSize(changes.size())
                    );
                    updates.put(index, builder.build());
                }
            }
            Metadata.Builder builder = null;
            for (Map.Entry<ProjectId, Map<Index, Settings>> entry : updatesByProject.entrySet()) {
                ProjectId projectId = entry.getKey();
                Map<Index, Settings> updates = entry.getValue();

                final ProjectMetadata origProject = metadata.getProject(projectId);
                final ProjectMetadata updatedProject = origProject.withIndexSettingsUpdates(updates);
                if (updatedProject != origProject) {
                    if (builder == null) {
                        builder = Metadata.builder(metadata);
                    }
                    builder.put(updatedProject);
                }
            }
            if (builder == null) {
                return metadata;
            } else {
                return builder.build();
            }
        }
        return metadata;
    }

    // for testing
    int numberOfChanges() {
        return changes.size();
    }

}
