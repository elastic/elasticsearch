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
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
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

    public Metadata applyChanges(Metadata metadata, RoutingTable routingTable) {
        if (changes.isEmpty() == false) {
            final Map<Index, Settings> updates = Maps.newHashMapWithExpectedSize(changes.size());
            for (Index index : changes) {
                var indexMetadata = metadata.getIndexSafe(index);
                if (routingTable.index(index).allPrimaryShardsActive()) {
                    assert indexMetadata.getResizeSourceIndex() != null : "no resize source index for " + index;

                    Settings.Builder builder = Settings.builder().put(indexMetadata.getSettings());
                    builder.remove(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY);
                    builder.remove(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY);
                    if (Strings.isNullOrEmpty(indexMetadata.getLifecyclePolicyName())) {
                        // Required by ILM after an index has been shrunk
                        builder.remove(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY);
                    }
                    updates.put(index, builder.build());
                }
            }
            return metadata.withIndexSettingsUpdates(updates);
        }
        return metadata;
    }
}
