/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.injection.guice.Inject;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public final class IndicesStore implements ClusterStateListener, Closeable {

    private static final Logger logger = LogManager.getLogger(IndicesStore.class);

    // TODO this class can be foled into either IndicesService and partially into IndicesClusterStateService
    // there is no need for a separate public service
    private final Settings settings;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final IndicesClusterStateService indicesClusterStateService;

    // Cache successful shard deletion checks to prevent unnecessary file system lookups
    private final Set<ShardId> folderNotFoundCache = new HashSet<>();

    @Inject
    public IndicesStore(
        Settings settings,
        IndicesService indicesService,
        ClusterService clusterService,
        IndicesClusterStateService indicesClusterStateService
    ) {
        this.settings = settings;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.indicesClusterStateService = indicesClusterStateService;
        // Doesn't make sense to delete shards on non-data nodes
        if (DiscoveryNode.canContainData(settings)) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void close() {
        if (DiscoveryNode.canContainData(settings)) {
            clusterService.removeListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.routingTableChanged() == false) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            return;
        }

        for (var routingTableEntry : event.state().globalRoutingTable().routingTables().entrySet()) {
            RoutingTable routingTable = routingTableEntry.getValue();
            ProjectId projectId = routingTableEntry.getKey();
            // remove entries from cache that don't exist in the routing table anymore (either closed or deleted indices)
            // - removing shard data of deleted indices is handled by IndicesClusterStateService
            // - closed indices don't need to be removed from the cache but we do it anyway for code simplicity
            folderNotFoundCache.removeIf(shardId -> routingTable.hasIndex(shardId.getIndex()) == false);
            // remove entries from cache which are allocated to this node
            final String localNodeId = event.state().nodes().getLocalNodeId();
            RoutingNode localRoutingNode = event.state().getRoutingNodes().node(localNodeId);
            if (localRoutingNode != null) {
                for (ShardRouting routing : localRoutingNode) {
                    folderNotFoundCache.remove(routing.shardId());
                }
            }

            for (IndexRoutingTable indexRoutingTable : routingTable) {
                // Note, closed indices will not have any routing information, so won't be deleted
                for (int i = 0; i < indexRoutingTable.size(); i++) {
                    IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(i);
                    ShardId shardId = indexShardRoutingTable.shardId();
                    if (folderNotFoundCache.contains(shardId) == false && shardCanBeDeleted(localNodeId, indexShardRoutingTable)) {
                        IndexService indexService = indicesService.indexService(indexRoutingTable.getIndex());
                        final IndexSettings indexSettings;
                        if (indexService == null) {
                            IndexMetadata indexMetadata = event.state()
                                .getMetadata()
                                .getProject(projectId)
                                .getIndexSafe(indexRoutingTable.getIndex());
                            indexSettings = new IndexSettings(indexMetadata, settings);
                        } else {
                            indexSettings = indexService.getIndexSettings();
                        }
                        IndicesService.ShardDeletionCheckResult shardDeletionCheckResult = indicesService.canDeleteShardContent(
                            shardId,
                            indexSettings
                        );
                        switch (shardDeletionCheckResult) {
                            case FOLDER_FOUND_CAN_DELETE:
                                indicesClusterStateService.onClusterStateShardsClosed(
                                    () -> deleteShardStore(shardId)
                                );
                                break;
                            case NO_FOLDER_FOUND:
                                folderNotFoundCache.add(shardId);
                                break;
                            case STILL_ALLOCATED:
                                // nothing to do
                                break;
                            default:
                                assert false : "unknown shard deletion check result: " + shardDeletionCheckResult;
                        }
                    }
                }
            }
        }
    }

    static boolean shardCanBeDeleted(String localNodeId, IndexShardRoutingTable indexShardRoutingTable) {
        assert indexShardRoutingTable.size() > 0;

        // a shard can be deleted if all its copies are active, and its not allocated on this node
        if (indexShardRoutingTable.size() == 0) {
            // should not really happen, there should always be at least 1 (primary) shard in a
            // shard replication group, in any case, protected from deleting something by mistake
            return false;
        }

        for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
            ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
            // be conservative here, check on started, not even active
            if (shardRouting.started() == false) {
                return false;
            }

            // check if shard is active on the current node
            if (localNodeId.equals(shardRouting.currentNodeId())) {
                return false;
            }
        }

        return true;
    }

    static boolean shardIsAllocatedOnNode(String localNodeId, IndexShardRoutingTable indexShardRoutingTable) {
        for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
            if (localNodeId.equals(indexShardRoutingTable.shard(copy).currentNodeId())) {
                return true;
            }
        }
        return false;
    }

    private void deleteShardStore(ShardId shardId) {
        clusterService.getClusterApplierService()
            .runOnApplierThread("indices_store ([" + shardId + "] delete unallocated shard)", Priority.HIGH, currentState -> {
                String localNodeId = currentState.nodes().getLocalNodeId();
                for (var routingTableEntry : currentState.globalRoutingTable().routingTables().entrySet()) {
                    IndexRoutingTable indexRouting = routingTableEntry.getValue().index(shardId.getIndex());
                    if (indexRouting != null) {
                        IndexShardRoutingTable currentShardRouting = indexRouting.shard(shardId.id());
                        if (currentShardRouting != null && shardIsAllocatedOnNode(localNodeId, currentShardRouting)) {
                            logger.trace(
                                "not deleting shard {}, shard has been re-allocated to this node",
                                shardId
                            );
                            return;
                        }
                    }
                }
                try {
                    indicesService.deleteShardStore("no longer used", shardId, currentState, IndexRemovalReason.NO_LONGER_ASSIGNED);
                } catch (Exception ex) {
                    logger.debug(() -> format("%s failed to delete unallocated shard, ignoring", shardId), ex);
                }
            }, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {}

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> format("%s unexpected error during deletion of unallocated shard", shardId), e);
                }
            });
    }
}
