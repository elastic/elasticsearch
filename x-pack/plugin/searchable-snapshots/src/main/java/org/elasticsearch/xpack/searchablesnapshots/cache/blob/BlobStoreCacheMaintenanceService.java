/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;

/**
 * A service that delete documents in the snapshot blob cache index when they are not required anymore.
 *
 * This service runs on the data node that contains the snapshot blob cache primary shard. It listens to cluster state updates to find
 * searchable snapshot indices that are deleted and checks if the index snapshot is still used by other searchable snapshot indices. If the
 * index snapshot is not used anymore then i triggers the deletion of corresponding cached blobs in the snapshot blob cache index using a
 * delete-by-query.
 */
public class BlobStoreCacheMaintenanceService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(BlobStoreCacheMaintenanceService.class);

    private final ClusterService clusterService;
    private final Client clientWithOrigin;
    private final String systemIndexName;
    private final ThreadPool threadPool;

    public BlobStoreCacheMaintenanceService(ClusterService clusterService, ThreadPool threadPool, Client client, String systemIndexName) {
        this.clientWithOrigin = new OriginSettingClient(Objects.requireNonNull(client), SEARCHABLE_SNAPSHOTS_ORIGIN);
        this.systemIndexName = Objects.requireNonNull(systemIndexName);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
            return; // state not fully recovered
        }
        if (event.indicesDeleted() == null || event.indicesDeleted().isEmpty()) {
            return; // no indices deleted in this cluster state update
        }
        final ShardRouting primary = systemIndexPrimaryShard(state);
        if (primary == null || Objects.equals(state.nodes().getLocalNodeId(), primary.currentNodeId()) == false) {
            return; // system index primary shard does not exist or is not assigned to this data node
        }

        final Set<MaintenanceTask> tasks = new HashSet<>();

        for (Index deletedIndex : event.indicesDeleted()) {
            final IndexMetadata indexMetadata = event.previousState().metadata().index(deletedIndex);
            if (indexMetadata != null) {
                final Settings indexSetting = indexMetadata.getSettings();
                if (SearchableSnapshotsSettings.isSearchableSnapshotStore(indexSetting)) {
                    assert state.metadata().hasIndex(deletedIndex) == false;

                    final SnapshotId snapshotId = new SnapshotId(
                        SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSetting),
                        SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSetting)
                    );
                    final IndexId indexId = new IndexId(
                        SNAPSHOT_INDEX_NAME_SETTING.get(indexSetting),
                        SNAPSHOT_INDEX_ID_SETTING.get(indexSetting)
                    );

                    // we should do nothing if the current cluster state contains another
                    // searchable snapshot index that uses the same index snapshot
                    if (hasSearchableSnapshotWith(state, snapshotId, indexId) == false) {
                        tasks.add(new MaintenanceTask(snapshotId, indexId, indexMetadata.getNumberOfShards(), clusterService::state));
                    }
                }
            }
        }
        tasks.forEach(maintenanceTask -> threadPool.generic().execute(maintenanceTask));
    }

    @Nullable
    private ShardRouting systemIndexPrimaryShard(final ClusterState state) {
        final IndexMetadata indexMetadata = state.metadata().index(systemIndexName);
        if (indexMetadata != null) {
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(indexMetadata.getIndex());
            if (indexRoutingTable != null) {
                return indexRoutingTable.shard(0).primaryShard();
            }
        }
        return null;
    }

    private static boolean hasSearchableSnapshotWith(final ClusterState state, final SnapshotId snapshotId, final IndexId indexId) {
        for (IndexMetadata indexMetadata : state.metadata()) {
            final Settings indexSettings = indexMetadata.getSettings();
            if (SearchableSnapshotsSettings.isSearchableSnapshotStore(indexSettings)) {
                final SnapshotId otherSnapshotId = new SnapshotId(
                    SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
                    SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
                );
                if (Objects.equals(snapshotId, otherSnapshotId)) {
                    final IndexId otherIndexId = new IndexId(
                        SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings),
                        SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
                    );
                    if (Objects.equals(indexId, otherIndexId)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private class MaintenanceTask extends AbstractRunnable {

        private final SnapshotId snapshotId;
        private final IndexId indexId;
        private final int numberOfShards;
        private final Supplier<ClusterState> state;

        MaintenanceTask(SnapshotId snapshotId, IndexId indexId, int numberOfShards, Supplier<ClusterState> clusterStateSupplier) {
            this.snapshotId = Objects.requireNonNull(snapshotId);
            this.indexId = Objects.requireNonNull(indexId);
            this.numberOfShards = numberOfShards;
            this.state = clusterStateSupplier;
        }

        @Override
        protected void doRun() {
            if (hasSearchableSnapshotWith(state.get(), snapshotId, indexId)) {
                logger.debug(
                    "snapshot blob cache maintenance task skipped, another index is using [snapshot:{}, index:{}]]",
                    snapshotId,
                    indexId
                );
                return;
            }

            final Set<String> paths = new HashSet<>(numberOfShards);
            for (int shard = 0; shard < numberOfShards; shard++) {
                paths.add(String.join("/", snapshotId.getUUID(), indexId.getId(), String.valueOf(shard)));
            }

            final DeleteByQueryRequest request = new DeleteByQueryRequest(systemIndexName);
            request.setQuery(QueryBuilders.termsQuery("blob.path", paths));
            clientWithOrigin.execute(DeleteByQueryAction.INSTANCE, request, new ActionListener<>() {
                @Override
                public void onResponse(BulkByScrollResponse response) {
                    logger.debug(
                        "snapshot blob cache maintenance task deleted [{}] documents for [snapshot:{}, index:{}]]",
                        response.getDeleted(),
                        snapshotId,
                        indexId
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "exception when executing snapshot blob cache maintenance task for [snapshot:{}, index:{}]]",
                            snapshotId,
                            indexId
                        ),
                        e
                    );
                }
            });
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(
                () -> new ParameterizedMessage("snapshot blob cache maintenance task [snapshot:{}, index:{}]] failed", snapshotId, indexId),
                e
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MaintenanceTask that = (MaintenanceTask) o;
            return numberOfShards == that.numberOfShards
                && Objects.equals(snapshotId, that.snapshotId)
                && Objects.equals(indexId, that.indexId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, indexId, numberOfShards);
        }
    }
}
