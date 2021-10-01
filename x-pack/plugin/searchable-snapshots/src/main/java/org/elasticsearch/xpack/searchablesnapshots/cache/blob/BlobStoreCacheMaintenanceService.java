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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    private final Client clientWithOrigin;
    private final String systemIndexName;
    private final ThreadPool threadPool;

    public BlobStoreCacheMaintenanceService(ThreadPool threadPool, Client client, String systemIndexName) {
        this.clientWithOrigin = new OriginSettingClient(Objects.requireNonNull(client), SEARCHABLE_SNAPSHOTS_ORIGIN);
        this.systemIndexName = Objects.requireNonNull(systemIndexName);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
            return; // state not fully recovered
        }
        final ShardRouting primary = systemIndexPrimaryShard(state);
        if (primary == null || Objects.equals(state.nodes().getLocalNodeId(), primary.currentNodeId()) == false) {
            return; // system index primary shard does not exist or is not assigned to this data node
        }
        if (event.indicesDeleted().isEmpty() == false) {
            threadPool.generic().execute(new MaintenanceTask(event));
        }
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

    static QueryBuilder buildDeleteByQuery(int numberOfShards, String snapshotUuid, String indexUuid) {
        final Set<String> paths = IntStream.range(0, numberOfShards)
            .mapToObj(shard -> String.join("/", snapshotUuid, indexUuid, String.valueOf(shard)))
            .collect(Collectors.toSet());
        assert paths.isEmpty() == false;
        return QueryBuilders.termsQuery("blob.path", paths);
    }

    private class MaintenanceTask extends AbstractRunnable {

        private final ClusterChangedEvent event;

        MaintenanceTask(ClusterChangedEvent event) {
            assert event.indicesDeleted().isEmpty() == false;
            this.event = Objects.requireNonNull(event);
        }

        @Override
        protected void doRun() {
            final Queue<Tuple<DeleteByQueryRequest, ActionListener<BulkByScrollResponse>>> queue = new LinkedList<>();
            final ClusterState state = event.state();

            for (Index deletedIndex : event.indicesDeleted()) {
                final IndexMetadata indexMetadata = event.previousState().metadata().index(deletedIndex);
                assert indexMetadata != null || state.metadata().indexGraveyard().containsIndex(deletedIndex)
                    : "no previous metadata found for " + deletedIndex;
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
                        if (hasSearchableSnapshotWith(state, snapshotId, indexId)) {
                            logger.debug(
                                "snapshot [{}] of index {} is in use, skipping maintenance of snapshot blob cache entries",
                                snapshotId,
                                indexId
                            );
                            continue;
                        }

                        final DeleteByQueryRequest request = new DeleteByQueryRequest(systemIndexName);
                        request.setQuery(buildDeleteByQuery(indexMetadata.getNumberOfShards(), snapshotId.getUUID(), indexId.getId()));
                        request.setRefresh(queue.isEmpty());

                        queue.add(Tuple.tuple(request, new ActionListener<>() {
                            @Override
                            public void onResponse(BulkByScrollResponse response) {
                                logger.debug(
                                    "blob cache maintenance task deleted [{}] entries after deletion of {} (snapshot:{}, index:{})",
                                    response.getDeleted(),
                                    deletedIndex,
                                    snapshotId,
                                    indexId
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.debug(
                                    () -> new ParameterizedMessage(
                                        "exception when executing blob cache maintenance task after deletion of {} (snapshot:{}, index:{})",
                                        deletedIndex,
                                        snapshotId,
                                        indexId
                                    ),
                                    e
                                );
                            }
                        }));
                    }
                }
            }

            if (queue.isEmpty() == false) {
                executeNextCleanUp(queue);
            }
        }

        void executeNextCleanUp(final Queue<Tuple<DeleteByQueryRequest, ActionListener<BulkByScrollResponse>>> queue) {
            assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
            final Tuple<DeleteByQueryRequest, ActionListener<BulkByScrollResponse>> next = queue.poll();
            if (next != null) {
                cleanUp(next.v1(), next.v2(), queue);
            }
        }

        void cleanUp(
            final DeleteByQueryRequest request,
            final ActionListener<BulkByScrollResponse> listener,
            final Queue<Tuple<DeleteByQueryRequest, ActionListener<BulkByScrollResponse>>> queue
        ) {
            assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
            clientWithOrigin.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.runAfter(listener, () -> {
                if (queue.isEmpty() == false) {
                    threadPool.generic().execute(() -> executeNextCleanUp(queue));
                }
            }));
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(
                () -> new ParameterizedMessage("snapshot blob cache maintenance task failed for cluster state update [{}]", event.source()),
                e
            );
        }
    }
}
