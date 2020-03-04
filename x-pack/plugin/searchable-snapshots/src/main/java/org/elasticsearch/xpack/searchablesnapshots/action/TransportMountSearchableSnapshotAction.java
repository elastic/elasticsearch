/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

public class TransportMountSearchableSnapshotAction
    extends TransportMasterNodeAction<MountSearchableSnapshotRequest, RestoreSnapshotResponse> {

    private final Client client;
    private final RepositoriesService repositoriesService;

    @Inject
    public TransportMountSearchableSnapshotAction(TransportService transportService, ClusterService clusterService, Client client,
                                                  ThreadPool threadPool, RepositoriesService repositoriesService,
                                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(MountSearchableSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters,
            MountSearchableSnapshotRequest::new, indexNameExpressionResolver);
        this.client = client;
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected String executor() {
        // Using the generic instead of the snapshot threadpool here as the snapshot threadpool might be blocked on long running tasks
        // which would block the request from getting an error response because of the ongoing task
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected RestoreSnapshotResponse read(StreamInput in) throws IOException {
        return new RestoreSnapshotResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(MountSearchableSnapshotRequest request, ClusterState state) {
        // Restoring a snapshot might change the global state and create/change an index,
        // so we need to check for METADATA_WRITE and WRITE blocks
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException != null) {
            return blockException;
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    /**
     * Return the index settings required to make a snapshot searchable
     */
    private static Settings getIndexSettings(String repoName, SnapshotId snapshotId, IndexId indexId) {
        return Settings.builder()
            .put(SearchableSnapshotRepository.SNAPSHOT_REPOSITORY_SETTING.getKey(), repoName)
            .put(SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_NAME_SETTING.getKey(), snapshotId.getName())
            .put(SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_ID_SETTING.getKey(), snapshotId.getUUID())
            .put(SearchableSnapshotRepository.SNAPSHOT_INDEX_ID_SETTING.getKey(), indexId.getId())
            .put(INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY)
            .put(IndexMetaData.SETTING_BLOCKS_WRITE, true)
            .build();
    }

    @Override
    protected void masterOperation(Task task, final MountSearchableSnapshotRequest request, final ClusterState state,
                                   final ActionListener<RestoreSnapshotResponse> listener) {
        final String repoName = request.repository();
        final String snapName = request.snapshot();
        final String indexName = request.snapshotIndex();

        // Retrieve IndexId and SnapshotId instances, which are then used to create a new restore
        // request, which is then sent on to the actual snapshot restore mechanism
        try {
            final Repository repository = repositoriesService.repository(repoName);
            final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
            repository.getRepositoryData(repositoryDataListener);
            repositoryDataListener.whenComplete(repoData -> {
                final Map<String, IndexId> indexIds = repoData.getIndices();

                if (indexIds.containsKey(indexName) == false && indexIds.get(indexName) != null) {
                    // Can't proceed, we need the index metadata
                    listener.onFailure(new ElasticsearchException("the index " + indexName +
                        " is missing from the snapshot repository metadata"));
                    return;
                }

                final IndexId indexId = indexIds.get(indexName);

                final Optional<SnapshotId> matchingSnapshotId = repoData.getSnapshotIds().stream()
                    .filter(s -> snapName.equals(s.getName())).findFirst();

                if (matchingSnapshotId.isEmpty()) {
                    // We also need the snapshot metadata, so if we can't find them, then fail
                    listener.onFailure(new ElasticsearchException("the snapshot " + snapName +
                        " is missing from the snapshot repository metadata"));
                    return;
                }

                final SnapshotId snapshotId = matchingSnapshotId.get();

                final RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(repoName, snapName);
                restoreRequest
                    // Restore the single index specified
                    .indices(indexName)
                    // Always rename it to the desired mounted index name
                    .renamePattern(".+")
                    .renameReplacement(request.mountedIndex())
                    // Pass through any restore settings
                    .settings(request.settings())
                    // Pass through index settings, adding the index-level settings required to use searchable snapshots
                    .indexSettings(Settings.builder().put(request.indexSettings())
                        .put(getIndexSettings(request.repository(), snapshotId, indexId))
                        .build())
                    // Pass through ignored index settings
                    .ignoreIndexSettings(request.ignoreIndexSettings())
                    // Don't include global state
                    .includeGlobalState(false)
                    // Don't include aliases
                    .includeAliases(false)
                    // Pass through the wait-for-completion flag
                    .waitForCompletion(request.waitForCompletion());

                // Finally, actually restore the snapshot, passing in the original listener
                client.admin().cluster().restoreSnapshot(restoreRequest, listener);
            }, listener::onFailure);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
