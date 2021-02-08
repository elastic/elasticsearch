/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotAllocator;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.index.IndexModule.INDEX_RECOVERY_TYPE_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.getDataTiersPreference;

/**
 * Action that mounts a snapshot as a searchable snapshot, by converting the mount request into a restore request with specific settings
 * using {@link #buildIndexSettings}.
 *
 * This action needs to run on the master node because it retrieves the {@link RepositoryData}.
 */
public class TransportMountSearchableSnapshotAction extends TransportMasterNodeAction<
    MountSearchableSnapshotRequest,
    RestoreSnapshotResponse> {

    private final Client client;
    private final RepositoriesService repositoriesService;
    private final XPackLicenseState licenseState;
    private final SystemIndices systemIndices;

    @Inject
    public TransportMountSearchableSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        Client client,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        XPackLicenseState licenseState,
        SystemIndices systemIndices
    ) {
        super(
            MountSearchableSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            MountSearchableSnapshotRequest::new,
            indexNameExpressionResolver,
            RestoreSnapshotResponse::new,
            // Avoid SNAPSHOT since snapshot threads may all be busy with long-running tasks which would block this action from responding
            // with an error. Avoid SAME since getting the repository metadata may block on IO.
            ThreadPool.Names.GENERIC
        );
        this.client = client;
        this.repositoriesService = repositoriesService;
        this.licenseState = Objects.requireNonNull(licenseState);
        this.systemIndices = Objects.requireNonNull(systemIndices);
    }

    @Override
    protected ClusterBlockException checkBlock(MountSearchableSnapshotRequest request, ClusterState state) {
        // The restore action checks the cluster blocks.
        return null;
    }

    /**
     * Return the index settings required to make a snapshot searchable
     */
    private static Settings buildIndexSettings(
        String repoUuid,
        String repoName,
        SnapshotId snapshotId,
        IndexId indexId,
        MountSearchableSnapshotRequest.Storage storage
    ) {
        final Settings.Builder settings = Settings.builder();

        if (repoUuid.equals(RepositoryData.MISSING_UUID) == false) {
            settings.put(SearchableSnapshots.SNAPSHOT_REPOSITORY_UUID_SETTING.getKey(), repoUuid);
        }

        settings.put(SearchableSnapshots.SNAPSHOT_REPOSITORY_NAME_SETTING.getKey(), repoName)
            .put(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.getKey(), snapshotId.getName())
            .put(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.getKey(), snapshotId.getUUID())
            .put(SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING.getKey(), indexId.getName())
            .put(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.getKey(), indexId.getId())
            .put(INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsConstants.SNAPSHOT_DIRECTORY_FACTORY_KEY)
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), SearchableSnapshotAllocator.ALLOCATOR_NAME)
            .put(INDEX_RECOVERY_TYPE_SETTING.getKey(), SearchableSnapshotsConstants.SNAPSHOT_RECOVERY_STATE_FACTORY_KEY);

        if (storage == MountSearchableSnapshotRequest.Storage.SHARED_CACHE) {
            settings.put(SearchableSnapshots.SNAPSHOT_PARTIAL_SETTING.getKey(), true)
                .put(DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS.getKey(), true);
        }

        return settings.build();
    }

    @Override
    protected void masterOperation(
        Task task,
        final MountSearchableSnapshotRequest request,
        final ClusterState state,
        final ActionListener<RestoreSnapshotResponse> listener
    ) {
        SearchableSnapshots.ensureValidLicense(licenseState);

        final String mountedIndexName = request.mountedIndexName();
        if (systemIndices.isSystemIndex(mountedIndexName)) {
            throw new ElasticsearchException("system index [{}] cannot be mounted as searchable snapshots", mountedIndexName);
        }

        final String repoName = request.repositoryName();
        final String snapName = request.snapshotName();
        final String indexName = request.snapshotIndexName();

        // Retrieve IndexId and SnapshotId instances, which are then used to create a new restore
        // request, which is then sent on to the actual snapshot restore mechanism
        final Repository repository = repositoriesService.repository(repoName);
        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        repository.getRepositoryData(repositoryDataListener);
        repositoryDataListener.whenComplete(repoData -> {
            final Map<String, IndexId> indexIds = repoData.getIndices();
            if (indexIds.containsKey(indexName) == false) {
                throw new IndexNotFoundException("index [" + indexName + "] not found in repository [" + repoName + "]");
            }
            final IndexId indexId = indexIds.get(indexName);

            final Optional<SnapshotId> matchingSnapshotId = repoData.getSnapshotIds()
                .stream()
                .filter(s -> snapName.equals(s.getName()))
                .findFirst();
            if (matchingSnapshotId.isEmpty()) {
                throw new ElasticsearchException("snapshot [" + snapName + "] not found in repository [" + repoName + "]");
            }
            final SnapshotId snapshotId = matchingSnapshotId.get();

            final String[] ignoreIndexSettings = Arrays.copyOf(request.ignoreIndexSettings(), request.ignoreIndexSettings().length + 1);
            ignoreIndexSettings[ignoreIndexSettings.length - 1] = IndexMetadata.SETTING_DATA_PATH;

            client.admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(repoName, snapName)
                        // Restore the single index specified
                        .indices(indexName)
                        // Always rename it to the desired mounted index name
                        .renamePattern(".+")
                        .renameReplacement(mountedIndexName)
                        // Pass through index settings, adding the index-level settings required to use searchable snapshots
                        .indexSettings(
                            Settings.builder()
                                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0) // can be overridden
                                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, false) // can be overridden
                                .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, getDataTiersPreference(request.storage()))
                                .put(request.indexSettings())
                                .put(
                                    buildIndexSettings(repoData.getUuid(), request.repositoryName(), snapshotId, indexId, request.storage())
                                )
                                .build()
                        )
                        // Pass through ignored index settings
                        .ignoreIndexSettings(ignoreIndexSettings)
                        // Don't include global state
                        .includeGlobalState(false)
                        // Don't include aliases
                        .includeAliases(false)
                        // Pass through the wait-for-completion flag
                        .waitForCompletion(request.waitForCompletion())
                        // Pass through the master-node timeout
                        .masterNodeTimeout(request.masterNodeTimeout())
                        // Fail the restore if the snapshot found above is swapped out from under us before the restore happens
                        .snapshotUuid(snapshotId.getUUID()),
                    listener
                );
        }, listener::onFailure);
    }
}
