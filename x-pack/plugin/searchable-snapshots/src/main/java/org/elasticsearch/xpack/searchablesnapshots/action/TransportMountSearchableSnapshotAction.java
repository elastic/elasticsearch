/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.allocation.SearchableSnapshotAllocator;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.index.IndexModule.INDEX_RECOVERY_TYPE_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;

/**
 * Action that mounts a snapshot as a searchable snapshot, by converting the mount request into a restore request with specific settings
 * using {@link #buildIndexSettings}.
 *
 * This action needs to run on the master node because it retrieves the {@link RepositoryData}.
 */
public class TransportMountSearchableSnapshotAction extends TransportMasterNodeAction<
    MountSearchableSnapshotRequest,
    RestoreSnapshotResponse> {

    private static final Collection<Setting<String>> DATA_TIER_ALLOCATION_SETTINGS = List.of(DataTier.TIER_PREFERENCE_SETTING);

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
            RestoreSnapshotResponse::new,
            // Use SNAPSHOT_META pool since we are slow due to loading repository metadata in this action
            threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
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
            .put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE)
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), SearchableSnapshotAllocator.ALLOCATOR_NAME)
            .put(INDEX_RECOVERY_TYPE_SETTING.getKey(), SearchableSnapshots.SNAPSHOT_RECOVERY_STATE_FACTORY_KEY);

        if (storage == MountSearchableSnapshotRequest.Storage.SHARED_CACHE) {
            settings.put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), true)
                .put(DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS.getKey(), true);

            settings.put(ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), ShardLimitValidator.FROZEN_GROUP);
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
        SearchableSnapshots.getSearchableRepository(repository); // just check it's valid

        final ListenableFuture<RepositoryData> repositoryDataListener = new ListenableFuture<>();
        repository.getRepositoryData(
            EsExecutors.DIRECT_EXECUTOR_SERVICE, // TODO fork to SNAPSHOT_META and drop the forking below, see #101445
            repositoryDataListener
        );
        repositoryDataListener.addListener(listener.delegateFailureAndWrap((delegate, repoData) -> {
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

            final IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(repoData, snapshotId, indexId);
            if (indexMetadata.isSearchableSnapshot()) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "index [%s] in snapshot [%s/%s:%s] is a snapshot of a searchable snapshot index "
                            + "backed by index [%s] in snapshot [%s/%s:%s] and cannot be mounted; did you mean to restore it instead?",
                        indexName,
                        repoName,
                        repository.getMetadata().uuid(),
                        snapName,
                        SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING.get(indexMetadata.getSettings()),
                        SearchableSnapshots.SNAPSHOT_REPOSITORY_NAME_SETTING.get(indexMetadata.getSettings()),
                        SearchableSnapshots.SNAPSHOT_REPOSITORY_UUID_SETTING.get(indexMetadata.getSettings()),
                        SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexMetadata.getSettings())
                    )
                );
            }

            final Set<String> ignoreIndexSettings = new LinkedHashSet<>(Arrays.asList(request.ignoreIndexSettings()));
            ignoreIndexSettings.add(IndexMetadata.SETTING_DATA_PATH);
            for (final String indexSettingKey : indexMetadata.getSettings().keySet()) {
                if (indexSettingKey.startsWith(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX)
                    || indexSettingKey.startsWith(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX)
                    || indexSettingKey.startsWith(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX)) {
                    ignoreIndexSettings.add(indexSettingKey);
                }
            }

            final Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0) // can be overridden
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, false) // can be overridden
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false) // can be overridden
                .put(DataTier.TIER_PREFERENCE, request.storage().defaultDataTiersPreference())
                .put(request.indexSettings())
                .put(buildIndexSettings(repoData.getUuid(), request.repositoryName(), snapshotId, indexId, request.storage()))
                .build();

            // todo: restore archives bad settings, for now we verify just the data tiers, since we know their dependencies are available
            // in settings
            for (Setting<String> dataTierAllocationSetting : DATA_TIER_ALLOCATION_SETTINGS) {
                dataTierAllocationSetting.get(indexSettings);
            }

            RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(request.masterNodeTimeout(), repoName, snapName)
                // Restore the single index specified
                .indices(indexName)
                // Always rename it to the desired mounted index name
                .renamePattern(".+")
                .renameReplacement(mountedIndexName)
                // Pass through index settings, adding the index-level settings required to use searchable snapshots
                .indexSettings(indexSettings)
                // Pass through ignored index settings
                .ignoreIndexSettings(ignoreIndexSettings.toArray(new String[0]))
                // Don't include global state
                .includeGlobalState(false)
                // Don't include aliases
                .includeAliases(false)
                // Pass through the wait-for-completion flag
                .waitForCompletion(request.waitForCompletion())
                // Fail the restore if the snapshot found above is swapped out from under us before the restore happens
                .snapshotUuid(snapshotId.getUUID())
                // Log snapshot restore at the DEBUG log level
                .quiet(true);
            // Specify the mount task as the parent of the refresh task
            restoreSnapshotRequest.setParentTask(clusterService.localNode().getId(), task.getId());

            client.admin().cluster().restoreSnapshot(restoreSnapshotRequest, delegate);
        }), threadPool.executor(ThreadPool.Names.SNAPSHOT_META), null);
    }
}
