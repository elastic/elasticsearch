/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.migration.TransportGetFeatureUpgradeStatusAction.NO_UPGRADE_REQUIRED_VERSION;
import static org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE;
import static org.elasticsearch.core.Strings.format;

/**
 * This is where the logic to actually perform the migration lives - {@link SystemIndexMigrator#run(SystemIndexMigrationTaskState)} will
 * be invoked when the migration process is started, plus any time the node running the migration drops from the cluster/crashes/etc.
 *
 * See {@link SystemIndexMigrationTaskState} for the data that's saved for node failover.
 */
public class SystemIndexMigrator extends AllocatedPersistentTask {
    private static final Logger logger = LogManager.getLogger(SystemIndexMigrator.class);

    private static final Version READY_FOR_MIGRATION_VERSION = Version.V_7_16_0;

    // Fixed properties & services
    private final ParentTaskAssigningClient baseClient;
    private final ClusterService clusterService;
    private final SystemIndices systemIndices;
    private final MetadataUpdateSettingsService metadataUpdateSettingsService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final IndexScopedSettings indexScopedSettings;

    // In-memory state
    // NOTE: This queue is not a thread-safe class. Use `synchronized (migrationQueue)` whenever you access this. I chose this rather than
    // a synchronized/concurrent collection or an AtomicReference because we often need to do compound operations, which are much simpler
    // with `synchronized` blocks than when only the collection accesses are protected.
    private final Queue<SystemIndexMigrationInfo> migrationQueue = new LinkedList<>();
    private final AtomicReference<Map<String, Object>> currentFeatureCallbackMetadata = new AtomicReference<>();

    public SystemIndexMigrator(
        Client client,
        long id,
        String type,
        String action,
        TaskId parentTask,
        SystemIndexMigrationTaskParams params,
        Map<String, String> headers,
        ClusterService clusterService,
        SystemIndices systemIndices,
        MetadataUpdateSettingsService metadataUpdateSettingsService,
        MetadataCreateIndexService metadataCreateIndexService,
        IndexScopedSettings indexScopedSettings
    ) {
        super(id, type, action, "system-index-migrator", parentTask, headers);
        this.baseClient = new ParentTaskAssigningClient(client, parentTask);
        this.clusterService = clusterService;
        this.systemIndices = systemIndices;
        this.metadataUpdateSettingsService = metadataUpdateSettingsService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.indexScopedSettings = indexScopedSettings;
    }

    public void run(SystemIndexMigrationTaskState taskState) {
        ClusterState clusterState = clusterService.state();

        final String stateIndexName;
        final String stateFeatureName;

        if (taskState != null) {
            currentFeatureCallbackMetadata.set(taskState.getFeatureCallbackMetadata());
            stateIndexName = taskState.getCurrentIndex();
            stateFeatureName = taskState.getCurrentFeature();

            SystemIndices.Feature feature = systemIndices.getFeature(stateFeatureName);
            if (feature == null) {
                markAsFailed(
                    new IllegalStateException(
                        "cannot migrate feature [" + stateFeatureName + "] because that feature is not installed on this node"
                    )
                );
                return;
            }

            if (stateIndexName != null && clusterState.metadata().hasIndex(stateIndexName) == false) {
                markAsFailed(new IndexNotFoundException(stateIndexName, "cannot migrate because that index does not exist"));
                return;
            }
        } else {
            stateIndexName = null;
            stateFeatureName = null;
        }

        synchronized (migrationQueue) {
            if (migrationQueue.isEmpty() == false && taskState == null) {
                // A null task state means this is a new execution, not resumed from a failure
                markAsFailed(new IllegalStateException("migration is already in progress, cannot start new migration"));
                return;
            }

            systemIndices.getFeatures()
                .stream()
                .flatMap(feature -> SystemIndexMigrationInfo.fromFeature(feature, clusterState.metadata(), indexScopedSettings))
                .filter(migrationInfo -> needsToBeMigrated(clusterState.metadata().index(migrationInfo.getCurrentIndexName())))
                .sorted() // Stable order between nodes
                .collect(Collectors.toCollection(() -> migrationQueue));

            List<String> closedIndices = migrationQueue.stream()
                .filter(SystemIndexMigrationInfo::isCurrentIndexClosed)
                .map(SystemIndexMigrationInfo::getCurrentIndexName)
                .toList();
            if (closedIndices.isEmpty() == false) {
                markAsFailed(
                    new IllegalStateException("indices must be open to be migrated, but indices " + closedIndices + " are closed")
                );
                return;
            }

            // The queue we just generated *should* be the same one as was generated on the last node, so the first entry in the queue
            // should be the same as is in the task state
            if (stateIndexName != null && stateFeatureName != null && migrationQueue.isEmpty() == false) {
                SystemIndexMigrationInfo nextMigrationInfo = migrationQueue.peek();
                // This should never, ever happen in testing mode, but could conceivably happen if there are different sets of plugins
                // installed on the previous node vs. this one.
                assert nextMigrationInfo.getFeatureName().equals(stateFeatureName)
                    && nextMigrationInfo.getCurrentIndexName().equals(stateIndexName)
                    : "index name ["
                        + stateIndexName
                        + "] or feature name ["
                        + stateFeatureName
                        + "] from task state did not match first index ["
                        + nextMigrationInfo.getCurrentIndexName()
                        + "] and feature ["
                        + nextMigrationInfo.getFeatureName()
                        + "] of locally computed queue, see logs";
                if (nextMigrationInfo.getCurrentIndexName().equals(stateIndexName) == false) {
                    if (clusterState.metadata().hasIndex(stateIndexName) == false) {
                        // If we don't have that index at all, and also don't have the next one
                        markAsFailed(
                            new IllegalStateException(
                                format(
                                    "failed to resume system index migration from index [%s], that index is not present in the cluster",
                                    stateIndexName
                                )
                            )
                        );
                    }
                    logger.warn(
                        () -> format(
                            "resuming system index migration with index [%s], which does not match index given in last task state [%s]",
                            nextMigrationInfo.getCurrentIndexName(),
                            stateIndexName
                        )
                    );
                }
            }
        }

        // Kick off our callback "loop" - finishIndexAndLoop calls back into prepareNextIndex
        cleanUpPreviousMigration(
            taskState,
            clusterState,
            state -> prepareNextIndex(state, state2 -> migrateSingleIndex(state2, this::finishIndexAndLoop), stateFeatureName)
        );
    }

    private void cleanUpPreviousMigration(
        SystemIndexMigrationTaskState taskState,
        ClusterState currentState,
        Consumer<ClusterState> listener
    ) {
        logger.debug("cleaning up previous migration, task state: [{}]", taskState == null ? "null" : Strings.toString(taskState));
        if (taskState != null && taskState.getCurrentIndex() != null) {
            SystemIndexMigrationInfo migrationInfo;
            try {
                migrationInfo = SystemIndexMigrationInfo.fromTaskState(
                    taskState,
                    systemIndices,
                    currentState.metadata(),
                    indexScopedSettings
                );
            } catch (Exception e) {
                markAsFailed(e);
                return;
            }
            final String newIndexName = migrationInfo.getNextIndexName();
            logger.info("removing index [{}] from previous incomplete migration", newIndexName);

            migrationInfo.createClient(baseClient)
                .admin()
                .indices()
                .prepareDelete(newIndexName)
                .execute(ActionListener.wrap(ackedResponse -> {
                    if (ackedResponse.isAcknowledged()) {
                        logger.debug("successfully removed index [{}]", newIndexName);
                        clearResults(clusterService, ActionListener.wrap(listener::accept, this::markAsFailed));
                    }
                }, this::markAsFailed));
        } else {
            logger.debug("no incomplete index to remove");
            clearResults(clusterService, ActionListener.wrap(listener::accept, this::markAsFailed));
        }
    }

    private void finishIndexAndLoop(BulkByScrollResponse bulkResponse) {
        // The BulkByScroll response is validated in #migrateSingleIndex, it's just here to satisfy the ActionListener type
        assert bulkResponse.isTimedOut() == false
            && (bulkResponse.getBulkFailures() == null || bulkResponse.getBulkFailures().isEmpty())
            && (bulkResponse.getSearchFailures() == null || bulkResponse.getSearchFailures().isEmpty())
            : "If this assertion gets triggered it means the validation in migrateSingleIndex isn't working right";
        SystemIndexMigrationInfo lastMigrationInfo = currentMigrationInfo();
        logger.info(
            "finished migrating old index [{}] from feature [{}] to new index [{}]",
            lastMigrationInfo.getCurrentIndexName(),
            lastMigrationInfo.getFeatureName(),
            lastMigrationInfo.getNextIndexName()
        );
        assert migrationQueue != null && migrationQueue.isEmpty() == false;
        synchronized (migrationQueue) {
            migrationQueue.remove();
        }
        SystemIndexMigrationInfo nextMigrationInfo = currentMigrationInfo();
        if (nextMigrationInfo == null || nextMigrationInfo.getFeatureName().equals(lastMigrationInfo.getFeatureName()) == false) {
            // The next feature name is different than the last one, so we just finished a feature - time to invoke its post-migration hook
            lastMigrationInfo.indicesMigrationComplete(
                currentFeatureCallbackMetadata.get(),
                clusterService,
                baseClient,
                ActionListener.wrap(successful -> {
                    if (successful == false) {
                        // GWB> Should we actually fail in this case instead of plugging along?
                        logger.warn(
                            "post-migration hook for feature [{}] indicated failure; feature migration metadata prior to failure was [{}]",
                            lastMigrationInfo.getFeatureName(),
                            currentFeatureCallbackMetadata.get()
                        );
                    }
                    recordIndexMigrationSuccess(lastMigrationInfo);
                }, this::markAsFailed)
            );
        } else {
            prepareNextIndex(
                clusterService.state(),
                state2 -> migrateSingleIndex(state2, this::finishIndexAndLoop),
                lastMigrationInfo.getFeatureName()
            );
        }
    }

    private void recordIndexMigrationSuccess(SystemIndexMigrationInfo lastMigrationInfo) {
        MigrationResultsUpdateTask updateTask = MigrationResultsUpdateTask.upsert(
            lastMigrationInfo.getFeatureName(),
            SingleFeatureMigrationResult.success(),
            ActionListener.wrap(state -> {
                prepareNextIndex(
                    state,
                    clusterState -> migrateSingleIndex(clusterState, this::finishIndexAndLoop),
                    lastMigrationInfo.getFeatureName()
                );
            }, this::markAsFailed)
        );
        updateTask.submit(clusterService);
    }

    private void prepareNextIndex(ClusterState clusterState, Consumer<ClusterState> listener, String lastFeatureName) {
        synchronized (migrationQueue) {
            assert migrationQueue != null;
            if (migrationQueue.isEmpty()) {
                logger.info("finished migrating feature indices");
                markAsCompleted();
                return;
            }
        }

        final SystemIndexMigrationInfo migrationInfo = currentMigrationInfo();
        assert migrationInfo != null : "the queue of indices to migrate should have been checked for emptiness before calling this method";
        logger.info(
            "preparing to migrate old index [{}] from feature [{}] to new index [{}]",
            migrationInfo.getCurrentIndexName(),
            migrationInfo.getFeatureName(),
            migrationInfo.getNextIndexName()
        );
        if (migrationInfo.getFeatureName().equals(lastFeatureName) == false) {
            // And then invoke the pre-migration hook for the next one.
            migrationInfo.prepareForIndicesMigration(clusterService, baseClient, ActionListener.wrap(newMetadata -> {
                currentFeatureCallbackMetadata.set(newMetadata);
                updateTaskState(migrationInfo, listener, newMetadata);
            }, this::markAsFailed));
        } else {
            // Otherwise, just re-use what we already have.
            updateTaskState(migrationInfo, listener, currentFeatureCallbackMetadata.get());
        }
    }

    private void updateTaskState(SystemIndexMigrationInfo migrationInfo, Consumer<ClusterState> listener, Map<String, Object> metadata) {
        final SystemIndexMigrationTaskState newTaskState = new SystemIndexMigrationTaskState(
            migrationInfo.getCurrentIndexName(),
            migrationInfo.getFeatureName(),
            metadata
        );
        logger.debug("updating task state to [{}]", Strings.toString(newTaskState));
        currentFeatureCallbackMetadata.set(metadata);
        updatePersistentTaskState(newTaskState, ActionListener.wrap(task -> {
            assert newTaskState.equals(task.getState()) : "task state returned by update method did not match submitted task state";
            logger.debug("new task state [{}] accepted", Strings.toString(newTaskState));
            listener.accept(clusterService.state());
        }, this::markAsFailed));
    }

    private static boolean needsToBeMigrated(IndexMetadata indexMetadata) {
        assert indexMetadata != null : "null IndexMetadata should be impossible, we're not consistently using the same cluster state";
        if (indexMetadata == null) {
            return false;
        }
        return indexMetadata.isSystem() && indexMetadata.getCreationVersion().before(NO_UPGRADE_REQUIRED_VERSION);
    }

    private void migrateSingleIndex(ClusterState clusterState, Consumer<BulkByScrollResponse> listener) {
        final SystemIndexMigrationInfo migrationInfo = currentMigrationInfo();
        String oldIndexName = migrationInfo.getCurrentIndexName();
        final IndexMetadata imd = clusterState.metadata().index(oldIndexName);
        if (imd.getState().equals(CLOSE)) {
            logger.error(
                "unable to migrate index [{}] from feature [{}] because it is closed",
                oldIndexName,
                migrationInfo.getFeatureName()
            );
            markAsFailed(new IllegalStateException("unable to migrate index [" + oldIndexName + "] because it is closed"));
            return;
        }
        Index oldIndex = imd.getIndex();
        String newIndexName = migrationInfo.getNextIndexName();

        /**
         * This should be on for all System indices except for .kibana_ indices. See allowsTemplates in KibanaPlugin.java for more info.
         */
        if (migrationInfo.allowsTemplates() == false) {
            final String v2template = MetadataIndexTemplateService.findV2Template(clusterState.metadata(), newIndexName, false);
            if (Objects.nonNull(v2template)) {
                logger.error(
                    "unable to create new index [{}] from feature [{}] because it would match composable template [{}]",
                    newIndexName,
                    migrationInfo.getFeatureName(),
                    v2template
                );
                markAsFailed(
                    new IllegalStateException(
                        "unable to create new index [" + newIndexName + "] because it would match composable template [" + v2template + "]"
                    )
                );
                return;
            }
            final List<IndexTemplateMetadata> v1templates = MetadataIndexTemplateService.findV1Templates(
                clusterState.metadata(),
                newIndexName,
                false
            );
            if (v1templates.isEmpty() == false) {
                logger.error(
                    "unable to create new index [{}] from feature [{}] because it would match legacy templates [{}]",
                    newIndexName,
                    migrationInfo.getFeatureName(),
                    v1templates
                );
                markAsFailed(
                    new IllegalStateException(
                        "unable to create new index [" + newIndexName + "] because it would match legacy templates [" + v1templates + "]"
                    )
                );
                return;
            }
        }

        logger.info("migrating index [{}] from feature [{}] to new index [{}]", oldIndexName, migrationInfo.getFeatureName(), newIndexName);
        ActionListener<BulkByScrollResponse> innerListener = ActionListener.wrap(listener::accept, this::markAsFailed);
        try {
            Exception versionException = checkNodeVersionsReadyForMigration(clusterState);
            if (versionException != null) {
                markAsFailed(versionException);
                return;
            }
            createIndex(migrationInfo, ActionListener.wrap(shardsAcknowledgedResponse -> {
                logger.debug(
                    "while migrating [{}] , got create index response: [{}]",
                    oldIndexName,
                    Strings.toString(shardsAcknowledgedResponse)
                );
                setWriteBlock(
                    oldIndex,
                    true,
                    ActionListener.wrap(setReadOnlyResponse -> reindex(migrationInfo, ActionListener.wrap(bulkByScrollResponse -> {
                        logger.debug(
                            "while migrating [{}], got reindex response: [{}]",
                            oldIndexName,
                            Strings.toString(bulkByScrollResponse)
                        );
                        if ((bulkByScrollResponse.getBulkFailures() != null && bulkByScrollResponse.getBulkFailures().isEmpty() == false)
                            || (bulkByScrollResponse.getSearchFailures() != null
                                && bulkByScrollResponse.getSearchFailures().isEmpty() == false)) {
                            removeReadOnlyBlockOnReindexFailure(
                                oldIndex,
                                innerListener,
                                logAndThrowExceptionForFailures(bulkByScrollResponse)
                            );
                        } else {
                            // Successful completion of reindexing - remove read only and delete old index
                            setWriteBlock(
                                oldIndex,
                                false,
                                ActionListener.wrap(
                                    setAliasAndRemoveOldIndex(migrationInfo, bulkByScrollResponse, innerListener),
                                    innerListener::onFailure
                                )
                            );
                        }
                    }, e -> {
                        logger.error(
                            () -> format(
                                "error occurred while reindexing index [%s] from feature [%s] to destination index [%s]",
                                oldIndexName,
                                migrationInfo.getFeatureName(),
                                newIndexName
                            ),
                            e
                        );
                        removeReadOnlyBlockOnReindexFailure(oldIndex, innerListener, e);
                    })), innerListener::onFailure)
                );
            }, innerListener::onFailure));
        } catch (Exception ex) {
            logger.error(
                () -> format(
                    "error occurred while migrating index [%s] from feature [%s] to new index [%s]",
                    oldIndexName,
                    migrationInfo.getFeatureName(),
                    newIndexName
                ),
                ex
            );
            removeReadOnlyBlockOnReindexFailure(oldIndex, innerListener, ex);
            innerListener.onFailure(ex);
        }
    }

    private void createIndex(SystemIndexMigrationInfo migrationInfo, ActionListener<ShardsAcknowledgedResponse> listener) {
        final CreateIndexClusterStateUpdateRequest createRequest = new CreateIndexClusterStateUpdateRequest(
            "migrate-system-index",
            migrationInfo.getNextIndexName(),
            migrationInfo.getNextIndexName()
        );

        Settings.Builder settingsBuilder = Settings.builder();
        if (Objects.nonNull(migrationInfo.getSettings())) {
            settingsBuilder.put(migrationInfo.getSettings());
            settingsBuilder.remove("index.blocks.write");
            settingsBuilder.remove("index.blocks.read");
            settingsBuilder.remove("index.blocks.metadata");
        }
        createRequest.waitForActiveShards(ActiveShardCount.ALL)
            .mappings(migrationInfo.getMappings())
            .settings(Objects.requireNonNullElse(settingsBuilder.build(), Settings.EMPTY));
        metadataCreateIndexService.createIndex(createRequest, listener);
    }

    private CheckedConsumer<AcknowledgedResponse, Exception> setAliasAndRemoveOldIndex(
        SystemIndexMigrationInfo migrationInfo,
        BulkByScrollResponse bulkByScrollResponse,
        ActionListener<BulkByScrollResponse> listener
    ) {
        final IndicesAliasesRequestBuilder aliasesRequest = migrationInfo.createClient(baseClient).admin().indices().prepareAliases();
        aliasesRequest.removeIndex(migrationInfo.getCurrentIndexName());
        aliasesRequest.addAlias(migrationInfo.getNextIndexName(), migrationInfo.getCurrentIndexName());

        // Copy all the aliases from the old index
        IndexMetadata imd = clusterService.state().metadata().index(migrationInfo.getCurrentIndexName());
        imd.getAliases().values().forEach(aliasToAdd -> {
            aliasesRequest.addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(migrationInfo.getNextIndexName())
                    .alias(aliasToAdd.alias())
                    .indexRouting(aliasToAdd.indexRouting())
                    .searchRouting(aliasToAdd.searchRouting())
                    .filter(aliasToAdd.filter() == null ? null : aliasToAdd.filter().string())
                    .writeIndex(null)
            );
        });

        // Technically this callback might have a different cluster state, but it shouldn't matter - these indices shouldn't be changing
        // while we're trying to migrate them.
        return unsetReadOnlyResponse -> aliasesRequest.execute(
            listener.delegateFailureAndWrap((l, deleteIndexResponse) -> l.onResponse(bulkByScrollResponse))
        );
    }

    /**
     * Makes the index readonly if it's not set as a readonly yet
     */
    private void setWriteBlock(Index index, boolean readOnlyValue, ActionListener<AcknowledgedResponse> listener) {
        final Settings readOnlySettings = Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), readOnlyValue).build();
        UpdateSettingsClusterStateUpdateRequest updateSettingsRequest = new UpdateSettingsClusterStateUpdateRequest().indices(
            new Index[] { index }
        ).settings(readOnlySettings).setPreserveExisting(false);

        metadataUpdateSettingsService.updateSettings(updateSettingsRequest, listener);
    }

    private void reindex(SystemIndexMigrationInfo migrationInfo, ActionListener<BulkByScrollResponse> listener) {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(migrationInfo.getCurrentIndexName());
        reindexRequest.setDestIndex(migrationInfo.getNextIndexName());
        reindexRequest.setRefresh(true);
        migrationInfo.createClient(baseClient).execute(ReindexAction.INSTANCE, reindexRequest, listener);
    }

    // Failure handlers
    private void removeReadOnlyBlockOnReindexFailure(Index index, ActionListener<BulkByScrollResponse> listener, Exception ex) {
        logger.info("removing read only block on [{}] because reindex failed [{}]", index, ex);
        setWriteBlock(index, false, ActionListener.wrap(unsetReadOnlyResponse -> listener.onFailure(ex), e1 -> listener.onFailure(ex)));
    }

    private static ElasticsearchException logAndThrowExceptionForFailures(BulkByScrollResponse bulkByScrollResponse) {
        String bulkFailures = (bulkByScrollResponse.getBulkFailures() != null)
            ? Strings.collectionToCommaDelimitedString(bulkByScrollResponse.getBulkFailures())
            : "";
        String searchFailures = (bulkByScrollResponse.getSearchFailures() != null)
            ? Strings.collectionToCommaDelimitedString(bulkByScrollResponse.getSearchFailures())
            : "";
        logger.error("error occurred while reindexing, bulk failures [{}], search failures [{}]", bulkFailures, searchFailures);
        return new ElasticsearchException(
            "error occurred while reindexing, bulk failures [{}], search failures [{}]",
            bulkFailures,
            searchFailures
        );
    }

    /**
     * Clears the migration queue, marks the task as failed, and saves the exception for later retrieval.
     *
     * This method is **ASYNC**, so if you're not using it as a listener, be sure to {@code return} after calling this.
     */
    @Override
    public void markAsFailed(Exception e) {
        SystemIndexMigrationInfo migrationInfo = currentMigrationInfo();
        synchronized (migrationQueue) {
            migrationQueue.clear();
        }
        String featureName = Optional.ofNullable(migrationInfo).map(SystemIndexMigrationInfo::getFeatureName).orElse("<unknown feature>");
        String indexName = Optional.ofNullable(migrationInfo).map(SystemIndexMigrationInfo::getCurrentIndexName).orElse("<unknown index>");

        MigrationResultsUpdateTask.upsert(
            featureName,
            SingleFeatureMigrationResult.failure(indexName, e),
            ActionListener.wrap(state -> super.markAsFailed(e), exception -> super.markAsFailed(e))
        ).submit(clusterService);
        super.markAsFailed(e);
    }

    private static Exception checkNodeVersionsReadyForMigration(ClusterState state) {
        final Version minNodeVersion = state.nodes().getMinNodeVersion();
        if (minNodeVersion.before(READY_FOR_MIGRATION_VERSION)) {
            return new IllegalStateException(
                "all nodes must be on version ["
                    + READY_FOR_MIGRATION_VERSION
                    + "] or later to migrate feature indices but lowest node version currently in cluster is ["
                    + minNodeVersion
                    + "]"
            );
        }
        return null;
    }

    /**
     * Creates a task that will clear the results of previous migration attempts.
     * @param clusterService The cluster service.
     * @param listener A listener that will be called upon successfully updating the cluster state.
     */
    private static void clearResults(ClusterService clusterService, ActionListener<ClusterState> listener) {
        submitUnbatchedTask(clusterService, "clear migration results", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                if (currentState.metadata().custom(FeatureMigrationResults.TYPE) != null) {
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.metadata()).removeCustom(FeatureMigrationResults.TYPE))
                        .build();
                }
                return currentState;
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(newState);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("failed to clear migration results when starting new migration", e);
                listener.onFailure(e);
            }
        });
        logger.debug("submitted update task to clear migration results");
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static void submitUnbatchedTask(
        ClusterService clusterService,
        @SuppressWarnings("SameParameterValue") String source,
        ClusterStateUpdateTask task
    ) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private SystemIndexMigrationInfo currentMigrationInfo() {
        synchronized (migrationQueue) {
            return migrationQueue.peek();
        }
    }
}
