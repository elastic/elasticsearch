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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.migration.TransportGetFeatureUpgradeStatusAction.NO_UPGRADE_REQUIRED_VERSION;
import static org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE;

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

        final String nextIndexName;
        final String featureName;

        if (taskState != null) {
            nextIndexName = taskState.getCurrentIndex();
            featureName = taskState.getCurrentFeature();

            SystemIndices.Feature feature = systemIndices.getFeatures().get(featureName);
            if (feature == null) {
                markAsFailed(
                    new IllegalStateException(
                        "cannot migrate feature [" + featureName + "] because that feature is not installed on this node"
                    )
                );
                return;
            }

            if (nextIndexName != null && clusterState.metadata().hasIndex(nextIndexName) == false) {
                markAsFailed(new IndexNotFoundException(nextIndexName, "cannot migrate because that index does not exist"));
                return;
            }
        } else {
            nextIndexName = null;
            featureName = null;
        }

        synchronized (migrationQueue) {
            if (migrationQueue.isEmpty() == false && taskState == null) {
                // A null task state means this is a new execution, not resumed from a failure
                markAsFailed(new IllegalStateException("migration is already in progress, cannot start new migration"));
                return;
            }

            systemIndices.getFeatures()
                .values()
                .stream()
                .flatMap(feature -> SystemIndexMigrationInfo.fromFeature(feature, clusterState.metadata(), indexScopedSettings))
                .filter(migrationInfo -> needsToBeMigrated(clusterState.metadata().index(migrationInfo.getCurrentIndexName())))
                .sorted() // Stable order between nodes
                .collect(Collectors.toCollection(() -> migrationQueue));

            List<String> closedIndices = migrationQueue.stream()
                .filter(SystemIndexMigrationInfo::isCurrentIndexClosed)
                .map(SystemIndexMigrationInfo::getCurrentIndexName)
                .collect(Collectors.toList());
            if (closedIndices.isEmpty() == false) {
                markAsFailed(
                    new IllegalStateException(
                        new ParameterizedMessage("indices must be open to be migrated, but indices {} are closed", closedIndices)
                            .getFormattedMessage()
                    )
                );
                return;
            }

            // The queue we just generated *should* be the same one as was generated on the last node, so the first entry in the queue
            // should be the same as is in the task state
            if (nextIndexName != null && featureName != null && migrationQueue.isEmpty() == false) {
                SystemIndexMigrationInfo nextMigrationInfo = migrationQueue.peek();
                // This should never, ever happen in testing mode, but could conceivably happen if there are different sets of plugins
                // installed on the previous node vs. this one.
                assert nextMigrationInfo.getFeatureName().equals(featureName)
                    && nextMigrationInfo.getCurrentIndexName().equals(nextIndexName) : "index name ["
                        + nextIndexName
                        + "] or feature name ["
                        + featureName
                        + "] from task state did not match first index ["
                        + nextMigrationInfo.getCurrentIndexName()
                        + "] and feature ["
                        + nextMigrationInfo.getFeatureName()
                        + "] of locally computed queue, see logs";
                if (nextMigrationInfo.getCurrentIndexName().equals(nextIndexName) == false) {
                    if (clusterState.metadata().hasIndex(nextIndexName) == false) {
                        // If we don't have that index at all, and also don't have the next one
                        markAsFailed(
                            new IllegalStateException(
                                new ParameterizedMessage(
                                    "failed to resume system index migration from index [{}], that index is not present in the cluster",
                                    nextIndexName
                                ).toString()
                            )
                        );
                    }
                    logger.warn(
                        new ParameterizedMessage(
                            "resuming system index migration with index [{}], which does not match index given in last task state [{}]",
                            nextMigrationInfo.getCurrentIndexName(),
                            nextIndexName
                        )
                    );
                }

            }
        }

        // Kick off our callback "loop" - finishIndexAndLoop calls back into prepareNextIndex
        cleanUpPreviousMigration(
            taskState,
            clusterState,
            state -> prepareNextIndex(state, state2 -> migrateSingleIndex(state2, this::finishIndexAndLoop))
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
            logger.info("Removing index [{}] from previous incomplete migration", newIndexName);

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
            logger.debug("No incomplete index to remove");
            clearResults(clusterService, ActionListener.wrap(listener::accept, this::markAsFailed));
        }
    }

    private void finishIndexAndLoop(BulkByScrollResponse bulkResponse) {
        // The BulkByScroll response is validated in #migrateSingleIndex, it's just here to satisfy the ActionListener type
        assert bulkResponse.isTimedOut() == false
            && (bulkResponse.getBulkFailures() == null || bulkResponse.getBulkFailures().isEmpty())
            && (bulkResponse.getSearchFailures() == null
                || bulkResponse.getSearchFailures()
                    .isEmpty()) : "If this assertion gets triggered it means the validation in migrateSingleIndex isn't working right";
        SystemIndexMigrationInfo migrationInfo = currentMigrationInfo();
        MigrationResultsUpdateTask updateTask = MigrationResultsUpdateTask.upsert(
            migrationInfo.getFeatureName(),
            SingleFeatureMigrationResult.success(),
            ActionListener.wrap(state -> {
                synchronized (migrationQueue) {
                    assert migrationQueue != null && migrationQueue.isEmpty() == false;
                    migrationQueue.remove();
                }
                prepareNextIndex(state, clusterState -> migrateSingleIndex(clusterState, this::finishIndexAndLoop));
            }, this::markAsFailed)
        );
        updateTask.submit(clusterService);
    }

    private void prepareNextIndex(ClusterState clusterState, Consumer<ClusterState> listener) {
        synchronized (migrationQueue) {
            assert migrationQueue != null;
            if (migrationQueue.isEmpty()) {
                logger.info("Finished migrating features.");
                markAsCompleted();
                return;
            }
        }

        final SystemIndexMigrationInfo migrationInfo = currentMigrationInfo();
        assert migrationInfo != null : "the queue of indices to migrate should have been checked for emptiness before calling this method";
        final SystemIndexMigrationTaskState newTaskState = new SystemIndexMigrationTaskState(
            migrationInfo.getCurrentIndexName(),
            migrationInfo.getFeatureName(),
            null // GWB> Replace this with the object we get from the pre-ugprade callback
        );
        logger.debug("updating task state to [{}]", Strings.toString(newTaskState));
        updatePersistentTaskState(newTaskState, ActionListener.wrap(task -> {
            assert newTaskState.equals(task.getState()) : "task state returned by update method did not match submitted task state";
            logger.debug("new task state [{}] accepted", Strings.toString(newTaskState));
            listener.accept(clusterState);
        }, this::markAsFailed));
    }

    private boolean needsToBeMigrated(IndexMetadata indexMetadata) {
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
            logger.error("unable to migrate index [{}] because it is closed", oldIndexName);
            markAsFailed(new IllegalStateException("unable to migrate index [" + oldIndexName + "] because it is closed"));
            return;
        }
        Index oldIndex = imd.getIndex();
        String newIndexName = migrationInfo.getNextIndexName();
        logger.debug("migrating index {} to new index {}", oldIndexName, newIndexName);
        ActionListener<BulkByScrollResponse> innerListener = ActionListener.wrap(listener::accept, this::markAsFailed);
        try {
            Exception versionException = checkNodeVersionsReadyForMigration(clusterState);
            if (versionException != null) {
                markAsFailed(versionException);
                return;
            }
            createIndex(migrationInfo, ActionListener.wrap(shardsAcknowledgedResponse -> {
                logger.debug("Got create index response: [{}]", Strings.toString(shardsAcknowledgedResponse));
                setWriteBlock(
                    oldIndex,
                    true,
                    ActionListener.wrap(setReadOnlyResponse -> reindex(migrationInfo, ActionListener.wrap(bulkByScrollResponse -> {
                        logger.debug("Got reindex response: [{}]", Strings.toString(bulkByScrollResponse));
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
                        logger.error("error occurred while reindexing", e);
                        removeReadOnlyBlockOnReindexFailure(oldIndex, innerListener, e);
                    })), innerListener::onFailure)
                );
            }, innerListener::onFailure));
        } catch (Exception ex) {
            logger.error("error occurred while migrating index", ex);
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

        createRequest.waitForActiveShards(ActiveShardCount.ALL)
            .mappings(migrationInfo.getMappings())
            .settings(Objects.requireNonNullElse(migrationInfo.getSettings(), Settings.EMPTY));
        metadataCreateIndexService.createIndex(createRequest, listener);
    }

    private CheckedConsumer<AcknowledgedResponse, Exception> setAliasAndRemoveOldIndex(
        SystemIndexMigrationInfo migrationInfo,
        BulkByScrollResponse bulkByScrollResponse,
        ActionListener<BulkByScrollResponse> listener
    ) {
        return unsetReadOnlyResponse -> migrationInfo.createClient(baseClient)
            .admin()
            .indices()
            .prepareAliases()
            .removeIndex(migrationInfo.getCurrentIndexName())
            .addAlias(migrationInfo.getNextIndexName(), migrationInfo.getCurrentIndexName())
            .execute(ActionListener.wrap(deleteIndexResponse -> listener.onResponse(bulkByScrollResponse), listener::onFailure));
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
        baseClient.execute(ReindexAction.INSTANCE, reindexRequest, listener);
    }

    // Failure handlers
    private void removeReadOnlyBlockOnReindexFailure(Index index, ActionListener<BulkByScrollResponse> listener, Exception ex) {
        logger.info("removing read only block on [{}] because reindex failed [{}]", index, ex);
        setWriteBlock(index, false, ActionListener.wrap(unsetReadOnlyResponse -> listener.onFailure(ex), e1 -> listener.onFailure(ex)));
    }

    private ElasticsearchException logAndThrowExceptionForFailures(BulkByScrollResponse bulkByScrollResponse) {
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
        String indexName = Optional.ofNullable(migrationInfo)
            .map(SystemIndexMigrationInfo::getCurrentIndexName)
            .orElse("<unknown index>");

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
        clusterService.submitStateUpdateTask("clear migration results", new ClusterStateUpdateTask() {
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
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(newState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("failed to clear migration results when starting new migration", e);
                listener.onFailure(e);
            }
        });
        logger.debug("submitted update task to clear migration results");
    }

    private SystemIndexMigrationInfo currentMigrationInfo() {
        synchronized (migrationQueue) {
            return migrationQueue.peek();
        }
    }
}
