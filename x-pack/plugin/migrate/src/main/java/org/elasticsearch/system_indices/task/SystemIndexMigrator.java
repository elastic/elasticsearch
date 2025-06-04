/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.migrate.action.CancelReindexDataStreamAction;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamEnrichedStatus;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.indices.SystemIndices.NO_UPGRADE_REQUIRED_INDEX_VERSION;

/**
 * This is where the logic to actually perform the migration lives - {@link SystemIndexMigrator#run(SystemIndexMigrationTaskState)} will
 * be invoked when the migration process is started, plus any time the node running the migration drops from the cluster/crashes/etc.
 *
 * See {@link SystemIndexMigrationTaskState} for the data that's saved for node failover.
 */
public class SystemIndexMigrator extends AllocatedPersistentTask {
    private static final Logger logger = LogManager.getLogger(SystemIndexMigrator.class);

    // Fixed properties & services
    private final ParentTaskAssigningClient baseClient;
    private final ClusterService clusterService;
    private final SystemIndices systemIndices;
    private final IndexScopedSettings indexScopedSettings;
    private final ThreadPool threadPool;

    // In-memory state
    // NOTE: This queue is not a thread-safe class. Use `synchronized (migrationQueue)` whenever you access this. I chose this rather than
    // a synchronized/concurrent collection or an AtomicReference because we often need to do compound operations, which are much simpler
    // with `synchronized` blocks than when only the collection accesses are protected.
    private final Queue<SystemResourceMigrationInfo> migrationQueue = new ArrayDeque<>();
    private final AtomicReference<Map<String, Object>> currentFeatureCallbackMetadata = new AtomicReference<>();

    public SystemIndexMigrator(
        Client client,
        long id,
        String type,
        String action,
        TaskId parentTask,
        Map<String, String> headers,
        ClusterService clusterService,
        SystemIndices systemIndices,
        IndexScopedSettings indexScopedSettings,
        ThreadPool threadPool
    ) {
        super(id, type, action, "system-index-migrator", parentTask, headers);
        this.baseClient = new ParentTaskAssigningClient(client, parentTask);
        this.clusterService = clusterService;
        this.systemIndices = systemIndices;
        this.indexScopedSettings = indexScopedSettings;
        this.threadPool = threadPool;
    }

    public void run(SystemIndexMigrationTaskState taskState) {
        ClusterState clusterState = clusterService.state();
        ProjectMetadata projectMetadata = clusterState.metadata().getProject();

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

            if (stateIndexName != null && projectMetadata.hasIndexAbstraction(stateIndexName) == false) {
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
                .flatMap(feature -> SystemResourceMigrationFactory.fromFeature(feature, projectMetadata, indexScopedSettings))
                .filter(migrationInfo -> needToBeMigrated(migrationInfo.getIndices(projectMetadata)))
                .sorted() // Stable order between nodes
                .collect(Collectors.toCollection(() -> migrationQueue));

            List<String> closedIndices = migrationQueue.stream()
                .filter(SystemResourceMigrationInfo::isCurrentIndexClosed)
                .map(SystemResourceMigrationInfo::getCurrentResourceName)
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
                SystemResourceMigrationInfo nextMigrationInfo = migrationQueue.peek();
                // This should never, ever happen in testing mode, but could conceivably happen if there are different sets of plugins
                // installed on the previous node vs. this one.
                assert nextMigrationInfo.getFeatureName().equals(stateFeatureName)
                    && nextMigrationInfo.getCurrentResourceName().equals(stateIndexName)
                    : "system index/data stream name ["
                        + stateIndexName
                        + "] or feature name ["
                        + stateFeatureName
                        + "] from task state did not match first index/data stream ["
                        + nextMigrationInfo.getCurrentResourceName()
                        + "] and feature ["
                        + nextMigrationInfo.getFeatureName()
                        + "] of locally computed queue, see logs";
                if (nextMigrationInfo.getCurrentResourceName().equals(stateIndexName) == false) {
                    if (projectMetadata.hasIndexAbstraction(stateIndexName) == false) {
                        // If we don't have that index at all, and also don't have the next one
                        markAsFailed(
                            new IllegalStateException(
                                format(
                                    "failed to resume system resource migration from resource [%s], that is not present in the cluster",
                                    stateIndexName
                                )
                            )
                        );
                    }
                    logger.warn(
                        () -> format(
                            "resuming system resource migration with resource [%s],"
                                + " which does not match resource given in last task state [%s]",
                            nextMigrationInfo.getCurrentResourceName(),
                            stateIndexName
                        )
                    );
                }
            }
        }

        // Kick off our callback "loop" - finishIndexAndLoop calls back into startFeatureMigration
        logger.debug("cleaning up previous migration, task state: [{}]", taskState == null ? "null" : Strings.toString(taskState));
        clearResults(clusterService, ActionListener.wrap(state -> startFeatureMigration(stateFeatureName), this::markAsFailed));
    }

    private void finishIndexAndLoop(SystemIndexMigrationInfo migrationInfo, BulkByScrollResponse bulkResponse) {
        // The BulkByScroll response is validated in #migrateSingleIndex, it's just here to satisfy the ActionListener type
        assert bulkResponse.isTimedOut() == false
            && (bulkResponse.getBulkFailures() == null || bulkResponse.getBulkFailures().isEmpty())
            && (bulkResponse.getSearchFailures() == null || bulkResponse.getSearchFailures().isEmpty())
            : "If this assertion gets triggered it means the validation in migrateSingleIndex isn't working right";
        logger.info(
            "finished migrating old index [{}] from feature [{}] to new index [{}]",
            migrationInfo.getCurrentIndexName(),
            migrationInfo.getFeatureName(),
            migrationInfo.getNextIndexName()
        );

        finishResourceAndLoop(migrationInfo);
    }

    private void finishDataStreamAndLoop(SystemDataStreamMigrationInfo migrationInfo) {
        logger.info(
            "finished migrating old indices from data stream [{}] from feature [{}] to new indices",
            migrationInfo.getCurrentResourceName(),
            migrationInfo.getFeatureName()
        );

        finishResourceAndLoop(migrationInfo);
    }

    private void finishResourceAndLoop(SystemResourceMigrationInfo lastMigrationInfo) {
        assert migrationQueue != null && migrationQueue.isEmpty() == false;
        synchronized (migrationQueue) {
            migrationQueue.remove();
        }
        SystemResourceMigrationInfo nextMigrationInfo = currentMigrationInfo();
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
                            "post-migration hook for feature [{}] indicated failure;"
                                + " feature migration metadata prior to failure was [{}]",
                            lastMigrationInfo.getFeatureName(),
                            currentFeatureCallbackMetadata.get()
                        );
                    }
                    recordIndexMigrationSuccess(lastMigrationInfo);
                }, this::markAsFailed)
            );
        } else {
            startFeatureMigration(lastMigrationInfo.getFeatureName());
        }
    }

    private void migrateResource(SystemResourceMigrationInfo migrationInfo, ClusterState clusterState) {
        if (migrationInfo instanceof SystemIndexMigrationInfo systemIndexMigrationInfo) {
            logger.info(
                "preparing to migrate old index [{}] from feature [{}] to new index [{}]",
                systemIndexMigrationInfo.getCurrentIndexName(),
                migrationInfo.getFeatureName(),
                systemIndexMigrationInfo.getNextIndexName()
            );
            migrateSingleIndex(systemIndexMigrationInfo, clusterState, this::finishIndexAndLoop);
        } else if (migrationInfo instanceof SystemDataStreamMigrationInfo systemDataStreamMigrationInfo) {
            logger.info(
                "preparing to migrate old indices from data stream [{}] from feature [{}] to new indices",
                systemDataStreamMigrationInfo.getCurrentResourceName(),
                migrationInfo.getFeatureName()
            );
            migrateDataStream(systemDataStreamMigrationInfo, this::finishDataStreamAndLoop);
        } else {
            throw new IllegalStateException("Unknown type of migration: " + migrationInfo.getClass());
        }
    }

    private void recordIndexMigrationSuccess(SystemResourceMigrationInfo lastMigrationInfo) {
        MigrationResultsUpdateTask updateTask = MigrationResultsUpdateTask.upsert(
            lastMigrationInfo.getFeatureName(),
            SingleFeatureMigrationResult.success(),
            ActionListener.wrap(state -> {
                startFeatureMigration(lastMigrationInfo.getFeatureName());
            }, this::markAsFailed)
        );
        updateTask.submit(clusterService);
    }

    private void startFeatureMigration(String lastFeatureName) {
        synchronized (migrationQueue) {
            assert migrationQueue != null;
            if (migrationQueue.isEmpty()) {
                logger.info("finished migrating feature indices");
                markAsCompleted();
                return;
            }
        }

        final SystemResourceMigrationInfo migrationInfo = currentMigrationInfo();
        assert migrationInfo != null : "the queue of indices to migrate should have been checked for emptiness before calling this method";
        if (migrationInfo.getFeatureName().equals(lastFeatureName) == false) {
            // And then invoke the pre-migration hook for the next one.
            migrationInfo.prepareForIndicesMigration(clusterService, baseClient, ActionListener.wrap(newMetadata -> {
                currentFeatureCallbackMetadata.set(newMetadata);
                updateTaskState(migrationInfo, state -> migrateResource(migrationInfo, state), newMetadata);
            }, this::markAsFailed));
        } else {
            // Otherwise, just re-use what we already have.
            updateTaskState(migrationInfo, state -> migrateResource(migrationInfo, state), currentFeatureCallbackMetadata.get());
        }
    }

    private void updateTaskState(SystemResourceMigrationInfo migrationInfo, Consumer<ClusterState> listener, Map<String, Object> metadata) {
        final SystemIndexMigrationTaskState newTaskState = new SystemIndexMigrationTaskState(
            migrationInfo.getCurrentResourceName(),
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

    private static boolean needToBeMigrated(Stream<IndexMetadata> indicesMetadata) {
        return indicesMetadata.anyMatch(indexMetadata -> {
            assert indexMetadata != null : "null IndexMetadata should be impossible, we're not consistently using the same cluster state";
            if (indexMetadata == null) {
                return false;
            }
            return indexMetadata.isSystem() && indexMetadata.getCreationVersion().before(NO_UPGRADE_REQUIRED_INDEX_VERSION);
        });
    }

    private void migrateSingleIndex(
        SystemIndexMigrationInfo migrationInfo,
        ClusterState clusterState,
        BiConsumer<SystemIndexMigrationInfo, BulkByScrollResponse> listener
    ) {
        String oldIndexName = migrationInfo.getCurrentIndexName();
        final ProjectMetadata projectMetadata = clusterState.metadata().getProject();
        final IndexMetadata imd = projectMetadata.index(oldIndexName);
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
            final String v2template = MetadataIndexTemplateService.findV2Template(projectMetadata, newIndexName, false);
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
                projectMetadata,
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
        ActionListener<BulkByScrollResponse> innerListener = ActionListener.wrap(
            response -> listener.accept(migrationInfo, response),
            this::markAsFailed
        );
        try {
            createIndexRetryOnFailure(migrationInfo, innerListener.delegateFailureAndWrap((delegate, shardsAcknowledgedResponse) -> {
                logger.debug(
                    "while migrating [{}] , got create index response: [{}]",
                    oldIndexName,
                    Strings.toString(shardsAcknowledgedResponse)
                );
                setWriteBlock(
                    oldIndex,
                    true,
                    delegate.delegateFailureAndWrap(
                        (delegate2, setReadOnlyResponse) -> reindex(migrationInfo, ActionListener.wrap(bulkByScrollResponse -> {
                            logger.debug(
                                "while migrating [{}], got reindex response: [{}]",
                                oldIndexName,
                                Strings.toString(bulkByScrollResponse)
                            );
                            if ((bulkByScrollResponse.getBulkFailures() != null
                                && bulkByScrollResponse.getBulkFailures().isEmpty() == false)
                                || (bulkByScrollResponse.getSearchFailures() != null
                                    && bulkByScrollResponse.getSearchFailures().isEmpty() == false)) {
                                removeReadOnlyBlockOnReindexFailure(
                                    oldIndex,
                                    delegate2,
                                    logAndThrowExceptionForFailures(bulkByScrollResponse)
                                );
                            } else {
                                // Successful completion of reindexing. Now we need to set the alias and remove the old index.
                                setAliasAndRemoveOldIndex(migrationInfo, ActionListener.wrap(aliasesResponse -> {
                                    if (aliasesResponse.hasErrors()) {
                                        var e = new ElasticsearchException("Aliases request had errors");
                                        for (var error : aliasesResponse.getErrors()) {
                                            e.addSuppressed(error);
                                        }
                                        throw e;
                                    }
                                    logger.info(
                                        "Successfully migrated old index [{}] to new index [{}] from feature [{}]",
                                        oldIndexName,
                                        migrationInfo.getNextIndexName(),
                                        migrationInfo.getFeatureName()
                                    );
                                    delegate2.onResponse(bulkByScrollResponse);
                                }, e -> {
                                    logger.error(
                                        () -> format(
                                            "An error occurred while changing aliases and removing the old index [%s] from feature [%s]",
                                            oldIndexName,
                                            migrationInfo.getFeatureName()
                                        ),
                                        e
                                    );
                                    removeReadOnlyBlockOnReindexFailure(oldIndex, delegate2, e);
                                }));
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
                            removeReadOnlyBlockOnReindexFailure(oldIndex, delegate2, e);
                        }))
                    )
                );
            }));
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

    private void createIndex(SystemIndexMigrationInfo migrationInfo, ActionListener<CreateIndexResponse> listener) {
        logger.info("creating new system index [{}] from feature [{}]", migrationInfo.getNextIndexName(), migrationInfo.getFeatureName());

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(migrationInfo.getNextIndexName());
        Settings.Builder settingsBuilder = Settings.builder();
        if (Objects.nonNull(migrationInfo.getSettings())) {
            settingsBuilder.put(migrationInfo.getSettings());
            settingsBuilder.remove("index.blocks.write");
            settingsBuilder.remove("index.blocks.read");
            settingsBuilder.remove("index.blocks.metadata");
        }
        createIndexRequest.cause(SystemIndices.MIGRATE_SYSTEM_INDEX_CAUSE)
            .ackTimeout(TimeValue.ZERO)
            .masterNodeTimeout(MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT)
            .waitForActiveShards(ActiveShardCount.ALL)
            .mapping(migrationInfo.getMappings())
            .settings(Objects.requireNonNullElse(settingsBuilder.build(), Settings.EMPTY));

        migrationInfo.createClient(baseClient).admin().indices().create(createIndexRequest, listener);
    }

    private void createIndexRetryOnFailure(SystemIndexMigrationInfo migrationInfo, ActionListener<CreateIndexResponse> listener) {
        createIndex(migrationInfo, listener.delegateResponse((l, e) -> {
            logger.warn(
                "createIndex failed with \"{}\", retrying after removing index [{}] from previous attempt",
                e.getMessage(),
                migrationInfo.getNextIndexName()
            );
            deleteIndex(migrationInfo, ActionListener.wrap(cleanupResponse -> createIndex(migrationInfo, l.delegateResponse((l3, e3) -> {
                e3.addSuppressed(e);
                logger.error(
                    "createIndex failed after retrying, aborting system index migration. index: " + migrationInfo.getNextIndexName(),
                    e3
                );
                l.onFailure(e3);
            })), e2 -> {
                e2.addSuppressed(e);
                logger.error("deleteIndex failed, aborting system index migration. index: " + migrationInfo.getNextIndexName(), e2);
                l.onFailure(e2);
            }));
        }));
    }

    private <T> void deleteIndex(SystemIndexMigrationInfo migrationInfo, ActionListener<AcknowledgedResponse> listener) {
        logger.info("removing index [{}] from feature [{}]", migrationInfo.getNextIndexName(), migrationInfo.getFeatureName());
        String newIndexName = migrationInfo.getNextIndexName();
        migrationInfo.createClient(baseClient).admin().indices().prepareDelete(newIndexName).execute(ActionListener.wrap(ackedResponse -> {
            if (ackedResponse.isAcknowledged()) {
                logger.info("successfully removed index [{}]", newIndexName);
                listener.onResponse(ackedResponse);
            } else {
                listener.onFailure(new ElasticsearchException("Failed to acknowledge index deletion for [" + newIndexName + "]"));
            }
        }, listener::onFailure));
    }

    private void setAliasAndRemoveOldIndex(SystemIndexMigrationInfo migrationInfo, ActionListener<IndicesAliasesResponse> listener) {
        final IndicesAliasesRequestBuilder aliasesRequest = migrationInfo.createClient(baseClient)
            .admin()
            .indices()
            .prepareAliases(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS); // TODO should these be longer?
        aliasesRequest.removeIndex(migrationInfo.getCurrentIndexName());
        aliasesRequest.addAlias(migrationInfo.getNextIndexName(), migrationInfo.getCurrentIndexName());

        // Copy all the aliases from the old index
        IndexMetadata imd = clusterService.state().metadata().getProject().index(migrationInfo.getCurrentIndexName());
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

        aliasesRequest.execute(listener);
    }

    /**
     * Sets the write block on the index to the given value.
     */
    @FixForMultiProject(description = "Don't use default project id to update settings")
    private void setWriteBlock(Index index, boolean readOnlyValue, ActionListener<AcknowledgedResponse> listener) {
        if (readOnlyValue) {
            // Setting the Block with an AddIndexBlockRequest ensures all shards have accounted for the block and all
            // in-flight writes are completed before returning.
            baseClient.admin()
                .indices()
                .addBlock(
                    new AddIndexBlockRequest(WRITE, index.getName()).masterNodeTimeout(MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT),
                    listener.delegateFailureAndWrap((l, response) -> {
                        if (response.isAcknowledged() == false) {
                            throw new ElasticsearchException("Failed to acknowledge read-only block index request");
                        }
                        l.onResponse(response);
                    })
                );
        } else {
            // The only way to remove a Block is via a settings update.
            final Settings readOnlySettings = Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false).build();
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(readOnlySettings, index.getName()).setPreserveExisting(
                false
            ).masterNodeTimeout(MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT).ackTimeout(TimeValue.ZERO);
            baseClient.execute(TransportUpdateSettingsAction.TYPE, updateSettingsRequest, listener);
        }
    }

    private void reindex(SystemIndexMigrationInfo migrationInfo, ActionListener<BulkByScrollResponse> listener) {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(migrationInfo.getCurrentIndexName());
        reindexRequest.setDestIndex(migrationInfo.getNextIndexName());
        reindexRequest.setRefresh(true);
        String migrationScript = migrationInfo.getMigrationScript();
        if (Strings.isNullOrEmpty(migrationScript) == false) {
            reindexRequest.setScript(Script.parse(migrationScript));
        }
        migrationInfo.createClient(baseClient).execute(ReindexAction.INSTANCE, reindexRequest, listener);
    }

    private void migrateDataStream(
        SystemDataStreamMigrationInfo migrationInfo,
        Consumer<SystemDataStreamMigrationInfo> completionListener
    ) {
        String dataStreamName = migrationInfo.getDataStreamName();
        logger.info("migrating data stream [{}] from feature [{}]", dataStreamName, migrationInfo.getFeatureName());

        ReindexDataStreamAction.ReindexDataStreamRequest reindexRequest = new ReindexDataStreamAction.ReindexDataStreamRequest(
            ReindexDataStreamAction.Mode.UPGRADE,
            dataStreamName
        );

        try {
            migrationInfo.createClient(baseClient)
                .execute(ReindexDataStreamAction.INSTANCE, reindexRequest, ActionListener.wrap(startMigrationResponse -> {
                    if (startMigrationResponse.isAcknowledged() == false) {
                        logger.error("failed to migrate indices from data stream [{}]", dataStreamName);
                        throw new ElasticsearchException(
                            "reindex system data stream ["
                                + dataStreamName
                                + "] from feature ["
                                + migrationInfo.getFeatureName()
                                + "] response is not acknowledge"
                        );
                    }
                    checkDataStreamMigrationStatus(migrationInfo, completionListener, false);
                }, e -> {
                    if (e instanceof ResourceAlreadyExistsException) {
                        // This might happen if the task has been reassigned to another node,
                        // in this case we can just wait for the data stream migration task to finish.
                        // But, there is a possibility that previously started data stream migration task has failed,
                        // in this case we need to cancel it and restart migration of the data stream.
                        logger.debug("data stream [{}] migration is already in progress", dataStreamName);
                        checkDataStreamMigrationStatus(migrationInfo, completionListener, true);
                    } else {
                        markAsFailed(e);
                    }
                }));
        } catch (Exception ex) {
            logger.error(
                () -> format(
                    "error occurred while migrating data stream [%s] from feature [%s]",
                    dataStreamName,
                    migrationInfo.getFeatureName()
                ),
                ex
            );
            markAsFailed(ex);
        }
    }

    private void checkDataStreamMigrationStatus(
        SystemDataStreamMigrationInfo migrationInfo,
        Consumer<SystemDataStreamMigrationInfo> completionListener,
        boolean restartMigrationOnError
    ) {
        String dataStreamName = migrationInfo.getDataStreamName();
        GetMigrationReindexStatusAction.Request getStatusRequest = new GetMigrationReindexStatusAction.Request(dataStreamName);

        migrationInfo.createClient(baseClient)
            .execute(GetMigrationReindexStatusAction.INSTANCE, getStatusRequest, ActionListener.wrap(migrationStatusResponse -> {
                ReindexDataStreamEnrichedStatus status = migrationStatusResponse.getEnrichedStatus();
                logger.debug(
                    "data stream [{}] reindexing status: pending {} out of {} indices",
                    dataStreamName,
                    status.pending(),
                    status.totalIndicesToBeUpgraded()
                );

                if (status.complete() == false) {
                    // data stream migration task is running, schedule another check without need to cancel-restart
                    threadPool.schedule(
                        () -> checkDataStreamMigrationStatus(migrationInfo, completionListener, false),
                        TimeValue.timeValueSeconds(1),
                        threadPool.generic()
                    );
                } else {
                    List<Tuple<String, Exception>> errors = status.errors();
                    if (errors != null && errors.isEmpty() == false || status.exception() != null) {

                        // data stream migration task existed before this task started it and is in failed state - cancel it and restart
                        if (restartMigrationOnError) {
                            cancelExistingDataStreamMigrationAndRetry(migrationInfo, completionListener);
                        } else {
                            List<Exception> exceptions = (status.exception() != null)
                                ? Collections.singletonList(status.exception())
                                : errors.stream().map(Tuple::v2).toList();
                            dataStreamMigrationFailed(migrationInfo, exceptions);
                        }
                    } else {
                        logger.info(
                            "successfully migrated old indices from data stream [{}] from feature [{}] to new indices",
                            dataStreamName,
                            migrationInfo.getFeatureName()
                        );
                        completionListener.accept(migrationInfo);
                    }
                }
            }, ex -> cancelExistingDataStreamMigrationAndMarkAsFailed(migrationInfo, ex)));
    }

    private void dataStreamMigrationFailed(SystemDataStreamMigrationInfo migrationInfo, Collection<Exception> exceptions) {
        logger.error(
            "error occurred while reindexing data stream [{}] from feature [{}], failures [{}]",
            migrationInfo.getDataStreamName(),
            migrationInfo.getFeatureName(),
            exceptions
        );

        ElasticsearchException ex = new ElasticsearchException(
            "error occurred while reindexing data stream [" + migrationInfo.getDataStreamName() + "]"
        );
        for (Exception exception : exceptions) {
            ex.addSuppressed(exception);
        }

        throw ex;
    }

    // Failure handlers
    private void removeReadOnlyBlockOnReindexFailure(Index index, ActionListener<BulkByScrollResponse> listener, Exception ex) {
        logger.info("removing read only block on [{}] because reindex failed [{}]", index, ex);
        setWriteBlock(index, false, ActionListener.wrap(unsetReadOnlyResponse -> listener.onFailure(ex), e1 -> listener.onFailure(ex)));
    }

    private void cancelExistingDataStreamMigrationAndRetry(
        SystemDataStreamMigrationInfo migrationInfo,
        Consumer<SystemDataStreamMigrationInfo> completionListener
    ) {
        logger.debug(
            "cancelling migration of data stream [{}] from feature [{}] for retry",
            migrationInfo.getDataStreamName(),
            migrationInfo.getFeatureName()
        );

        ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                migrateDataStream(migrationInfo, completionListener);
            } else {
                String dataStreamName = migrationInfo.getDataStreamName();
                logger.error(
                    "failed to cancel migration of data stream [{}] from feature [{}] during retry",
                    dataStreamName,
                    migrationInfo.getFeatureName()
                );
                throw new ElasticsearchException(
                    "failed to cancel migration of data stream ["
                        + dataStreamName
                        + "] from feature ["
                        + migrationInfo.getFeatureName()
                        + "] response is not acknowledge"
                );
            }
        }, this::markAsFailed);

        cancelDataStreamMigration(migrationInfo, listener);
    }

    private void cancelExistingDataStreamMigrationAndMarkAsFailed(SystemDataStreamMigrationInfo migrationInfo, Exception exception) {
        logger.info(
            "cancelling migration of data stream [{}] from feature [{}]",
            migrationInfo.getDataStreamName(),
            migrationInfo.getFeatureName()
        );

        // we don't really care here if the request wasn't acknowledged
        ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(response -> markAsFailed(exception), ex -> {
            exception.addSuppressed(ex);
            markAsFailed(exception);
        });

        cancelDataStreamMigration(migrationInfo, listener);
    }

    private void cancelDataStreamMigration(SystemDataStreamMigrationInfo migrationInfo, ActionListener<AcknowledgedResponse> listener) {
        String dataStreamName = migrationInfo.getDataStreamName();

        CancelReindexDataStreamAction.Request cancelRequest = new CancelReindexDataStreamAction.Request(dataStreamName);
        try {
            migrationInfo.createClient(baseClient).execute(CancelReindexDataStreamAction.INSTANCE, cancelRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
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
        SystemResourceMigrationInfo migrationInfo = currentMigrationInfo();
        synchronized (migrationQueue) {
            migrationQueue.clear();
        }
        String featureName = Optional.ofNullable(migrationInfo)
            .map(SystemResourceMigrationInfo::getFeatureName)
            .orElse("<unknown feature>");
        String indexName = Optional.ofNullable(migrationInfo)
            .map(SystemResourceMigrationInfo::getCurrentResourceName)
            .orElse("<unknown resource>");

        MigrationResultsUpdateTask.upsert(
            featureName,
            SingleFeatureMigrationResult.failure(indexName, e),
            ActionListener.wrap(state -> super.markAsFailed(e), exception -> super.markAsFailed(e))
        ).submit(clusterService);
        super.markAsFailed(e);
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
                if (currentState.metadata().getProject().custom(FeatureMigrationResults.TYPE) != null) {
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.metadata()).removeProjectCustom(FeatureMigrationResults.TYPE))
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

    private SystemResourceMigrationInfo currentMigrationInfo() {
        synchronized (migrationQueue) {
            return migrationQueue.peek();
        }
    }
}
