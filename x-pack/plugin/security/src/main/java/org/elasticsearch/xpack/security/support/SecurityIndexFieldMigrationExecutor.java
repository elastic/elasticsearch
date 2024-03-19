/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.core.security.support.MigrateSecurityIndexFieldTaskParams;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;

public class SecurityIndexFieldMigrationExecutor extends PersistentTasksExecutor<MigrateSecurityIndexFieldTaskParams> {
    private static final Logger logger = LogManager.getLogger(SecurityIndexFieldMigrationExecutor.class);
    private final SecuritySystemIndices securitySystemIndices;
    private final Client client;

    private static final String MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_META_KEY = "security-index-field-migration-completed";

    // Queue for posting tasks to update the migration status in cluster state on the master node
    private final MasterServiceTaskQueue<UpdateSecurityIndexFieldMigrationCompleteTask> migrateSecurityIndexFieldCompletedTaskQueue;

    public SecurityIndexFieldMigrationExecutor(
        ClusterService clusterService,
        String taskName,
        Executor executor,
        SecuritySystemIndices securitySystemIndices,
        Client client
    ) {
        super(taskName, executor);
        this.securitySystemIndices = securitySystemIndices;
        this.client = client;
        this.migrateSecurityIndexFieldCompletedTaskQueue = clusterService.createTaskQueue(
            "security-index-field-migration-completed-queue",
            Priority.LOW,
            MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_TASK_EXECUTOR
        );
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, MigrateSecurityIndexFieldTaskParams params, PersistentTaskState state) {
        ActionListener<Void> listener = ActionListener.wrap((res) -> {
            logger.info("Security Index Field Migration complete - written to cluster state");
            task.markAsCompleted();
        }, (exception) -> {
            logger.warn("Security Index Field Migration failed: " + exception);
            task.markAsFailed(exception);
        });

        SecurityIndexManager securityIndex = securitySystemIndices.getMainIndexManager();

        if (params.getMigrationNeeded() == false) {
            this.writeMetadataMigrated(listener);
        } else if (securityIndex.isAvailable(PRIMARY_SHARDS) == false || securityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(new IllegalStateException("Security index not available"));
        } else {
            UpdateByQueryRequestBuilder updateByQueryRequestBuilder = new UpdateByQueryRequestBuilder(client);
            updateByQueryRequestBuilder.filter(
                // Skipping api key since already migrated
                new BoolQueryBuilder().should(QueryBuilders.termQuery("type", "user"))
                    .should(QueryBuilders.termQuery("type", "role"))
                    .should(QueryBuilders.termQuery("type", "role-mapping"))
                    .should(QueryBuilders.termQuery("type", "application-privilege"))
                    .should(QueryBuilders.termQuery("doc_type", "role-mapping"))
            );
            updateByQueryRequestBuilder.source(securitySystemIndices.getMainIndexManager().aliasName());
            updateByQueryRequestBuilder.script(
                new Script(ScriptType.INLINE, "painless", "ctx._source.metadata_flattened = ctx._source.metadata", Collections.emptyMap())
            );

            securityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    UpdateByQueryAction.INSTANCE,
                    updateByQueryRequestBuilder.request(),
                    ActionListener.wrap(bulkByScrollResponse -> {
                        logger.info("Migrated [{}] security index documents", bulkByScrollResponse.getUpdated());
                        this.writeMetadataMigrated(listener);
                    }, listener::onFailure)
                )
            );
        }
    }

    /**
     * Batch executor responsible for updating the cluster state using {@link UpdateSecurityIndexFieldMigrationCompleteTask}
     */
    private static final SimpleBatchedExecutor<
        UpdateSecurityIndexFieldMigrationCompleteTask,
        Void> MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_TASK_EXECUTOR = new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(UpdateSecurityIndexFieldMigrationCompleteTask task, ClusterState clusterState) {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(UpdateSecurityIndexFieldMigrationCompleteTask task, Void unused) {
                task.listener.onResponse(null);
            }
        };

    /**
     * Task to update the {@link IndexMetadata} for the .security index in cluster state with the completion status for an index field
     * migration.
     */
    public static class UpdateSecurityIndexFieldMigrationCompleteTask implements ClusterStateTaskListener {
        private final ActionListener<Void> listener;

        UpdateSecurityIndexFieldMigrationCompleteTask(ActionListener<Void> listener) {
            this.listener = listener;
        }

        ClusterState execute(ClusterState currentState) {
            IndexMetadata indexMetadata = SecurityIndexManager.resolveConcreteIndex(
                SecuritySystemIndices.SECURITY_MAIN_ALIAS,
                currentState.metadata()
            );
            if (indexMetadata != null) {
                IndexMetadata updatededIndexMetadata = new IndexMetadata.Builder(indexMetadata).putCustom(
                    MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_META_KEY,
                    Map.of("completed", "true")
                ).build();
                Metadata metadata = Metadata.builder(currentState.metadata()).put(updatededIndexMetadata, true).build();
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            return currentState;
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    public boolean shouldStartMetadataMigration(ClusterState clusterState) {
        IndexMetadata indexMetadata = SecurityIndexManager.resolveConcreteIndex(
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            clusterState.metadata()
        );
        if (indexMetadata != null) {
            Map<String, String> customMetadata = indexMetadata.getCustomData(MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_META_KEY);
            if (customMetadata != null) {
                String result = customMetadata.get("completed");
                return result == null || Boolean.parseBoolean(result) == false;
            }
        }

        return true;
    }

    public void writeMetadataMigrated(ActionListener<Void> listener) {
        this.migrateSecurityIndexFieldCompletedTaskQueue.submitTask(
            "Updating cluster state to show that the security index field migration has been completed",
            new UpdateSecurityIndexFieldMigrationCompleteTask(listener),
            null
        );
    }
}
