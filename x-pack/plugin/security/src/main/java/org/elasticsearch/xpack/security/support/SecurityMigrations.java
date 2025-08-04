/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.support.SecurityMigrationTaskParams;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.RoleMappingsCleanupMigrationStatus.READY;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.RoleMappingsCleanupMigrationStatus.SKIP;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SecurityMainIndexMappingVersion.ADD_MANAGE_ROLES_PRIVILEGE;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SecurityMainIndexMappingVersion.ADD_REMOTE_CLUSTER_AND_DESCRIPTION_FIELDS;

public class SecurityMigrations {

    /**
     * Interface for creating SecurityMigrations that will be automatically applied once to existing .security indices
     * IMPORTANT: A new index version needs to be added to {@link org.elasticsearch.index.IndexVersions} for the migration to be triggered
     */
    public interface SecurityMigration {
        /**
         * Method that will execute the actual migration - needs to be idempotent and non-blocking
         *
         * @param indexManager for the security index
         * @param client the index client
         * @param listener listener to provide updates back to caller
         */
        void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener);

        /**
         * Any node features that are required for this migration to run. This makes sure that all nodes in the cluster can handle any
         * changes in behaviour introduced by the migration.
         *
         * @return a set of features needed to be supported or an empty set if no change in behaviour is expected
         */
        Set<NodeFeature> nodeFeaturesRequired();

        /**
         * Check that any pre-conditions are met before launching migration
         *
         * @param securityIndexManagerState current state of the security index
         * @return true if pre-conditions met, otherwise false
         */
        default boolean checkPreConditions(SecurityIndexManager.IndexState securityIndexManagerState) {
            return true;
        }

        /**
         * The min mapping version required to support this migration. This makes sure that the index has at least the min mapping that is
         * required to support the migration.
         *
         * @return the minimum mapping version required to apply this migration
         */
        int minMappingVersion();
    }

    public static final Integer ROLE_METADATA_FLATTENED_MIGRATION_VERSION = 1;
    public static final Integer CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION = 2;
    private static final Logger logger = LogManager.getLogger(SecurityMigration.class);

    public static final TreeMap<Integer, SecurityMigration> MIGRATIONS_BY_VERSION = new TreeMap<>(
        Map.of(
            ROLE_METADATA_FLATTENED_MIGRATION_VERSION,
            new RoleMetadataFlattenedMigration(),
            CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION,
            new CleanupRoleMappingDuplicatesMigration()
        )
    );

    public static class RoleMetadataFlattenedMigration implements SecurityMigration {
        @Override
        public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
            BoolQueryBuilder filterQuery = new BoolQueryBuilder().filter(QueryBuilders.termQuery("type", "role"))
                .mustNot(QueryBuilders.existsQuery("metadata_flattened"));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(filterQuery).size(0).trackTotalHits(true);
            SearchRequest countRequest = new SearchRequest(indexManager.forCurrentProject().getConcreteIndexName());
            countRequest.source(searchSourceBuilder);

            client.search(countRequest, ActionListener.wrap(response -> {
                // If there are no roles, skip migration
                if (response.getHits().getTotalHits().value() > 0) {
                    logger.info("Preparing to migrate [" + response.getHits().getTotalHits().value() + "] roles");
                    updateRolesByQuery(indexManager, client, filterQuery, listener);
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure));
        }

        private void updateRolesByQuery(
            SecurityIndexManager indexManager,
            Client client,
            BoolQueryBuilder filterQuery,
            ActionListener<Void> listener
        ) {
            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(indexManager.forCurrentProject().getConcreteIndexName());
            updateByQueryRequest.setQuery(filterQuery);
            updateByQueryRequest.setScript(
                new Script(ScriptType.INLINE, "painless", "ctx._source.metadata_flattened = ctx._source.metadata", Collections.emptyMap())
            );
            client.admin()
                .cluster()
                .execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(bulkByScrollResponse -> {
                    logger.info("Migrated [" + bulkByScrollResponse.getTotal() + "] roles");
                    listener.onResponse(null);
                }, listener::onFailure));
        }

        @Override
        public Set<NodeFeature> nodeFeaturesRequired() {
            return Set.of();
        }

        @Override
        public int minMappingVersion() {
            return ADD_REMOTE_CLUSTER_AND_DESCRIPTION_FIELDS.id();
        }
    }

    public static class CleanupRoleMappingDuplicatesMigration implements SecurityMigration {
        @Override
        public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
            final IndexState projectSecurityIndex = indexManager.forCurrentProject();
            if (projectSecurityIndex.getRoleMappingsCleanupMigrationStatus() == SKIP) {
                listener.onResponse(null);
                return;
            }
            assert projectSecurityIndex.getRoleMappingsCleanupMigrationStatus() == READY;

            getRoleMappings(client, ActionListener.wrap(roleMappings -> {
                List<String> roleMappingsToDelete = getDuplicateRoleMappingNames(roleMappings.mappings());
                if (roleMappingsToDelete.isEmpty() == false) {
                    logger.info("Found [" + roleMappingsToDelete.size() + "] role mapping(s) to cleanup in .security index.");
                    deleteNativeRoleMappings(client, roleMappingsToDelete, listener);
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure));
        }

        private void getRoleMappings(Client client, ActionListener<GetRoleMappingsResponse> listener) {
            executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                GetRoleMappingsAction.INSTANCE,
                new GetRoleMappingsRequestBuilder(client).request(),
                listener
            );
        }

        private void deleteNativeRoleMappings(Client client, List<String> names, ActionListener<Void> listener) {
            assert names.isEmpty() == false;
            ActionListener<DeleteRoleMappingResponse> groupListener = new GroupedActionListener<>(
                names.size(),
                ActionListener.wrap(responses -> {
                    long foundRoleMappings = responses.stream().filter(DeleteRoleMappingResponse::isFound).count();
                    if (responses.size() > foundRoleMappings) {
                        logger.warn(
                            "[" + (responses.size() - foundRoleMappings) + "] Role mapping(s) not found during role mapping clean up."
                        );
                    }
                    if (foundRoleMappings > 0) {
                        logger.info("Deleted [" + foundRoleMappings + "] duplicated role mapping(s) from .security index");
                    }
                    listener.onResponse(null);
                }, listener::onFailure)
            );

            for (String name : names) {
                executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    DeleteRoleMappingAction.INSTANCE,
                    new DeleteRoleMappingRequestBuilder(client).name(name).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).request(),
                    groupListener
                );
            }

        }

        @Override
        public boolean checkPreConditions(SecurityIndexManager.IndexState securityIndexManagerState) {
            // Block migration until expected role mappings are in cluster state and in the correct format or skip if no role mappings
            // are expected
            return securityIndexManagerState.roleMappingsCleanupMigrationStatus == READY
                || securityIndexManagerState.roleMappingsCleanupMigrationStatus == SKIP;
        }

        @Override
        public Set<NodeFeature> nodeFeaturesRequired() {
            return Set.of();
        }

        @Override
        public int minMappingVersion() {
            return ADD_MANAGE_ROLES_PRIVILEGE.id();
        }

        // Visible for testing
        protected static List<String> getDuplicateRoleMappingNames(ExpressionRoleMapping... roleMappings) {
            // Partition role mappings on if they're cluster state role mappings (true) or native role mappings (false)
            Map<Boolean, List<ExpressionRoleMapping>> partitionedRoleMappings = Arrays.stream(roleMappings)
                .collect(Collectors.partitioningBy(ExpressionRoleMapping::isReadOnly));

            Set<String> clusterStateRoleMappings = partitionedRoleMappings.get(true)
                .stream()
                .map(ExpressionRoleMapping::getName)
                .map(ExpressionRoleMapping::removeReadOnlySuffixIfPresent)
                .collect(Collectors.toSet());

            return partitionedRoleMappings.get(false)
                .stream()
                .map(ExpressionRoleMapping::getName)
                .filter(clusterStateRoleMappings::contains)
                .toList();
        }
    }

    public static class Manager {

        private static final int MAX_SECURITY_MIGRATION_RETRY_COUNT = 10;

        private final PersistentTasksService persistentTasksService;
        private final SecuritySystemIndices systemIndices;

        // Node local retry count for migration jobs that's checked only on the master node to make sure
        // submit migration jobs doesn't get out of hand and retries forever if they fail. Reset by a
        // restart or master node change.
        private final AtomicInteger nodeLocalMigrationRetryCount;

        public Manager(ClusterService clusterService, PersistentTasksService persistentTasksService, SecuritySystemIndices systemIndices) {
            this.persistentTasksService = persistentTasksService;
            this.systemIndices = systemIndices;
            this.nodeLocalMigrationRetryCount = new AtomicInteger(0);
            systemIndices.getMainIndexManager().addStateListener((projectId, oldState, newState) -> {
                // Only consider applying migrations if it's the master node and the security index exists
                if (clusterService.state().nodes().isLocalNodeElectedMaster() && newState.indexExists()) {
                    applyPendingSecurityMigrations(projectId, newState);
                }
            });
        }

        @FixForMultiProject
        // TODO : The migration task needs to be project aware
        private void applyPendingSecurityMigrations(ProjectId projectId, SecurityIndexManager.IndexState newState) {
            // If no migrations have been applied and the security index is on the latest version (new index), all migrations can be skipped
            if (newState.migrationsVersion == 0 && newState.createdOnLatestVersion) {
                submitPersistentMigrationTask(SecurityMigrations.MIGRATIONS_BY_VERSION.lastKey(), false);
                return;
            }

            Map.Entry<Integer, SecurityMigrations.SecurityMigration> nextMigration = SecurityMigrations.MIGRATIONS_BY_VERSION.higherEntry(
                newState.migrationsVersion
            );

            // Check if next migration that has not been applied is eligible to run on the current cluster
            if (nextMigration == null
                || systemIndices.getMainIndexManager()
                    .getProject(projectId)
                    .isEligibleSecurityMigration(nextMigration.getValue()) == false) {
                // Reset retry counter if all eligible migrations have been applied successfully
                nodeLocalMigrationRetryCount.set(0);
            } else if (nodeLocalMigrationRetryCount.get() > MAX_SECURITY_MIGRATION_RETRY_COUNT) {
                logger.warn("Security migration failed [" + nodeLocalMigrationRetryCount.get() + "] times, restart node to retry again.");
            } else if (systemIndices.getMainIndexManager().getProject(projectId).isReadyForSecurityMigration(nextMigration.getValue())) {
                submitPersistentMigrationTask(newState.migrationsVersion);
            }
        }

        private void submitPersistentMigrationTask(int migrationsVersion) {
            submitPersistentMigrationTask(migrationsVersion, true);
        }

        private void submitPersistentMigrationTask(int migrationsVersion, boolean securityMigrationNeeded) {
            nodeLocalMigrationRetryCount.incrementAndGet();
            persistentTasksService.sendStartRequest(
                SecurityMigrationTaskParams.TASK_NAME,
                SecurityMigrationTaskParams.TASK_NAME,
                new SecurityMigrationTaskParams(migrationsVersion, securityMigrationNeeded),
                TimeValue.THIRTY_SECONDS /* TODO should this be configurable? longer by default? infinite? */,
                ActionListener.wrap((response) -> {
                    logger.debug("Security migration task submitted");
                }, (exception) -> {
                    // Do nothing if the task is already in progress
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // Do not count ResourceAlreadyExistsException as failure
                        nodeLocalMigrationRetryCount.decrementAndGet();
                    } else {
                        logger.warn("Submit security migration task failed: " + exception.getCause());
                    }
                })
            );
        }

        public PersistentTasksExecutor<?> getPersistentTasksExecutor(Client client, ThreadPool threadPool) {
            return new SecurityMigrationExecutor(
                SecurityMigrationTaskParams.TASK_NAME,
                threadPool.executor(ThreadPool.Names.MANAGEMENT),
                systemIndices.getMainIndexManager(),
                client,
                SecurityMigrations.MIGRATIONS_BY_VERSION
            );
        }
    }
}
