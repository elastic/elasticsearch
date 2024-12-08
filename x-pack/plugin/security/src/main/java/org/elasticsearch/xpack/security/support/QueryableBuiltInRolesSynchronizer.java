/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.authz.store.QueryableBuiltInRolesStore;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * Synchronizes built-in roles to the .security index.
 * The .security index is created if it does not exist.
 * <p>
 * The synchronization is executed only on the elected master node
 * after the cluster has recovered and roles need to be synced.
 * The goal is to reduce the potential for conflicting operations.
 * While in most cases, there should be only a single node that’s
 * attempting to create/update/delete roles, it’s still possible
 * that the master node changes in the middle of the syncing process.
 */
public final class QueryableBuiltInRolesSynchronizer implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(QueryableBuiltInRolesSynchronizer.class);

    /**
     * This is a temporary feature flag to allow enabling the synchronization of built-in roles to the .security index.
     * Initially, it is disabled by default due to the number of tests that need to be adjusted now that .security index
     * is created earlier in the cluster lifecycle.
     * <p>
     * Once all tests are adjusted, this flag will be set to enabled by default and later removed altogether.
     */
    public static final boolean QUERYABLE_BUILT_IN_ROLES_ENABLED;
    static {
        final var propertyValue = System.getProperty("es.queryable_built_in_roles_enabled");
        if (propertyValue == null || propertyValue.isEmpty() || "false".equals(propertyValue)) {
            QUERYABLE_BUILT_IN_ROLES_ENABLED = false;
        } else if ("true".equals(propertyValue)) {
            QUERYABLE_BUILT_IN_ROLES_ENABLED = true;
        } else {
            throw new IllegalStateException(
                "system property [es.queryable_built_in_roles_enabled] may only be set to [true] or [false], but was ["
                    + propertyValue
                    + "]"
            );
        }
    }

    public static final NodeFeature QUERYABLE_BUILT_IN_ROLES_FEATURE = new NodeFeature("security.queryable_built_in_roles");

    /**
     * Index metadata key of the digest of built-in roles indexed in the .security index.
     * <p>
     * The value is a map of built-in role names to their digests (calculated by sha256 of the role definition).
     */
    public static final String METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST_KEY = "queryable_built_in_roles_digest";

    private static final SimpleBatchedExecutor<MarkRolesAsSyncedTask, Map<String, String>> MARK_ROLES_AS_SYNCED_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Map<String, String>> executeTask(MarkRolesAsSyncedTask task, ClusterState clusterState) {
                return task.execute(clusterState);
            }

            @Override
            public void taskSucceeded(MarkRolesAsSyncedTask task, Map<String, String> value) {
                task.success(value);
            }
        };

    private final MasterServiceTaskQueue<MarkRolesAsSyncedTask> markRolesAsSyncedTaskQueue;

    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final QueryableBuiltInRoles.Provider rolesProvider;
    private final SecurityIndexManager securityIndex;
    private final QueryableBuiltInRolesStore queryableBuiltInRolesStore;
    private final Executor executor;
    private final AtomicBoolean synchronizationInProgress = new AtomicBoolean(false);

    private volatile boolean securityIndexDeleted = false;

    public QueryableBuiltInRolesSynchronizer(
        ClusterService clusterService,
        FeatureService featureService,
        QueryableBuiltInRolesProviderFactory rolesProviderFactory,
        QueryableBuiltInRolesStore queryableBuiltInRolesStore,
        ReservedRolesStore reservedRolesStore,
        FileRolesStore fileRolesStore,
        SecurityIndexManager securityIndex,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.rolesProvider = rolesProviderFactory.createProvider(reservedRolesStore, fileRolesStore);
        this.queryableBuiltInRolesStore = queryableBuiltInRolesStore;
        this.securityIndex = securityIndex;
        this.executor = threadPool.generic();
        this.markRolesAsSyncedTaskQueue = clusterService.createTaskQueue(
            "mark-built-in-roles-as-synced-task-queue",
            Priority.LOW,
            MARK_ROLES_AS_SYNCED_TASK_EXECUTOR
        );
        this.rolesProvider.addListener(this::builtInRolesChanged);
        this.clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                clusterService.removeListener(QueryableBuiltInRolesSynchronizer.this);
            }

            @Override
            public void beforeStart() {
                clusterService.addListener(QueryableBuiltInRolesSynchronizer.this);
            }
        });
    }

    private void builtInRolesChanged(QueryableBuiltInRoles roles) {
        logger.debug("Built-in roles changed, attempting to sync to .security index");
        final ClusterState state = clusterService.state();
        if (shouldSyncBuiltInRoles(state)) {
            syncBuiltInRoles(roles, state);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (isSecurityIndexDeleted(event)) {
            this.securityIndexDeleted = true;
            logger.trace("Security index has been deleted, skipping built-in roles synchronization");
            return;
        } else if (isSecurityIndexCreated(event)) {
            this.securityIndexDeleted = false;
            logger.trace("Security index has been created, attempting to sync built-in roles");
        }
        if (shouldSyncBuiltInRoles(state)) {
            final QueryableBuiltInRoles roles = rolesProvider.getRoles();
            syncBuiltInRoles(roles, state);
        }
    }

    private void syncBuiltInRoles(QueryableBuiltInRoles roles, ClusterState state) {
        final Map<String, String> indexedRolesDigests = readIndexedBuiltInRolesDigests(state);
        if (roles.rolesDigest().equals(indexedRolesDigests)) {
            logger.debug("Security index already contains the latest built-in roles indexed, skipping synchronization");
            return;
        }
        if (synchronizationInProgress.compareAndSet(false, true)) {
            executor.execute(() -> doSyncBuiltinRoles(indexedRolesDigests, roles, ActionListener.wrap(v -> {
                logger.info("Successfully synced [" + roles.roleDescriptors().size() + "] built-in roles to .security index");
                synchronizationInProgress.set(false);
            }, e -> {
                if (isExpectedFailure(e)) {
                    logger.trace("Failed to sync built-in roles to .security index", e);
                } else {
                    logger.warn("Failed to sync built-in roles to .security index due to unexpected exception", e);
                }
                synchronizationInProgress.set(false);
            })));
        }
    }

    /**
     * Some failures are expected and should not be logged as errors.
     * These exceptions are either:
     * - transient (e.g. connection errors),
     * - recoverable (e.g. no longer master, index reallocating or caused by concurrent operations)
     * - not recoverable but expected (e.g. index closed).
     *
     * @param e to check
     * @return {@code true} if the exception is expected and should not be logged as an error
     */
    private boolean isExpectedFailure(final Exception e) {
        final Throwable cause = ExceptionsHelper.unwrapCause(e);
        return ExceptionsHelper.isNodeOrShardUnavailableTypeException(e)
            || TransportActions.isShardNotAvailableException(e)
            || e instanceof IndexClosedException
            || e instanceof IndexPrimaryShardNotAllocatedException
            || e instanceof NotMasterException
            || cause instanceof ResourceAlreadyExistsException
            || cause instanceof VersionConflictEngineException
            || cause instanceof DocumentMissingException
            || (cause instanceof ElasticsearchException
                && "Failed to mark built-in roles as synced. The expected roles digests have changed.".equals(cause.getMessage()));
    }

    private boolean shouldSyncBuiltInRoles(ClusterState state) {
        if (false == state.nodes().isLocalNodeElectedMaster()) {
            logger.trace("Local node is not the master, skipping built-in roles synchronization");
            return false;
        }
        if (false == state.clusterRecovered()) {
            logger.trace("Cluster state has not recovered yet, skipping built-in roles synchronization");
            return false;
        }
        if (queryableBuiltInRolesStore.isEnabled() == false) {
            logger.trace("Role store is not enabled, skipping built-in roles synchronization");
            return false;
        }
        if (state.nodes().getDataNodes().isEmpty()) {
            logger.trace("No data nodes in the cluster, skipping built-in roles synchronization");
            return false;
        }
        if (state.nodes().isMixedVersionCluster()) {
            // To keep things simple and avoid potential overwrites with an older version of built-in roles,
            // we only sync built-in roles if all nodes are on the same version.
            logger.trace("Not all nodes are on the same version, skipping built-in roles synchronization");
            return false;
        }
        if (false == featureService.clusterHasFeature(state, QUERYABLE_BUILT_IN_ROLES_FEATURE)) {
            logger.trace("Not all nodes support queryable built-in roles feature, skipping built-in roles synchronization");
            return false;
        }
        if (securityIndexDeleted) {
            logger.trace("Security index has been deleted, skipping built-in roles synchronization");
            return false;
        }
        if (isSecurityIndexClosed(state)) {
            logger.trace("Security index is closed, skipping built-in roles synchronization");
            return false;
        }
        return true;
    }

    private void doSyncBuiltinRoles(Map<String, String> indexedRolesDigests, QueryableBuiltInRoles roles, ActionListener<Void> listener) {
        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
            if (frozenSecurityIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS) == false) {
                listener.onFailure(frozenSecurityIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS));

            } else {
                final Collection<RoleDescriptor> rolesToUpsert = rolesToUpsert(roles, indexedRolesDigests);
                indexRoles(rolesToUpsert, frozenSecurityIndex, ActionListener.wrap(onResponse -> {
                    final Set<String> rolesToDelete = rolesToDelete(roles, indexedRolesDigests);
                    if (rolesToDelete.isEmpty()) {
                        markRolesAsSynced(frozenSecurityIndex.getConcreteIndexName(), indexedRolesDigests, roles.rolesDigest(), listener);
                    } else {
                        deleteRoles(frozenSecurityIndex, roles, rolesToDelete, indexedRolesDigests, listener);
                    }
                }, listener::onFailure));
            }
        });
    }

    private static Set<String> rolesToDelete(QueryableBuiltInRoles roles, Map<String, String> indexedRolesDigests) {
        return indexedRolesDigests == null ? Set.of() : Sets.difference(indexedRolesDigests.keySet(), roles.rolesDigest().keySet());
    }

    private static Collection<RoleDescriptor> rolesToUpsert(QueryableBuiltInRoles roles, Map<String, String> indexedRolesDigests) {
        final Set<RoleDescriptor> rolesToUpsert = new HashSet<>();
        for (var role : roles.roleDescriptors()) {
            final String roleDigest = roles.rolesDigest().get(role.getName());
            if (indexedRolesDigests == null || indexedRolesDigests.containsKey(role.getName()) == false) {
                rolesToUpsert.add(role);  // a new role to create
            } else if (indexedRolesDigests.get(role.getName()).equals(roleDigest) == false) {
                rolesToUpsert.add(role);  // an existing role that needs to be updated
            }
        }
        return rolesToUpsert;
    }

    private void deleteRoles(
        SecurityIndexManager securityIndex,
        QueryableBuiltInRoles roles,
        Set<String> rolesToDelete,
        Map<String, String> indexedRolesDigests,
        ActionListener<Void> listener
    ) {
        queryableBuiltInRolesStore.deleteRoles(securityIndex, rolesToDelete, ActionListener.wrap(deleteResponse -> {
            final Optional<Exception> deleteFailure = deleteResponse.getItems()
                .stream()
                .filter(BulkRolesResponse.Item::isFailed)
                .map(BulkRolesResponse.Item::getCause)
                .findAny();
            if (deleteFailure.isPresent()) {
                listener.onFailure(deleteFailure.get());
            } else {
                markRolesAsSynced(securityIndex.getConcreteIndexName(), indexedRolesDigests, roles.rolesDigest(), listener);
            }
        }, listener::onFailure));
    }

    private void indexRoles(Collection<RoleDescriptor> rolesToUpsert, SecurityIndexManager securityIndex, ActionListener<Void> listener) {
        queryableBuiltInRolesStore.putRoles(securityIndex, rolesToUpsert, ActionListener.wrap(response -> {
            final Optional<Exception> indexFailure = response.getItems()
                .stream()
                .filter(BulkRolesResponse.Item::isFailed)
                .map(BulkRolesResponse.Item::getCause)
                .findAny();
            if (indexFailure.isPresent()) {
                listener.onFailure(indexFailure.get());
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    private boolean isSecurityIndexDeleted(ClusterChangedEvent event) {
        final IndexMetadata previousSecurityIndexMetadata = resolveSecurityIndexMetadata(event.previousState().metadata());
        final IndexMetadata currentSecurityIndexMetadata = resolveSecurityIndexMetadata(event.state().metadata());
        return previousSecurityIndexMetadata != null && currentSecurityIndexMetadata == null;
    }

    private boolean isSecurityIndexCreated(ClusterChangedEvent event) {
        final IndexMetadata previousSecurityIndexMetadata = resolveSecurityIndexMetadata(event.previousState().metadata());
        final IndexMetadata currentSecurityIndexMetadata = resolveSecurityIndexMetadata(event.state().metadata());
        return previousSecurityIndexMetadata == null && currentSecurityIndexMetadata != null;
    }

    private boolean isSecurityIndexClosed(ClusterState state) {
        final IndexMetadata indexMetadata = resolveSecurityIndexMetadata(state.metadata());
        return indexMetadata != null && indexMetadata.getState() == IndexMetadata.State.CLOSE;
    }

    /**
     * This method marks the built-in roles as synced in the .security index
     * by setting the new roles digests in the metadata of the .security index.
     * <p>
     * The marking is done as a compare and swap operation to ensure that the roles
     * are marked as synced only when new roles are indexed. The operation is idempotent
     * and will succeed if the expected roles digests are equal to the digests in the
     * .security index or if they are equal to the new roles digests.
     */
    private void markRolesAsSynced(
        String concreteSecurityIndexName,
        Map<String, String> expectedRolesDigests,
        Map<String, String> newRolesDigests,
        ActionListener<Void> listener
    ) {
        markRolesAsSyncedTaskQueue.submitTask(
            "mark built-in roles as synced task",
            new MarkRolesAsSyncedTask(ActionListener.wrap(response -> {
                if (newRolesDigests.equals(response) == false) {
                    logger.trace(
                        () -> Strings.format(
                            "Another master node most probably indexed a newer versions of built-in roles in the meantime. "
                                + "Expected: [%s], Actual: [%s]",
                            newRolesDigests,
                            response
                        )
                    );
                    listener.onFailure(
                        new ElasticsearchException("Failed to mark built-in roles as synced. The expected roles digests have changed.")
                    );
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure), concreteSecurityIndexName, expectedRolesDigests, newRolesDigests),
            null
        );
    }

    private Map<String, String> readIndexedBuiltInRolesDigests(ClusterState state) {
        final IndexMetadata indexMetadata = resolveSecurityIndexMetadata(state.metadata());
        if (indexMetadata == null) {
            return null;
        }
        return indexMetadata.getCustomData(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST_KEY);
    }

    private static IndexMetadata resolveSecurityIndexMetadata(final Metadata metadata) {
        final Index index = resolveConcreteSecurityIndex(metadata);
        if (index != null) {
            return metadata.getIndexSafe(index);
        }
        return null;
    }

    private static Index resolveConcreteSecurityIndex(final Metadata metadata) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(SECURITY_MAIN_ALIAS);
        if (indexAbstraction != null) {
            final List<Index> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException("alias [" + SECURITY_MAIN_ALIAS + "] points to more than one index [" + indices + "]");
            }
            return indices.get(0);
        }
        return null;
    }

    static class MarkRolesAsSyncedTask implements ClusterStateTaskListener {

        private final ActionListener<Map<String, String>> listener;
        private final String concreteSecurityIndexName;
        private final Map<String, String> expectedRoleDigests;
        private final Map<String, String> newRoleDigests;

        MarkRolesAsSyncedTask(
            ActionListener<Map<String, String>> listener,
            String concreteSecurityIndexName,
            @Nullable Map<String, String> expectedRoleDigests,
            @Nullable Map<String, String> newRoleDigests
        ) {
            this.listener = listener;
            this.concreteSecurityIndexName = concreteSecurityIndexName;
            this.expectedRoleDigests = expectedRoleDigests;
            this.newRoleDigests = newRoleDigests;
        }

        Tuple<ClusterState, Map<String, String>> execute(ClusterState state) {
            IndexMetadata indexMetadata = state.metadata().index(concreteSecurityIndexName);
            if (indexMetadata == null) {
                throw new IndexNotFoundException(concreteSecurityIndexName);
            }
            Map<String, String> existingRoleDigests = indexMetadata.getCustomData(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST_KEY);
            if (Objects.equals(expectedRoleDigests, existingRoleDigests)) {
                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                if (newRoleDigests != null) {
                    indexMetadataBuilder.putCustom(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST_KEY, newRoleDigests);
                } else {
                    indexMetadataBuilder.removeCustom(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST_KEY);
                }
                indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
                ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(state.metadata().indices());
                builder.put(concreteSecurityIndexName, indexMetadataBuilder.build());
                return new Tuple<>(
                    ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).indices(builder.build()).build()).build(),
                    newRoleDigests
                );
            } else {
                // returns existing value when expectation is not met
                return new Tuple<>(state, existingRoleDigests);
            }
        }

        void success(Map<String, String> value) {
            listener.onResponse(value);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

}
