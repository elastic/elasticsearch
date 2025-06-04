/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
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
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.security.support.QueryableBuiltInRolesUtils.determineRolesToDelete;
import static org.elasticsearch.xpack.security.support.QueryableBuiltInRolesUtils.determineRolesToUpsert;
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
        if ("false".equals(propertyValue)) {
            QUERYABLE_BUILT_IN_ROLES_ENABLED = false;
        } else if (propertyValue == null || propertyValue.isEmpty() || "true".equals(propertyValue)) {
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
    private final NativeRolesStore nativeRolesStore;
    private final Executor executor;
    private final AtomicBoolean synchronizationInProgress = new AtomicBoolean(false);

    private volatile boolean securityIndexDeleted = false;

    /**
     * The max consecutive failed sync attempts before skipping further sync attempts.
     */
    static final int MAX_FAILED_SYNC_ATTEMPTS = 10;

    /**
     * The counter of unexpected sync failures. Reset to 0 when a successful sync occurs or when a master node is restarted.
     */
    private final AtomicInteger failedSyncAttempts = new AtomicInteger(0);

    /**
     * Constructs a new built-in roles synchronizer.
     *
     * @param clusterService the cluster service to register as a listener
     * @param featureService the feature service to check if the cluster has the queryable built-in roles feature
     * @param rolesProviderFactory the factory to create the built-in roles provider
     * @param nativeRolesStore the native roles store to sync the built-in roles to
     * @param reservedRolesStore the reserved roles store to fetch the built-in roles from
     * @param fileRolesStore the file roles store to fetch the built-in roles from
     * @param threadPool the thread pool
     */
    public QueryableBuiltInRolesSynchronizer(
        ClusterService clusterService,
        FeatureService featureService,
        QueryableBuiltInRolesProviderFactory rolesProviderFactory,
        NativeRolesStore nativeRolesStore,
        ReservedRolesStore reservedRolesStore,
        FileRolesStore fileRolesStore,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.rolesProvider = rolesProviderFactory.createProvider(reservedRolesStore, fileRolesStore);
        this.nativeRolesStore = nativeRolesStore;
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
            syncBuiltInRoles(roles);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (isSecurityIndexDeleted(event)) {
            this.securityIndexDeleted = true;
            logger.trace("Received security index deletion event, skipping built-in roles synchronization");
            return;
        } else if (isSecurityIndexCreatedOrRecovered(event)) {
            this.securityIndexDeleted = false;
            logger.trace("Security index has been created/recovered, attempting to sync built-in roles");
        }
        if (shouldSyncBuiltInRoles(state)) {
            final QueryableBuiltInRoles roles = rolesProvider.getRoles();
            syncBuiltInRoles(roles);
        }
    }

    /**
     * @return {@code true} if the synchronization of built-in roles is in progress, {@code false} otherwise
     */
    public boolean isSynchronizationInProgress() {
        return synchronizationInProgress.get();
    }

    private void syncBuiltInRoles(final QueryableBuiltInRoles roles) {
        if (synchronizationInProgress.compareAndSet(false, true)) {
            try {
                final Map<String, String> indexedRolesDigests = readIndexedBuiltInRolesDigests(clusterService.state());
                if (roles.rolesDigest().equals(indexedRolesDigests)) {
                    logger.debug("Security index already contains the latest built-in roles indexed, skipping roles synchronization");
                    resetFailedSyncAttempts();
                    synchronizationInProgress.set(false);
                } else {
                    executor.execute(() -> doSyncBuiltinRoles(indexedRolesDigests, roles, ActionListener.wrap(v -> {
                        logger.info("Successfully synced [{}] built-in roles to .security index", roles.roleDescriptors().size());
                        resetFailedSyncAttempts();
                        synchronizationInProgress.set(false);
                    }, e -> {
                        handleException(e);
                        synchronizationInProgress.set(false);
                    })));
                }
            } catch (Exception e) {
                logger.error("Failed to sync built-in roles", e);
                failedSyncAttempts.incrementAndGet();
                synchronizationInProgress.set(false);
            }
        }
    }

    private void handleException(Exception e) {
        boolean isUnexpectedFailure = false;
        if (e instanceof BulkRolesResponseException bulkException) {
            final boolean isBulkDeleteFailure = bulkException instanceof BulkDeleteRolesResponseException;
            for (final Map.Entry<String, Exception> bulkFailure : bulkException.getFailures().entrySet()) {
                final String logMessage = Strings.format(
                    "Failed to [%s] built-in role [%s]",
                    isBulkDeleteFailure ? "delete" : "create/update",
                    bulkFailure.getKey()
                );
                if (isExpectedFailure(bulkFailure.getValue())) {
                    logger.info(logMessage, bulkFailure.getValue());
                } else {
                    isUnexpectedFailure = true;
                    logger.warn(logMessage, bulkFailure.getValue());
                }
            }
        } else if (isExpectedFailure(e)) {
            logger.info("Failed to sync built-in roles to .security index", e);
        } else {
            isUnexpectedFailure = true;
            logger.warn("Failed to sync built-in roles to .security index due to unexpected exception", e);
        }
        if (isUnexpectedFailure) {
            failedSyncAttempts.incrementAndGet();
        }
    }

    private void resetFailedSyncAttempts() {
        if (failedSyncAttempts.get() > 0) {
            logger.trace("resetting failed sync attempts to 0");
            failedSyncAttempts.set(0);
        }
    }

    /**
     * Package protected for testing purposes.
     *
     * @return the number of failed sync attempts
     */
    int getFailedSyncAttempts() {
        return failedSyncAttempts.get();
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
    private static boolean isExpectedFailure(final Exception e) {
        final Throwable cause = ExceptionsHelper.unwrapCause(e);
        return ExceptionsHelper.isNodeOrShardUnavailableTypeException(cause)
            || TransportActions.isShardNotAvailableException(cause)
            || cause instanceof IndexClosedException
            || cause instanceof IndexPrimaryShardNotAllocatedException
            || cause instanceof NotMasterException
            || cause instanceof ResourceAlreadyExistsException
            || cause instanceof VersionConflictEngineException
            || cause instanceof DocumentMissingException
            || cause instanceof FailedToMarkBuiltInRolesAsSyncedException
            || (e instanceof FailedToCommitClusterStateException && "node closed".equals(cause.getMessage()));
    }

    private boolean shouldSyncBuiltInRoles(final ClusterState state) {
        if (false == state.nodes().isLocalNodeElectedMaster()) {
            logger.trace("Local node is not the master, skipping built-in roles synchronization");
            return false;
        }
        if (failedSyncAttempts.get() >= MAX_FAILED_SYNC_ATTEMPTS) {
            logger.debug(
                "Failed to sync built-in roles to .security index [{}] times. Skipping built-in roles synchronization.",
                failedSyncAttempts.get()
            );
            return false;
        }
        if (false == state.clusterRecovered()) {
            logger.trace("Cluster state has not recovered yet, skipping built-in roles synchronization");
            return false;
        }
        if (nativeRolesStore.isEnabled() == false) {
            logger.trace("Native roles store is not enabled, skipping built-in roles synchronization");
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
            logger.trace("Security index is deleted, skipping built-in roles synchronization");
            return false;
        }
        if (isSecurityIndexClosed(state)) {
            logger.trace("Security index is closed, skipping built-in roles synchronization");
            return false;
        }
        return true;
    }

    private void doSyncBuiltinRoles(
        final Map<String, String> indexedRolesDigests,
        final QueryableBuiltInRoles roles,
        final ActionListener<Void> listener
    ) {
        final Set<RoleDescriptor> rolesToUpsert = determineRolesToUpsert(roles, indexedRolesDigests);
        final Set<String> rolesToDelete = determineRolesToDelete(roles, indexedRolesDigests);

        assert Sets.intersection(rolesToUpsert.stream().map(RoleDescriptor::getName).collect(toSet()), rolesToDelete).isEmpty()
            : "The roles to upsert and delete should not have any common roles";

        if (rolesToUpsert.isEmpty() && rolesToDelete.isEmpty()) {
            logger.debug("No changes to built-in roles to sync to .security index");
            listener.onResponse(null);
            return;
        }

        indexRoles(rolesToUpsert, listener.delegateFailureAndWrap((l1, indexResponse) -> {
            deleteRoles(rolesToDelete, l1.delegateFailureAndWrap((l2, deleteResponse) -> {
                markRolesAsSynced(indexedRolesDigests, roles.rolesDigest(), l2);
            }));
        }));
    }

    private void deleteRoles(final Set<String> rolesToDelete, final ActionListener<Void> listener) {
        if (rolesToDelete.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        nativeRolesStore.deleteRoles(rolesToDelete, WriteRequest.RefreshPolicy.IMMEDIATE, false, ActionListener.wrap(deleteResponse -> {
            final Map<String, Exception> deleteFailure = deleteResponse.getItems()
                .stream()
                .filter(BulkRolesResponse.Item::isFailed)
                .collect(toMap(BulkRolesResponse.Item::getRoleName, BulkRolesResponse.Item::getCause));
            if (deleteFailure.isEmpty()) {
                listener.onResponse(null);
            } else {
                listener.onFailure(new BulkDeleteRolesResponseException(deleteFailure));
            }
        }, listener::onFailure));
    }

    private void indexRoles(final Collection<RoleDescriptor> rolesToUpsert, final ActionListener<Void> listener) {
        if (rolesToUpsert.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        nativeRolesStore.putRoles(WriteRequest.RefreshPolicy.IMMEDIATE, rolesToUpsert, false, ActionListener.wrap(response -> {
            final Map<String, Exception> indexFailures = response.getItems()
                .stream()
                .filter(BulkRolesResponse.Item::isFailed)
                .collect(toMap(BulkRolesResponse.Item::getRoleName, BulkRolesResponse.Item::getCause));
            if (indexFailures.isEmpty()) {
                listener.onResponse(null);
            } else {
                listener.onFailure(new BulkIndexRolesResponseException(indexFailures));
            }
        }, listener::onFailure));
    }

    private boolean isSecurityIndexDeleted(final ClusterChangedEvent event) {
        final IndexMetadata previousSecurityIndexMetadata = resolveSecurityIndexMetadata(event.previousState().metadata());
        final IndexMetadata currentSecurityIndexMetadata = resolveSecurityIndexMetadata(event.state().metadata());
        return previousSecurityIndexMetadata != null && currentSecurityIndexMetadata == null;
    }

    private boolean isSecurityIndexCreatedOrRecovered(final ClusterChangedEvent event) {
        final IndexMetadata previousSecurityIndexMetadata = resolveSecurityIndexMetadata(event.previousState().metadata());
        final IndexMetadata currentSecurityIndexMetadata = resolveSecurityIndexMetadata(event.state().metadata());
        return previousSecurityIndexMetadata == null && currentSecurityIndexMetadata != null;
    }

    private boolean isSecurityIndexClosed(final ClusterState state) {
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
        final Map<String, String> expectedRolesDigests,
        final Map<String, String> newRolesDigests,
        final ActionListener<Void> listener
    ) {
        final IndexMetadata securityIndexMetadata = resolveSecurityIndexMetadata(clusterService.state().metadata());
        if (securityIndexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(SECURITY_MAIN_ALIAS));
            return;
        }
        final Index concreteSecurityIndex = securityIndexMetadata.getIndex();
        markRolesAsSyncedTaskQueue.submitTask(
            "mark built-in roles as synced task",
            new MarkRolesAsSyncedTask(listener.delegateFailureAndWrap((l, response) -> {
                if (newRolesDigests.equals(response) == false) {
                    logger.debug(
                        () -> Strings.format(
                            "Another master node most probably indexed a newer versions of built-in roles in the meantime. "
                                + "Expected: [%s], Actual: [%s]",
                            newRolesDigests,
                            response
                        )
                    );
                    l.onFailure(
                        new FailedToMarkBuiltInRolesAsSyncedException(
                            "Failed to mark built-in roles as synced. The expected role digests have changed."
                        )
                    );
                } else {
                    l.onResponse(null);
                }
            }), concreteSecurityIndex.getName(), expectedRolesDigests, newRolesDigests),
            null
        );
    }

    private Map<String, String> readIndexedBuiltInRolesDigests(final ClusterState state) {
        final IndexMetadata indexMetadata = resolveSecurityIndexMetadata(state.metadata());
        if (indexMetadata == null) {
            return null;
        }
        return indexMetadata.getCustomData(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST_KEY);
    }

    private static IndexMetadata resolveSecurityIndexMetadata(final Metadata metadata) {
        return SecurityIndexManager.resolveConcreteIndex(SECURITY_MAIN_ALIAS, metadata.getProject());
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

        public Map<String, String> getNewRoleDigests() {
            return newRoleDigests;
        }

        Tuple<ClusterState, Map<String, String>> execute(ClusterState state) {
            IndexMetadata indexMetadata = state.metadata().getProject().index(concreteSecurityIndexName);
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
                ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(state.metadata().getProject().indices());
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

    private static class BulkDeleteRolesResponseException extends BulkRolesResponseException {

        BulkDeleteRolesResponseException(Map<String, Exception> failures) {
            super("Failed to bulk delete built-in roles", failures);
        }

    }

    private static class BulkIndexRolesResponseException extends BulkRolesResponseException {

        BulkIndexRolesResponseException(Map<String, Exception> failures) {
            super("Failed to bulk create/update built-in roles", failures);
        }

    }

    private abstract static class BulkRolesResponseException extends RuntimeException {

        private final Map<String, Exception> failures;

        BulkRolesResponseException(String message, Map<String, Exception> failures) {
            super(message);
            assert failures != null && failures.isEmpty() == false;
            this.failures = failures;
            failures.values().forEach(this::addSuppressed);
        }

        Map<String, Exception> getFailures() {
            return failures;
        }

    }

    private static class FailedToMarkBuiltInRolesAsSyncedException extends RuntimeException {

        FailedToMarkBuiltInRolesAsSyncedException(String message) {
            super(message);
        }

    }

}
