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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
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
    private final RolesSync sync = new RolesSync();

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
        return sync.inProgress();
    }

    private void syncBuiltInRoles(final QueryableBuiltInRoles roles) {
        if (sync.startSync()) {
            doSyncBuiltInRoles(roles);
        }
    }

    private void doSyncBuiltInRoles(final QueryableBuiltInRoles roles) {
        // The sync slot held by this invocation must be ended exactly once. The notifyOnce wrapper drops any
        // second completion, e.g. a cluster state update task listener erroneously notified again after a
        // master failover, which would otherwise release a slot it no longer owns and corrupt the RolesSync
        // state machine. The success log also lives inside the guarded listener, so a dropped stale completion
        // logs nothing. A plain ActionListener (not ActionListener#wrap) is used deliberately: wrap reroutes
        // exceptions thrown by the onResponse consumer into the onFailure consumer, which would run the
        // sync-ending logic twice for a single completion.
        final ActionListener<Void> syncEndListener = ActionListener.notifyOnce(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("Successfully synced [{}] built-in roles to .security index", roles.roleDescriptors().size());
                resetFailedSyncAttempts();
                endSyncAndRunPendingSync();
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    handleException(e);
                } finally {
                    // the slot must be ended even if the exception handling itself fails, or it would leak
                    endSyncAndRunPendingSync();
                }
            }
        });
        final Map<String, String> indexedRolesDigests;
        try {
            indexedRolesDigests = readIndexedBuiltInRolesDigests(clusterService.state());
        } catch (Exception e) {
            syncEndListener.onFailure(e);
            return;
        }
        if (roles.rolesDigest().equals(indexedRolesDigests)) {
            logger.debug("Security index already contains the latest built-in roles indexed, skipping roles synchronization");
            // completed inline rather than through syncEndListener so that the "Successfully synced" log only
            // fires when changes were actually applied, and so that an exception from ending the slot (the
            // endSync corruption tripwire) propagates to the caller instead of being fed back into the already
            // consumed notifyOnce listener, which would silently swallow it
            resetFailedSyncAttempts();
            endSyncAndRunPendingSync();
        } else {
            // ActionRunnable (rather than a bare lambda) guarantees the listener is completed even if the
            // executor rejects the task or applyRoleChanges throws before using the listener; otherwise the
            // sync slot would leak and built-in roles would never be synced again on this node.
            try {
                executor.execute(ActionRunnable.wrap(syncEndListener, listener -> applyRoleChanges(indexedRolesDigests, roles, listener)));
            } catch (Exception e) {
                // the executor threw instead of delivering the rejection to the runnable, so the listener has
                // not been notified yet
                syncEndListener.onFailure(e);
            }
        }
    }

    /**
     * Ends the sync slot held by the completed sync and, if a sync request arrived while that sync was running
     * (coalesced into the {@code RUNNING_WITH_PENDING_REQUEST} state), runs the pending sync against fresh cluster state.
     * This is not failure-retry logic: a failed sync is not re-attempted here, it only gets another chance on a
     * future trigger (and only while under {@link #MAX_FAILED_SYNC_ATTEMPTS}).
     */
    private void endSyncAndRunPendingSync() {
        // Each loop iteration holds the sync slot (state RUNNING) after endSync() returned true. Nothing in the
        // loop body may throw, as an exception escaping while the slot is held would either leak the slot or
        // lead an enclosing listener to end it a second time.
        while (sync.endSync()) {
            try {
                if (shouldSyncBuiltInRoles(clusterService.state())) {
                    logger.debug("Running pending built-in roles synchronization");
                    doSyncBuiltInRoles(rolesProvider.getRoles());
                    return; // doSyncBuiltInRoles took over the sync slot and ends it via its own listener
                }
            } catch (Exception e) {
                // same expected/unexpected classification as handleException applies on the main failure path:
                // transient conditions must not count toward MAX_FAILED_SYNC_ATTEMPTS
                if (isExpectedFailure(e)) {
                    logger.info("Failed to run pending built-in roles synchronization", e);
                } else {
                    logger.warn("Failed to run pending built-in roles synchronization", e);
                    failedSyncAttempts.incrementAndGet();
                }
            }
            // pending sync not needed anymore (or it failed to start): loop to either serve another pending
            // request or release the slot
        }
    }

    static class RolesSync {
        private static final int IDLE = 0;
        private static final int RUNNING = 1;
        private static final int RUNNING_WITH_PENDING_REQUEST = 2;

        private final AtomicInteger state = new AtomicInteger(IDLE);

        boolean startSync() {
            while (true) {
                int s = state.get();
                if (s == IDLE) {
                    if (state.compareAndSet(IDLE, RUNNING)) return true;
                } else if (s == RUNNING) {
                    if (state.compareAndSet(RUNNING, RUNNING_WITH_PENDING_REQUEST)) return false;
                } else {
                    return false; // already RUNNING_WITH_PENDING_REQUEST
                }
            }
        }

        /**
         * Ends the sync slot acquired by a previous successful {@link #startSync()}. Must be called exactly once
         * per acquired slot. Returns {@code true} if a sync request arrived while the slot was held (the slot
         * stays held and the caller must run another sync), or {@code false} if the slot was released.
         *
         * @throws IllegalStateException if no sync is in progress. This indicates a double completion for a
         *         single slot. Failing fast here is essential: the CAS loop below can never make progress from
         *         IDLE and would otherwise spin forever on the calling thread, typically the master service
         *         thread, hanging the entire cluster. Note the contrast with the notifyOnce listener in
         *         doSyncBuiltInRoles, which silently drops duplicate completions: that wrapper can prove a
         *         second notification is a stale duplicate of its own slot, whereas at this level a stale call
         *         is indistinguishable from state corruption (if another sync is RUNNING it would silently
         *         steal that sync's slot), so any call that reaches this state must be surfaced, not ignored.
         */
        boolean endSync() {
            while (true) {
                int s = state.get();
                if (s == IDLE) {
                    // deliberately an exception rather than an assert so tests exercise the exact production
                    // behavior; callers up the completion chain surface it loudly (see MasterService)
                    throw new IllegalStateException("endSync called while no sync is in progress");
                } else if (s == RUNNING_WITH_PENDING_REQUEST) {
                    if (state.compareAndSet(RUNNING_WITH_PENDING_REQUEST, RUNNING)) return true;
                } else {
                    if (state.compareAndSet(RUNNING, IDLE)) return false;
                }
            }
        }

        boolean inProgress() {
            return state.get() != IDLE;
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
            || cause instanceof FailedToMarkBuiltInRolesAsSyncedException;
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

    private void applyRoleChanges(
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
        return indexMetadata != null && isClosedOrClosing(state, indexMetadata);
    }

    /**
     * Returns true if the given index is closed or is in the process of being closed. The close API installs
     * {@link MetadataIndexStateService#INDEX_CLOSED_BLOCK} before flipping {@link IndexMetadata#getState()} to
     * {@link IndexMetadata.State#CLOSE}, so checking the state field alone misses the in-progress window.
     */
    private static boolean isClosedOrClosing(final ClusterState state, final IndexMetadata indexMetadata) {
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            return true;
        }
        return state.blocks()
            .hasIndexBlockWithId(
                state.metadata().projectFor(indexMetadata.getIndex()).id(),
                indexMetadata.getIndex().getName(),
                MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID
            );
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
        final ClusterState currentState = clusterService.state();
        final IndexMetadata securityIndexMetadata = resolveSecurityIndexMetadata(currentState.metadata());
        if (securityIndexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(SECURITY_MAIN_ALIAS));
            return;
        }
        if (isClosedOrClosing(currentState, securityIndexMetadata)) {
            listener.onFailure(new IndexClosedException(securityIndexMetadata.getIndex()));
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
            final var project = state.metadata().getProject();
            IndexMetadata indexMetadata = project.index(concreteSecurityIndexName);
            if (indexMetadata == null) {
                throw new IndexNotFoundException(concreteSecurityIndexName);
            }
            if (isClosedOrClosing(state, indexMetadata)) {
                // Bail if the index closed (or began closing) since the sync started. IndexClosedException is
                // in isExpectedFailure, so this does not count against failedSyncAttempts.
                throw new IndexClosedException(indexMetadata.getIndex());
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
                ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(project.indices());
                builder.put(concreteSecurityIndexName, indexMetadataBuilder.build());
                return new Tuple<>(
                    ClusterState.builder(state)
                        .putProjectMetadata(ProjectMetadata.builder(project).indices(builder.build()).build())
                        .build(),
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
