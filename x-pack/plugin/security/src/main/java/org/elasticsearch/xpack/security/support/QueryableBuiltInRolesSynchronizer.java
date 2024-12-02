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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
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
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class QueryableBuiltInRolesSynchronizer implements ClusterStateListener {

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

    public static final String METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST = "queryable_built_in_roles_digest";

    private static final SimpleBatchedExecutor<MarkBuiltinRolesAsSyncedTask, Map<String, String>> MARK_ROLES_AS_SYNCED_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Map<String, String>> executeTask(MarkBuiltinRolesAsSyncedTask task, ClusterState clusterState) {
                return task.execute(clusterState);
            }

            @Override
            public void taskSucceeded(MarkBuiltinRolesAsSyncedTask task, Map<String, String> value) {
                task.success(value);
            }
        };

    private final MasterServiceTaskQueue<MarkBuiltinRolesAsSyncedTask> markRolesAsSyncedTaskQueue;

    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final QueryableBuiltInRoles.Provider builtinRolesProvider;
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
        this.builtinRolesProvider = rolesProviderFactory.createProvider(reservedRolesStore, fileRolesStore);
        this.queryableBuiltInRolesStore = queryableBuiltInRolesStore;
        this.securityIndex = securityIndex;
        this.executor = threadPool.generic();
        this.markRolesAsSyncedTaskQueue = clusterService.createTaskQueue(
            "mark-built-in-roles-as-synced-task-queue",
            Priority.LOW,
            MARK_ROLES_AS_SYNCED_TASK_EXECUTOR
        );
        this.builtinRolesProvider.addListener(this::builtInRolesChanged);

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
        logger.debug("Built-in roles changed, syncing to security index");
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
            final QueryableBuiltInRoles roles = builtinRolesProvider.getRoles();
            syncBuiltInRoles(roles, state);
        }
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

    private void syncBuiltInRoles(QueryableBuiltInRoles roles, ClusterState state) {
        final Map<String, String> indexedRolesDigests = readIndexedBuiltInRolesDigests(state);
        if (roles.rolesDigest().equals(indexedRolesDigests)) {
            logger.debug("Security index already contains the latest built-in roles indexed, skipping synchronization");
            return;
        }
        if (synchronizationInProgress.compareAndSet(false, true)) {
            executor.execute(() -> doSyncBuiltinRoles(indexedRolesDigests, roles, ActionListener.wrap(v -> {
                logger.info("Successfully synced built-in roles to security index");
                synchronizationInProgress.set(false);
            }, e -> {
                if (false == e instanceof UnavailableShardsException
                    && false == e instanceof IndexNotFoundException
                    && false == e instanceof NotMasterException) {
                    logger.warn("Failed to sync built-in roles to security index", e);
                } else {
                    logger.trace("Failed to sync built-in roles to security index", e);
                }
                synchronizationInProgress.set(false);
            })));
        }
    }

    private boolean shouldSyncBuiltInRoles(ClusterState state) {
        if (securityIndexDeleted) {
            logger.debug("Security index has been deleted, skipping built-in roles synchronization");
            return false;
        }
        if (queryableBuiltInRolesStore.isEnabled() == false) {
            logger.trace("Role store is not enabled, skipping built-in roles synchronization");
            return false;
        }
        if (false == state.clusterRecovered()) {
            logger.debug("Cluster state has not recovered yet, skipping built-in roles synchronization");
            return false;
        }
        if (false == state.nodes().isLocalNodeElectedMaster()) {
            logger.trace("Local node is not the master, skipping built-in roles synchronization");
            return false;
        }
        if (state.nodes().getDataNodes().isEmpty()) {
            logger.debug("No data nodes in the cluster, skipping built-in roles synchronization");
            return false;
        }
        // to keep things simple and avoid potential overwrites with an older version of built-in roles,
        // we only sync built-in roles if all nodes are on the same version
        if (isMixedVersionCluster(state.nodes())) {
            logger.debug("Not all nodes are on the same version, skipping built-in roles synchronization");
            return false;
        }
        if (false == featureService.clusterHasFeature(state, QUERYABLE_BUILT_IN_ROLES_FEATURE)) {
            logger.debug("Not all nodes support queryable built-in roles feature, skipping built-in roles synchronization");
            return false;
        }
        return true;
    }

    private static boolean isMixedVersionCluster(DiscoveryNodes nodes) {
        Version version = null;
        for (DiscoveryNode node : nodes) {
            if (version == null) {
                version = node.getVersion();
            } else if (version.equals(node.getVersion()) == false) {
                return true;
            }
        }
        return false;
    }

    private void doSyncBuiltinRoles(Map<String, String> indexedRolesDigests, QueryableBuiltInRoles roles, ActionListener<Void> listener) {
        // This will create .security index if it does not exist
        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
            if (frozenSecurityIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS) == false) {
                listener.onFailure(frozenSecurityIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS));
            } else if (frozenSecurityIndex.indexIsClosed()) {
                listener.onFailure(new ElasticsearchException(frozenSecurityIndex.getConcreteIndexName() + " is closed"));
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
                rolesToUpsert.add(role);
            } else if (indexedRolesDigests.get(role.getName()).equals(roleDigest) == false) {
                rolesToUpsert.add(role);
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
            if (deleteResponse.getItems().stream().anyMatch(BulkRolesResponse.Item::isFailed)) {
                listener.onFailure(new IllegalStateException("Automatic deletion of built-in roles failed"));
            } else {
                markRolesAsSynced(securityIndex.getConcreteIndexName(), indexedRolesDigests, roles.rolesDigest(), listener);
            }
        }, listener::onFailure));
    }

    private void indexRoles(Collection<RoleDescriptor> rolesToUpsert, SecurityIndexManager securityIndex, ActionListener<Void> listener) {
        queryableBuiltInRolesStore.putRoles(securityIndex, rolesToUpsert, ActionListener.wrap(response -> {
            if (response.getItems().stream().anyMatch(BulkRolesResponse.Item::isFailed)) {
                listener.onFailure(new IllegalStateException("Automatic indexing of built-in roles failed"));
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    private void markRolesAsSynced(
        String concreteSecurityIndexName,
        Map<String, String> expectedRolesDigests,
        Map<String, String> newRolesDigests,
        ActionListener<Void> listener
    ) {
        markRolesAsSyncedTaskQueue.submitTask(
            "mark built-in roles as synced task",
            new MarkBuiltinRolesAsSyncedTask(ActionListener.wrap(response -> {
                if (newRolesDigests.equals(response) == false) {
                    // TODO: This should be expected and can happen if other node have already marked the roles as synced
                    listener.onFailure(new IllegalStateException("Failed to mark built-in roles as synced."));
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
        return indexMetadata.getCustomData(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST);
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
                throw new IllegalStateException("Alias [" + SECURITY_MAIN_ALIAS + "] points to more than one index: " + indices);
            }
            return indices.get(0);
        }
        return null;
    }

    static class MarkBuiltinRolesAsSyncedTask implements ClusterStateTaskListener {

        private final ActionListener<Map<String, String>> listener;
        private final String index;
        private final Map<String, String> expected;
        private final Map<String, String> value;

        MarkBuiltinRolesAsSyncedTask(
            ActionListener<Map<String, String>> listener,
            String index,
            @Nullable Map<String, String> expected,
            @Nullable Map<String, String> value
        ) {
            this.listener = listener;
            this.index = index;
            this.expected = expected;
            this.value = value;
        }

        Tuple<ClusterState, Map<String, String>> execute(ClusterState state) {
            IndexMetadata indexMetadata = state.metadata().index(index);
            if (indexMetadata == null) {
                throw new IndexNotFoundException(index);
            }
            Map<String, String> existingValue = indexMetadata.getCustomData(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST);
            if (Objects.equals(expected, existingValue)) {
                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                if (value != null) {
                    indexMetadataBuilder.putCustom(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST, value);
                } else {
                    indexMetadataBuilder.removeCustom(METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST);
                }
                indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
                ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(state.metadata().indices());
                builder.put(index, indexMetadataBuilder.build());
                return new Tuple<>(
                    ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).indices(builder.build()).build()).build(),
                    value
                );
            } else {
                // returns existing value when expectation is not met
                return new Tuple<>(state, existingValue);
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
