/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.SetIndexMetadataPropertyAction;
import org.elasticsearch.xpack.core.security.action.SetIndexMetadataPropertyRequest;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.QueryableRolesProvider.QueryableRoles;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class QueryableRolesSynchronizationExecutor implements ClusterStateListener {

    private static final String METADATA_INDEXED_BUILT_IN_ROLES = "indexed-built-in-roles";

    private static final Logger logger = LogManager.getLogger(QueryableRolesSynchronizationExecutor.class);

    private final QueryableRolesProvider rolesProvider;
    private final NativeRolesStore nativeRolesStore;
    private final SecurityIndexManager securityIndex;
    private final Client client;
    private final Executor executor;

    private final AtomicBoolean rolesSynchronizationInProgress = new AtomicBoolean(false);

    public QueryableRolesSynchronizationExecutor(
        QueryableRolesProvider rolesProvider,
        NativeRolesStore nativeRolesStore,
        SecurityIndexManager securityIndex,
        Client client,
        ThreadPool threadPool
    ) {
        this.rolesProvider = rolesProvider;
        this.nativeRolesStore = nativeRolesStore;
        this.securityIndex = securityIndex;
        this.client = client;
        this.executor = threadPool.generic();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (nativeRolesStore.isEnabled() == false) {
            // TODO: Should we even attempt to index builtin roles if native role management is not enabled?
            return;
        }
        final ClusterState state = event.state();
        // cluster state has not recovered yet, nothing to do
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        // only the master node should sync the roles
        if (event.localNodeMaster() == false) {
            return;
        }
        // no data nodes, hence no security index to sync to
        if (state.nodes().getDataNodes().isEmpty()) {
            return;
        }
        // TODO: Should we attempt to sync roles if we are in the mixed cluster?
        final QueryableRoles roles = rolesProvider.roles();
        final Map<String, String> indexedRolesVersions = readIndexedRolesVersion(state);
        if (roles.roleVersions().equals(indexedRolesVersions)) {
            logger.info("Security index already contains the latest built-in roles indexed");
            return;
        }
        if (rolesSynchronizationInProgress.compareAndSet(false, true)) {
            executor.execute(() -> syncBuiltinRoles(indexedRolesVersions, roles, ActionListener.wrap(v -> {
                logger.info("Successfully synced built-in roles to security index: {}", roles);
                rolesSynchronizationInProgress.set(false);
            }, e -> {
                logger.warn("Failed to sync built-in roles to security index: {}", roles, e);
                rolesSynchronizationInProgress.set(false);
            })));
        }
    }

    private void syncBuiltinRoles(Map<String, String> indexedRolesVersions, QueryableRoles roles, ActionListener<Void> listener) {
        // This will create .security index if it does not exist and execute all migrations.
        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
            if (frozenSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
                listener.onFailure(frozenSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
            } else {
                // we will first create new or update the existing roles in .security index
                // then potentially delete the roles from .security index that have been removed
                indexRoles(roles.roleDescriptors().values(), frozenSecurityIndex, ActionListener.wrap(onResponse -> {
                    Set<String> rolesToDelete = Sets.difference(indexedRolesVersions.keySet(), ReservedRolesStore.names());
                    if (false == rolesToDelete.isEmpty()) {
                        deleteRoles(rolesToDelete, roles.roleVersions(), frozenSecurityIndex, indexedRolesVersions, listener);
                    } else {
                        markRolesAsIndexed(indexedRolesVersions, roles.roleVersions(), listener);
                    }
                }, listener::onFailure));
            }
        });

    }

    private void deleteRoles(
        Set<String> rolesToDelete,
        Map<String, String> roleVersions,
        SecurityIndexManager frozenSecurityIndex,
        Map<String, String> indexedRolesVersions,
        ActionListener<Void> listener
    ) {
        nativeRolesStore.deleteRoles(
            frozenSecurityIndex,
            rolesToDelete,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            false,
            ActionListener.wrap(deleteResponse -> {
                if (deleteResponse.getItems().stream().anyMatch(BulkRolesResponse.Item::isFailed)) {
                    listener.onFailure(
                        new ElasticsearchStatusException("Automatic deletion of built-in roles failed", RestStatus.INTERNAL_SERVER_ERROR)
                    );
                } else {
                    markRolesAsIndexed(indexedRolesVersions, roleVersions, listener);
                }

            }, listener::onFailure)
        );
    }

    private void indexRoles(
        Collection<RoleDescriptor> roleDescriptors,
        SecurityIndexManager frozenSecurityIndex,
        ActionListener<Void> listener
    ) {
        nativeRolesStore.putRoles(
            frozenSecurityIndex,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            roleDescriptors,
            false,
            ActionListener.wrap(response -> {
                if (response.getItems().stream().anyMatch(BulkRolesResponse.Item::isFailed)) {
                    logger.warn("Automatic indexing of built-in roles failed: {}", response);
                    listener.onFailure(new ElasticsearchException("Automatic indexing of built-in roles failed"));
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure)
        );
    }

    private void markRolesAsIndexed(
        Map<String, String> expectedRolesVersion,
        Map<String, String> newRolesVersion,
        ActionListener<Void> listener
    ) {
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            SetIndexMetadataPropertyAction.INSTANCE,
            new SetIndexMetadataPropertyRequest(
                TimeValue.MINUS_ONE,
                SECURITY_MAIN_ALIAS,
                METADATA_INDEXED_BUILT_IN_ROLES,
                expectedRolesVersion,
                newRolesVersion
            ),
            ActionListener.wrap(response -> {
                if (newRolesVersion.equals(response.value()) == false) {
                    listener.onFailure(new IllegalStateException("Failed to mark reserved roles as indexed"));
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure)
        );
    }

    private Map<String, String> readIndexedRolesVersion(ClusterState state) {
        IndexMetadata indexMetadata = state.metadata().index(SECURITY_MAIN_ALIAS);
        if (indexMetadata == null) {
            return null;
        }
        Map<String, String> currentlyIndexedRoles = indexMetadata.getCustomData(METADATA_INDEXED_BUILT_IN_ROLES);
        return currentlyIndexedRoles == null ? Map.of() : currentlyIndexedRoles;
    }

}
